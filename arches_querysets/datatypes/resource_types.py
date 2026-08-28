import logging
import uuid
from itertools import chain

from arches import __version__ as _arches_version_str
from packaging.version import Version

arches_version = Version(_arches_version_str)
from arches.app.datatypes import datatypes
from arches.app.models import models
from arches.app.utils import permission_backend

from django.core.cache import caches

from arches_querysets.conf import settings as qs_settings
from django.utils.translation import get_language
from django.utils.translation import gettext as _

logger = logging.getLogger(__name__)

# These two datatypes name a different resource than the one being displayed
# which is why they need permission filtering applied to the resources they point at.
RESOURCE_INSTANCE_DATATYPES = ("resource-instance", "resource-instance-list")


class ResourceInstanceDataType(datatypes.ResourceInstanceDataType):
    def transform_value_for_tile(self, value, **kwargs):
        parsed = super().transform_value_for_tile(value, **kwargs)
        if parsed is None:
            parsed = value

        graph_configs_by_graph_id = {
            graph_config["graphid"]: graph_config
            for graph_config in kwargs.get("graphs", [])
        }

        if not isinstance(parsed, list):
            parsed = [parsed]
        transformed = []
        for inner_val in parsed:
            match inner_val:
                case models.ResourceInstance():
                    transformed.append(
                        self.from_id_string(
                            str(inner_val.pk),
                            graph_configs_by_graph_id.get(inner_val.graph_id),
                        )
                    )
                case uuid.UUID():
                    # TODO: handle multiple graph configs, requires db?
                    transformed.append(self.from_id_string(str(inner_val)))
                case str():
                    # TODO: handle multiple graph configs, requires db?
                    transformed.append(self.from_id_string(inner_val))
                case _:
                    transformed.append(inner_val)
        return transformed

    def get_resource(self, tile):
        try:
            return tile.resourceinstance
        except models.ResourceInstance.DoesNotExist:
            return None
        except AttributeError:
            resource_id = tile["resourceinstance_id"]
            return models.ResourceInstance.objects.filter(pk=resource_id).first()

    def get_display_value(self, tile, node, *, details=None, **kwargs):
        if details is None:
            data = self.get_tile_data(tile)
            value = data.get(str(node.nodeid))
            resource = self.get_resource(tile)
            user = getattr(tile, "_user", None)
            details = self.get_details(value, resource=resource, user=user)
        return ", ".join(
            [detail["display_value"] or "" for detail in details if detail]
        )

    def to_python(self, value, *, resource=None):
        if not (related_resources := self.get_related_resources(value, resource)):
            return None
        return related_resources[0]

    def get_display_value_context_in_bulk(self, values):
        """Pre-fetch all target ResourceInstance objects for a batch of tile values."""
        all_resource_ids = []
        for value in values:
            for inner_val in value or []:
                if inner_val and (rid := inner_val.get("resourceId")):
                    all_resource_ids.append(rid)
        return models.ResourceInstance.objects.filter(pk__in=all_resource_ids)

    def set_display_value_context_in_bulk(self, datatype_context):
        cache = caches[qs_settings.RESOURCE_INSTANCE_CACHE]
        for resource_instance in datatype_context:
            cache.set(str(resource_instance.pk), resource_instance)

    def get_related_resources(self, value, resource):
        if not value:
            return []
        related_resources = []

        cache = caches[qs_settings.RESOURCE_INSTANCE_CACHE]
        uncached_values = []
        for inner_val in value:
            if not inner_val:
                continue
            target_resource_id = uuid.UUID(inner_val["resourceId"])
            cached_resource = cache.get(str(target_resource_id))
            if cached_resource is not None:
                related_resources.append(cached_resource)
            else:
                uncached_values.append(inner_val)

        if not uncached_values:
            return related_resources

        logger.debug(
            "Falling back to per-resource queries for %s uncached related resources",
            len(uncached_values),
        )
        # No bulk cache — fall back to per-resource relation queries.
        # arches_version==9.0.0
        if arches_version >= Version("8.0"):
            relations = resource.from_resxres.all()
        else:
            relations = resource.resxres_resource_instance_ids_from.all()

        def handle_missing_data(to_resource_id):
            msg = f"Missing ResourceXResource target: {to_resource_id}"
            logger.warning(msg)

        for inner_val in uncached_values:
            if not inner_val:
                continue
            target_resource_id = uuid.UUID(inner_val["resourceId"])
            if not relations:
                # arches_version==9.0.0
                if arches_version >= Version("8.0"):
                    relations = models.ResourceXResource.objects.filter(
                        to_resource_id=target_resource_id
                    ).select_related("to_resource")
                else:
                    relations = models.ResourceXResource.objects.filter(
                        resourceinstanceidto_id=target_resource_id
                    ).select_related("resourceinstanceidto")

            for relation in relations:
                to_resource_id = (
                    relation.resourceinstanceidto_id
                    # arches_version==9.0.0
                    if arches_version < Version("8.0")
                    else relation.to_resource_id
                )
                if to_resource_id == target_resource_id:
                    try:
                        to_resource = (
                            relation.resourceinstanceidto
                            # arches_version==9.0.0
                            if arches_version < Version("8.0")
                            else relation.to_resource
                        )
                        if to_resource is None:
                            raise models.ResourceInstance.DoesNotExist
                    except models.ResourceInstance.DoesNotExist:
                        handle_missing_data(to_resource_id)
                        break
                    related_resources.append(to_resource)
                    break

        return related_resources

    def get_details(self, value, *, resource=None, user=None, **kwargs):
        lang = get_language()
        related_resources = self.get_related_resources(value, resource)
        related_resources_by_id = {
            related_resource.pk: related_resource
            for related_resource in related_resources
        }
        permitted_resource_ids = self._get_permitted_related_resource_ids(
            related_resources, user
        )
        ret = []
        for inner_val in value or []:
            if not inner_val:
                continue
            target_resource_id = uuid.UUID(inner_val["resourceId"])
            related = related_resources_by_id.get(target_resource_id)
            if related is None:
                ret.append(
                    {
                        "resource_id": None,
                        "display_value": _("Missing"),
                    }
                )
                continue
            if (
                permitted_resource_ids is not None
                and target_resource_id not in permitted_resource_ids
            ):
                # The user can't view this related resource: omit it entirely
                # rather than revealing its existence or name.
                continue
            descriptor = related.descriptors.get(lang) or next(
                iter(related.descriptors.values()), None
            )
            ret.append(
                {
                    "resource_id": str(related.pk),
                    "display_value": descriptor["name"] if descriptor else "",
                }
            )
        return ret

    @staticmethod
    def _get_permitted_related_resource_ids(related_resources, user):
        """Returns None (no filtering) when user is not provided, otherwise the
        subset of related_resources' primary keys the user is permitted to view."""
        if user is None or not related_resources:
            return None
        try:
            permitted_queryset = permission_backend.filter_resource_queryset(
                user,
                models.ResourceInstance.objects.filter(
                    pk__in=[
                        related_resource.pk for related_resource in related_resources
                    ]
                ),
            )
        except Exception:
            # Some permission frameworks (e.g. the deprecated default-allow
            # framework) don't support filtering an arbitrary queryset by
            # per-object view permission. Treat that as "no filtering" rather
            # than raising, since default-allow already grants broad read
            # access by design.
            logger.warning(
                "Active permission framework doesn't support filtering "
                "related resources by view permission; skipping that check.",
                exc_info=True,
            )
            return None
        return set(permitted_queryset.values_list("resourceinstanceid", flat=True))

    @staticmethod
    def from_id_string(uuid_string, graph_config=None):
        if graph_config is None:
            graph_config = {}
        return {
            "resourceId": uuid_string,
            "ontologyProperty": graph_config.get("ontologyProperty", ""),
            "inverseOntologyProperty": graph_config.get("inverseOntologyProperty", ""),
        }

    @staticmethod
    def values_match(value1, value2):
        if not isinstance(value1, list) or not isinstance(value2, list):
            return value1 == value2
        copy1 = [{**inner_val} for inner_val in value1]
        copy2 = [{**inner_val} for inner_val in value2]
        for inner_val in chain(copy1, copy2):
            inner_val.pop("resourceXresourceId", None)
        return copy1 == copy2


class ResourceInstanceListDataType(ResourceInstanceDataType):
    def collects_multiple_values(self):
        return True

    def to_python(self, value, *, resource=None):
        if not (related_resources := self.get_related_resources(value, resource)):
            return None
        return related_resources

    def get_details(self, value, *, resource=None, user=None, **kwargs):
        if not value:
            return []
        details = super().get_details(
            value=value, resource=resource, user=user, **kwargs
        )
        resource_display_value_map = {
            str(detail["resource_id"]): detail["display_value"] for detail in details
        }
        related_resources = self.get_related_resources(value, resource)
        permitted_resource_ids = self._get_permitted_related_resource_ids(
            related_resources, user
        )
        resolved_resource_ids = {
            related_resource.pk for related_resource in related_resources
        }
        ret = []
        for resource_dict in value:
            target_resource_id = uuid.UUID(resource_dict["resourceId"])
            if (
                permitted_resource_ids is not None
                and target_resource_id in resolved_resource_ids
                and target_resource_id not in permitted_resource_ids
            ):
                # The user can't view this related resource: omit it entirely
                # rather than revealing its existence or name.
                continue
            ret.append(
                {
                    "resource_id": resource_dict["resourceId"],
                    "display_value": resource_display_value_map.get(
                        resource_dict["resourceId"], _("Missing")
                    ),
                }
            )
        return ret
