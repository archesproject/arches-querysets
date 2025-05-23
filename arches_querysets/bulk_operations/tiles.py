import uuid
from collections import defaultdict
from functools import partial
from operator import attrgetter

from django.core.exceptions import ValidationError
from django.db import ProgrammingError, transaction
from django.http import HttpRequest
from django.utils.translation import gettext as _

from arches import VERSION as arches_version
from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.models.models import Language, Node, TileModel, ResourceInstance
from arches.app.models.tile import Tile, TileValidationError

from arches_querysets.utils import datatype_transforms
from arches_querysets.utils.models import (
    field_attnames,
    get_nodegroups_here_and_below,
    pop_arches_model_kwargs,
)


NOT_PROVIDED = object()


class BulkTileOperation:
    def __init__(self, *, entry, request=None, save_kwargs=None):
        self.to_insert = set()
        self.to_update = set()
        self.to_delete = set()
        self.errors_by_node_alias = defaultdict(list)
        self.entry = entry  # resource or tile
        self.datatype_factory = DataTypeFactory()
        self.request = request
        if not self.request:
            self.request = HttpRequest()
        self.save_kwargs = save_kwargs or {}
        self.transaction_id = uuid.uuid4()

        if isinstance(entry, TileModel):
            self.resourceid = self.entry.resourceinstance_id
            # TODO: write perms, but don't falsify self.new_resource_created
            self.nodegroups = get_nodegroups_here_and_below(self.entry.nodegroup)
            existing_tiles = entry.__class__.objects.filter(
                resourceinstance_id=self.resourceid,
                nodegroup_id__in=[ng.pk for ng in self.nodegroups],
            ).order_by("sortorder")
        else:
            self.resourceid = self.entry.pk
            self.nodegroups = []  # not necessary to populate.
            existing_tiles = self.entry._annotated_tiles

        self.grouping_nodes_by_nodegroup_id = self._get_grouping_node_lookup()
        self.existing_tiles_by_nodegroup_alias = defaultdict(list)
        for tile in existing_tiles:
            self.existing_tiles_by_nodegroup_alias[tile.find_nodegroup_alias()].append(
                tile
            )
        self.new_resource_created = bool(existing_tiles)

    def _get_grouping_node_lookup(self):
        from arches_querysets.models import SemanticResource, SemanticTile

        lookup = {}
        if isinstance(self.entry, SemanticResource):
            for node in self.entry._permitted_nodes:
                if node.pk == node.nodegroup_id:
                    lookup[node.pk] = node
        elif isinstance(self.entry, SemanticTile):
            # TODO: look into whether this is repetitive.
            for nodegroup in self.nodegroups:
                if arches_version >= (8, 0):
                    lookup[nodegroup.pk] = nodegroup.grouping_node
                else:
                    # TODO: evaluate permissions when doing write
                    for node in nodegroup.node_set.all():
                        if node.pk == node.nodegroup_id:
                            lookup[node.pk] = node
                            break
        else:
            raise TypeError

        return lookup

    def run(self):
        self.validate()
        try:
            self._persist()
        except ProgrammingError as e:
            if isinstance(self.entry, TileModel):
                nodegroup_alias = self.entry.find_nodegroup_alias()
            if e.args and "excess_tiles" in e.args[0]:
                msg = _("Tile Cardinality Error")
                raise ValidationError({nodegroup_alias: msg}) from e
            raise

    def validate(self):
        self._update_tiles()

    def _update_tiles(self):
        """Move values from resource or tile to prefetched tiles, and validate.
        Raises ValidationError if new data fails datatype validation.
        """
        original_tile_data_by_tile_id = {}
        if isinstance(self.entry, TileModel):
            self._update_tile_for_grouping_node(
                self.entry.nodegroup.grouping_node,
                self.entry,
                original_tile_data_by_tile_id,
            )
        else:
            for grouping_node in self.grouping_nodes_by_nodegroup_id.values():
                if grouping_node.nodegroup.parentnodegroup_id:
                    continue
                self._update_tile_for_grouping_node(
                    grouping_node,
                    self.entry,
                    original_tile_data_by_tile_id,
                    delete_siblings=True,
                )

        if self.errors_by_node_alias:
            raise ValidationError(
                {
                    alias: ValidationError([e["message"] for e in errors])
                    for alias, errors in self.errors_by_node_alias.items()
                }
            )

    def _update_tile_for_grouping_node(
        self,
        grouping_node,
        container,
        original_tile_data_by_tile_id,
        delete_siblings=False,
    ):
        from arches_querysets.models import SemanticTile

        # TODO: Find something more clean than this double if/else.
        if isinstance(container, dict):
            aliased_data = container.get("aliased_data")
        else:
            aliased_data = container.aliased_data
        if isinstance(aliased_data, dict):
            new_tiles = aliased_data.get(grouping_node.alias, NOT_PROVIDED)
        else:
            new_tiles = getattr(aliased_data, grouping_node.alias, NOT_PROVIDED)
        # Is this grouping node the entry point?
        if (
            isinstance(self.entry, SemanticTile)
            and self.entry.nodegroup_id == grouping_node.pk
        ):
            new_tiles = [container]
        if grouping_node.nodegroup.cardinality == "1":
            if new_tiles is None:
                new_tiles = []
            elif not isinstance(new_tiles, list):
                # TODO: ensure this line is even reachable now.
                new_tiles = [new_tiles]
        if all(isinstance(tile, TileModel) for tile in new_tiles):
            new_tiles.sort(key=attrgetter("sortorder"))
        else:
            parent_tile = container if isinstance(container, TileModel) else None
            new_tiles = [
                SemanticTile.deserialize(tile_dict, parent_tile=parent_tile)
                for tile_dict in new_tiles
            ]
        existing_tiles = self.existing_tiles_by_nodegroup_alias[grouping_node.alias]
        if not existing_tiles:
            next_sort_order = 0
        else:
            next_sort_order = max(t.sortorder or 0 for t in existing_tiles) + 1
        for existing_tile, new_tile in self._pair_tiles(existing_tiles, new_tiles):
            if new_tile is NOT_PROVIDED:
                if delete_siblings:
                    self.to_delete.add(existing_tile)
                continue
            if existing_tile is NOT_PROVIDED:
                new_tile.nodegroup_id = grouping_node.nodegroup_id
                new_tile.resourceinstance_id = self.resourceid
                new_tile.sortorder = next_sort_order
                next_sort_order += 1
                for node in grouping_node.nodegroup.node_set.all():
                    new_tile.data[str(node.pk)] = None
                new_tile._incoming_tile = new_tile
                # TODO: reimplement correct nodegroup for parenttile check somewhere.
                new_tile.full_clean(exclude={"parenttile"})
                self.to_insert.add(new_tile)
            else:
                original_tile_data_by_tile_id[existing_tile.pk] = {**existing_tile.data}
                existing_tile._incoming_tile = new_tile
                self.to_update.add(existing_tile)

        nodes = grouping_node.nodegroup.node_set.all()
        languages = Language.objects.all()
        for tile in self.to_insert | self.to_update:
            if tile.nodegroup_id != grouping_node.pk:
                # TODO: this is a symptom this should be refactored.
                continue
            if arches_version >= (8, 0):
                children = tile.nodegroup.children.all()
            else:
                children = tile.nodegroup.nodegroup_set.all()
                for child_nodegroup in children:
                    # TODO: Obvious N+1 problem here.
                    grouping_node = (
                        Node.objects.filter(pk=child_nodegroup.pk)
                        .prefetch_related("nodegroup__node_set")
                        .get()
                    )
                    child_nodegroup.grouping_node = grouping_node
            for child_nodegroup in children:
                self._update_tile_for_grouping_node(
                    grouping_node=child_nodegroup.grouping_node,
                    container=tile._incoming_tile,
                    original_tile_data_by_tile_id=original_tile_data_by_tile_id,
                    delete_siblings=True,
                )
            self._validate_and_patch_from_tile_values(
                tile,
                nodes=nodes,
                languages=languages,
            )

        # https://github.com/archesproject/arches-querysets/issues/11
        # for tile in self.to_insert | self.to_update:
        #     if tile.nodegroup.pk != grouping_node.pk:
        #         # TODO: this is a symptom this should be refactored.
        #         continue
        #     # Remove blank tiles if they have no children.
        #     if (
        #         not any(tile.data.values())
        #         and not tile.children.exists()
        #         # Check unsaved children.
        #         and not any(
        #             getattr(tile._incoming_tile, child_tile_alias, None)
        #             for child_tile_alias in grouping_node.nodegroup.children.values_list(
        #                 # TODO: 7.6 compat
        #                 "grouping_node__alias",
        #                 flat=True,
        #             )
        #         )
        #     ):
        #         if tile._state.adding:
        #             self.to_insert.remove(tile)
        #         else:
        #             self.to_update.remove(tile)
        #             self.to_delete.add(tile)

        for tile in self.to_insert | self.to_update:
            if tile.nodegroup.pk != grouping_node.pk:
                # TODO: this is a symptom this should be refactored.
                continue
            # Remove no-op upserts.
            if (
                original_data := original_tile_data_by_tile_id.pop(tile.pk, None)
            ) and tile._tile_update_is_noop(original_data):
                self.to_update.remove(tile)

    def _pair_tiles(self, existing_tiles, new_tiles):
        pairs = []
        matched_new_tiles = []
        for existing_tile in existing_tiles:
            for tile in new_tiles:
                if existing_tile.pk == tile.pk:
                    pairs.append((existing_tile, tile))
                    matched_new_tiles.append(tile)
                    break
            else:
                pairs.append((existing_tile, NOT_PROVIDED))
        for new_tile in new_tiles:
            if new_tile not in matched_new_tiles:
                pairs.append((NOT_PROVIDED, new_tile))
        return pairs

    def _validate_and_patch_from_tile_values(self, tile, *, nodes, languages):
        """Validate data found on ._incoming_tile and move it to .data.
        Update errors_by_node_alias in place."""
        from arches_querysets.models import AliasedData, SemanticTile

        for node in nodes:
            if isinstance(tile._incoming_tile, SemanticTile) and isinstance(
                tile._incoming_tile.aliased_data, AliasedData
            ):
                value_to_validate = getattr(
                    tile._incoming_tile.aliased_data, node.alias, NOT_PROVIDED
                )
            else:
                value_to_validate = tile._incoming_tile.aliased_data.get(
                    node.alias, NOT_PROVIDED
                )
            if value_to_validate is NOT_PROVIDED:
                continue
            self._run_datatype_methods(tile, value_to_validate, node, languages)

    def _run_datatype_methods(self, tile, value_to_validate, node, languages):
        """
        Call datatype methods when merging value_to_validate into the tile.

        1. transform_value_for_tile()
        2. merge_tile_value() -- only exists in arches-querysets for now
        3. clean()
        4. validate()
        5. pre_tile_save()

        TODO: pre_structure_tile_data()?
        TODO: move this to Tile.full_clean()?
        https://github.com/archesproject/arches/issues/10851#issuecomment-2427305853
        """
        node_id_str = str(node.pk)
        datatype_instance = self.datatype_factory.get_instance(node.datatype)

        transform_fn, merge_fn, clean_fn, validate_fn, pre_tile_save_fn = (
            self._get_overridden_datatype_methods(datatype_instance)
        )

        if value_to_validate is None:
            tile.data[node_id_str] = None
            return
        try:
            transformed = transform_fn(
                value_to_validate, languages=languages, **node.config
            )
        except ValueError:  # BooleanDataType raises.
            # validate() will handle.
            transformed = value_to_validate

        # Merge the transformed data into the tile.data.
        # We just overwrite the old value unless a datatype has another idea.
        if merge_fn:
            merge_fn(tile, node_id_str, transformed)
        else:
            tile.data[node_id_str] = transformed

        clean_fn(tile, node_id_str)

        if errors := validate_fn(transformed, node=node):
            self.errors_by_node_alias[node.alias].extend(errors)

        try:
            pre_tile_save_fn(tile, node_id_str)
        except TypeError:  # GeoJSONDataType raises.
            self.errors_by_node_alias[node.alias].append(
                datatype_instance.create_error_message(
                    tile.data[node_id_str], None, None, None
                )
            )

    def _get_overridden_datatype_methods(self, datatype_instance):
        """
        Get datatype methods, possibly overridden by more robust ones
        from arches_querysets.datatype_transforms.
        """
        assert datatype_instance.datatype_name
        snake_dt = datatype_instance.datatype_name.replace("-", "_")

        if transform_fn := getattr(
            datatype_transforms,
            f"{snake_dt}_transform_value_for_tile",
            None,
        ):
            transform_fn = partial(transform_fn, datatype_instance)
        else:
            transform_fn = datatype_instance.transform_value_for_tile
        if merge_fn := getattr(
            datatype_transforms,
            f"{snake_dt}_merge_tile_value",
            None,
        ):
            merge_fn = partial(merge_fn, datatype_instance)
        if clean_fn := getattr(datatype_transforms, f"{snake_dt}_clean", None):
            clean_fn = partial(clean_fn, datatype_instance)
        else:
            clean_fn = datatype_instance.clean
        if validate_fn := getattr(datatype_transforms, f"{snake_dt}_validate", None):
            validate_fn = partial(validate_fn, datatype_instance)
        else:
            validate_fn = datatype_instance.validate
        if pre_tile_save_fn := getattr(
            datatype_transforms, f"{snake_dt}_pre_tile_save", None
        ):
            pre_tile_save_fn = partial(pre_tile_save_fn, datatype_instance)
        else:
            pre_tile_save_fn = datatype_instance.pre_tile_save

        return transform_fn, merge_fn, clean_fn, validate_fn, pre_tile_save_fn

    def _persist(self):
        # Instantiate proxy models for now, but TODO: expose this
        # functionality on vanilla models, and in bulk.
        upserts = self.to_insert | self.to_update
        insert_proxies = [
            # TODO: make readable.
            Tile(
                **(pop_arches_model_kwargs(vars(insert), Tile._meta.get_fields())[1]),
            )
            for insert in self.to_insert
        ]
        update_proxies = list(
            Tile.objects.filter(pk__in=[tile.pk for tile in self.to_update])
        )
        upsert_proxies = insert_proxies + update_proxies
        delete_proxies = Tile.objects.filter(
            pk__in=[tile.pk for tile in self.to_delete]
        )

        with transaction.atomic():
            # Interact with the database in bulk as much as possible, but
            # run certain side effects from Tile.save() one-at-a-time until
            # proxy model methods can be refactored. Then run in bulk.
            for upsert_proxy, vanilla_instance in zip(
                sorted(upsert_proxies, key=attrgetter("pk")),
                sorted(upserts, key=attrgetter("pk")),
                strict=True,
            ):
                upsert_proxy._existing_data = upsert_proxy.data
                upsert_proxy._existing_provisionaledits = upsert_proxy.provisionaledits

                # Sync proxy instance fields.
                for field in field_attnames(vanilla_instance):
                    setattr(upsert_proxy, field, getattr(vanilla_instance, field))

                # Some functions expect to always drill into request.user
                # https://github.com/archesproject/arches/issues/8471
                try:
                    upsert_proxy._Tile__preSave(request=self.request)
                    upsert_proxy.check_for_missing_nodes()  # also runs clean()
                    upsert_proxy.check_for_constraint_violation()
                except TileValidationError as tve:
                    raise ValidationError(tve.message) from tve
                (
                    oldprovisionalvalue,
                    newprovisionalvalue,
                    provisional_edit_log_details,
                ) = vanilla_instance._apply_provisional_edit(
                    upsert_proxy,
                    upsert_proxy._existing_data,
                    upsert_proxy._existing_provisionaledits,
                    user=getattr(self.request, "user", None),
                )
                # Remember the values needed for the edit log updates later.
                upsert_proxy._oldprovisionalvalue = oldprovisionalvalue
                upsert_proxy._newprovisionalvalue = newprovisionalvalue
                upsert_proxy._provisional_edit_log_details = (
                    provisional_edit_log_details
                )
                upsert_proxy._existing_data = vanilla_instance.data

            for delete_proxy in delete_proxies:
                delete_proxy._Tile__preDelete(request=self.request)

            if self.to_insert:
                inserted = sorted(
                    TileModel.objects.bulk_create(self.to_insert), key=attrgetter("pk")
                )
                # Pay the cost of a second TileModel -> Tile transform until refactored.
                refreshed_insert_proxies = list(
                    Tile.objects.filter(pk__in=[t.pk for t in inserted]).order_by("pk")
                )
                for before, after in zip(
                    insert_proxies, refreshed_insert_proxies, strict=True
                ):
                    after._newprovisionalvalue = before._newprovisionalvalue
                    after._provisional_edit_log_details = (
                        before._provisional_edit_log_details
                    )
                upsert_proxies = refreshed_insert_proxies + update_proxies
            else:
                insert_proxies = TileModel.objects.none()
            if self.to_update:
                TileModel.objects.bulk_update(
                    self.to_update,
                    {"data", "parenttile", "provisionaledits", "sortorder"},
                )
            if self.to_delete:
                TileModel.objects.filter(pk__in=[t.pk for t in self.to_delete]).delete()

            if isinstance(self.entry, ResourceInstance):
                self.entry.save_without_related_objects(**self.save_kwargs)
            else:
                self.entry.dummy_save(**self.save_kwargs)

            for upsert_tile in upserts:
                if arches_version < (8, 0):
                    grouping_node = self.grouping_nodes_by_nodegroup_id[
                        upsert_tile.nodegroup_id
                    ]
                else:
                    grouping_node = upsert_tile.nodegroup.grouping_node
                for node in grouping_node.nodegroup.node_set.all():
                    datatype = self.datatype_factory.get_instance(node.datatype)
                    datatype.post_tile_save(
                        upsert_tile, str(node.pk), request=self.request
                    )

            for upsert_proxy in upsert_proxies:
                upsert_proxy._Tile__postSave()

            # Save edits: could be done in bulk once above side effects are un-proxied.
            for insert_proxy in insert_proxies:
                insert_proxy.save_edit(
                    user=getattr(self.request, "user", None),
                    edit_type="tile create",
                    old_value={},
                    new_value=insert_proxy.data,
                    newprovisionalvalue=insert_proxy._newprovisionalvalue,
                    provisional_edit_log_details=insert_proxy._provisional_edit_log_details,
                    transaction_id=self.transaction_id,
                    new_resource_created=self.new_resource_created,
                    note=None,
                )
            for update_proxy in update_proxies:
                update_proxy.save_edit(
                    user=getattr(self.request, "user", None),
                    edit_type="tile edit",
                    old_value=update_proxy._existing_data,
                    new_value=update_proxy.data,
                    newprovisionalvalue=update_proxy._newprovisionalvalue,
                    oldprovisionalvalue=update_proxy._oldprovisionalvalue,
                    provisional_edit_log_details=update_proxy._provisional_edit_log_details,
                    transaction_id=self.transaction_id,
                )
            for delete_proxy in delete_proxies:
                delete_proxy.save_edit(
                    user=getattr(self.request, "user", None),
                    edit_type="tile delete",
                    old_value=delete_proxy.data,
                    provisional_edit_log_details=None,
                    transaction_id=self.transaction_id,
                )
