"""
Module to hold improved versions of datatype methods
until we can verify correctness/desirability & upstream the changes.
"""

import ast
import copy
import json
import logging
import uuid

from django.utils.translation import get_language, gettext as _

from arches import __version__ as arches_version
from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.models import models
from arches.app.utils.betterJSONSerializer import JSONSerializer
from arches.app.utils.i18n import rank_label


logger = logging.getLogger(__name__)


def resource_instance_to_json(self, tile, node):
    return resource_instance_list_to_json(self, tile, node)


def resource_instance_list_to_json(self, tile, node):
    data = self.get_tile_data(tile)
    if not data:
        return []
    ret = []

    for inner_val in data.get(str(node.nodeid)):
        if not inner_val:
            continue
        copy = {**inner_val}
        lang = get_language()
        for rxr in (
            tile._enriched_resource.from_resxres.all()
            if arches_version >= "8"
            else tile._enriched_resource.resxres_resource_instance_ids_from.all()
        ):
            to_resource_id = (
                rxr.resourceinstanceidto_id
                if arches_version < "8"
                else rxr.to_resource_id
            )
            if to_resource_id == uuid.UUID(inner_val["resourceId"]):
                to_resource = (
                    rxr.resourceinstanceidto
                    if arches_version < "8"
                    else rxr.to_resource
                )
                if not to_resource:
                    msg = f"Missing ResourceXResource target: {to_resource_id}"
                    logger.warning(msg)
                    copy["display_value"] = _("Missing")
                    break
                display_val = to_resource.descriptors[lang]["name"]
                copy["display_value"] = display_val
                break
        ret.append(copy)

    return ret


def concept_transform_value_for_tile(self, value, **kwargs):
    if isinstance(value, uuid.UUID):
        return str(value)
    return self.transform_value_for_tile(value, **kwargs)


def concept_to_json(self, tile, node):
    data = self.get_tile_data(tile)
    if data:
        value_data = {}
        if val := data[str(node.nodeid)]:
            value_data = JSONSerializer().serializeToPython(
                self.get_value(uuid.UUID(val))
            )
        return self.compile_json(tile, node, **value_data)


def concept_list_validate(
    self,
    value,
    row_number=None,
    source="",
    node=None,
    nodeid=None,
    strict=False,
    **kwargs,
):
    errors = []
    # iterate list of values and use the concept validation on each one
    if value is not None:
        validate_concept = DataTypeFactory().get_instance("concept")
        for v in value:
            if isinstance(v, uuid.UUID):
                val = str(v)
            else:
                val = v.strip()
            errors += validate_concept.validate(val, row_number)
    return errors


def concept_list_transform_value_for_tile(self, value, **kwargs):
    if not isinstance(value, list):
        value = [value]
    if all(isinstance(inner, uuid.UUID) for inner in value):
        return [str(inner) for inner in value]
    return self.transform_value_for_tile(value, **kwargs)


def concept_list_to_json(self, tile, node):
    new_values = []
    data = self.get_tile_data(tile)
    if data:
        for val in data[str(node.nodeid)] or []:
            new_val = self.get_value(uuid.UUID(val))
            new_values.append(new_val)
    return self.compile_json(tile, node, concept_details=new_values)


def file_list_transform_value_for_tile(self, value, *, languages, **kwargs):
    if not value:
        return value

    stringified_list = ",".join([file_info.get("name") for file_info in value])
    final_value = self.transform_value_for_tile(
        stringified_list, languages=languages, **kwargs
    )

    for file_info in final_value:
        for key, val in file_info.items():
            if key not in {"altText", "attribution", "description", "title"}:
                continue
            original_val = val
            if not isinstance(original_val, dict):
                file_info[key] = {}
            for lang in languages:
                if lang.code not in file_info[key]:
                    file_info[key][lang.code] = {
                        "value": original_val if lang.code == get_language() else "",
                        "direction": lang.default_direction,
                    }

    return final_value


def file_list_merge_tile_value(self, tile, node_id_str, transformed) -> None:
    if not (existing_tile_value := tile.data.get(node_id_str)):
        tile.data[node_id_str] = transformed
        return
    for file_info in transformed:
        for key, val in file_info.items():
            if key not in {"altText", "attribution", "description", "title"}:
                continue
            for existing_file_info in existing_tile_value:
                if existing_file_info.get("file_id") == file_info.get("file_id"):
                    file_info[key] = existing_file_info[key] | val
                break
    tile.data[node_id_str] = transformed


def file_list_to_representation(self, value):
    """Resolve localized string metadata to a single language value."""
    if not value:
        return value
    final_value = copy.deepcopy(value)
    for file_info in final_value:
        for key, val in file_info.items():
            if not isinstance(val, dict):
                continue
            lang_val_pairs = [(lang, lang_val) for lang, lang_val in val.items()]
            if not lang_val_pairs:
                continue
            ranked = sorted(
                lang_val_pairs,
                key=lambda pair: rank_label(source_lang=pair[0]),
                reverse=True,
            )
            file_info[key] = ranked[0][1].get("value")
    return final_value


def string_to_json(self, tile, node):
    data = self.get_tile_data(tile)
    if data:
        return self.compile_json(tile, node, **data.get(str(node.nodeid)) or {})


def string_merge_tile_value(self, tile, node_id_str, transformed) -> None:
    tile.data[node_id_str] = (tile.data.get(node_id_str) or {}) | transformed


def string_to_representation(self, value):
    """Resolve localized string metadata to a single language value."""
    if not value or not isinstance(value, dict):
        return ""
    lang_val_pairs = [(lang, obj["value"]) for lang, obj in value.items()]
    if not lang_val_pairs:
        return
    ranked = sorted(
        lang_val_pairs,
        key=lambda pair: rank_label(source_lang=pair[0]),
        reverse=True,
    )
    return ranked[0][1]


def resource_instance_transform_value_for_tile(self, value, **kwargs):
    def from_id_string(uuid_string, graph_id=None):
        nonlocal kwargs
        for graph_config in kwargs.get("graphs", []):
            if graph_id is None or str(graph_id) == graph_config["graphid"]:
                break
        else:
            graph_config = {}
        return {
            "resourceId": uuid_string,
            "ontologyProperty": graph_config.get("ontologyProperty", ""),
            "inverseOntologyProperty": graph_config.get("inverseOntologyProperty", ""),
        }

    try:
        if isinstance(value, str):
            value = [value]
            raise TypeError
        return json.loads(value)
    except ValueError:
        # do this if json (invalid) is formatted with single quotes, re #6390
        try:
            return ast.literal_eval(value)
        except:
            return value
    except TypeError:
        if isinstance(value, list):
            transformed = []
            for inner in value:
                match inner:
                    case models.ResourceInstance():
                        transformed.append(
                            from_id_string(str(inner.pk), inner.graph_id)
                        )
                    case uuid.UUID():
                        # TODO: handle multiple graph configs, requires db?
                        transformed.append(from_id_string(str(inner)))
                    case str():
                        # TODO: handle multiple graph configs, requires db?
                        transformed.append(from_id_string(inner))
                    case _:
                        # TODO: move this to validate?
                        inner.pop("display_value", None)
                        transformed.append(inner)
            return transformed
        if isinstance(value, models.ResourceInstance):
            return [from_id_string(str(value.pk), value.graph_id)]
        raise
