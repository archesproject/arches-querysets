"""
Module to hold improved versions of datatype methods
until we can verify correctness/desirability & upstream the changes.

For instance, some of this might have been alleviated by calling
pre_structure_tile_data()?
"""

import ast
import copy
import json
import uuid
from datetime import date, datetime

from django.conf import settings
from django.utils.translation import get_language

from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.models import models
from arches.app.utils.betterJSONSerializer import JSONSerializer
from arches.app.utils.i18n import rank_label


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
        for rxr in tile._enriched_resource.resxres_resource_instance_ids_from.all():
            if rxr.resourceinstanceidto_id == uuid.UUID(inner_val["resourceId"]):
                display_val = rxr.resourceinstanceidto.descriptors[lang]["name"]
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


def file_list_to_representation(self, value):
    """Resolve localized string metadata to a single language value."""
    final_value = copy.deepcopy(value)
    for file_data in final_value:
        for key, val in file_data.items():
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
            file_data[key] = ranked[0][1].get("value")
    return final_value


def string_to_json(self, tile, node):
    data = self.get_tile_data(tile)
    if data:
        return self.compile_json(tile, node, **data.get(str(node.nodeid)) or {})


def date_transform_value_for_tile(self, value, **kwargs):
    value = None if value == "" else value
    if value is not None:
        if type(value) == list:
            value = value[0]
        elif (
            type(value) == str and len(value) < 4 and value.startswith("-") is False
        ):  # a year before 1000 but not BCE
            value = value.zfill(4)
    if isinstance(value, (date, datetime)):
        v = value
    else:
        valid_date_format, valid = self.get_valid_date_format(value)
        if valid:
            v = datetime.strptime(value, valid_date_format)
        else:
            v = datetime.strptime(value, settings.DATE_IMPORT_EXPORT_FORMAT)
    # The .astimezone() function throws an error on Windows for dates before 1970
    if isinstance(v, datetime):
        try:
            v = v.astimezone()
        except:
            v = self.backup_astimezone(v)
        value = v.isoformat(timespec="milliseconds")
    elif isinstance(v, date):
        value = v.isoformat()
    return value


def resource_instance_transform_value_for_tile(self, value, **kwargs):
    def from_id_string(uuid_string, graph_id=None):
        nonlocal kwargs
        for graph_config in kwargs.get("graphs", []):
            if graph_id is None or str(graph_id) == graph_config["graphid"]:
                break
        else:
            graph_config = {"ontologyProperty": {}, "inverseOntologyProperty": {}}
        return {
            "resourceId": uuid_string,
            "ontologyProperty": graph_config["ontologyProperty"],
            "inverseOntologyProperty": graph_config["inverseOntologyProperty"],
        }

    try:
        return json.loads(value)
    except ValueError:
        # do this if json (invalid) is formatted with single quotes, re #6390
        try:
            return ast.literal_eval(value)
        except:
            return value
    except TypeError:
        # data should come in as json but python list is accepted as well
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
