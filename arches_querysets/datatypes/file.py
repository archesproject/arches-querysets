from django.utils.translation import get_language
from uuid import UUID

from arches import __version__ as _arches_version_str
from packaging.version import Version

arches_version = Version(_arches_version_str)
from arches.app.datatypes import datatypes
from arches.app.models import models
from arches.app.models.models import File


class FileListDataType(datatypes.FileListDataType):
    localized_metadata_keys = {"altText", "attribution", "description", "title"}

    def get_display_value(self, tile, node, **kwargs):
        data = self.get_tile_data(tile)
        files = data[str(node.nodeid)]
        file_urls = ""
        if files is not None:
            file_urls = " | ".join(
                [file["url"] or "" for file in files if "url" in file]
            )

        return file_urls

    def transform_value_for_tile(self, value, *, languages=None, **kwargs):
        if not value:
            return value
        if not languages:  # pragma: no cover
            languages = models.Language.objects.all()
        language = get_language()
        # arches == 9.0.0 - remove the stringifieid_list in favor of the 8.1.0 logic
        if arches_version < Version("8.1"):
            original_value = value
            if isinstance(value, str):
                pass  # parent handles CSV strings directly
            elif isinstance(value, list) and all(
                isinstance(file_info, dict) for file_info in value
            ):
                pass  # parent now handles list of dicts directly
            else:
                raise TypeError(value)
            value = super().transform_value_for_tile(
                value, languages=languages, **kwargs
            )
            new_value = []
            for file in value:
                if not isinstance(original_value, str):
                    matching_file_info = next(
                        (
                            file_dict
                            for file_dict in original_value
                            if file_dict.get("name") == file.get("name")
                        ),
                        None,
                    )
                    if matching_file_info:
                        new_value.append({**matching_file_info, **file})
                else:
                    new_value.append(file)
        else:
            new_value = super().transform_value_for_tile(
                value, languages=languages, **kwargs
            )

        # Remove file object created in transform_value_for_tile
        # after discussion with chiatt, this behavior is only really needed
        # for the bulk loader (and causes integrity problems/duplicity) -
        # file will be recreated later in post_tile_save.  Skip this deletion
        if "bulk_import" not in kwargs and (
            "is_existing_tile" not in kwargs or not kwargs["is_existing_tile"]
        ):
            File.objects.filter(
                fileid__in=[file["file_id"] for file in new_value if file["file_id"] and isinstance(file["file_id"], UUID)]
            ).delete()
            for file_dict in new_value:
                file_dict["file_id"] = None
                file_dict["url"] = None

        for file_info in new_value:
            for key, val in file_info.items():
                if key not in self.localized_metadata_keys:
                    continue
                original_val = val
                if not isinstance(original_val, dict):
                    file_info[key] = {}
                for lang in languages:
                    if lang.code not in file_info[key]:
                        file_info[key][lang.code] = {
                            "value": original_val if lang.code == language else "",
                            "direction": lang.default_direction,
                        }

        return new_value
