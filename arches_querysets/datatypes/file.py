from django.utils.translation import get_language

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
        original_value = value
        # arches == 9.0.0 - remove the stringifieid_list in favor of the 8.1.0 logic
        if arches_version < Version("8.1"):
            if isinstance(value, str):
                stringified_list = value
            elif isinstance(value, list) and all(
                isinstance(file_info, dict) for file_info in value
            ):
                stringified_list = ",".join(
                    [file_info.get("name") for file_info in value]
                )
            else:
                raise TypeError(value)
            value = super().transform_value_for_tile(
                stringified_list, languages=languages, **kwargs
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

        # Undo the phantom file/file_id fabricated above for genuinely new
        # entries ( no file_id in the original payload ), so post_tile_save
        # can link the real upload.
        if "bulk_import" not in kwargs:
            # Can't align positionally; skip rather than risk deleting a
            # File row we can't positively identify as brand-new.
            new_file_dicts = []
            if isinstance(original_value, list) and len(original_value) == len(
                new_value
            ):
                new_file_dicts = [
                    file_dict
                    for original_entry, file_dict in zip(original_value, new_value)
                    if not (
                        isinstance(original_entry, dict)
                        and original_entry.get("file_id")
                    )
                ]

            if new_file_dicts:
                File.objects.filter(
                    fileid__in=[file_dict["file_id"] for file_dict in new_file_dicts]
                ).delete()
                for file_dict in new_file_dicts:
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
