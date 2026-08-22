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
        raw_input_value = value
        # arches == 9.0.0 - remove the stringifieid_list in favor of the 8.1.0 logic
        if arches_version < Version("8.1"):
            original_value = value
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

        # Remove file object created in transform_value_for_tile so that
        # post_tile_save can recreate it from the actual uploaded binary in
        # request.FILES, matching by name against an entry with url=None.
        # Only applies to newly-uploaded entries (no pre-existing file_id) --
        # entries that already had a file_id were passed through unchanged
        # above and must keep pointing at their already-saved File record.
        # Bulk import is excluded because it saves the real file content
        # directly in transform_value_for_tile, with no later request.FILES
        # to match against.

        if "bulk_import" not in kwargs:
            original_items = (
                raw_input_value
                if isinstance(raw_input_value, list)
                else [raw_input_value] if isinstance(raw_input_value, dict) else []
            )
            preexisting_file_ids = {
                item.get("file_id")
                for item in original_items
                if isinstance(item, dict) and item.get("file_id")
            }
            newly_uploaded_file_ids = [
                file_dict["file_id"]
                for file_dict in new_value
                if file_dict.get("file_id") not in preexisting_file_ids
            ]
            File.objects.filter(fileid__in=newly_uploaded_file_ids).delete()
            for file_dict in new_value:
                if file_dict.get("file_id") in newly_uploaded_file_ids:
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
