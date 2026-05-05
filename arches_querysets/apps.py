from django.apps import AppConfig


class ArchesQuerySetsConfig(AppConfig):
    name = "arches_querysets"
    verbose_name = "Arches QuerySets"
    is_arches_application = True

    def ready(self):
        # add app-local settings to project settings only when missing
        from django.conf import settings
        from . import settings as local_settings

        # ensure concept cache exists to avoid errors in ConceptDataType
        if not hasattr(settings, "CACHES"):
            settings.CACHES = {}
        if "querysets_concept_cache" not in settings.CACHES:
            settings.CACHES["querysets_concept_cache"] = getattr(
                local_settings, "CACHES"
            )["querysets_concept_cache"]
        if "querysets_resource_instance_cache" not in settings.CACHES:
            settings.CACHES["querysets_resource_instance_cache"] = getattr(
                local_settings, "CACHES"
            )["querysets_resource_instance_cache"]
