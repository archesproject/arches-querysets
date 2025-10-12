from django.db import models


class JSONBPathQueryArray(models.Func):
    function = "JSONB_PATH_QUERY_ARRAY"
    arity = 2
    output_field = models.JSONField()


class AllStrings(JSONBPathQueryArray):
    """
    Sugar so that instead of doing:
    TileTree.objects.get_tiles("datatype_lookups", "datatypes_1").values(
        all_strings=JSONBPathQueryArray('string_alias', models.Value("$.*.value"))
    ).values('all_strings')

    You can do:
    TileTree.objects.get_tiles("datatype_lookups", "datatypes_1").values(
        all_strings=AllStrings('string_alias')
    ).values('all_strings')

    This should demonstrate how to encapsulate similar JSON Path query lookups.
    """

    def __init__(self, *expressions, **kwargs):
        if not expressions[1:]:
            expressions = (expressions[0], models.Value("$.*.value"))

        return super().__init__(*expressions, **kwargs)
