from arches_querysets.models import SemanticResource
from tests.utils import GraphTestCase


class LookupTests(GraphTestCase):
    def test_cardinality_1_resource_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        # Exact
        for lookup, value in [
            ("boolean", True),
            ("number", 42.0),  # Use a float so that stringifying causes failure.
            ("url__url_label", "42.com"),
            ("non-localized-string", "forty-two"),
            ("string__en__value", "forty-two"),
            ("date", "2042-04-02"),
            # More natural lookups in test_resource_instance_lookups()
            ("resource-instance__0__ontologyProperty", ""),
            ("resource-instance-list__0__ontologyProperty", ""),
            ("concept", "00000000-0000-0000-0000-000000000001"),
            ("concept-list", ["00000000-0000-0000-0000-000000000001"]),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

    def test_cardinality_n_resource_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        # Exact
        for lookup, value in [
            # ("boolean-n__contains", True),
            # ("number-n__contains", 42.0),  # Use a float so that stringifying causes failure.
            # ("url__url_label", "42.com"),
            ("non-localized-string-n__contains", "forty-two"),
            # ("string-n__en__value", "forty-two"),
            # ("date-n__contains", "2042-04-02"),
            # More natural lookups in test_resource_instance_lookups()
            # ("resource-instance-n__0__ontologyProperty", ""),
            # ("resource-instance-list-n__0__ontologyProperty", ""),
            ("concept-n__contains", "00000000-0000-0000-0000-000000000001"),
            # ("concept-list-n__contains", ["00000000-0000-0000-0000-000000000001"]),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

    def test_localized_string_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        for lookup, value in [
            ("string__any_lang_startswith", "forty"),
            ("string__any_lang_istartswith", "FORTY"),
            ("string__any_lang_contains", "fort"),
            ("string__any_lang_icontains", "FORT"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

        # Negatives
        for lookup, value in [
            ("string__any_lang_startswith", "orty-two"),
            ("string__any_lang_istartswith", "ORTY-TWO"),
            ("string__any_lang_contains", "orty-three"),
            ("string__any_lang_icontains", "ORTY-THREE"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertFalse(resources.filter(**{lookup: value}))

    def test_resource_instance_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        for lookup, value in [
            ("resource-instance__id", str(self.resource.pk)),
            ("resource-instance-list__contains", str(self.resource.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))
