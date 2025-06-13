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
            ("non_localized_string", "forty-two"),
            ("string__en__value", "forty-two"),
            ("date", "2042-04-02"),
            # More natural lookups in test_resource_instance_lookups()
            ("resource_instance__0__ontologyProperty", ""),
            ("resource_instance_list__0__ontologyProperty", ""),
            ("concept", str(self.concept_value.pk)),
            ("concept_list", [str(self.concept_value.pk)]),
            # TODO: More natural lookups
            ("node_value", str(self.cardinality_1_tile.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

    def test_cardinality_n_resource_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        # Exact
        for lookup, value in [
            # ("boolean_n__contains", True),
            # ("number_n__contains", 42.0),  # Use a float so that stringifying causes failure.
            # ("url__url_label", "42.com"),
            ("non_localized_string_n__contains", "forty-two"),
            # ("string_n__en__value", "forty-two"),
            # ("date_n__contains", "2042-04-02"),
            # More natural lookups in test_resource_instance_lookups()
            # ("resource_instance_n__0__ontologyProperty", ""),
            # ("resource_instance_list_n__0__ontologyProperty", ""),
            ("concept_n__contains", str(self.concept_value.pk)),
            # ("concept_list_n__contains", [str(self.value.pk)]),
            # TODO: More natural lookups
            # ("node_value_n__contains", [str(self.cardinality_n_tile.pk)]),
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
            ("resource_instance__id", str(self.resource_42.pk)),
            ("resource_instance_list__contains", str(self.resource_42.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))
