from arches_querysets.models import ResourceTileTree
from tests.utils import GraphTestCase


class LookupTests(GraphTestCase):
    def test_cardinality_1_lookups(self):
        resources = ResourceTileTree.get_tiles("datatype_lookups")

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

    def test_cardinality_n_lookups(self):
        resources = ResourceTileTree.get_tiles("datatype_lookups")

        # Exact
        for lookup, value in [
            ("boolean_n__contains", [True]),
            ("number_n__contains", [42.0]),
            # ("url_n__url_label", "42.com"),
            ("non_localized_string_n__contains", ["forty-two"]),
            ("date_n__contains", ["2042-04-02"]),
            ("concept_n__contains", [str(self.concept_value.pk)]),
            # ("concept_list_n__contains", [str(self.concept_value.pk)]),
            ("node_value_n__contains", [str(self.cardinality_n_tile.pk)]),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

    def test_localized_string_lookups(self):
        resources = ResourceTileTree.get_tiles("datatype_lookups")

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

    def test_localized_string_lookups_n(self):
        resources = ResourceTileTree.get_tiles("datatype_lookups")

        for lookup, value in [
            ("string_n__any_lang_startswith", "forty"),
            ("string_n__any_lang_istartswith", "FORTY"),
            ("string_n__any_lang_contains", "fort"),
            ("string_n__any_lang_icontains", "FORT"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

        # Negatives
        for lookup, value in [
            ("string_n__any_lang_startswith", "orty-two"),
            ("string_n__any_lang_istartswith", "ORTY-TWO"),
            ("string_n__any_lang_contains", "orty-three"),
            ("string_n__any_lang_icontains", "ORTY-THREE"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertFalse(resources.filter(**{lookup: value}))

    def test_resource_instance_lookups(self):
        resources = ResourceTileTree.get_tiles("datatype_lookups")

        for lookup, value in [
            ("resource_instance__id", str(self.resource_42.pk)),
            ("resource_instance_list__contains", str(self.resource_42.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

    def test_resource_instance_lookups_n(self):
        resources = ResourceTileTree.get_tiles("datatype_lookups")

        for lookup, value in [
            ("resource_instance_n__ids_contain", str(self.resource_42.pk)),
            # ("resource_instance_list_n__ids__contain", str(self.resource_42.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))
