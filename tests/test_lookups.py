from django.db.models import Max, Min

from arches_querysets.models import ResourceTileTree, TileTree
from arches_querysets.utils.tests import GraphTestCase


class GenericLookupTests(GraphTestCase):
    def setUp(self):
        self.resources = ResourceTileTree.get_tiles("datatype_lookups")
        self.tiles_1 = TileTree.get_tiles(
            "datatype_lookups", nodegroup_alias="datatypes_1"
        )
        self.tiles_n = TileTree.get_tiles(
            "datatype_lookups", nodegroup_alias="datatypes_n"
        )

    def test_cardinality_1(self):
        # Exact
        for lookup, value in [
            ("boolean", True),
            ("number", 42.0),  # Use a float so that stringifying causes failure.
            ("url__url_label", "42.com"),
            ("non_localized_string", "forty-two"),
            ("string__en__value", "forty-two"),
            ("date", "2042-04-02"),
            # More natural lookups in ResourceInstanceLookupTests
            ("resource_instance__0__resourceId", str(self.resource_42.pk)),
            ("resource_instance_list__0__resourceId", str(self.resource_42.pk)),
            ("concept", self.concept_value.pk),
            ("concept_list", [str(self.concept_value.pk)]),
            ("node_value", self.cardinality_1_tile.pk),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(self.resources.filter(**{lookup: value}))
                self.assertTrue(self.tiles_1.filter(**{lookup: value}))

    def test_cardinality_n(self):
        # Contains
        for lookup, value in [
            ("boolean_n__contains", [True]),
            ("number_n__contains", [42.0]),
            ("url_n__0__url_label", "42.com"),
            ("non_localized_string_n__contains", ["forty-two"]),
            ("date_n__contains", ["2042-04-02"]),
            # better lookups for RI{list} below.
            ("resource_instance__0__resourceId", str(self.resource_42.pk)),
            # you likely want ids_contain, below.
            ("resource_instance_list__0__resourceId", str(self.resource_42.pk)),
            ("concept_n__contains", [self.concept_value.pk]),
            # you likely want any_contains, below.
            ("concept_list_n__0__contains", str(self.concept_value.pk)),
            ("node_value_n__contains", [self.cardinality_n_tile.pk]),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(self.resources.filter(**{lookup: value}))

    def test_cardinality_1_values(self):
        for node in self.data_nodes_1:
            with self.subTest(alias=node.alias):
                self.assertTrue(self.resources.values(node.alias))
                self.assertTrue(self.tiles_1.values(node.alias))
                self.assertTrue(self.resources.values_list(node.alias))
                self.assertTrue(self.tiles_1.values_list(node.alias))

    def test_cardinality_n_values(self):
        for node in self.data_nodes_n:
            with self.subTest(alias=node.alias):
                self.assertTrue(self.resources.values(node.alias))
                self.assertTrue(self.tiles_n.values(node.alias))
                self.assertTrue(self.resources.values_list(node.alias))
                self.assertTrue(self.tiles_n.values_list(node.alias))


class NonLocalizedStringLookupTests(GenericLookupTests):
    def test_cardinality_1(self):
        self.assertTrue(self.resources.filter(non_localized_string__contains="forty"))

    def test_cardinality_n(self):
        self.assertTrue(
            self.resources.filter(non_localized_string_n__contains=["forty-two"])
        )
        self.assertFalse(
            self.resources.filter(non_localized_string_n__contains=["forty"])
        )
        self.assertTrue(
            self.resources.filter(non_localized_string_n__any_contains="forty")
        )
        self.assertFalse(
            self.resources.filter(non_localized_string_n__any_contains="FORTY")
        )
        self.assertTrue(
            self.resources.filter(non_localized_string_n__any_icontains="FORTY")
        )


class LocalizedStringLookupTests(GenericLookupTests):
    def test_cardinality_1(self):
        for lookup, value in [
            ("string__any_lang_startswith", "forty"),
            ("string__any_lang_istartswith", "FORTY"),
            ("string__any_lang_contains", "fort"),
            ("string__any_lang_icontains", "FORT"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(self.resources.filter(**{lookup: value}))

        # Negatives
        for lookup, value in [
            ("string__any_lang_startswith", "orty-two"),
            ("string__any_lang_istartswith", "ORTY-TWO"),
            ("string__any_lang_contains", "orty-three"),
            ("string__any_lang_icontains", "ORTY-THREE"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertFalse(self.resources.filter(**{lookup: value}))

    def test_cardinality_n(self):
        for lookup, value in [
            ("string_n__any_lang_startswith", "forty"),
            ("string_n__any_lang_istartswith", "FORTY"),
            ("string_n__any_lang_contains", "fort"),
            ("string_n__any_lang_icontains", "FORT"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(self.resources.filter(**{lookup: value}))

        # Negatives
        for lookup, value in [
            ("string_n__any_lang_startswith", "orty-two"),
            ("string_n__any_lang_istartswith", "ORTY-TWO"),
            ("string_n__any_lang_contains", "orty-three"),
            ("string_n__any_lang_icontains", "ORTY-THREE"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertFalse(self.resources.filter(**{lookup: value}))


class ResourceInstanceLookupTests(GenericLookupTests):
    def test_cardinality_1(self):
        for lookup, value in [
            ("resource_instance__id", str(self.resource_42.pk)),
            ("resource_instance_list__contains", str(self.resource_42.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(self.resources.filter(**{lookup: value}))

    def test_cardinality_n(self):
        for lookup, value in [
            ("resource_instance_n__ids_contain", str(self.resource_42.pk)),
            ("resource_instance_list_n__ids_contain", str(self.resource_42.pk)),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(self.resources.filter(**{lookup: value}))


class AggregateTests(GenericLookupTests):
    def test_number(self):
        # Edit the resource that usually has None in all nodes to have a value of 43.
        resource2 = ResourceTileTree.get_tiles("datatype_lookups").get(
            pk=self.resource_none.pk
        )
        resource2.aliased_data.datatypes_1.aliased_data.number = 43
        resource2.aliased_data.datatypes_n[0].aliased_data.number_n = 43
        resource2.save(force_admin=True)

        # Per-table aggregate on cardinality-1 value
        node_alias = "number"
        query = self.resources.aggregate(Min(node_alias), Max(node_alias))
        self.assertEqual(query[f"{node_alias}__min"], 42.0)
        self.assertEqual(query[f"{node_alias}__max"], 43.0)

        # Per-table aggregate on arrays, e.g. [43] > [42], but [43, 42] < [43, 44]
        node_alias = "number_n"
        query = self.resources.aggregate(Min(node_alias), Max(node_alias))
        self.assertEqual(query[f"{node_alias}__min"], [42.0])
        self.assertEqual(query[f"{node_alias}__max"], [43.0])
