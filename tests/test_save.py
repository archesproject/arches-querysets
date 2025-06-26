import uuid
from arches_querysets.models import SemanticResource
from tests.utils import GraphTestCase


class SaveTileTests(GraphTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        resources = SemanticResource.as_model(
            "datatype_lookups", as_representation=True
        )
        cls.semantic_resource_42 = resources.get(pk=cls.resource_42.pk)
        cls.datatype_1 = cls.semantic_resource_42.aliased_data.datatypes_1
        cls.datatype_n = cls.semantic_resource_42.aliased_data.datatypes_n

        cls.semantic_resource_none = resources.get(pk=cls.resource_none.pk)
        cls.datatype_1_none = cls.semantic_resource_none.aliased_data.datatypes_1
        cls.datatype_n_none = cls.semantic_resource_none.aliased_data.datatypes_n

    def test_blank_tile_save_with_defaults(self):
        # Saving a blank tile should populate default values if defaults are defined
        self.add_default_values_for_widgets()

        # Existing tiles with `None`'s should not be updated with defaults during save
        self.semantic_resource_none.save()
        for (
            key,
            value,
        ) in self.semantic_resource_none.aliased_data.datatypes_1.data.items():
            self.assertIsNone(value, f"Expected None for {key}")

        # fill_blanks only intializes a tile for nodegroups that don't yet have
        # a tile. Remove those tiles so we can use fill_blanks.
        self.semantic_resource_42.aliased_data.datatypes_1.delete()
        self.semantic_resource_42.refresh_from_db()
        self.semantic_resource_42.fill_blanks()
        # self.semantic_resource_42.aliased_data.datatypes_1.tileid = uuid.uuid4()
        self.semantic_resource_42.save()
        for (
            nodeid,
            value,
        ) in self.semantic_resource_42.aliased_data.datatypes_1.data.items():
            self.assertEqual(value, self.default_vals_by_nodeid[nodeid])

        # fill_blanks gives an unsaved empty tile, but we also need to test that inserting
        # a tile (ie from the frontend) will fill defaults if no values are provided
        self.semantic_resource_42.aliased_data.datatypes_1.delete()
        self.semantic_resource_42.refresh_from_db()
        self.semantic_resource_42.fill_blanks()

        # mock a new tile via fill_blanks, but overwrite default values set by fill_blanks
        for node in self.semantic_resource_42.aliased_data.datatypes_1.data:
            self.semantic_resource_42.aliased_data.datatypes_1.data[node] = None
        # Save should stock defaults
        self.semantic_resource_42.aliased_data.datatypes_1.save()

        for (
            nodeid,
            value,
        ) in self.semantic_resource_42.aliased_data.datatypes_1.data.items():
            self.assertEqual(value, self.default_vals_by_nodeid[nodeid])
