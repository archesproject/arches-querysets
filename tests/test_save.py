from arches.app.models.models import TileModel
from django.core.exceptions import ValidationError

from arches_querysets.models import ResourceTileTree, TileTree
from arches_querysets.utils.tests import GraphTestCase


class SaveTileTests(GraphTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        resources = ResourceTileTree.get_tiles(
            "datatype_lookups", as_representation=True
        )
        cls.resource_42 = resources.get(pk=cls.resource_42.pk)
        cls.resource_42.graph_publication_id = cls.graph.publication_id
        cls.resource_42.save()
        cls.datatype_1 = cls.resource_42.aliased_data.datatypes_1
        cls.datatype_n = cls.resource_42.aliased_data.datatypes_n

        cls.resource_none = resources.get(pk=cls.resource_none.pk)
        cls.resource_none.graph_publication_id = cls.graph.publication_id
        cls.resource_none.save()
        cls.datatype_1_none = cls.resource_none.aliased_data.datatypes_1
        cls.datatype_n_none = cls.resource_none.aliased_data.datatypes_n

    def assert_default_values_present(self, resource):
        for node_id_str, value in resource.aliased_data.datatypes_1.data.items():
            node = [node for node in self.data_nodes if str(node.pk) == node_id_str][0]
            with self.subTest(alias=node.alias):
                default_value = self.default_vals_by_nodeid[node_id_str]
                expected = TileTree.get_cleaned_default_value(node, default_value)
                self.assertEqual(value, expected)

    def test_blank_tile_save_with_defaults(self):
        # Existing tiles with `None`'s should not be updated with defaults during save
        self.resource_none.save()
        for key, value in self.resource_none.aliased_data.datatypes_1.data.items():
            self.assertIsNone(value, f"Expected None for {key}")

        # fill_blanks only intializes a tile for nodegroups that don't yet have
        # a tile. Remove those tiles so we can use fill_blanks.
        self.resource_42.aliased_data.datatypes_1.delete()
        self.resource_42.refresh_from_db()
        self.resource_42.fill_blanks()
        # Saving a blank tile should populate default values if defaults are defined.
        self.resource_42.save()
        self.assert_default_values_present(self.resource_42)

        # fill_blanks gives an unsaved empty tile, but we also need to test that inserting
        # a tile (ie from the frontend) will fill defaults if no values are provided
        self.resource_42.aliased_data.datatypes_1.delete()
        self.resource_42.refresh_from_db()
        self.resource_42.fill_blanks()

        # mock a new tile via fill_blanks, but overwrite default values set by fill_blanks
        for node in self.resource_42.aliased_data.datatypes_1.data:
            self.resource_42.aliased_data.datatypes_1.data[node] = None
        # Save should stock defaults
        self.resource_42.aliased_data.datatypes_1.save()
        self.assert_default_values_present(self.resource_42)

    def test_fill_blanks(self):
        self.resource_none.tilemodel_set.all().delete()
        self.resource_none.fill_blanks()
        self.assertIsInstance(self.resource_none.aliased_data.datatypes_1, TileTree)
        self.assertIsInstance(self.resource_none.aliased_data.datatypes_n[0], TileTree)
        self.assertIsInstance(
            self.resource_none.aliased_data.datatypes_1.aliased_data.datatypes_1_child,
            TileTree,
        )

        # Remove the child, fill_blanks() again.
        self.resource_none.aliased_data.datatypes_1.aliased_data.datatypes_1_child = (
            None
        )
        self.resource_none.fill_blanks()
        self.assertIsInstance(
            self.resource_none.aliased_data.datatypes_1.aliased_data.datatypes_1_child,
            TileTree,
        )

        msg = "Attempted to append to a populated cardinality-1 nodegroup"
        with self.assertRaisesMessage(RuntimeError, msg):
            self.resource_none.append_tile("datatypes_1")

    def test_parent_tile_backfilled_on_child_tile_save(self):
        self.resource_none.tilemodel_set.all().delete()
        new_child_tile = TileTree(
            resourceinstance=self.resource_none,
            nodegroup=self.nodegroup_1_child,
            number_child=4,
            # TODO(arches_version==9.0.0): in Arches 8+, data={} can be removed.
            data={},
        )
        new_child_tile.save()
        # The parent property holds the richer TileTree
        self.assertIsInstance(new_child_tile.parent, TileTree)
        # The regular Django field is untouched (still a vanilla TileModel)
        self.assertNotIsInstance(new_child_tile.parenttile, TileTree)
        self.assertIsInstance(new_child_tile.parenttile, TileModel)

    def test_cardinality_error(self):
        with self.assertRaises(ValidationError) as ctx:
            TileTree.objects.create(
                nodegroup=self.nodegroup_1, resourceinstance=self.resource_42, data={}
            )
        self.assertEqual(
            ctx.exception.message_dict, {"datatypes_1": ["Tile Cardinality Error"]}
        )
