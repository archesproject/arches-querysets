import uuid

from arches.app.models.models import Node, TileModel
from arches.app.utils.betterJSONSerializer import JSONDeserializer, JSONSerializer
from arches_querysets.models import ResourceTileTree
from tests.utils import GraphTestCase


class SerializationTests(GraphTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        # Create child nodegroups and tiles.
        cls.nodegroup_1_child, _ = cls.create_nodegroup(
            "datatypes_1_child", "1", parent_nodegroup=cls.nodegroup_1
        )
        cls.nodegroup_n_child, _ = cls.create_nodegroup(
            "datatypes_n_child", "n", parent_nodegroup=cls.nodegroup_n
        )
        cls.cardinality_1_child_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_1_child,
            resourceinstance=cls.resource_42,
            data={},
            parenttile=cls.cardinality_1_tile,
        )
        cls.cardinality_n_child_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_n_child,
            resourceinstance=cls.resource_42,
            data={},
            parenttile=cls.cardinality_n_tile,
        )

        # Clone nodes.
        for node in cls.data_nodes:
            node.pk = uuid.uuid4()
            node.name = node.name + "-child"
            node.alias = node.alias + "_child"
            node.nodegroup = (
                cls.nodegroup_1_child
                if node.nodegroup == cls.nodegroup_1
                else cls.nodegroup_n_child
            )
        Node.objects.bulk_create(cls.data_nodes)

        # Create data for the child non-localized-string node only.
        # TileModel.save() will initialize the other nodes to None.
        cls.non_localized_string_child_node = Node.objects.get(
            alias="non_localized_string_child"
        )
        cls.non_localized_string_child_node_n = Node.objects.get(
            alias="non_localized_string_n_child"
        )
        cls.cardinality_1_child_tile.data = {
            str(cls.non_localized_string_child_node): "child-1-value",
        }
        cls.cardinality_1_child_tile.save()
        cls.cardinality_n_child_tile.data = {
            str(cls.non_localized_string_child_node_n): "child-n-value",
        }
        cls.cardinality_n_child_tile.save()

        cls.resource = ResourceTileTree.get_tiles(
            "datatype_lookups", as_representation=True
        ).get(pk=cls.resource_42.pk)

    def test_serialization_via_better_json_serializer(self):
        dict_string = JSONSerializer().serialize(self.resource)
        resource_dict = JSONDeserializer().deserialize(dict_string)
        # Django model fields are present.
        self.assertIn("graph_id", resource_dict)
        tile_dict = resource_dict["aliased_data"]["datatypes_1"]
        self.assertIn("nodegroup_id", tile_dict)
        # Node values are present.
        self.assertIn("non_localized_string", tile_dict["aliased_data"])
        # Special properties are not present.
        self.assertNotIn("data", tile_dict)
        self.assertNotIn("parent", tile_dict)

        # Child tiles appear under nodegroup aliases.
        child_tile_dict = tile_dict["aliased_data"]["datatypes_1_child"]
        self.assertIn("tileid", child_tile_dict)

        # Cardinality N tiles appear in an array.
        tile_list = resource_dict["aliased_data"]["datatypes_n"][0]
        child_tile_list = tile_list["aliased_data"]["datatypes_n_child"]
        self.assertIn("tileid", child_tile_list[0])
