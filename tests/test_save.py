from arches_querysets.models import ResourceTileTree, TileTree
from tests.utils import GraphTestCase


class SaveTileTests(GraphTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        resources = ResourceTileTree.get_tiles("datatype_lookups")
        cls.semantic_resource_none = resources.get(pk=cls.resource_none.pk)

    def test_fill_blanks(self):
        self.semantic_resource_none.tilemodel_set.all().delete()
        self.semantic_resource = ResourceTileTree.get_tiles("datatype_lookups").get(
            pk=self.resource_none.pk
        )
        self.semantic_resource_none.fill_blanks()
        self.assertIsInstance(
            self.semantic_resource_none.aliased_data.datatypes_1, TileTree
        )
        self.assertIsInstance(
            self.semantic_resource_none.aliased_data.datatypes_n[0], TileTree
        )
