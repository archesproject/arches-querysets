from arches_querysets.models import TileTree
from tests.utils import GraphTestCase


class InternalsTests(GraphTestCase):
    def test_node_alias_collision_with_model_field(self):
        self.file_list_node_1.alias = "file"
        self.file_list_node_1.save()
        # Previously, this clashed with the related query name "file"
        qs = TileTree.get_tiles(graph_slug="datatype_lookups", nodegroup_alias="file")
        # TODO: determine the reserved namespace to use here.
        self.assertFalse(qs.filter(_arches_querysets_file__isnull=False).exists())
