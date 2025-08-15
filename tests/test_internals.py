from arches_querysets.models import TileTree
from arches_querysets.utils.tests import GraphTestCase


class InternalsTests(GraphTestCase):
    def test_node_alias_collision_with_model_field(self):
        self.file_list_node_1.alias = "file"
        self.file_list_node_1.save()
        # Previously, this clashed with the related query name "file"
        qs = TileTree.get_tiles(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        # TODO: determine the reserved namespace to use here.
        self.assertEqual(
            qs.filter(_arches_querysets_file__isnull=True)[0].resourceinstance_id,
            self.resource_none.pk,
        )
