from arches_querysets.models import GraphWithPrefetching, ResourceTileTree
from tests.utils import GraphTestCase


class PerformanceTests(GraphTestCase):
    def test_get_graph_objects(self):
        # 1: graph
        # 2: graph -> node
        # 3: graph -> node -> cardxnodexwidget
        # 4: graph -> node -> nodegroup
        # 5: graph -> node -> nodegroup -> node
        # 6: graph -> node -> nodegroup -> node -> cardxnodexwidget
        # 7: graph -> node -> nodegroup -> card
        # 8: graph -> node -> nodegroup -> child nodegroup
        # 9: graph -> node -> nodegroup -> child nodegroup -> nodes
        # 10: graph -> node -> nodegroup -> child nodegroup -> child_nodegroup (none!)
        with self.assertNumQueries(10):
            GraphWithPrefetching.prefetch("datatype_lookups")

    def test_get_resources(self):
        # 1-10: test_get_graph_objects()
        # 11: resource
        # 12: tile depth 1
        # 13: tile -> nodegroup
        # 14: tile -> tile depth 2
        # 15: tile -> resource
        # (15 is a little unfortunate, but worth it for resourcexresource prefetches.)
        # 16: tile -> resource -> resourcexresource
        # 17: related resources
        # 18: concept value
        # 19: (N+1 BUG: core arches) another concept value
        with self.assertNumQueries(19):
            self.assertEqual(len(ResourceTileTree.get_tiles("datatype_lookups")), 2)
