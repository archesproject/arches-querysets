from arches_querysets.datatypes import DataTypeFactory
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
        # 8: graph -> node -> nodegroup -> child nodegroup -> child_nodegroup (none!)
        with self.assertNumQueries(8):
            GraphWithPrefetching.prefetch("datatype_lookups")

    def test_get_resources(self):
        # Clear the value lookups to avoid flakiness.
        factory = DataTypeFactory()
        concept_dt = factory.get_instance("concept")
        concept_dt.value_lookup = {}
        concept_list_dt = factory.get_instance("concept-list")
        concept_list_dt.value_lookup = {}

        # 1-8: test_get_graph_objects()
        # 9: resource
        # 10: tile depth 1
        # 11: tile -> nodegroup
        # 12: tile -> tile depth 2
        # 13: tile -> resource
        # (13 is a little unfortunate, but worth it for resourcexresource prefetches.)
        # 14: tile -> resource -> resourcexresource
        # 15: related resources
        # 16: concept value
        # 17: (N+1 BUG: core arches) another concept value
        with self.assertNumQueries(17):
            self.assertEqual(len(ResourceTileTree.get_tiles("datatype_lookups")), 2)
