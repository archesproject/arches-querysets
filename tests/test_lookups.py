from django.test import TestCase

from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.models.graph import Graph
from arches.app.models.models import (
    DDataType,
    Node,
    NodeGroup,
    ResourceInstance,
    TileModel,
    Value,
)

from arches_querysets.models import SemanticResource
from arches_querysets.utils.datatype_transforms import (
    resource_instance_transform_value_for_tile,
)


class LookupTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.graph = Graph.objects.create_graph(
            name="Datatype Lookups", is_resource=True
        )
        cls.graph.slug = "datatype_lookups"
        cls.graph.save()
        cls.top_node = Node.objects.create(
            graph=cls.graph, istopnode=True, datatype="semantic"
        )
        cls.grouping_node = Node(
            graph=cls.graph, alias="datatypes", istopnode=False, datatype="semantic"
        )
        cls.nodegroup = NodeGroup.objects.create(
            pk=cls.grouping_node.pk, grouping_node=cls.grouping_node
        )
        cls.grouping_node.nodegroup = cls.nodegroup
        cls.grouping_node.save()

        data_nodes = [
            Node(
                datatype=datatype,
                alias=datatype,
                name=datatype,
                istopnode=False,
                nodegroup=cls.nodegroup,
                graph=cls.graph,
            )
            for datatype in DDataType.objects.all()
        ]
        Node.objects.bulk_create(data_nodes)

        cls.datatype_factory = DataTypeFactory()
        ri_datatype = cls.datatype_factory.get_instance("resource-instance")

        # TODO: cards, widgets

        cls.resource = ResourceInstance.objects.create(graph=cls.graph)

        cls.sample_data = {
            "boolean": True,
            "number": 42,
            "non-localized-string": "forty-two",
            "string": {
                "en": {
                    "value": "forty-two",
                    "direction": "ltr",
                },
            },
            "url": {
                "url": "http://www.42.com/",
                "url_label": "42.com",
            },
            "date": "2042-04-02",
            "resource-instance": resource_instance_transform_value_for_tile(
                ri_datatype, cls.resource
            ),
            "resource-instance-list": resource_instance_transform_value_for_tile(
                ri_datatype, cls.resource
            ),
            "concept": "00000000-0000-0000-0000-000000000001",
            "concept-list": ["00000000-0000-0000-0000-000000000001"],
            # reference (?)
        }

        cls.tile = TileModel.objects.create(
            nodegroup=cls.nodegroup,
            resourceinstance=cls.resource,
            sortorder=0,
            data={
                str(node.pk): cls.sample_data[node.datatype.pk]
                for node in data_nodes
                if node.datatype.pk in cls.sample_data
            },
        )

    def test_cardinality_1_resource_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        # Exact
        for lookup, value in [
            ("boolean", True),
            ("number", 42.0),  # Use a float so that stringifying causes failure.
            ("url__url_label", "42.com"),
            ("non-localized-string", "forty-two"),
            ("string__en__value", "forty-two"),
            ("date", "2042-04-02"),
            # ("resource-instance__id", str(self.resource.pk)),
            # ("resource-instance__ids", [str(self.resource.pk)]),
            ("resource-instance__0__ontologyProperty", ""),
            ("resource-instance-list__0__ontologyProperty", ""),
            ("concept", "00000000-0000-0000-0000-000000000001"),
            ("concept-list", ["00000000-0000-0000-0000-000000000001"]),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))

        # Contains

    def test_localized_string_lookups(self):
        resources = SemanticResource.as_model("datatype_lookups")

        for lookup, value in [
            ("string__any_lang_startswith", "forty"),
            ("string__any_lang_istartswith", "FORTY"),
            ("string__any_lang_contains", "fort"),
            ("string__any_lang_icontains", "FORT"),
        ]:
            with self.subTest(lookup=lookup, value=value):
                self.assertTrue(resources.filter(**{lookup: value}))
