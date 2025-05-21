from django.test import TestCase

from arches import VERSION as arches_version
from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.models.models import (
    CardModel,
    CardXNodeXWidget,
    DDataType,
    Node,
    NodeGroup,
    ResourceInstance,
    TileModel,
)

from arches_querysets.models import GraphWithPrefetching
from arches_querysets.utils.datatype_transforms import (
    resource_instance_transform_value_for_tile,
)


class GraphTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.graph = GraphWithPrefetching.objects.create_graph(
            name="Datatype Lookups", is_resource=True
        )

        cls.grouping_node_1 = Node(
            graph=cls.graph, alias="datatypes-1", istopnode=False, datatype="semantic"
        )
        cls.nodegroup_1 = NodeGroup.objects.create(pk=cls.grouping_node_1.pk)
        if arches_version >= (8, 0):
            cls.nodegroup_1.grouping_node = cls.grouping_node_1
            cls.nodegroup_1.save()
        cls.grouping_node_1.nodegroup = cls.nodegroup_1
        cls.grouping_node_1.save()

        cls.grouping_node_n = Node(
            graph=cls.graph, alias="datatypes-n", istopnode=False, datatype="semantic"
        )
        cls.nodegroup_n = NodeGroup.objects.create(
            pk=cls.grouping_node_n.pk,
            cardinality="n",
        )
        if arches_version >= (8, 0):
            cls.nodegroup_n.grouping_node = cls.grouping_node_n
            cls.nodegroup_n.save()
        cls.grouping_node_n.nodegroup = cls.nodegroup_n
        cls.grouping_node_n.save()

        datatypes = DDataType.objects.all()
        data_nodes_1 = [
            Node(
                datatype=datatype.pk,
                alias=datatype.pk,
                name=datatype.pk,
                istopnode=False,
                nodegroup=cls.nodegroup_1,
                graph=cls.graph,
            )
            for datatype in datatypes
        ]
        data_nodes_n = [
            Node(
                datatype=datatype.pk,
                alias=datatype.pk + "-n",
                name=datatype.pk + "-n",
                istopnode=False,
                nodegroup=cls.nodegroup_n,
                graph=cls.graph,
            )
            for datatype in datatypes
        ]
        nodes = Node.objects.bulk_create(data_nodes_1 + data_nodes_n)

        cards = [
            CardModel(
                graph=cls.graph,
                nodegroup=nodegroup,
            )
            for nodegroup in [cls.nodegroup_1, cls.nodegroup_n]
        ]
        cards = CardModel.objects.bulk_create(cards)

        node_widgets = [
            CardXNodeXWidget(
                node=node,
                widget_id=cls.find_default_widget_id(node, datatypes),
                card=node.nodegroup.cardmodel_set.all()[0],
            )
            for node in [n for n in nodes if n.datatype != "semantic"]
        ]
        CardXNodeXWidget.objects.bulk_create(node_widgets)

        cls.datatype_factory = DataTypeFactory()
        ri_datatype = cls.datatype_factory.get_instance("resource-instance")

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

        cls.cardinality_1_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_1,
            resourceinstance=cls.resource,
            data={
                str(node.pk): cls.sample_data[node.datatype]
                for node in data_nodes_1
                if node.datatype in cls.sample_data
            },
        )

        cls.cardinality_n_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_n,
            resourceinstance=cls.resource,
            data={
                str(node.pk): cls.sample_data[node.datatype]
                for node in data_nodes_n
                if node.datatype in cls.sample_data
            },
        )

    @classmethod
    def find_default_widget_id(cls, node, datatypes):
        for datatype in datatypes:
            if node.datatype == datatype.pk:
                return datatype.defaultwidget_id
        return None
