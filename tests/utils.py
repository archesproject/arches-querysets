import datetime

from django.test import TestCase

from arches import VERSION as arches_version
from arches.app.models.models import (
    CardModel,
    CardXNodeXWidget,
    Concept,
    DDataType,
    Node,
    NodeGroup,
    ResourceInstance,
    ResourceXResource,
    TileModel,
)

from arches_querysets.datatypes.datatypes import DataTypeFactory
from arches_querysets.models import GraphWithPrefetching


class GraphTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.datatype_factory = DataTypeFactory()  # custom!
        cls.create_graph()
        cls.create_nodegroups_and_grouping_nodes()
        cls.create_data_collecting_nodes()
        # cls.create_edges() -- edges not fathomed
        cls.create_cards()
        cls.create_widgets()
        cls.create_tiles()
        cls.create_resource_x_resources()

    @classmethod
    def create_graph(cls):
        cls.graph = GraphWithPrefetching.objects.create_graph(
            name="Datatype Lookups", is_resource=True
        )

    @classmethod
    def create_nodegroups_and_grouping_nodes(cls):
        cls.grouping_node_1 = Node(
            graph=cls.graph, alias="datatypes_1", istopnode=False, datatype="semantic"
        )
        cls.nodegroup_1 = NodeGroup.objects.create(pk=cls.grouping_node_1.pk)
        if arches_version >= (8, 0):
            cls.nodegroup_1.grouping_node = cls.grouping_node_1
            cls.nodegroup_1.save()
        cls.grouping_node_1.nodegroup = cls.nodegroup_1
        cls.grouping_node_1.save()

        cls.grouping_node_n = Node(
            graph=cls.graph, alias="datatypes_n", istopnode=False, datatype="semantic"
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

    @classmethod
    def create_data_collecting_nodes(cls):
        cls.datatypes = DDataType.objects.all()
        cls.data_nodes_1 = [
            Node(
                datatype=datatype.pk,
                alias=datatype.pk.replace("-", "_"),
                name=datatype.pk,
                istopnode=False,
                nodegroup=cls.nodegroup_1,
                graph=cls.graph,
                config=datatype.defaultconfig,
            )
            for datatype in cls.datatypes
        ]
        cls.data_nodes_n = [
            Node(
                datatype=datatype.pk,
                alias=datatype.pk.replace("-", "_") + "_n",
                name=datatype.pk + "-n",
                istopnode=False,
                nodegroup=cls.nodegroup_n,
                graph=cls.graph,
                config=datatype.defaultconfig,
            )
            for datatype in cls.datatypes
        ]
        cls.nodes = Node.objects.bulk_create(cls.data_nodes_1 + cls.data_nodes_n)

        cls.ri_node_1, cls.ri_node_n = [
            node for node in cls.nodes if node.datatype == "resource-instance"
        ]
        cls.ri_list_node_1, cls.ri_list_node_n = [
            node for node in cls.nodes if node.datatype == "resource-instance-list"
        ]
        cls.date_node_1, cls.date_node_n = [
            node for node in cls.nodes if node.datatype == "date"
        ]
        for node in cls.nodes:
            if node.datatype == "node-value":
                if node.nodegroup.cardinality == "1":
                    node.config["nodeid"] = str(cls.date_node_1.pk)
                else:
                    node.config["nodeid"] = str(cls.date_node_n.pk)
                node.save()

    @classmethod
    def create_cards(cls):
        cards = [
            CardModel(
                graph=cls.graph,
                nodegroup=nodegroup,
            )
            for nodegroup in [cls.nodegroup_1, cls.nodegroup_n]
        ]
        cards = CardModel.objects.bulk_create(cards)

    @classmethod
    def create_widgets(cls):
        node_widgets = [
            CardXNodeXWidget(
                node=node,
                widget_id=cls.find_default_widget_id(node, cls.datatypes),
                card=node.nodegroup.cardmodel_set.all()[0],
            )
            for node in [n for n in cls.nodes if n.datatype != "semantic"]
        ]
        CardXNodeXWidget.objects.bulk_create(node_widgets)

    @classmethod
    def create_tiles(cls):
        ri_dt = cls.datatype_factory.get_instance("resource-instance")
        ri_list_dt = cls.datatype_factory.get_instance("resource-instance-list")

        cls.resource = ResourceInstance.objects.create(
            graph=cls.graph, descriptors={"en": {"name": "Test Resource"}}
        )
        cls.concept = Concept.objects.get(pk="00000000-0000-0000-0000-000000000001")
        cls.value = cls.concept.value_set.get()

        cls.cardinality_1_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_1,
            resourceinstance=cls.resource,
            data={},
        )
        cls.cardinality_n_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_n,
            resourceinstance=cls.resource,
            data={},
        )

        cls.sample_data_1 = {
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
            "resource-instance": ri_dt.transform_value_for_tile(cls.resource),
            "resource-instance-list": ri_list_dt.transform_value_for_tile(cls.resource),
            "concept": str(cls.value.pk),
            "concept-list": [str(cls.value.pk)],
            "node-value": str(cls.cardinality_1_tile.pk),
            # reference (?)
            # randomly create geometry
        }
        cls.sample_data_n = {
            **cls.sample_data_1,
            "node-value": str(cls.cardinality_n_tile.pk),
        }

        cls.cardinality_1_tile.data = {
            str(node.pk): (
                cls.sample_data_1[node.datatype]
                if node.datatype in cls.sample_data_1
                else None
            )
            for node in cls.data_nodes_1
        }
        cls.cardinality_1_tile.save()
        cls.cardinality_n_tile.data = {
            str(node.pk): (
                cls.sample_data_n[node.datatype]
                if node.datatype in cls.sample_data_n
                else None
            )
            for node in cls.data_nodes_n
        }
        cls.cardinality_n_tile.save()

    @classmethod
    def create_resource_x_resources(cls):
        if arches_version < (8, 0):
            from_resource_attr = "resourceinstanceidto"
            to_resource_attr = "resourceinstanceidfrom"
            from_graph_attr = "resourceinstancefrom_graphid"
            to_graph_attr = "resourceinstanceto_graphid"
            tile_attr = "tileid"
            node_attr = "nodeid"
        else:
            from_resource_attr = "from_resource"
            to_resource_attr = "to_resource"
            from_graph_attr = "from_resource_graph"
            to_graph_attr = "to_resource_graph"
            tile_attr = "tile"
            node_attr = "node"
        rxrs = [
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource,
                    to_resource_attr: cls.resource,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_1_tile,
                    node_attr: cls.ri_node_1,
                }
            ),
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource,
                    to_resource_attr: cls.resource,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_n_tile,
                    node_attr: cls.ri_node_n,
                }
            ),
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource,
                    to_resource_attr: cls.resource,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_1_tile,
                    node_attr: cls.ri_list_node_1,
                }
            ),
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource,
                    to_resource_attr: cls.resource,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_n_tile,
                    node_attr: cls.ri_list_node_n,
                }
            ),
        ]
        for rxr in rxrs:
            rxr.created = datetime.datetime.now()
            rxr.modified = datetime.datetime.now()
        ResourceXResource.objects.bulk_create(rxrs)

    @classmethod
    def find_default_widget_id(cls, node, datatypes):
        for datatype in datatypes:
            if node.datatype == datatype.pk:
                return datatype.defaultwidget_id
        return None
