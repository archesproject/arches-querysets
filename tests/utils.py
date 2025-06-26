import datetime
import uuid

from django.test import TestCase

from arches import VERSION as arches_version
from arches.app.models.graph import Graph
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
        cls.add_default_values_for_widgets()
        cls.create_resources()
        cls.create_tiles_with_data()
        cls.create_tiles_with_none()
        cls.create_relations()
        Graph.objects.get(pk=cls.graph.pk).publish(user=None)

    @classmethod
    def create_graph(cls):
        cls.graph = GraphWithPrefetching.objects.create_graph(
            name="Datatype Lookups", is_resource=True
        )

    @classmethod
    def create_nodegroup(cls, alias, cardinality, parent_nodegroup=None):
        grouping_node = Node(
            graph=cls.graph, alias=alias, istopnode=False, datatype="semantic"
        )
        nodegroup = NodeGroup.objects.create(
            pk=grouping_node.pk,
            cardinality=cardinality,
            parentnodegroup=parent_nodegroup,
        )
        if arches_version >= (8, 0):
            nodegroup.grouping_node = grouping_node
            nodegroup.save()
        grouping_node.nodegroup = nodegroup
        grouping_node.save()
        return nodegroup, grouping_node

    @classmethod
    def create_nodegroups_and_grouping_nodes(cls):
        cls.nodegroup_1, cls.grouping_node_1 = cls.create_nodegroup("datatypes_1", "1")
        cls.nodegroup_n, cls.grouping_node_n = cls.create_nodegroup("datatypes_n", "n")

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
        cls.string_node_1, cls.string_node_n = [
            node for node in cls.nodes if node.datatype == "string"
        ]
        cls.node_value_node_1, cls.node_value_node_n = [
            node for node in cls.nodes if node.datatype == "node-value"
        ]
        cls.url_node_1, cls.url_node_n = [
            node for node in cls.nodes if node.datatype == "url"
        ]
        cls.node_value_node_1.config["nodeid"] = str(cls.date_node_1.pk)
        cls.node_value_node_1.save()
        cls.node_value_node_n.config["nodeid"] = str(cls.date_node_n.pk)
        cls.node_value_node_n.save()

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
    def add_default_values_for_widgets(cls):
        node_widgets = CardXNodeXWidget.objects.filter(
            node__graph=cls.graph,
        )
        cls.default_vals_by_nodeid = {}
        cls.default_vals_by_datatype = {
            "non-localized-string": "The answer to life, the universe, and everything.",
            "string": {
                "en": {
                    "value": "The answer to life, the universe, and everything.",
                    "direction": "ltr",
                }
            },
            "url": {"url": "http://arthurdent.com", "url_label": ""},
            "boolean": False,
            "number": 7,
            "date": "1979-10-12T00:00:00.000-05:00",
            # "resource-instance": None,
            # "resource-instance-list": [],
            # "concept": None,
            # "concept-list": [],
        }
        for widget in node_widgets:
            if widget.node.datatype in cls.default_vals_by_datatype:
                widget.config["defaultValue"] = cls.default_vals_by_datatype[
                    widget.node.datatype
                ]
                cls.default_vals_by_nodeid[str(widget.node.pk)] = (
                    cls.default_vals_by_datatype[widget.node.datatype]
                )
            else:
                cls.default_vals_by_nodeid[str(widget.node.pk)] = None
            if arches_version < (8, 0):
                widget.save()
        if arches_version >= (8, 0):
            CardXNodeXWidget.objects.bulk_update(node_widgets, ["config"])

    @classmethod
    def create_resources(cls):
        cls.resource_42 = ResourceInstance.objects.create(
            graph=cls.graph,
            descriptors={"en": {"name": "Resource referencing 42"}},
            name="Resource referencing 42",
        )
        cls.resource_none = ResourceInstance.objects.create(
            graph=cls.graph,
            descriptors={"en": {"name": "Resource referencing None"}},
            name="Resource referencing None",
        )

    @classmethod
    def create_tiles_with_data(cls):
        ri_dt = cls.datatype_factory.get_instance("resource-instance")
        ri_list_dt = cls.datatype_factory.get_instance("resource-instance-list")

        cls.concept = Concept.objects.get(pk="00000000-0000-0000-0000-000000000001")
        cls.concept_value = cls.concept.value_set.get()

        cls.cardinality_1_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_1,
            resourceinstance=cls.resource_42,
            data={},
        )
        cls.cardinality_n_tile = TileModel.objects.create(
            nodegroup=cls.nodegroup_n,
            resourceinstance=cls.resource_42,
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
            "resource-instance": ri_dt.transform_value_for_tile(cls.resource_42),
            "resource-instance-list": ri_list_dt.transform_value_for_tile(
                cls.resource_42
            ),
            "concept": str(cls.concept_value.pk),
            "concept-list": [str(cls.concept_value.pk)],
            "node-value": str(cls.cardinality_1_tile.pk),
            "file-list": [
                {
                    "url": "http://www.archesproject.org/blog/static/42.png",
                    "name": "42_accessibility_improvements.png",
                    "size": 2042,
                    "type": "image/png",
                    "index": 0,
                    "title": {
                        "en": {
                            "value": "42 Accessibility Improvements",
                            "direction": "ltr",
                        }
                    },
                    "width": 2042,
                    "height": 2042,
                    "status": "added",
                    "altText": {
                        "en": {
                            "value": "Illustration of recent accessibility improvements",
                            "direction": "ltr",
                        }
                    },
                    "content": f"blob:http://localhost:8000/{uuid.uuid4()}",
                    "file_id": str(uuid.uuid4()),
                    "accepted": True,
                    "attribution": {"en": {"value": "Arches", "direction": "ltr"}},
                    "description": {
                        "en": {
                            "value": "Recent versions of arches have 42 improved accessibility characteristics.",
                            "direction": "ltr",
                        }
                    },
                    "lastModified": 1723503486969,
                }
            ],
            # TODO: geojson-feature-collection
            # TODO: domain-value{-list}
            # TODO(maybe): edtf
            # TODO(maybe): annotation
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
    def create_tiles_with_none(cls):
        cls.cardinality_1_tile_none = TileModel.objects.create(
            nodegroup=cls.nodegroup_1,
            resourceinstance=cls.resource_none,
            data={},
        )
        cls.cardinality_n_tile_none = TileModel.objects.create(
            nodegroup=cls.nodegroup_n,
            resourceinstance=cls.resource_none,
            data={},
        )

    @classmethod
    def create_relations(cls):
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
                    from_resource_attr: cls.resource_42,
                    to_resource_attr: cls.resource_42,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_1_tile,
                    node_attr: cls.ri_node_1,
                }
            ),
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource_42,
                    to_resource_attr: cls.resource_42,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_n_tile,
                    node_attr: cls.ri_node_n,
                }
            ),
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource_42,
                    to_resource_attr: cls.resource_42,
                    from_graph_attr: cls.graph,
                    to_graph_attr: cls.graph,
                    tile_attr: cls.cardinality_1_tile,
                    node_attr: cls.ri_list_node_1,
                }
            ),
            ResourceXResource(
                **{
                    from_resource_attr: cls.resource_42,
                    to_resource_attr: cls.resource_42,
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
