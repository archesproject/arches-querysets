import unittest
from http import HTTPStatus

from django.core.management import call_command
from django.urls import reverse
from arches import VERSION as arches_version
from arches.app.models.graph import Graph
from arches.app.models.models import EditLog

from arches_querysets.rest_framework.serializers import (
    ArchesResourceSerializer,
    ArchesResourceTopNodegroupsSerializer,
    ArchesSingleNodegroupSerializer,
    ArchesTileSerializer,
)
from arches_querysets.utils.tests import GraphTestCase


class RestFrameworkTests(GraphTestCase):
    test_child_nodegroups = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        call_command("add_test_users", verbosity=0)
        # Address flakiness.
        cls.resource_42.graph_publication = cls.resource_42.graph.publication
        cls.resource_42.save()

    def test_create_tile_for_new_resource(self):
        create_url = reverse(
            "arches_querysets:api-tiles",
            kwargs={"graph": "datatype_lookups", "nodegroup_alias": "datatypes_n"},
        )
        request_body = {"aliased_data": {"string_n": "create_value"}}

        # Anonymous user lacks editing permissions.
        with self.assertLogs("django.request", level="WARNING"):
            forbidden_response = self.client.post(
                create_url, request_body, content_type="application/json"
            )
            self.assertEqual(forbidden_response.status_code, HTTPStatus.FORBIDDEN)

        # Dev user can edit.
        self.client.login(username="dev", password="dev")
        response = self.client.post(
            create_url, request_body, content_type="application/json"
        )

        # The response includes the context.
        self.assertEqual(response.status_code, HTTPStatus.CREATED)
        self.assertIn("aliased_data", response.json())
        self.assertEqual(
            response.json()["aliased_data"]["string_n"],
            {
                "display_value": "create_value",
                "node_value": {
                    "en": {"value": "create_value", "direction": "ltr"},
                },
                "details": [],
            },
        )
        self.assertEqual(response.status_code, HTTPStatus.CREATED, response.content)

        self.assertSequenceEqual(
            EditLog.objects.filter(
                resourceinstanceid=response.json()["resourceinstance"],
            )
            .values_list("edittype", flat=True)
            .order_by("edittype"),
            ["create", "tile create"],
        )

    def test_create_tile_for_existing_resource(self):
        create_url = reverse(
            "arches_querysets:api-tiles",
            kwargs={"graph": "datatype_lookups", "nodegroup_alias": "datatypes_n"},
        )
        request_body = {
            "aliased_data": {"string_n": "create_value"},
            "resourceinstance": str(self.resource_42.pk),
        }
        self.client.login(username="dev", password="dev")
        response = self.client.post(
            create_url, request_body, content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTPStatus.CREATED)
        self.assertEqual(response.json()["resourceinstance"], str(self.resource_42.pk))
        self.assertEqual(
            response.json()["aliased_data"]["string_n"],
            {
                "display_value": "create_value",
                "node_value": {
                    "en": {"value": "create_value", "direction": "ltr"},
                },
                "details": [],
            },
        )

    @unittest.skipIf(arches_version < (8, 0), reason="Arches 8+ only logic")
    def test_out_of_date_resource(self):
        Graph.objects.get(pk=self.graph.pk).publish(user=None)

        update_url = reverse(
            "arches_querysets:api-resource",
            kwargs={"graph": "datatype_lookups", "pk": str(self.resource_42.pk)},
        )
        self.client.login(username="dev", password="dev")
        request_body = {"aliased_data": {"datatypes_1": None}}
        with self.assertLogs("django.request", level="WARNING"):
            response = self.client.put(
                update_url, request_body, content_type="application/json"
            )
        self.assertContains(
            response,
            "Graph Has Different Publication",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    def test_instantiate_empty_resource_serializer(self):
        serializer = ArchesResourceSerializer(graph_slug="datatype_lookups")
        self.assertIsNone(serializer.data["resourceinstanceid"])
        # Default values are stocked.
        self.assertEqual(
            serializer.data["aliased_data"]["datatypes_1"]["aliased_data"]["number"][
                "node_value"
            ],
            7,
        )

    def test_instantiate_empty_tile_serializer(self):
        serializer = ArchesTileSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        self.assertIsNone(serializer.data["tileid"])
        # Default values are stocked.
        self.assertEqual(serializer.data["aliased_data"]["number"]["node_value"], 7)

    def test_exclude_children_option(self):
        serializer = ArchesResourceSerializer(graph_slug="datatype_lookups")
        self.assertIn(
            "datatypes_1_child",
            serializer.data["aliased_data"]["datatypes_1"]["aliased_data"],
        )
        serializer = ArchesResourceTopNodegroupsSerializer(
            graph_slug="datatype_lookups"
        )
        self.assertNotIn(
            "datatypes_1_child",
            serializer.data["aliased_data"]["datatypes_1"]["aliased_data"],
        )
        serializer = ArchesTileSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        self.assertIn("datatypes_1_child", serializer.data["aliased_data"])
        serializer = ArchesSingleNodegroupSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        self.assertNotIn("datatypes_1_child", serializer.data["aliased_data"])

    def test_blank_views_exclude_children_option(self):
        response = self.client.get(
            reverse(
                "arches_querysets:api-resource-blank",
                kwargs={"graph": "datatype_lookups"},
            )
        )
        self.assertContains(response, "datatypes_1_child")

        response = self.client.get(
            reverse(
                "arches_querysets:api-resource-blank",
                kwargs={"graph": "datatype_lookups"},
            ),
            QUERY_STRING="exclude_children=true",
        )
        self.assertNotContains(response, "datatypes_1_child")

        response = self.client.get(
            reverse(
                "arches_querysets:api-tile-blank",
                kwargs={"graph": "datatype_lookups", "nodegroup_alias": "datatypes_1"},
            )
        )
        self.assertContains(response, "datatypes_1_child")

        response = self.client.get(
            reverse(
                "arches_querysets:api-tile-blank",
                kwargs={
                    "graph": "datatype_lookups",
                    "nodegroup_alias": "datatypes_1",
                },
            ),
            QUERY_STRING="exclude_children=true",
        )
        self.assertNotContains(response, "datatypes_1_child")

    def test_filter_kwargs(self):
        node_alias = "string"

        response = self.client.get(
            reverse(
                "arches_querysets:api-resources",
                kwargs={"graph": "datatype_lookups"},
            ),
            # Additional lookups tested in test_lookups.py
            QUERY_STRING=f"aliased_data__{node_alias}__any_lang_icontains=forty",
        )
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(
            response.json()["results"][0]["resourceinstanceid"],
            str(self.resource_42.pk),
        )

        response = self.client.get(
            reverse(
                "arches_querysets:api-tiles",
                kwargs={"graph": "datatype_lookups", "nodegroup_alias": "datatypes_1"},
            ),
            QUERY_STRING=f"aliased_data__{node_alias}__any_lang_icontains=forty",
        )
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(
            response.json()["results"][0]["resourceinstance"], str(self.resource_42.pk)
        )
