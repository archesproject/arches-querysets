import unittest
from http import HTTPStatus

from django.core.management import call_command
from django.urls import reverse
from arches import VERSION as arches_version
from arches.app.models.graph import Graph
from arches.app.models.models import EditLog

from arches_querysets.utils.tests import GraphTestCase


class RestFrameworkTests(GraphTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        call_command("add_test_users", verbosity=0)
        # Address flakiness.
        cls.resource_42.graph_publication = cls.resource_42.graph.publication
        cls.resource_42.save()

    def test_create_tile_for_new_resource(self):
        create_url = reverse(
            "api-tiles",
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
            "api-tiles",
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
            "api-resource",
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
