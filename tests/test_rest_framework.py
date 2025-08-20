import unittest
from http import HTTPStatus
from unittest.mock import patch

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
from arches_querysets.utils.models import ensure_request
from arches_querysets.utils.tests import GraphTestCase


MUTABLE_PERMITTED_NODEGROUPS = set()


class RestFrameworkTests(GraphTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        call_command("add_test_users", verbosity=0)

    def patched_ensure_request(self, request, force_admin):
        request.user.userprofile.viewable_nodegroups = {str(self.nodegroup_id)}
        self.set_single_viewable_nodegroup(request, self.nodegroup_1.pk)
        return ensure_request(request, force_admin)

    def test_create_tile_for_new_resource(self):
        create_url = reverse(
            "arches_querysets:api-tiles",
            kwargs={"graph": "datatype_lookups", "nodegroup_alias": "datatypes_n"},
        )
        request_body = {"aliased_data": {"string_alias_n": "create_value"}}

        # Anonymous user lacks editing permissions.
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
            response.json()["aliased_data"]["string_alias_n"],
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
            "aliased_data": {"string_alias_n": "create_value"},
            "resourceinstance": str(self.resource_42.pk),
        }
        self.client.login(username="dev", password="dev")
        response = self.client.post(
            create_url, request_body, content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTPStatus.CREATED)
        self.assertEqual(response.json()["resourceinstance"], str(self.resource_42.pk))
        self.assertEqual(
            response.json()["aliased_data"]["string_alias_n"],
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
            serializer.data["aliased_data"]["datatypes_1"]["aliased_data"][
                "number_alias"
            ]["node_value"],
            7,
        )

    def test_instantiate_empty_tile_serializer(self):
        serializer = ArchesTileSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        self.assertIsNone(serializer.data["tileid"])
        # Default values are stocked.
        self.assertEqual(
            serializer.data["aliased_data"]["number_alias"]["node_value"], 7
        )

    def test_bind_data_to_serializer(self):
        # Get some default data from the serializer.
        static_data = ArchesTileSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        ).data
        # Pretend that data came from somewhere else, and process it, e.g. in a script.
        serializer = ArchesTileSerializer(
            graph_slug="datatype_lookups",
            nodegroup_alias="datatypes_1",
            data=static_data,
        )
        self.assertTrue(serializer.is_valid())
        # serializer.save() etc

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

    @patch(
        "arches.app.models.models.UserProfile.viewable_nodegroups",
        MUTABLE_PERMITTED_NODEGROUPS,
    )
    def test_serializer_observes_nodegroup_permissions(self):
        resource_serializer = ArchesResourceSerializer(graph_slug="datatype_lookups")
        self.assertNotIn("datatypes_1", resource_serializer.data["aliased_data"])

        # A TileSerializer where the topmost nodegroup is not permitted raises
        tile_serializer = ArchesTileSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        with self.assertRaises(PermissionError):
            tile_serializer.data

        # Otherwise we just return whatever part of the tree we can.
        MUTABLE_PERMITTED_NODEGROUPS.add(str(self.nodegroup_1.pk))
        tile_serializer = ArchesTileSerializer(
            graph_slug="datatype_lookups", nodegroup_alias="datatypes_1"
        )
        self.assertIn("number_alias", tile_serializer.data["aliased_data"])
        self.assertNotIn("datatypes_1_child", tile_serializer.data["aliased_data"])

    def test_filter_kwargs(self):
        node_alias = "string_alias"

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

        node_alias = "string_alias_n"
        response = self.client.get(
            reverse(
                "arches_querysets:api-tiles",
                kwargs={"graph": "datatype_lookups", "nodegroup_alias": "datatypes_n"},
            ),
            QUERY_STRING=f"aliased_data__{node_alias}__isnull=true",
        )
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(
            response.json()["results"][0]["resourceinstance"],
            str(self.resource_none.pk),
        )

    def test_bogus_graph_slug(self):
        response = self.client.get(
            reverse("arches_querysets:api-resources", kwargs={"graph": "bogus"})
        )
        self.assertContains(
            response,
            "No nodes found for graph slug",
            status_code=HTTPStatus.BAD_REQUEST,
        )
        response = self.client.get(
            reverse(
                "arches_querysets:api-tiles",
                kwargs={"graph": "bogus", "nodegroup_alias": "bogus"},
            )
        )
        self.assertContains(
            response,
            "No nodes found for graph slug",
            status_code=HTTPStatus.BAD_REQUEST,
        )


class RestFrameworkPerformanceTests(GraphTestCase):
    @patch("arches_querysets.rest_framework.serializers.get_nodegroup_alias_lookup")
    def test_derivation_of_nodegroup_aliases(self, mocked_util):
        """Querying nodegroup aliases should only be done once in the view layer,
        not multiple times when building nested serializers. The serializer layer
        still has fallback code to support scripts, see test_bind_data_to_serializer(),
        but it shouldn't be called when using views.
        """
        self.client.get(
            reverse(
                "arches_querysets:api-resources", kwargs={"graph": "datatype_lookups"}
            )
        )
        mocked_util.assert_not_called()

    def test_resource_list_view_performance(self):
        # 1: auth
        # 2: auth groups
        # 3: node alias lookup in get_tiles()
        # 4-16: PerformanceTests.test_get_graph_objects()
        # 17: resource count (paginator)
        # 18: select resources limit 500
        # 19: tile depth 1
        # 20: resourcexresource depth 1
        # 21: tile depth 2
        # 22: resourcexresource depth 2
        # 23: tile depth 3: none!
        # 24: userprofile
        # 25-29: arches perms (BUG: core arches)
        with self.assertNumQueries(29):
            response = self.client.get(
                reverse(
                    "arches_querysets:api-resources",
                    kwargs={"graph": "datatype_lookups"},
                ),
                # Some datatypes are inefficient in fetching data for display values,
                # e.g. nodes, so make sure we're getting the resource with only Nones
                QUERY_STRING="aliased_data__number_alias__isnull=true",
            )
        self.assertContains(response, "datatypes_1_child", status_code=HTTPStatus.OK)
        self.assertEqual(response.json()["count"], 1)
        top_tile = response.json()["results"][0]["aliased_data"]["datatypes_1"]
        self.assertIsNone(top_tile["aliased_data"]["number_alias"]["node_value"])

    def test_tile_list_view_performance(self):
        # 1: auth
        # 2: auth groups
        # 3: node alias lookup in get_tiles()
        # 4-16: PerformanceTests.test_get_graph_objects()
        # 17: tile count (paginator)
        # 18: select tiles limit 500
        # 19: resourcexresource depth 1
        # 20: tile depth 2
        # 21: resourcexresource depth 2
        # 22: tile depth 3: none!
        # 23: userprofile
        # 24-28: arches perms (BUG: core arches)
        with self.assertNumQueries(28):
            response = self.client.get(
                reverse(
                    "arches_querysets:api-tiles",
                    kwargs={
                        "graph": "datatype_lookups",
                        "nodegroup_alias": "datatypes_1",
                    },
                ),
                # Some datatypes are inefficient in fetching data for display values,
                # e.g. nodes, so make sure we're getting the resource with only Nones
                QUERY_STRING="aliased_data__number_alias__isnull=true",
            )
        self.assertContains(response, "datatypes_1_child", status_code=HTTPStatus.OK)
        self.assertEqual(response.json()["count"], 1)
        top_tile = response.json()["results"][0]
        self.assertIsNone(top_tile["aliased_data"]["number_alias"]["node_value"])

    def test_resource_blank_view_performance(self):
        # 1-7: perms
        # 8: get_nodegroup_alias_lookup()
        # 9-12: NodeFetcherMixin._find_graph_nodes()
        with self.assertNumQueries(12):
            response = self.client.get(
                reverse(
                    "arches_querysets:api-resource-blank",
                    kwargs={"graph": "datatype_lookups"},
                )
            )
        self.assertContains(response, "datatypes_1_child")
