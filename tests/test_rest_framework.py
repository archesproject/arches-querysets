from http import HTTPStatus

from django.core.management import call_command
from django.urls import reverse

from tests.utils import GraphTestCase


class RestFrameworkTests(GraphTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        call_command("add_test_users", verbosity=0)

    def test_create_tile_existing_resource(self):
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

        # Dev user can edit ...
        self.client.login(username="dev", password="dev")
        with self.assertLogs("django.request", level="WARNING"):
            response = self.client.post(
                create_url, request_body, content_type="application/json"
            )

        self.assertJSONEqual(
            response.content,
            {"resourceinstance": ["This field cannot be null."]},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST, response.content)

        # ... if a resource is specified.
        request_body["resourceinstance"] = str(self.resource_42.pk)
        response = self.client.post(
            create_url, request_body, content_type="application/json"
        )

        # The response includes the context.
        self.assertEqual(
            response.json()["aliased_data"]["string_n"],
            {
                "display_value": "create_value",
                "interchange_value": {
                    "en": {"value": "create_value", "direction": "ltr"},
                },
            },
        )
        self.assertEqual(response.status_code, HTTPStatus.CREATED, response.content)
