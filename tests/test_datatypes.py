import json

from arches_querysets.models import SemanticResource
from tests.utils import GraphTestCase


class DatatypeTests(GraphTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        resources = SemanticResource.as_model(
            "datatype_lookups", as_representation=True
        )
        cls.semantic_resource = resources.get(pk=cls.resource.pk)

    def test_as_representation(self):
        datatype_1 = self.semantic_resource.aliased_data.datatypes_1
        datatype_n = self.semantic_resource.aliased_data.datatypes_n

        display_values = {
            # Start with the tile representation.
            **self.sample_data_1,
            # Some representations are different.
            # Boolean resolves to a string.
            "boolean": str(self.sample_data_1["boolean"]),
            # Number resolves to a string. TODO: localize?
            "number": str(self.sample_data_1["number"]),
            # String resolves to active language.
            "string": "forty-two",
            # Resource Instance{list} resolves to localized name.
            "resource-instance": "Test Resource",
            "resource-instance-list": "Test Resource",
            # Concept{list} resolves to concept value.
            "concept": "Arches",
            "concept-list": "Arches",
            # Node value resolves to node value.
            "node-value": self.sample_data_1["date"],
            # BUG: URL resolves to the entire object.
            "url": json.dumps(self.sample_data_1["url"]),
        }

        # TODO: assert values -- notice concepts still have a Value instance
        # values = {}

        # The representation is available on the nodegroup .aliased_data.
        for datatype, representation in display_values.items():
            node_alias = datatype.replace("-", "_")
            with self.subTest(datatype=datatype):
                aliased_data = getattr(datatype_1.aliased_data, node_alias)
                self.assertEqual(
                    aliased_data.get("display_value"),
                    representation,
                )
                aliased_data_n = getattr(datatype_n[0].aliased_data, node_alias + "_n")
                self.assertEqual(
                    aliased_data_n.get("display_value"),
                    representation,
                )
