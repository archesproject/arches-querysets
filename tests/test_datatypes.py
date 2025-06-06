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
        cls.datatype_1 = cls.semantic_resource.aliased_data.datatypes_1
        cls.datatype_n = cls.semantic_resource.aliased_data.datatypes_n

    def test_as_representation_display_values(self):
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

        # The representation is available on the nodegroup .aliased_data.
        for datatype, representation in display_values.items():
            node_alias = datatype.replace("-", "_")
            for aliased_data, cardinality in [
                (self.datatype_1.aliased_data, "1"),
                (self.datatype_n[0].aliased_data, "n"),
            ]:
                with self.subTest(datatype=datatype, cardinality=cardinality):
                    lookup = node_alias if cardinality == "1" else node_alias + "_n"
                    value = getattr(aliased_data, lookup)
                    self.assertEqual(value.get("display_value"), representation)

    def test_as_representation_interchange_values(self):
        # TODO: assert values -- notice concepts still have a Value instance
        interchange_values = {
            # Start with the tile representation.
            **self.sample_data_1,
            # Some interchange values are different.
            # String resolves to active language.
            "string": "forty-two",
            # Resource Instance{list} resolves to stringified UUID. (TODO: unstring?)
            "resource-instance": [
                {
                    "resourceId": str(self.resource.pk),
                    "display_value": self.resource.descriptors["en"]["name"],
                }
            ],
            # Concept{list} resolves to concept value.
            # TODO...
        }
        interchange_values["resource-instance-list"] = interchange_values[
            "resource-instance"
        ]

        # The interchange value is available on the nodegroup .aliased_data.
        for datatype, interchange_value in interchange_values.items():
            node_alias = datatype.replace("-", "_")
            for aliased_data, cardinality in [
                (self.datatype_1.aliased_data, "1"),
                (self.datatype_n[0].aliased_data, "n"),
            ]:
                with self.subTest(datatype=datatype, cardinality=cardinality):
                    lookup = node_alias if cardinality == "1" else node_alias + "_n"
                    value = getattr(aliased_data, lookup)
                    self.assertEqual(value.get("interchange_value"), interchange_value)
