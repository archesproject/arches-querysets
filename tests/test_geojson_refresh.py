"""On save, geojson refresh runs once per changed tile (super().after_update_all(tile))
-- never the whole-DB refresh (after_update_all() with no tile)."""

from unittest import mock

from arches.app.datatypes.core import geojson_feature_collection
from arches.app.models.models import Node

from arches_querysets.datatypes.datatypes import DataTypeFactory
from arches_querysets.models import ResourceTileTree
from arches_querysets.utils.tests import GraphTestCase


def _refreshed_tiles(refresh_mock):
    """The tile passed to each super().after_update_all(tile) call (None = whole-DB)."""
    return [c.args[0] for c in refresh_mock.call_args_list]


class GeojsonAfterUpdateAllTests(GraphTestCase):
    def setUp(self):
        # Drop datatypes that don't round-trip equal to their stored value (file-list
        # transforms files; arches < 8.0 reads our date back as an ISO timestamp),
        # else an unchanged tile looks dirty and gets refreshed.
        Node.objects.filter(
            graph=self.graph, datatype__in={"file-list", "date"}
        ).delete()
        self.resource = ResourceTileTree.get_tiles("datatype_lookups").get(
            pk=self.resource_42.pk
        )
        self.geojson_nodegroup_ids = {self.nodegroup_1.pk, self.nodegroup_n.pk}

    def test_uses_per_tile_refresh_not_whole_table(self):
        # inherits arches' geojson datatype -- not the bare upstream class
        # this was a bug earlier I believe
        factory_cls = type(DataTypeFactory().get_instance("geojson-feature-collection"))
        base = geojson_feature_collection.GeojsonFeatureCollectionDataType
        self.assertTrue(issubclass(factory_cls, base))
        self.assertIsNot(factory_cls, base)

        self.resource.aliased_data.datatypes_1.aliased_data.non_localized_string_alias = (
            "changed-1"
        )

        with mock.patch.object(
            geojson_feature_collection.GeojsonFeatureCollectionDataType,
            "after_update_all",
        ) as refresh:
            self.resource.save(force_admin=True)

        tiles = _refreshed_tiles(refresh)
        self.assertNotIn(None, tiles)
        self.assertEqual(len(tiles), 1)
        self.assertIn(tiles[0].nodegroup_id, self.geojson_nodegroup_ids)

    def test_each_changed_tile_refreshed_once(self):
        self.resource.aliased_data.datatypes_1.aliased_data.non_localized_string_alias = (
            "changed-1"
        )
        self.resource.aliased_data.datatypes_n[
            0
        ].aliased_data.non_localized_string_alias_n = "changed-n"

        with mock.patch.object(
            geojson_feature_collection.GeojsonFeatureCollectionDataType,
            "after_update_all",
        ) as refresh:
            self.resource.save(force_admin=True)

        tiles = _refreshed_tiles(refresh)
        self.assertNotIn(None, tiles)
        # Two distinct tiles, one targeted refresh each, one per geojson nodegroup.
        self.assertEqual(len(tiles), 2)
        self.assertEqual({t.nodegroup_id for t in tiles}, self.geojson_nodegroup_ids)
