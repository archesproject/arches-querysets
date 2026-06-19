from arches.app.datatypes import datatypes
from arches.app.models.models import Node


class GeojsonFeatureCollectionDataType(datatypes.GeojsonFeatureCollectionDataType):
    def after_update_all(self, tile=None, changed_tiles=None):
        if changed_tiles is None:
            return super().after_update_all(tile)
        geojson_nodegroup_ids = set(
            Node.objects.filter(
                nodegroup_id__in={t.nodegroup_id for t in changed_tiles},
                datatype="geojson-feature-collection",
            ).values_list("nodegroup_id", flat=True)
        )
        for tile in changed_tiles:
            if tile.nodegroup_id in geojson_nodegroup_ids:
                super().after_update_all(tile)
