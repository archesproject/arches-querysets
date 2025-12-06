from arches.app.datatypes import datatypes
from django.utils.translation import get_language

class BooleanDataType(datatypes.BooleanDataType):
    def get_display_value(self, tile, node, **kwargs):
        data = self.get_tile_data(tile)
        language = get_language()

        if data:
            trueDisplay = node.config["trueLabel"][language]
            falseDisplay = node.config["falseLabel"][language]
            raw_value = data.get(str(node.nodeid))
            if raw_value is not None:
                return trueDisplay if raw_value else falseDisplay