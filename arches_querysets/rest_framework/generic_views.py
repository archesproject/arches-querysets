from rest_framework.generics import ListCreateAPIView, RetrieveUpdateDestroyAPIView

from arches_querysets.rest_framework.permissions import ReadOnly, ResourceEditor
from arches_querysets.rest_framework.serializers import (
    ArchesResourceSerializer,
    ArchesTileSerializer,
)
from arches_querysets.rest_framework.view_mixins import ArchesModelAPIMixin


class ArchesResourceListCreateView(ArchesModelAPIMixin, ListCreateAPIView):
    permission_classes = [ResourceEditor | ReadOnly]
    serializer_class = ArchesResourceSerializer
    parser_classes = [
        "rest_framework.parsers.JSONParser",
        "arches_querysets.rest_framework.multipart_json_parser.MultiPartJSONParser",
    ]


class ArchesResourceDetailView(ArchesModelAPIMixin, RetrieveUpdateDestroyAPIView):
    permission_classes = [ResourceEditor | ReadOnly]
    serializer_class = ArchesResourceSerializer
    parser_classes = [
        "rest_framework.parsers.JSONParser",
        "arches_querysets.rest_framework.multipart_json_parser.MultiPartJSONParser",
    ]


class ArchesTileListCreateView(ArchesModelAPIMixin, ListCreateAPIView):
    permission_classes = [ResourceEditor | ReadOnly]
    serializer_class = ArchesTileSerializer
    parser_classes = [
        "rest_framework.parsers.JSONParser",
        "arches_querysets.rest_framework.multipart_json_parser.MultiPartJSONParser",
    ]


class ArchesTileDetailView(ArchesModelAPIMixin, RetrieveUpdateDestroyAPIView):
    permission_classes = [ResourceEditor | ReadOnly]
    serializer_class = ArchesTileSerializer
    parser_classes = [
        "rest_framework.parsers.JSONParser",
        "arches_querysets.rest_framework.multipart_json_parser.MultiPartJSONParser",
    ]
