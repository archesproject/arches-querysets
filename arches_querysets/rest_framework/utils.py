from django.db import models

from arches.app.models.models import Node


def get_nodegroup_alias_lookup(graph_slug):
    """Only needed on Arches 7.6, where we lack a grouping_node field."""
    return {
        node.pk: node.alias
        for node in Node.objects.filter(
            pk=models.F("nodegroup_id"),
            graph__slug=graph_slug,
        ).only("alias")
    }
