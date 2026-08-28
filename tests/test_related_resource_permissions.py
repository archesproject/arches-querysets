"""Related resources named by resource-instance{-list} nodes -- e.g. their
display name -- must not leak to a user who isn't permitted to view them.
See arches_querysets.datatypes.resource_types.ResourceInstanceDataType.

python manage.py test tests.test_related_resource_permissions --settings="tests.test_settings"
"""

import unittest
import uuid
from contextlib import contextmanager

from django.contrib.auth.models import User
from django.test import TestCase

from arches import __version__ as _arches_version_str
from packaging.version import Version

arches_version = Version(_arches_version_str)
from arches.app.models.models import (
    GraphModel,
    Node,
    NodeGroup,
    ResourceInstance,
    ResourceXResource,
    TileModel,
)
from arches.app.models.system_settings import settings as system_settings
from arches.app.utils import permission_backend
from arches.app.utils.permission_backend import assign_perm

from arches_querysets.models import TileTree


@contextmanager
def _permission_framework(dotted_path):
    """django.test.override_settings doesn't reach here: permission_backend
    reads arches.app.models.system_settings.settings, a LazySettings instance
    wrapping its own separate copy of Django's settings, so PERMISSION_FRAMEWORK
    has to be patched directly, alongside permission_backend's own memoized
    framework instance."""
    original_setting = system_settings.PERMISSION_FRAMEWORK
    original_cached_framework = permission_backend._PERMISSION_FRAMEWORK
    system_settings.PERMISSION_FRAMEWORK = dotted_path
    permission_backend._PERMISSION_FRAMEWORK = None
    try:
        yield
    finally:
        system_settings.PERMISSION_FRAMEWORK = original_setting
        permission_backend._PERMISSION_FRAMEWORK = original_cached_framework


def _make_nodegroup_and_node(graph, alias, datatype):
    nodegroup = NodeGroup.objects.create(nodegroupid=uuid.uuid4(), cardinality="1")
    node = Node.objects.create(
        nodeid=uuid.uuid4(),
        name=alias,
        alias=alias,
        datatype=datatype,
        graph=graph,
        nodegroup=nodegroup,
        istopnode=True,
    )
    return nodegroup, node


def _tile(nodegroup, resource, data):
    return TileModel.objects.create(
        tileid=uuid.uuid4(),
        nodegroup=nodegroup,
        resourceinstance=resource,
        data=data,
        provisionaledits=None,
    )


def _related_resource_link(target):
    return {
        "resourceId": str(target.pk),
        "ontologyProperty": "",
        "inverseOntologyProperty": "",
        "resourceXresourceId": str(uuid.uuid4()),
    }


def _link_resources(from_resource, to_resource):
    # arches_version==9.0.0
    if arches_version < Version("8.0"):
        from_resource_attr = "resourceinstanceidfrom"
        to_resource_attr = "resourceinstanceidto"
    else:
        from_resource_attr = "from_resource"
        to_resource_attr = "to_resource"
    return ResourceXResource.objects.create(
        resourcexid=uuid.uuid4(),
        **{from_resource_attr: from_resource, to_resource_attr: to_resource},
    )


class RelatedResourcePermissionFilteringTests(TestCase):
    def setUp(self):
        cm = _permission_framework(
            "arches_default_deny.ArchesDefaultDenyPermissionFramework"
        )
        cm.__enter__()
        self.addCleanup(cm.__exit__, None, None, None)

    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_user(
            username="related_resource_permissions_user", password="password123"
        )

        cls.graph = GraphModel.objects.create(
            graphid=uuid.uuid4(),
            slug="related-resource-permissions-graph",
            isresource=True,
        )
        # arches_version==9.0.0
        if arches_version >= Version("8.0"):
            cls.graph.is_active = True
            cls.graph.save()

        cls.related_list_nodegroup, cls.related_list_node = _make_nodegroup_and_node(
            cls.graph, "related_list", "resource-instance-list"
        )
        cls.related_single_nodegroup, cls.related_single_node = (
            _make_nodegroup_and_node(cls.graph, "related_single", "resource-instance")
        )

        cls.resource = ResourceInstance.objects.create(
            resourceinstanceid=uuid.uuid4(), graph=cls.graph
        )
        cls.readable_target = ResourceInstance.objects.create(
            resourceinstanceid=uuid.uuid4(),
            graph=cls.graph,
            descriptors={
                "en": {"name": "Readable Target", "description": "", "map_popup": ""}
            },
        )
        cls.unreadable_target = ResourceInstance.objects.create(
            resourceinstanceid=uuid.uuid4(),
            graph=cls.graph,
            descriptors={
                "en": {"name": "Secret Target", "description": "", "map_popup": ""}
            },
        )
        # Never backed by a ResourceInstance: a genuinely broken relation,
        # which must stay distinguishable from a permission-denied one.
        cls.missing_target_id = uuid.uuid4()

        assign_perm("view_resourceinstance", cls.user, cls.resource)
        assign_perm("view_resourceinstance", cls.user, cls.readable_target)
        # unreadable_target deliberately has no view_resourceinstance grant.

        _tile(
            cls.related_list_nodegroup,
            cls.resource,
            {
                str(cls.related_list_node.nodeid): [
                    _related_resource_link(cls.readable_target),
                    _related_resource_link(cls.unreadable_target),
                    {
                        "resourceId": str(cls.missing_target_id),
                        "ontologyProperty": "",
                        "inverseOntologyProperty": "",
                        "resourceXresourceId": str(uuid.uuid4()),
                    },
                ]
            },
        )
        _tile(
            cls.related_single_nodegroup,
            cls.resource,
            {
                str(cls.related_single_node.nodeid): [
                    _related_resource_link(cls.unreadable_target)
                ]
            },
        )
        for target in (cls.readable_target, cls.unreadable_target):
            _link_resources(cls.resource, target)

    def _get_value(self, node, *, user):
        tiles = TileTree.objects.get_tiles(
            graph_slug=self.graph.slug,
            nodegroup_alias=node.alias,
            resource_ids=[self.resource.pk],
            nodes=[node],
            depth=0,
            as_representation=True,
            user=user,
        )
        return getattr(tiles.get().aliased_data, node.alias)

    @unittest.skipIf(
        arches_version < Version("8.2.0a9"),
        reason="permission_backend.filter_resource_queryset() unavailable before this version",
    )
    def test_list_datatype_omits_unpermitted_related_resource(self):
        value = self._get_value(self.related_list_node, user=self.user)

        detail_ids = {d["resource_id"] for d in value["details"]}
        self.assertIn(str(self.readable_target.pk), detail_ids)
        self.assertNotIn(str(self.unreadable_target.pk), detail_ids)

        self.assertIn("Readable Target", value["display_value"])
        self.assertNotIn("Secret Target", value["display_value"])

        node_value_ids = {v["resourceId"] for v in value["node_value"]}
        self.assertIn(str(self.readable_target.pk), node_value_ids)
        self.assertNotIn(str(self.unreadable_target.pk), node_value_ids)

    def test_list_datatype_still_reports_genuinely_missing_relation(self):
        """A broken relation (no resolvable target at all) must stay visible
        as "Missing", unlike a permission-denied one, which is fully omitted."""
        value = self._get_value(self.related_list_node, user=self.user)
        missing_detail = next(
            d
            for d in value["details"]
            if d["resource_id"] == str(self.missing_target_id)
        )
        self.assertEqual(missing_detail["display_value"], "Missing")

    @unittest.skipIf(
        arches_version < Version("8.2.0a9"),
        reason="permission_backend.filter_resource_queryset() unavailable before this version",
    )
    def test_single_datatype_omits_unpermitted_related_resource(self):
        value = self._get_value(self.related_single_node, user=self.user)
        self.assertEqual(value["details"], [])
        self.assertEqual(value["display_value"], "")
        self.assertEqual(value["node_value"], [])

    def test_no_user_applies_no_filtering(self):
        value = self._get_value(self.related_single_node, user=None)
        self.assertEqual(
            value["details"][0]["resource_id"], str(self.unreadable_target.pk)
        )


class DefaultAllowFrameworkFallbackTests(TestCase):
    """The default-allow permission framework's filter_resource_queryset()
    always raises (it doesn't support per-object filtering). Passing a user
    under that framework must fall back to unfiltered results, not crash."""

    def setUp(self):
        cm = _permission_framework(
            "arches_default_allow.ArchesDefaultAllowPermissionFramework"
        )
        cm.__enter__()
        self.addCleanup(cm.__exit__, None, None, None)

    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_user(
            username="default_allow_fallback_user", password="password123"
        )
        cls.graph = GraphModel.objects.create(
            graphid=uuid.uuid4(),
            slug="default-allow-fallback-graph",
            isresource=True,
        )
        # arches_version==9.0.0
        if arches_version >= Version("8.0"):
            cls.graph.is_active = True
            cls.graph.save()
        cls.related_nodegroup, cls.related_node = _make_nodegroup_and_node(
            cls.graph, "related", "resource-instance"
        )
        cls.resource = ResourceInstance.objects.create(
            resourceinstanceid=uuid.uuid4(), graph=cls.graph
        )
        cls.target = ResourceInstance.objects.create(
            resourceinstanceid=uuid.uuid4(),
            graph=cls.graph,
            descriptors={
                "en": {"name": "Some Target", "description": "", "map_popup": ""}
            },
        )
        _tile(
            cls.related_nodegroup,
            cls.resource,
            {str(cls.related_node.nodeid): [_related_resource_link(cls.target)]},
        )
        _link_resources(cls.resource, cls.target)

    def test_user_provided_does_not_crash_and_applies_no_filtering(self):
        tiles = TileTree.objects.get_tiles(
            graph_slug=self.graph.slug,
            nodegroup_alias=self.related_node.alias,
            resource_ids=[self.resource.pk],
            nodes=[self.related_node],
            depth=0,
            as_representation=True,
            user=self.user,
        )
        value = getattr(tiles.get().aliased_data, self.related_node.alias)
        self.assertEqual(value["details"][0]["resource_id"], str(self.target.pk))
