"""Tests for bulk-import permissions (community gate AND records-import action)."""

import pytest
from flask_principal import Identity, RoleNeed, UserNeed
from invenio_access.models import ActionRoles
from invenio_access.permissions import system_identity
from invenio_accounts.proxies import current_accounts
from invenio_communities.generators import CommunityRoleNeed
from invenio_records_resources.services.errors import PermissionDeniedError

from invenio_record_importer_kcworks.permissions import import_records_action
from invenio_record_importer_kcworks.proxies import current_record_importer_service
from invenio_record_importer_kcworks.roles import (
    DEFAULT_BULK_IMPORT_ROLE,
    ensure_bulk_import_role,
)


def test_ensure_bulk_import_role(running_app, db):
    """Ensure creates the role and binds import_records_action; idempotent."""
    name = ensure_bulk_import_role()
    assert name == DEFAULT_BULK_IMPORT_ROLE
    assert ensure_bulk_import_role() == name

    role = current_accounts.datastore.find_role(DEFAULT_BULK_IMPORT_ROLE)
    assert role is not None
    bound = [
        row
        for row in ActionRoles.query_by_action(import_records_action).all()
        if row.role_id == role.id
    ]
    assert len(bound) == 1


def test_import_permission_system_process(running_app, db, minimal_community_factory):
    """System process may import (bypasses community and capability)."""
    community = minimal_community_factory(slug="perm-sys")
    service = current_record_importer_service
    assert service.check_permission(
        system_identity, "import_records", record=community._record
    )


def test_import_permission_administration(running_app, db, minimal_community_factory):
    """Administration access bypasses community and capability gates."""
    community = minimal_community_factory(slug="perm-admin")
    identity = Identity(1)
    identity.provides.add(UserNeed(1))
    identity.provides.add(RoleNeed("administration"))
    service = current_record_importer_service
    assert service.check_permission(
        identity, "import_records", record=community._record
    )


def test_import_permission_owner_with_action(
    running_app, db, minimal_community_factory
):
    """Community owner plus bulk-import role may import."""
    ensure_bulk_import_role()
    community = minimal_community_factory(slug="perm-owner-action")
    identity = Identity(2)
    identity.provides.add(UserNeed(2))
    identity.provides.add(CommunityRoleNeed(str(community.id), "owner"))
    identity.provides.add(RoleNeed(DEFAULT_BULK_IMPORT_ROLE))
    service = current_record_importer_service
    assert service.check_permission(
        identity, "import_records", record=community._record
    )


def test_import_permission_owner_without_action_denied(
    running_app, db, minimal_community_factory
):
    """Community owner alone is not enough without the import action."""
    community = minimal_community_factory(slug="perm-owner-only")
    identity = Identity(3)
    identity.provides.add(UserNeed(3))
    identity.provides.add(CommunityRoleNeed(str(community.id), "owner"))
    service = current_record_importer_service
    with pytest.raises(PermissionDeniedError):
        service.require_permission(
            identity, "import_records", record=community._record
        )


def test_import_permission_action_without_community_denied(
    running_app, db, minimal_community_factory
):
    """Bulk-import role alone is not enough without a community role."""
    ensure_bulk_import_role()
    community = minimal_community_factory(slug="perm-action-only")
    identity = Identity(4)
    identity.provides.add(UserNeed(4))
    identity.provides.add(RoleNeed(DEFAULT_BULK_IMPORT_ROLE))
    service = current_record_importer_service
    with pytest.raises(PermissionDeniedError):
        service.require_permission(
            identity, "import_records", record=community._record
        )


def test_import_permission_denied_plain_user(
    running_app, db, minimal_community_factory
):
    """Authenticated user without community role or action is denied."""
    community = minimal_community_factory(slug="perm-deny")
    identity = Identity(5)
    identity.provides.add(UserNeed(5))
    service = current_record_importer_service
    with pytest.raises(PermissionDeniedError):
        service.require_permission(
            identity, "import_records", record=community._record
        )


def test_import_permission_reader_with_action_denied_under_members_review(
    running_app, db, minimal_community_factory
):
    """Reader + bulk-import role still denied when review_policy is members."""
    ensure_bulk_import_role()
    community = minimal_community_factory(
        slug="perm-reader-members-review",
        access={"review_policy": "members"},
    )
    identity = Identity(6)
    identity.provides.add(UserNeed(6))
    identity.provides.add(CommunityRoleNeed(str(community.id), "reader"))
    identity.provides.add(RoleNeed(DEFAULT_BULK_IMPORT_ROLE))
    service = current_record_importer_service
    with pytest.raises(PermissionDeniedError):
        service.require_permission(
            identity, "import_records", record=community._record
        )
