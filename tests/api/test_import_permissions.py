"""Tests for bulk-import RBAC (admin or bulk-importer role)."""

from flask_principal import Identity, RoleNeed, UserNeed
from invenio_access.permissions import system_identity
from invenio_administration.permissions import administration_access_action
from invenio_records_resources.services.errors import PermissionDeniedError
import pytest

from invenio_record_importer_kcworks.roles import (
    DEFAULT_BULK_IMPORT_ROLE,
    ensure_bulk_import_role,
)
from invenio_record_importer_kcworks.proxies import current_record_importer_service


def test_ensure_bulk_import_role(running_app, db):
    """Role ensure is idempotent and uses the default name."""
    name = ensure_bulk_import_role(running_app.app)
    assert name == DEFAULT_BULK_IMPORT_ROLE
    assert ensure_bulk_import_role(running_app.app) == name


def test_import_permission_system_process(running_app, db, minimal_community_factory):
    """System process may import."""
    community = minimal_community_factory(slug="perm-sys")
    service = current_record_importer_service
    # Empty metadata still hits permission before work; use require_permission only.
    service.require_permission(
        system_identity, "import_records", record=community._record
    )


def test_import_permission_bulk_role(running_app, db, user_factory, minimal_community_factory):
    """Bulk-importer RoleNeed is sufficient."""
    community = minimal_community_factory(slug="perm-role")
    user = user_factory(email="bulk@example.com").user
    identity = Identity(user.id)
    identity.provides.add(UserNeed(user.id))
    identity.provides.add(RoleNeed(DEFAULT_BULK_IMPORT_ROLE))
    service = current_record_importer_service
    service.require_permission(
        identity, "import_records", record=community._record
    )


def test_import_permission_administration(
    running_app, db, user_factory, minimal_community_factory
):
    """Administration access action is sufficient."""
    community = minimal_community_factory(slug="perm-admin")
    user = user_factory(email="admin-import@example.com").user
    identity = Identity(user.id)
    identity.provides.add(UserNeed(user.id))
    identity.provides.add(administration_access_action)
    service = current_record_importer_service
    service.require_permission(
        identity, "import_records", record=community._record
    )


def test_import_permission_denied_plain_user(
    running_app, db, user_factory, minimal_community_factory
):
    """Authenticated user without role/admin is denied."""
    community = minimal_community_factory(slug="perm-deny")
    user = user_factory(email="nobody@example.com").user
    identity = Identity(user.id)
    identity.provides.add(UserNeed(user.id))
    service = current_record_importer_service
    with pytest.raises(PermissionDeniedError):
        service.require_permission(
            identity, "import_records", record=community._record
        )
