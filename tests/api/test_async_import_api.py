"""Tests for async community-scoped import API (Layer 1: row CRUD + HTTP)."""

import json

import pytest
from flask_principal import Identity, UserNeed
from invenio_communities.generators import CommunityRoleNeed
from invenio_communities.utils import load_community_needs
from invenio_records_resources.services.errors import PermissionDeniedError

from invenio_record_importer_kcworks.errors import (
    ActiveImportExistsError,
    ImportCommunityMismatchError,
    ImportNotFoundError,
)
from invenio_record_importer_kcworks.models import ImportStatusEnum, RecordImport
from invenio_record_importer_kcworks.proxies import current_record_importer_service
from invenio_record_importer_kcworks.roles import DEFAULT_BULK_IMPORT_ROLE


def _owner_identity_with_import(user, community, assign_roles):
    """Owner + bulk-importer identity for service-level import calls.

    Returns:
        Flask-Principal identity with community owner and bulk-import needs.
    """
    _user, identity = assign_roles(user, [DEFAULT_BULK_IMPORT_ROLE])
    identity.provides.add(CommunityRoleNeed(str(community.id), "owner"))
    load_community_needs(identity)
    return identity


def _create_body(metadata=None, **flags):
    """Build a v2 create JSON body (legacy field shape + api_version).

    Returns:
        Dict suitable for `json.dumps` on POST create.
    """
    body = {
        "api_version": 2,
        "metadata": metadata if metadata is not None else [{"title": "One"}],
        "review_required": True,
        "strict_validation": True,
        "all_or_none": True,
        "no_updates": False,
        "notify_record_owners": False,
        "id_scheme": "import-recid",
        "alternate_id_scheme": "",
    }
    body.update(flags)
    return body


# --- Service ---


def test_create_get_list_current_cancel(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """Happy path: create → get → current → list → cancel."""
    u = user_factory(email="async-happy@example.com")
    community = minimal_community_factory(slug="async-happy", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    created = service.create_import(
        identity,
        community["slug"],
        records_metadata=[{"title": "One"}],
        options={"api_version": 2, "all_or_none": True},
    )
    assert created["status"] == "awaiting_files"
    assert created["community_id"] == str(community.id)
    assert created["created_by"] == u.user.id
    assert created["records_metadata"] == [{"title": "One"}]
    assert created["options"]["api_version"] == 2
    import_id = created["id"]

    fetched = service.get_import(identity, community["slug"], import_id)
    assert fetched["id"] == import_id

    current = service.get_current_import(identity, community["slug"])
    assert current["id"] == import_id
    assert current["status"] == "awaiting_files"

    listed = service.list_imports(identity, community["slug"])
    assert listed["total"] == 1
    assert listed["imports"][0]["id"] == import_id

    cancelled = service.cancel_import(identity, community["slug"], import_id)
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancelled_at"] is not None
    assert cancelled["completed_at"] is not None

    current_after = service.get_current_import(identity, community["slug"])
    assert current_after["id"] == import_id
    assert current_after["status"] == "cancelled"


def test_create_import_conflict_when_active(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """Second create while an import is active raises ActiveImportExistsError."""
    u = user_factory(email="async-409@example.com")
    community = minimal_community_factory(slug="async-409", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    first = service.create_import(
        identity,
        str(community.id),
        records_metadata=[],
        options={"api_version": 2},
    )
    with pytest.raises(ActiveImportExistsError) as exc_info:
        service.create_import(
            identity,
            str(community.id),
            records_metadata=[],
            options={"api_version": 2},
        )
    assert exc_info.value.existing_import_id == first["id"]


def test_create_import_denied_without_permission(
    running_app, db, minimal_community_factory, user_factory
):
    """Plain authenticated user cannot create an import."""
    u = user_factory(email="async-deny@example.com")
    community = minimal_community_factory(slug="async-deny", owner=u.user.id)
    identity = Identity(u.user.id)
    identity.provides.add(UserNeed(u.user.id))
    service = current_record_importer_service

    with pytest.raises(PermissionDeniedError):
        service.create_import(
            identity,
            community["slug"],
            records_metadata=[],
            options={"api_version": 2},
        )


def test_get_import_not_found_vs_community_mismatch(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """Missing row and wrong-community row raise distinct errors."""
    u = user_factory(email="async-mismatch@example.com")
    community_a = minimal_community_factory(slug="async-mm-a", owner=u.user.id)
    community_b = minimal_community_factory(slug="async-mm-b", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community_a, assign_roles)
    # Owner of A also needs owner on B for the mismatch call.
    identity.provides.add(CommunityRoleNeed(str(community_b.id), "owner"))
    load_community_needs(identity)
    service = current_record_importer_service

    created = service.create_import(
        identity,
        community_a["slug"],
        records_metadata=[],
        options={"api_version": 2},
    )

    with pytest.raises(ImportNotFoundError):
        service.get_import(
            identity, community_a["slug"], "00000000-0000-0000-0000-000000000000"
        )

    with pytest.raises(ImportCommunityMismatchError):
        service.get_import(identity, community_b["slug"], created["id"])


def test_run_import_stub_queued_to_running(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """`run_import` stub transitions queued → running only."""
    u = user_factory(email="async-run@example.com")
    community = minimal_community_factory(slug="async-run", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    created = service.create_import(
        identity,
        community["slug"],
        records_metadata=[],
        options={"api_version": 2},
    )
    row = db.session.get(RecordImport, created["id"])
    row.status = ImportStatusEnum.QUEUED
    db.session.commit()

    result = service.run_import(identity, created["id"])
    assert result["status"] == "running"


# --- HTTP ---


def test_api_create_get_list_current_cancel(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """HTTP happy path for v2 create and community-scoped routes."""
    app = running_app.app
    u = user_factory(email="async-http@example.com", token=True, oauth_id=None)
    community = minimal_community_factory(slug="async-http", owner=u.user.id)
    assign_roles(u, [DEFAULT_BULK_IMPORT_ROLE])
    token = u.allowed_token
    base = f"{app.config['SITE_API_URL']}/import/{community['slug']}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with app.test_client() as client:
        create_resp = client.post(
            base,
            data=json.dumps(_create_body()),
            headers=headers,
        )
        assert create_resp.status_code == 202, create_resp.get_data(as_text=True)
        created = create_resp.json
        assert created["status"] == "awaiting_files"
        import_id = created["id"]

        get_resp = client.get(f"{base}/{import_id}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json["id"] == import_id

        current_resp = client.get(f"{base}/current", headers=headers)
        assert current_resp.status_code == 200
        assert current_resp.json["id"] == import_id

        list_resp = client.get(base, headers=headers)
        assert list_resp.status_code == 200
        assert list_resp.json["total"] == 1

        cancel_resp = client.delete(f"{base}/{import_id}", headers=headers)
        assert cancel_resp.status_code == 200
        assert cancel_resp.json["status"] == "cancelled"


def test_api_create_conflict_409(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """Second JSON create while active returns 409."""
    app = running_app.app
    u = user_factory(email="async-http-409@example.com", token=True, oauth_id=None)
    community = minimal_community_factory(slug="async-http-409", owner=u.user.id)
    assign_roles(u, [DEFAULT_BULK_IMPORT_ROLE])
    token = u.allowed_token
    base = f"{app.config['SITE_API_URL']}/import/{community['slug']}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = json.dumps(_create_body())

    with app.test_client() as client:
        first = client.post(base, data=body, headers=headers)
        assert first.status_code == 202
        second = client.post(base, data=body, headers=headers)
        assert second.status_code == 409
        assert second.json["status"] == 409
        assert second.json["existing_import_id"] == first.json["id"]


def test_api_create_denied_403(
    running_app, db, minimal_community_factory, user_factory
):
    """Authenticated user without community role / import action gets 403."""
    app = running_app.app
    u = user_factory(email="async-http-deny@example.com", token=True, oauth_id=None)
    community = minimal_community_factory(slug="async-http-deny", owner=u.user.id)
    token = u.allowed_token
    base = f"{app.config['SITE_API_URL']}/import/{community['slug']}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with app.test_client() as client:
        resp = client.post(
            base,
            data=json.dumps(_create_body()),
            headers=headers,
        )
        assert resp.status_code == 403


def test_api_cancel_current(
    running_app, db, minimal_community_factory, user_factory, assign_roles
):
    """DELETE /current cancels the active import."""
    app = running_app.app
    u = user_factory(email="async-http-cur@example.com", token=True, oauth_id=None)
    community = minimal_community_factory(slug="async-http-cur", owner=u.user.id)
    assign_roles(u, [DEFAULT_BULK_IMPORT_ROLE])
    token = u.allowed_token
    base = f"{app.config['SITE_API_URL']}/import/{community['slug']}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with app.test_client() as client:
        created = client.post(base, data=json.dumps(_create_body()), headers=headers)
        assert created.status_code == 202
        cancel = client.delete(f"{base}/current", headers=headers)
        assert cancel.status_code == 200
        assert cancel.json["status"] == "cancelled"
        assert cancel.json["id"] == created.json["id"]
