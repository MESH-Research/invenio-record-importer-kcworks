"""Tests for the `RecordImport` database model."""

import uuid
from datetime import UTC, datetime

import pytest
from invenio_accounts.proxies import current_accounts

from invenio_record_importer_kcworks.models import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    ImportStatusEnum,
    RecordImport,
)


def _default_owner_id() -> int:
    """User id for the community factory's default owner.

    Returns:
        The datastore user id for `myuser@inveniosoftware.org`.
    """
    user = current_accounts.datastore.get_user("myuser@inveniosoftware.org")
    assert user is not None
    return user.id


def test_record_imports_table_registered(running_app, db):
    """Model entry point registers `record_imports` on SQLAlchemy metadata."""
    assert "record_imports" in db.metadata.tables


def test_create_persists_and_round_trips(running_app, db, minimal_community_factory):
    """`RecordImport.create` persists JSON fields and reloads cleanly."""
    community = minimal_community_factory(slug="import-model-create")
    created_by = _default_owner_id()
    metadata = [{"title": "One", "files": {"entries": {}}}]
    options = {"api_version": 2, "all_or_none": True}
    manifest = [{"filename": "one.pdf", "status": "pending"}]

    row = RecordImport.create(
        community_id=community.id,
        created_by=created_by,
        records_metadata=metadata,
        options=options,
        staging_manifest=manifest,
    )
    db.session.commit()

    loaded = db.session.get(RecordImport, row.id)
    assert loaded is not None
    assert loaded.community_id == uuid.UUID(str(community.id))
    assert loaded.created_by == created_by
    assert loaded.status == ImportStatusEnum.STAGED
    assert loaded.records_metadata == metadata
    assert loaded.options == options
    assert loaded.staging_manifest == manifest
    assert loaded.progress == {}
    assert loaded.result is None
    assert loaded.task_id is None
    assert loaded.is_active is True
    assert loaded.is_terminal is False

    payload = loaded.to_dict()
    assert payload["id"] == str(loaded.id)
    assert payload["community_id"] == str(community.id)
    assert payload["created_by"] == created_by
    assert payload["status"] == "staged"
    assert payload["records_metadata"] == metadata
    assert payload["options"] == options
    assert payload["staging_manifest"] == manifest
    assert payload["progress"] == {}
    assert payload["result"] is None
    assert payload["task_id"] is None
    assert payload["cancelled_at"] is None
    assert payload["completed_at"] is None
    assert payload["created"] is not None
    assert payload["updated"] is not None


@pytest.mark.parametrize("status", sorted(ACTIVE_STATUSES, key=lambda s: s.value))
def test_active_statuses(running_app, db, minimal_community_factory, status):
    """Active statuses report `is_active` and not terminal."""
    community = minimal_community_factory(slug=f"import-active-{status.value}")
    row = RecordImport.create(
        community_id=community.id,
        created_by=_default_owner_id(),
        status=status,
    )
    db.session.commit()
    assert row.is_active is True
    assert row.is_terminal is False


@pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES, key=lambda s: s.value))
def test_terminal_statuses(running_app, db, minimal_community_factory, status):
    """Terminal statuses report `is_terminal` and not active."""
    community = minimal_community_factory(slug=f"import-terminal-{status.value}")
    row = RecordImport.create(
        community_id=community.id,
        created_by=_default_owner_id(),
        status=status,
    )
    db.session.commit()
    assert row.is_active is False
    assert row.is_terminal is True


def test_cancel_and_completed_fields_serialize(
    running_app, db, minimal_community_factory
):
    """Optional cancel/completed timestamps round-trip through `to_dict`."""
    community = minimal_community_factory(slug="import-model-cancelled")
    row = RecordImport.create(
        community_id=community.id,
        created_by=_default_owner_id(),
        status=ImportStatusEnum.CANCELLED,
    )
    now = datetime.now(UTC)
    row.cancelled_at = now
    row.completed_at = now
    row.task_id = uuid.uuid4()
    row.result = {"status": "cancelled", "message": "stopped"}
    db.session.commit()

    payload = row.to_dict()
    assert payload["status"] == "cancelled"
    assert payload["cancelled_at"] == now.isoformat()
    assert payload["completed_at"] == now.isoformat()
    assert payload["task_id"] == str(row.task_id)
    assert payload["result"] == {"status": "cancelled", "message": "stopped"}
