"""Tests for Layer 2 staging MPU / commit on async imports."""

import json
import uuid
from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from marshmallow import ValidationError

from invenio_record_importer_kcworks.errors import (
    ImportStagingIncompleteError,
)
from invenio_record_importer_kcworks.proxies import current_record_importer_service
from invenio_record_importer_kcworks.roles import DEFAULT_BULK_IMPORT_ROLE
from invenio_record_importer_kcworks.services import staging as staging_helper

# MD5 of empty bytes — valid shape for Content-MD5 tests (not content-accurate).
_TEST_MD5_HEX = "d41d8cd98f00b204e9800998ecf8427e"
_TEST_CHECKSUM = f"md5:{_TEST_MD5_HEX}"


def _minimal_rdm_record(**overrides):
    """Minimal valid RDM record dict for create-payload validation.

    Returns:
        Deep-copied record metadata suitable for `metadata[]`.
    """
    record = {
        "pids": {},
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": False},
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": "Brown",
                        "given_name": "Troy",
                        "type": "personal",
                    }
                }
            ],
            "publication_date": "2020-06-01",
            "publisher": "Acme Inc",
            "resource_type": {"id": "image-photograph"},
            "title": "A Romans story",
        },
    }
    record.update(overrides)
    return deepcopy(record)


def _owner_identity_with_import(user, community, assign_roles):
    """Owner + bulk-importer identity for service-level import calls.

    Returns:
        Flask-Principal identity with community owner and bulk-import needs.
    """
    from invenio_communities.generators import CommunityRoleNeed
    from invenio_communities.utils import load_community_needs

    _user, identity = assign_roles(user, [DEFAULT_BULK_IMPORT_ROLE])
    identity.provides.add(CommunityRoleNeed(str(community.id), "owner"))
    load_community_needs(identity)
    return identity


@pytest.fixture
def staging_mocks(monkeypatch, running_app):
    """Stub S3 staging helpers; set a staging bucket on the app.

    Returns:
        Dict with `delay` mock and `task_id` string used by commit.
    """
    app = running_app.app
    app.config["S3_BUCKET_NAME"] = "test-import-bucket"
    app.config["RECORD_IMPORTER_STAGING_BUCKET"] = "test-import-bucket"
    app.config["RECORD_IMPORTER_STAGING_SINGLE_PUT_MAX"] = 5 * 2**20

    monkeypatch.setattr(
        staging_helper,
        "create_multipart_upload",
        lambda key, content_type=None, client=None: "upload-123",
    )
    monkeypatch.setattr(
        staging_helper,
        "presign_parts",
        lambda key, upload_id, part_specs, client=None, expires_in=None: [
            {
                "part_number": int(spec["part_number"]),
                "url": (
                    f"https://s3.example/{key}?part={spec['part_number']}"
                    f"&md5={spec.get('checksum', '')}"
                ),
                "expiration": "2099-01-01T00:00:00Z",
                "checksum": spec.get("checksum"),
                "size": spec.get("size"),
            }
            for spec in part_specs
        ],
    )
    monkeypatch.setattr(
        staging_helper,
        "presign_put_object",
        lambda key, content_type=None, content_md5=None, client=None, expires_in=None: {
            "part_number": 1,
            "url": f"https://s3.example/{key}?put=1&md5={content_md5 or ''}",
            "expiration": "2099-01-01T00:00:00Z",
            "checksum": content_md5,
        },
    )
    monkeypatch.setattr(
        staging_helper,
        "complete_multipart_upload",
        lambda key, upload_id, parts, client=None: {"ETag": '"etag-mpu"'},
    )
    monkeypatch.setattr(
        staging_helper,
        "abort_multipart_upload",
        lambda key, upload_id, client=None: None,
    )
    monkeypatch.setattr(
        staging_helper,
        "delete_object",
        lambda key, client=None: None,
    )
    monkeypatch.setattr(
        staging_helper,
        "head_object",
        lambda key, client=None: {"ETag": '"etag-put"', "ContentLength": 100},
    )

    task_id = str(uuid.uuid4())
    delay_mock = MagicMock(return_value=MagicMock(id=task_id))
    monkeypatch.setattr(
        "invenio_record_importer_kcworks.tasks.run_record_import.delay",
        delay_mock,
    )
    return {"delay": delay_mock, "task_id": task_id}


def test_compute_part_layout_grows_for_huge_files(running_app):
    """Part size grows so part count stays within max_parts."""
    max_parts = int(
        running_app.app.config["RECORD_IMPORTER_STAGING_MAX_PARTS"]
    )
    part_size, num_parts = staging_helper.compute_part_layout(5 * 2**20)
    assert num_parts == 1
    assert part_size >= 5 * 2**20

    huge = max_parts * (5 * 2**20) + 1
    part_size, num_parts = staging_helper.compute_part_layout(huge)
    assert num_parts <= max_parts
    assert part_size * num_parts >= huge


def test_create_complete_commit_put_path(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    assign_roles,
    staging_mocks,
):
    """Create with files returns PUT URLs; complete + commit enqueues run."""
    u = user_factory(email="stage-put@example.com")
    community = minimal_community_factory(slug="stage-put", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    created = service.create_import(
        identity,
        community["slug"],
        records_metadata=[_minimal_rdm_record()],
        options={"api_version": 2},
        files=[
            {
                "filename": "paper.pdf",
                "size": 100,
                "content_type": "application/pdf",
                "parts": [
                    {
                        "part_number": 1,
                        "size": 100,
                        "checksum": _TEST_CHECKSUM,
                    }
                ],
            }
        ],
    )
    import_id = created["id"]
    assert created["status"] == "uploading"
    assert len(created["uploads"]) == 1
    upload = created["uploads"][0]
    assert upload["transfer"] == "put"
    assert upload["upload_id"] is None
    assert len(upload["parts"]) == 1
    assert upload["filename"] == "paper.pdf"
    assert "md5=" in upload["parts"][0]["url"]

    completed = service.complete_import_file(
        identity,
        community["slug"],
        import_id,
        "paper.pdf",
        checksum="md5:abc",
    )
    assert completed["entry"]["status"] == "complete"
    assert completed["entry"]["checksum"] == "md5:abc"
    assert completed["import"]["status"] == "staged"

    committed = service.commit_import(identity, community["slug"], import_id)
    assert committed["status"] == "queued"
    assert committed["task_id"] == staging_mocks["task_id"]
    staging_mocks["delay"].assert_called_once_with(import_id)


def test_create_complete_multipart_path(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    assign_roles,
    staging_mocks,
):
    """Create with a large file returns MPU URLs; complete with part etags."""
    u = user_factory(email="stage-mpu@example.com")
    community = minimal_community_factory(slug="stage-mpu", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    part_size = 5 * 2**20
    big = part_size + 1
    created = service.create_import(
        identity,
        community["slug"],
        records_metadata=[],
        options={"api_version": 2},
        files=[
            {
                "filename": "big.bin",
                "size": big,
                "parts": [
                    {
                        "part_number": 1,
                        "size": part_size,
                        "checksum": _TEST_CHECKSUM,
                    },
                    {
                        "part_number": 2,
                        "size": 1,
                        "checksum": _TEST_CHECKSUM,
                    },
                ],
            }
        ],
    )
    assert created["status"] == "uploading"
    upload = created["uploads"][0]
    assert upload["transfer"] == "multipart"
    assert upload["upload_id"] == "upload-123"
    assert len(upload["parts"]) == 2

    parts = [
        {"part_number": p["part_number"], "etag": f'"etag-{p["part_number"]}"'}
        for p in upload["parts"]
    ]
    completed = service.complete_import_file(
        identity,
        community["slug"],
        created["id"],
        "big.bin",
        parts=parts,
    )
    assert completed["entry"]["status"] == "complete"
    assert completed["entry"]["etag"] == "etag-mpu"
    assert completed["import"]["status"] == "staged"


def test_commit_rejects_incomplete(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    assign_roles,
    staging_mocks,
):
    """Commit fails while a file is still uploading."""
    u = user_factory(email="stage-incomplete@example.com")
    community = minimal_community_factory(slug="stage-inc", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    created = service.create_import(
        identity,
        community["slug"],
        records_metadata=[],
        options={"api_version": 2},
        files=[
            {
                "filename": "a.pdf",
                "size": 100,
                "parts": [
                    {
                        "part_number": 1,
                        "size": 100,
                        "checksum": _TEST_CHECKSUM,
                    }
                ],
            }
        ],
    )
    with pytest.raises(ImportStagingIncompleteError) as exc:
        service.commit_import(identity, community["slug"], created["id"])
    assert "a.pdf" in exc.value.incomplete


def test_create_metadata_only_enqueues(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    assign_roles,
    staging_mocks,
):
    """Create with no files enqueues run_import immediately."""
    u = user_factory(email="stage-meta@example.com")
    community = minimal_community_factory(slug="stage-meta", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    created = service.create_import(
        identity,
        community["slug"],
        records_metadata=[_minimal_rdm_record()],
        options={"api_version": 2},
    )
    assert created["uploads"] == []
    assert created["status"] == "queued"
    assert created["task_id"] == staging_mocks["task_id"]
    staging_mocks["delay"].assert_called_once_with(created["id"])


def test_create_no_files_rejects_metadata_with_entries(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    assign_roles,
    staging_mocks,
):
    """Metadata-only create fails when a record declares files.entries."""
    u = user_factory(email="stage-meta-files@example.com")
    community = minimal_community_factory(slug="stage-meta-f", owner=u.user.id)
    identity = _owner_identity_with_import(u.user, community, assign_roles)
    service = current_record_importer_service

    record = _minimal_rdm_record()
    record["files"] = {
        "enabled": True,
        "entries": {"paper.pdf": {"key": "paper.pdf"}},
    }
    with pytest.raises(ValidationError) as exc_info:
        service.create_import(
            identity,
            community["slug"],
            records_metadata=[record],
            options={"api_version": 2},
            files=[],
        )
    assert "metadata" in exc_info.value.messages


def test_api_create_with_files_complete_commit(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    assign_roles,
    staging_mocks,
):
    """HTTP create(files) → complete → commit returns 202 queued."""
    app = running_app.app
    u = user_factory(email="stage-http@example.com", token=True, oauth_id=None)
    community = minimal_community_factory(slug="stage-http", owner=u.user.id)
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
            data=json.dumps(
                {
                    "api_version": 2,
                    "metadata": [],
                    "files": [
                        {
                            "filename": "doc.pdf",
                            "size": 100,
                            "content_type": "application/pdf",
                            "parts": [
                                {
                                    "part_number": 1,
                                    "size": 100,
                                    "checksum": _TEST_CHECKSUM,
                                }
                            ],
                        }
                    ],
                }
            ),
            headers=headers,
        )
        assert create_resp.status_code == 202, create_resp.get_data(as_text=True)
        assert create_resp.json["status"] == "uploading"
        assert len(create_resp.json["uploads"]) == 1
        assert create_resp.json["uploads"][0]["transfer"] == "put"
        import_id = create_resp.json["id"]

        complete_resp = client.post(
            f"{base}/{import_id}/files/doc.pdf/complete",
            data=json.dumps({}),
            headers=headers,
        )
        assert complete_resp.status_code == 200
        assert complete_resp.json["entry"]["status"] == "complete"

        commit_resp = client.post(f"{base}/{import_id}/commit", headers=headers)
        assert commit_resp.status_code == 202
        assert commit_resp.json["status"] == "queued"
