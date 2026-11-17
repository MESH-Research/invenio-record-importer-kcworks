# Part of Knowledge Commons Works
# Copyright (C) 2024-2026 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Regression tests for re-importing changed files on published records."""

from __future__ import annotations

import copy
import tempfile
from pathlib import Path

from flask_login import login_user
from invenio_access.permissions import authenticated_user, system_identity
from invenio_access.utils import get_identity
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.records.api import RDMRecord
from invenio_search.proxies import current_search_client

from invenio_record_importer_kcworks.record_loader import RecordLoader
from invenio_record_importer_kcworks.services.files import FilesHelper
from invenio_record_importer_kcworks.types import FileData
from tests.helpers.sample_records import sample_metadata_journal_article_pdf


def _refresh_record_search() -> None:
    """Make newly published records visible to DOI / source-id lookup."""
    RDMRecord.index.refresh()
    current_search_client.indices.refresh(index="*rdmrecords*")


def _build_import_payload(
    *,
    file_key: str,
    file_size: int,
    import_recid: str,
    doi: str,
) -> dict:
    """Return import metadata with a single file entry and stable identifiers."""
    metadata_in = copy.deepcopy(sample_metadata_journal_article_pdf)
    metadata_in["files"] = {
        "enabled": True,
        "entries": {
            file_key: {
                "key": file_key,
                "mimetype": "application/pdf",
                "size": file_size,
            }
        },
    }
    metadata_in["metadata"]["identifiers"] = [
        {"identifier": import_recid, "scheme": "import-recid"},
    ]
    metadata_in["pids"] = {
        "doi": {
            "client": "datacite",
            "identifier": doi,
            "provider": "datacite",
        }
    }
    metadata_in["parent"] = {
        "access": {"owned_by": [{"email": "test@example.com"}]},
    }
    return metadata_in


def _file_data(path: Path, *, key: str) -> FileData:
    """Build a `FileData` for `path` using `key` as the upload filename.

    `filename` must match the entry key (basename) so `FilesHelper` can resolve
    the stream; the on-disk path is only used to open the stream.

    Returns:
        A `FileData` whose stream reads from `path`.
    """
    return FileData(
        filename=key,
        stream=open(path, "rb"),
        content_type="application/pdf",
        mimetype="application/pdf",
        mimetype_params={},
    )


def test_reimport_published_record_with_changed_file_size(
    db,
    running_app,
    search_clear,
    minimal_community_factory,
    user_factory,
    mock_send_remote_api_update_fixture,
    celery_worker,
    mailbox,
    test_sample_files_folder,
    reindex_resource_types,
    reindex_date_types,
):
    """Re-importing a published record with a changed file size should succeed.

    Covers the path that previously called published `files.init_files` (denied)
    when metadata was unchanged and only the file bytes/size differed.
    """
    u = user_factory(email="test@example.com", token=True, oauth_id=None)
    user_id = u.user.id
    identity = get_identity(u.user)
    identity.provides.add(authenticated_user)
    login_user(u.user)

    community = minimal_community_factory(owner=user_id, slug="neh").to_dict()

    original_path = test_sample_files_folder / "sample.pdf"
    original_bytes = original_path.read_bytes()
    original_size = len(original_bytes)
    file_key = "sample.pdf"
    import_recid = "hc:reimport-file-size-1"
    doi = "10.17613/reimport-file-size-success"

    metadata_in = _build_import_payload(
        file_key=file_key,
        file_size=original_size,
        import_recid=import_recid,
        doi=doi,
    )

    first_file = _file_data(original_path, key=file_key)
    try:
        first = RecordLoader(user_id=user_id, community_id=community["id"]).load(
            index=0,
            import_data=copy.deepcopy(metadata_in),
            files=[first_file],
            notify_record_owners=False,
        )
    finally:
        first_file.stream.close()

    assert first.status == "new_record"
    assert first.errors == []
    record_id = first.record_created["record_data"]["id"]
    published = records_service.read(system_identity, id_=record_id).to_dict()
    assert published["is_published"] is True
    assert published["deletion_status"]["is_deleted"] is False
    assert published["files"]["entries"][file_key]["size"] == original_size

    _refresh_record_search()

    changed_bytes = original_bytes + b"\n# reimport size change\n"
    changed_size = len(changed_bytes)
    assert changed_size != original_size
    # Basename must match the file key so FilesHelper can resolve FileData.
    with tempfile.TemporaryDirectory() as tmp_dir:
        changed_path = Path(tmp_dir) / file_key
        changed_path.write_bytes(changed_bytes)

        metadata_changed = _build_import_payload(
            file_key=file_key,
            file_size=changed_size,
            import_recid=import_recid,
            doi=doi,
        )
        second_file = _file_data(changed_path, key=file_key)
        try:
            second = RecordLoader(user_id=user_id, community_id=community["id"]).load(
                index=0,
                import_data=copy.deepcopy(metadata_changed),
                files=[second_file],
                notify_record_owners=False,
            )
        finally:
            second_file.stream.close()

    assert second.errors == []
    assert second.record_created["status"] == "unchanged_existing_published"
    assert second.uploaded_files[file_key]["status"] == "uploaded"

    _refresh_record_search()
    republished = records_service.read(system_identity, id_=record_id).to_dict()
    assert republished["deletion_status"]["is_deleted"] is False
    assert republished["is_published"] is True
    assert republished["files"]["entries"][file_key]["size"] == changed_size


def test_file_upload_failure_does_not_delete_published_record(
    db,
    running_app,
    search_clear,
    minimal_community_factory,
    user_factory,
    mock_send_remote_api_update_fixture,
    celery_worker,
    mailbox,
    test_sample_files_folder,
    reindex_resource_types,
    reindex_date_types,
    monkeypatch,
):
    """File upload failure on an existing published record must not soft-delete it."""
    u = user_factory(email="test@example.com", token=True, oauth_id=None)
    user_id = u.user.id
    identity = get_identity(u.user)
    identity.provides.add(authenticated_user)
    login_user(u.user)

    community = minimal_community_factory(owner=user_id, slug="neh").to_dict()

    original_path = test_sample_files_folder / "sample.pdf"
    original_size = original_path.stat().st_size
    file_key = "sample.pdf"
    import_recid = "hc:reimport-file-size-fail-1"
    doi = "10.17613/reimport-file-size-fail"

    metadata_in = _build_import_payload(
        file_key=file_key,
        file_size=original_size,
        import_recid=import_recid,
        doi=doi,
    )
    first_file = _file_data(original_path, key=file_key)
    try:
        first = RecordLoader(user_id=user_id, community_id=community["id"]).load(
            index=0,
            import_data=copy.deepcopy(metadata_in),
            files=[first_file],
            notify_record_owners=False,
        )
    finally:
        first_file.stream.close()

    assert first.status == "new_record"
    assert first.errors == []
    record_id = first.record_created["record_data"]["id"]
    _refresh_record_search()

    def _fail_handle_record_files(*args, **kwargs):
        return {
            file_key: {
                "status": "failed",
                "messages": ["Forced upload failure for test."],
            }
        }

    monkeypatch.setattr(FilesHelper, "handle_record_files", _fail_handle_record_files)

    changed_size = original_size + 32
    metadata_changed = _build_import_payload(
        file_key=file_key,
        file_size=changed_size,
        import_recid=import_recid,
        doi=doi,
    )
    # Stream is unused because handle_record_files is mocked, but load still
    # expects a FileData list when entries are present.
    second_file = _file_data(original_path, key=file_key)
    try:
        second = RecordLoader(user_id=user_id, community_id=community["id"]).load(
            index=0,
            import_data=copy.deepcopy(metadata_changed),
            files=[second_file],
            notify_record_owners=False,
        )
    finally:
        second_file.stream.close()

    assert second.status == "error"
    assert any("file_upload_error" in err for err in second.errors)
    assert second.record_created.get("status") == "unchanged_existing_published"

    still_there = records_service.read(system_identity, id_=record_id).to_dict()
    assert still_there["deletion_status"]["is_deleted"] is False
    assert still_there["is_published"] is True
    assert still_there["id"] == record_id
