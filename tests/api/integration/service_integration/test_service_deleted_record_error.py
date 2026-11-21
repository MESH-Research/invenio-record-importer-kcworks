# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Service-level tests for soft-deleted record re-import errors."""

import copy
from typing import Any

from flask_login import login_user
from invenio_access.permissions import authenticated_user, system_identity
from invenio_access.utils import get_identity
from invenio_communities.utils import load_community_needs
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.records.api import RDMDraft
from invenio_search.proxies import current_search_client

from invenio_record_importer_kcworks.proxies import current_record_importer_service
from invenio_record_importer_kcworks.utils import generate_random_string
from tests.helpers.sample_records import sample_metadata_journal_article_pdf


def _soft_delete_published(record_id: str) -> None:
    """Soft-delete a published record and refresh search indices."""
    records_service.delete_record(
        system_identity,
        id_=record_id,
        data={
            "removal_reason": {"id": "test-record"},
            "is_visible": False,
        },
    )
    RDMDraft.index.refresh()
    current_search_client.indices.refresh(index="*rdm*")


def test_import_records_errors_on_deleted_import_recid_match(
    running_app,
    db,
    minimal_community_factory,
    user_factory,
    search_clear,
    mock_send_remote_api_update_fixture,
    removal_reason_v,
):
    """Re-importing a soft-deleted import-recid match returns deleted_record_error."""
    u = user_factory(email="deleted-reimport@example.com", token=True, oauth_id=None)
    identity = get_identity(u.user)
    identity.provides.add(authenticated_user)
    login_user(u.user)

    community_record = minimal_community_factory(slug="neh-deleted", owner=u.user.id)
    community = community_record.to_dict()
    load_community_needs(identity)

    import_recid = f"deleted-svc-{generate_random_string(8)}"
    metadata: dict[str, Any] = copy.deepcopy(sample_metadata_journal_article_pdf)
    metadata["files"] = {"enabled": False}
    metadata["metadata"]["identifiers"] = [
        {"identifier": import_recid, "scheme": "import-recid"}
    ]
    # Avoid DOI match path; exercise import-recid deleted detection.
    metadata.pop("pids", None)

    service = current_record_importer_service
    first = service.import_records(
        identity=identity,
        file_data=[],
        metadata=[metadata],
        community_id=community["id"],
        notify_record_owners=False,
    )
    assert first["status"] == "success"
    assert len(first["data"]) == 1
    record_id = first["data"][0]["record_id"]
    assert record_id
    assert first["data"][0]["metadata"]["status"] == "published"

    _soft_delete_published(record_id)

    second = service.import_records(
        identity=identity,
        file_data=[],
        metadata=[copy.deepcopy(metadata)],
        community_id=community["id"],
        notify_record_owners=False,
    )

    assert second["status"] == "error"
    assert second["data"] == []
    assert len(second["errors"]) == 1
    error_item = second["errors"][0]
    assert error_item["item_index"] == 0
    assert error_item["source_id"] == import_recid

    deleted_errors = [
        e["deleted_record_error"]
        for e in error_item["errors"]
        if "deleted_record_error" in e
    ]
    assert len(deleted_errors) == 1
    message = deleted_errors[0]
    assert record_id in message
    assert "import-recid" in message
    assert import_recid in message
    assert "Restore that record before re-importing" in message
    assert "import with a new identifier" in message
