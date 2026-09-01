# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the MIT License

"""Tests for RecordsHelper.create_invenio_record."""

from copy import deepcopy
from pprint import pprint

import pytest
from invenio_access.permissions import system_identity
from invenio_pidstore.errors import PIDUnregistered
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.records.api import RDMDraft
from invenio_search.proxies import current_search_client

from invenio_record_importer_kcworks.errors import NoUpdates
from invenio_record_importer_kcworks.services.records import RecordsHelper
from invenio_record_importer_kcworks.utils import generate_random_string
from tests.fixtures.records import TestRecordMetadata
from tests.helpers.sample_records import (
    rec583,
    rec11451,
    rec16079,
    rec22625,
    rec22647,
    rec28491,
    rec33383,
    rec34031,
    rec38367,
    rec42615,
    rec44881,
    rec45177,
    rec48799,
)


@pytest.mark.parametrize(
    "json_payload",
    [
        {
            "access": {"record": "public", "files": "public"},
            "custom_fields": {},
            "pids": {},
            "files": {"enabled": True},
            "metadata": {
                "creators": [
                    {
                        "person_or_org": {
                            "family_name": "Brown",
                            "given_name": "Troy",
                            "type": "personal",
                        },
                        "role": {
                            "id": "author",
                            "title": {"en": "Author"},
                        },
                    },
                    {
                        "person_or_org": {
                            "family_name": "Collins",
                            "given_name": "Thomas",
                            "identifiers": [
                                {
                                    "scheme": "orcid",
                                    "identifier": "0000-0002-1825-0097",
                                }
                            ],
                            "name": "Collins, Thomas",
                            "type": "personal",
                        },
                        "affiliations": [{"id": "cern", "name": "Entity One"}],
                        "role": {
                            "id": "author",
                            "title": {"en": "Author"},
                        },
                    },
                    {
                        "person_or_org": {
                            "name": "Troy Inc.",
                            "type": "organizational",
                        }
                    },
                ],
                "publication_date": "2020-06-01",
                "publisher": "MESH Research",
                "resource_type": {"id": "image-photograph"},
                "title": "A Romans story",
            },
        },
        rec42615["input"],
        rec22625["input"],
        rec45177["input"],
        rec44881["input"],
        rec22647["input"],
        rec11451["input"],
        rec34031["input"],
        rec16079["input"],
        rec33383["input"],
        rec38367["input"],
        rec48799["input"],
        rec583["input"],
        rec28491["input"],
    ],
)
def test_create_invenio_record(
    running_app,
    db,
    nested_unit_of_work,
    monkeypatch,
    create_records_custom_fields,
    create_communities_custom_fields,
    location,
    admin,
    json_payload,
    search_clear,
    reindex_resource_types,
):
    """Test RecordsHelper.create_invenio_record method.

    Shape checks use ``TestRecordMetadata.compare_draft`` built only from the
    create input (no ``expected_loaded`` dumps). Vocabulary expansions on
    metadata are tolerated by the fixture helpers.
    """
    app = running_app.app
    monkeypatch.setattr(
        "invenio_records_resources.services.uow.UnitOfWork", nested_unit_of_work
    )

    # Avoid duplicate DOI collisions across parametrized cases.
    if "doi" in json_payload.get("pids", {}):
        random_doi = json_payload["pids"]["doi"]["identifier"].split("-")[0]
        random_doi = f"{random_doi}-{generate_random_string(5)}"
        json_payload["pids"]["doi"]["identifier"] = random_doi

    # Input for create: strip parent owners from samples; force files enabled
    # with no uploads (this test does not attach binaries).
    json_payload = {
        "custom_fields": deepcopy(json_payload.get("custom_fields", {})),
        "metadata": deepcopy(json_payload["metadata"]),
        "pids": deepcopy(json_payload.get("pids", {})),
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": True},
    }

    # create_invenio_record uses system_identity → owned_by is system.
    test_metadata = TestRecordMetadata(metadata_in=json_payload, app=app, owner_id=None)

    records_helper = RecordsHelper()
    actual = records_helper.create_invenio_record(
        json_payload,
        no_updates=False,
    )
    actual_record = actual["record_data"]
    actual_id = actual_record["id"]

    test_metadata.compare_draft(actual_record)

    # revision_id is intentionally skipped by compare_draft
    assert isinstance(actual_record["revision_id"], int)

    # Confirm the record is retrievable via search (not covered by compare_draft)
    with pytest.raises(PIDUnregistered):
        records_service.read(system_identity, actual_id)
    RDMDraft.index.refresh()
    current_search_client.indices.refresh(index="*rdm*")
    confirm_created = records_service.search_drafts(
        system_identity, q=f'id:"{actual_id}"'
    ).to_dict()
    assert confirm_created["hits"]["total"] >= 1

    deleted = records_helper.delete_invenio_record(actual_id)
    assert deleted is True

    RDMDraft.index.refresh()
    current_search_client.indices.refresh(index="*rdm*")
    confirm_deleted = records_service.search_drafts(
        system_identity, q=f'id:"{actual_id}"'
    ).to_dict()
    pprint(confirm_deleted)
    assert confirm_deleted["hits"]["total"] == 0


def _minimal_import_recid_payload(import_recid: str, title: str = "Import recid test"):
    """Build a minimal draft payload with import-recid and no DOI.

    Returns:
        dict: Record metadata payload suitable for ``create_invenio_record``.
    """
    return {
        "access": {"record": "public", "files": "public"},
        "custom_fields": {},
        "pids": {},
        "files": {"enabled": False},
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": "Doe",
                        "given_name": "Jane",
                        "type": "personal",
                        "name": "Doe, Jane",
                    },
                    "role": {"id": "author"},
                }
            ],
            "publication_date": "2024-01-01",
            "publisher": "MESH Research",
            "resource_type": {"id": "image-photograph"},
            "title": title,
            "identifiers": [
                {"scheme": "import-recid", "identifier": import_recid},
            ],
        },
    }


def test_source_ids_from_metadata_and_record_has_identifier():
    """Unit helpers extract and match configured source identifiers."""
    metadata = {
        "metadata": {
            "identifiers": [
                {"scheme": "doi", "identifier": "10.1/xyz"},
                {"scheme": "import-recid", "identifier": "src-1"},
            ]
        }
    }
    assert RecordsHelper._source_ids_from_metadata(
        metadata, ["import-recid", "neh-recid"]
    ) == [("import-recid", "src-1")]
    assert RecordsHelper._source_ids_from_metadata(metadata, ["neh-recid"]) == []
    assert RecordsHelper._record_has_identifier(metadata, "import-recid", "src-1")
    assert not RecordsHelper._record_has_identifier(metadata, "import-recid", "other")


def test_create_invenio_record_reuses_existing_by_import_recid(
    app,
    db,
    nested_unit_of_work,
    monkeypatch,
    create_records_custom_fields,
    create_communities_custom_fields,
    location,
    admin,
    search_clear,
    resource_type_v,
    creators_role_v,
):
    """Second create with the same import-recid reuses the draft (no duplicate)."""
    monkeypatch.setattr(
        "invenio_records_resources.services.uow.UnitOfWork", nested_unit_of_work
    )
    import_recid = f"test-import-{generate_random_string(8)}"
    payload = _minimal_import_recid_payload(import_recid)
    helper = RecordsHelper()

    first = helper.create_invenio_record(payload, no_updates=True)
    assert first["status"] == "new_record"
    first_id = first["record_data"]["id"]

    RDMDraft.index.refresh()
    current_search_client.indices.refresh(index="*rdm*")

    second = helper.create_invenio_record(deepcopy(payload), no_updates=True)
    assert second["status"] == "unchanged_existing_draft"
    assert second["record_data"]["id"] == first_id

    helper.delete_invenio_record(first_id)


def test_create_invenio_record_no_updates_raises_on_import_recid_change(
    app,
    db,
    nested_unit_of_work,
    monkeypatch,
    create_records_custom_fields,
    create_communities_custom_fields,
    location,
    admin,
    search_clear,
    resource_type_v,
    creators_role_v,
):
    """Changed metadata with same import-recid raises NoUpdates when flagged."""
    monkeypatch.setattr(
        "invenio_records_resources.services.uow.UnitOfWork", nested_unit_of_work
    )
    import_recid = f"test-import-{generate_random_string(8)}"
    payload = _minimal_import_recid_payload(import_recid, title="Original title")
    helper = RecordsHelper()

    first = helper.create_invenio_record(payload, no_updates=True)
    first_id = first["record_data"]["id"]

    RDMDraft.index.refresh()
    current_search_client.indices.refresh(index="*rdm*")

    changed = deepcopy(payload)
    changed["metadata"]["title"] = "Updated title"
    with pytest.raises(NoUpdates):
        helper.create_invenio_record(changed, no_updates=True)

    updated = helper.create_invenio_record(changed, no_updates=False)
    assert updated["status"] in ("updated_draft", "unchanged_existing_draft")
    assert updated["record_data"]["id"] == first_id
    assert updated["record_data"]["metadata"]["title"] == "Updated title"

    helper.delete_invenio_record(first_id)
