# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the MIT License

"""Tests for RecordsHelper.create_invenio_record."""

import datetime
from copy import deepcopy
from pprint import pprint

import pytest
import pytz
from dateutil.parser import isoparse
from invenio_access.permissions import system_identity
from invenio_pidstore.errors import PIDUnregistered
from invenio_rdm_records.proxies import current_rdm_records_service as records_service

from invenio_record_importer_kcworks.services.records import RecordsHelper
from invenio_record_importer_kcworks.utils import generate_random_string, valid_date
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
    "json_payload,expected_status_code,expected_json",
    [
        (
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
                            "affiliations": [
                                {"id": "cern", "name": "Entity One"}
                            ],
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
            201,
            {
                "updated": "2023-05-30T18:57:05.296257+00:00",
                "parent": {
                    "communities": {},
                    "id": "###",
                    "access": {"links": [], "owned_by": [{"user": "3"}]},
                },
                "revision_id": 4,
                "is_draft": True,
                "custom_fields": {},
                "pids": {},
                "is_published": False,
                "media_files": {
                    "enabled": False,
                    "order": [],
                    "count": 0,
                    "entries": {},
                    "total_bytes": 0,
                },
                "metadata": {
                    "title": "A Romans story",
                    "creators": [
                        {
                            "person_or_org": {
                                "name": "Brown, Troy",
                                "given_name": "Troy",
                                "family_name": "Brown",
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
                            "role": {
                                "id": "author",
                                "title": {"en": "Author"},
                            },
                            "affiliations": [
                                {
                                    "id": "cern",
                                    "name": ("CERN"),
                                }
                            ],
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
                    "resource_type": {
                        "id": "image-photograph",
                        "title": {"en": "Photo"},
                    },
                },
                "status": "draft",
                "id": "4gqj3-d0z12",
                "created": "2023-05-30T18:57:05.271354+00:00",
                "expires_at": "2023-05-30 18:57:05.271380",
                "files": {
                    "enabled": True,
                    "order": [],
                    "count": 0,
                    "entries": {},
                    "total_bytes": 0,
                },
                "versions": {
                    "is_latest_draft": True,
                    "index": 1,
                    "is_latest": False,
                },
                "access": {
                    "files": "public",
                    "embargo": {"active": False, "reason": None},
                    "record": "public",
                    "status": "metadata-only",
                },
            },
        ),
        (rec42615["input"], 201, rec42615["expected_loaded"]),
        (rec22625["input"], 201, rec22625["expected_loaded"]),
        (rec45177["input"], 201, rec45177["expected_loaded"]),
        (rec44881["input"], 201, rec44881["expected_loaded"]),
        (rec22647["input"], 201, rec22647["expected_loaded"]),
        (rec11451["input"], 201, rec11451["expected_loaded"]),
        (rec34031["input"], 201, rec34031["expected_loaded"]),
        (rec16079["input"], 201, rec16079["expected_loaded"]),
        (rec33383["input"], 201, rec33383["expected_loaded"]),
        (rec38367["input"], 201, rec38367["expected_loaded"]),
        (rec48799["input"], 201, rec48799["expected_loaded"]),
        (rec583["input"], 201, rec583["expected_loaded"]),
        (rec28491["input"], 201, rec28491["expected_loaded"]),
    ],
)
def test_create_invenio_record(
    app,
    db,
    affiliations_v,
    contributors_role_v,
    date_type_v,
    creators_role_v,
    licenses_v,
    subject_v,
    community_type_v,
    resource_type_v,
    description_type_v,
    language_v,
    create_records_custom_fields,
    create_communities_custom_fields,
    location,
    admin,
    json_payload,
    expected_status_code,
    expected_json,
):
    """Test RecordsHelper.create_invenio_record method."""
    TESTING_SERVER_DOMAIN = app.config.get("SITE_UI_URL")

    # fix because can't create duplicate dois
    if "doi" in json_payload["pids"].keys():
        random_doi = json_payload["pids"]["doi"]["identifier"].split("-")[0]
        random_doi = f"{random_doi}-{generate_random_string(5)}"
        json_payload["pids"]["doi"]["identifier"] = random_doi
        expected_json["pids"]["doi"]["identifier"] = random_doi

    # prepare json to use for record creation
    json_payload = {
        "custom_fields": deepcopy(json_payload["custom_fields"]),
        "metadata": deepcopy(json_payload["metadata"]),
        "pids": json_payload["pids"],
    }
    json_payload["access"] = {"record": "public", "files": "public"}
    json_payload["files"] = {"enabled": True}

    # prepare expected json for output (some differences from input)
    # REMEMBER: normalized here to simulate normalized output with
    # odd input
    expected_json = deepcopy(expected_json)

    # Create record and sanitize the result to ease comparison
    records_helper = RecordsHelper()
    actual = records_helper.create_invenio_record(
        json_payload,
        no_updates=False,
    )
    actual_record = actual["record_data"]
    actual_id = actual_record["id"]

    # Test response content
    simple_fields = [
        f
        for f in actual_record.keys()
        if f
        not in [
            "links",
            "parent",
            "id",
            "created",
            "updated",
            "versions",
            "expires_at",
            "is_draft",
            "access",
            "files",
            "status",
            "revision_id",
            "is_published",
        ]
    ]
    for s in simple_fields:
        print(actual_record[s])
        if s == "errors":
            print("errors*****")
            pprint(actual_record[s])
        else:
            assert actual_record[s] == expected_json[s]
    assert actual_record["versions"] == {
        "is_latest_draft": True,
        "index": 1,
        "is_latest": False,
    }
    assert actual_record["is_draft"] is True
    assert actual_record["access"] == {
        "files": "public",
        "embargo": {"active": False, "reason": None},
        "record": "public",
        "status": "metadata-only",
    }
    assert actual_record["status"] == "draft"
    assert isinstance(actual_record["revision_id"], int)
    assert actual_record["is_published"] is False

    links = {
        "access": f"{TESTING_SERVER_DOMAIN}/api/records/###/access",
        "access_grants": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/access/grants"
        ),
        "access_groups": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/access/groups"
        ),
        "access_links": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/access/links"
        ),
        "access_request": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/access/request"
        ),
        "access_users": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/access/users"
        ),
        "archive": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/files-archive"
        ),
        "archive_media": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/"
            "media-files-archive"
        ),
        "communities": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/communities"
        ),
        "communities-suggestions": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/"
            "communities-suggestions"
        ),
        "files": (f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/files"),
        "media_files": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/media-files"
        ),
        "publish": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/actions/publish"
        ),
        "record": f"{TESTING_SERVER_DOMAIN}/api/records/###",
        "record_html": f"{TESTING_SERVER_DOMAIN}/records/###",
        "requests": (f"{TESTING_SERVER_DOMAIN}/api/records/###/requests"),
        "reserve_doi": (
            f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/pids/doi"
        ),
        "review": (f"{TESTING_SERVER_DOMAIN}/api/records/###/draft/review"),
        "self": f"{TESTING_SERVER_DOMAIN}/api/records/###/draft",
        "self_html": f"{TESTING_SERVER_DOMAIN}/uploads/###",
        "self_iiif_manifest": (
            f"{TESTING_SERVER_DOMAIN}/api/iiif/draft:###/manifest"
        ),
        "self_iiif_sequence": (
            f"{TESTING_SERVER_DOMAIN}/api/iiif/draft:###/sequence/default"
        ),
        "versions": f"{TESTING_SERVER_DOMAIN}/api/records/###/versions",
    }
    actual_doi = ""
    if "doi" in actual_record["links"].keys():
        actual_doi = actual_record["pids"]["doi"]["identifier"]
        links["doi"] = "https://handle.stage.datacite.org/$$$"
    for label, link in actual_record["links"].items():
        assert link == links[label].replace("###", actual_id).replace(
            "$$$", actual_doi
        )

    assert actual_record["files"] == {
        "enabled": True,
        "entries": {},
        "count": 0,
        "order": [],
        "total_bytes": 0,
    }
    assert valid_date(actual_record["created"])
    assert isoparse(actual_record["created"]) - pytz.utc.localize(
        datetime.datetime.utcnow()
    ) <= datetime.timedelta(seconds=60)
    assert valid_date(actual_record["updated"])
    assert isoparse(actual_record["updated"]) - pytz.utc.localize(
        datetime.datetime.utcnow()
    ) <= datetime.timedelta(seconds=60)
    print("ACTUAL &&&&")
    pprint(actual_record)

    # Confirm the record is retrievable
    with pytest.raises(PIDUnregistered):
        records_service.read(system_identity, actual_id)
    confirm_created = records_service.search_drafts(
        system_identity, q=f'id:"{actual_id}"'
    ).to_dict()
    pprint(actual_id)
    pprint(confirm_created)
    print("Confirming record was created...")

    # Clean up created record from live db
    deleted = records_helper.delete_invenio_record(actual_id)
    assert deleted is True

    # Confirm it no longer exists
    confirm_deleted = records_service.search_drafts(
        system_identity, q=f'id:"{actual_id}"'
    ).to_dict()
    pprint(confirm_deleted)
    assert confirm_deleted["hits"]["total"] == 0

