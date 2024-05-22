#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 MESH Research
#
# core-migrate is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

from click.testing import CliRunner
from copy import deepcopy
from invenio_record_importer.main import cli
from invenio_record_importer.utils import valid_date, generate_random_string
from invenio_record_importer.serializer import add_date_info
from invenio_record_importer.record_loader import (
    api_request,
    create_invenio_record,
    create_invenio_user,
    delete_invenio_draft_record,
    create_invenio_community,
    create_full_invenio_record,
    upload_draft_files,
)
from invenio_record_importer.utils import (
    _clean_backslashes_and_spaces,
    _normalize_punctuation,
)
import datetime
import json
import re
import os
from pprint import pprint
import pytest
import pytz
from dateutil.parser import isoparse
from .helpers.sample_records import (
    rec11451,
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
    rec583,
    rec16079,
)


@pytest.mark.parametrize(
    "expected,row,bad_data_dict",
    [
        (
            {
                "publication_date": "2023",
                "issued": "2023-01-01",
                "description": "Publication date",
            },
            {
                "date_issued": "2023",
                "date": "2023-01-01",
            },
            {},
        ),
        (
            {
                "publication_date": "2015",
                "issued": "2015-02",
                "description": "Publication date",
            },
            {
                "date_issued": "2015",
                "date": "02.2015",
            },
            {},
        ),
        (
            {
                "publication_date": "2010",
                "issued": "2010-11-01",
                "description": "Publication date",
            },
            {
                "date_issued": "2010",
                "date": "Nov. 1, 2010",
            },
            {},
        ),
        (
            {
                "publication_date": "2023",
                "issued": "2023-04-22",
                "description": "Publication date",
            },
            {
                "date_issued": "2023",
                "date": "22ND APRIL,2023",
            },
            {},
        ),
        (
            {
                "publication_date": "2021",
                "issued": "2021/2022",
                "description": "Publication date",
            },
            {
                "date_issued": "2021",
                "date": "2021-2022",
            },
            {},
        ),
        (
            {
                "publication_date": "2019",
                "issued": "2019-02",
                "description": "Publication date",
            },
            {
                "date_issued": "2019",
                "date": "02/2019",
            },
            {},
        ),
        (
            {
                "publication_date": "2023",
                "issued": "2023-02-04",
                "description": "Publication date",
            },
            {
                "date_issued": "2023",
                "date": "4 de febrero 2023",
            },
            {},
        ),
        (
            {
                "publication_date": "2015",
                "issued": "2015-03",
                "description": "Publication date",
            },
            {
                "date_issued": "2015",
                "date": "March, 2015.",
            },
            {},
        ),
        (
            {
                "publication_date": "2021",
                "issued": "2021-10",
                "description": "Publication date",
            },
            {
                "date_issued": "2021",
                "date": "October, 2021.",
            },
            {},
        ),
        (
            {
                "publication_date": "2002",
                "issued": "2002/2015",
                "description": "Publication date",
            },
            {
                "date_issued": "2002",
                "date": "2002/2015",
            },
            {},
        ),
        (
            {
                "publication_date": "2022",
                "issued": "1991-04-22",
                "description": "Publication date",
            },
            {
                "date_issued": "2022",
                "date": "04/91/22",
            },
            {},
        ),
        (
            {
                "publication_date": "2022",
                "issued": "2017-06-30",
                "description": "Publication date",
            },
            {
                "date_issued": "2022",
                "date": "2017 06 30",
            },
            {},
        ),
        (
            {
                "publication_date": "2022",
                "issued": "2022/2023",
                "description": "Publication date",
            },
            {
                "date_issued": "2022",
                "date": "2022/23",
            },
            {},
        ),
    ],
)
def test_add_date_info(expected, row, bad_data_dict):
    actual_dict, actual_bad_data = add_date_info(
        {"metadata": {}},
        {
            **row,
            "id": "1001634-235",
            "record_change_date": "",
            "record_creation_date": "",
        },
        bad_data_dict,
    )
    assert actual_dict == {
        "metadata": {
            "publication_date": expected["publication_date"],
            "dates": [
                {
                    "date": expected["issued"],
                    "type": {
                        "id": "issued",
                        "title": {
                            "en": "Issued",
                            "de": "Veröffentlicht",
                        },
                    },
                    "description": expected["description"],
                }
            ],
        }
    }


@pytest.mark.parametrize(
    "sample_record",
    [
        (rec42615),
        (rec22625),
        (rec45177),
        (rec44881),
        (rec22647),
        (rec11451),
        (rec34031),
        (rec16079),
        (rec33383),
        (rec38367),
        (rec48799),
        (rec583),
        (rec28491),
    ],
)
def test_serialize_json(sample_record, serialized_records):
    """ """
    actual_json = serialized_records["actual_serialized_json"]
    expected_json = sample_record["expected_serialized"]

    expected_pid = expected_json["metadata"]["identifiers"][0]["identifier"]
    actual_json_item = [
        j
        for j in actual_json
        for i in j["metadata"]["identifiers"]
        if i["identifier"] == expected_pid
    ][0]

    # FAST vocab bug
    if "subjects" in expected_json["metadata"].keys():
        ports = [
            s
            for s in expected_json["metadata"]["subjects"]
            if s["subject"] == "Portuguese colonies"
        ]
        if ports:
            ports[0]["scheme"] = "FAST-geographic"

    for k in expected_json.keys():
        if k in ["custom_fields", "metadata"]:
            for i in expected_json[k].keys():
                assert actual_json_item[k][i] == expected_json[k][i]
        else:
            assert actual_json_item[k] == expected_json[k]

    assert all(k for k in expected_json.keys() if k in actual_json_item.keys())
    assert not any(
        [k for k in actual_json_item.keys() if k not in expected_json.keys()]
    )

    assert actual_json_item == expected_json


top_level_record_keys = [
    "links",
    "updated",
    "parent",
    "revision_id",
    "is_draft",
    "custom_fields",
    "pids",
    "is_published",
    "metadata",
    "stats",
    "status",
    "id",
    "created",
    "files",
    "versions",
    "access",
]

request_header_keys = [
    "Server",
    "Date",
    "Content-Type",
    "Transfer-Encoding",
    "Connection",
    "Vary",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
    "Retry-After",
    "Permissions-Policy",
    "X-Frame-Options",
    "X-XSS-Protection",
    "X-Content-Type-Options",
    "Content-Security-Policy",
    "Strict-Transport-Security",
    "Referrer-Policy",
    "X-Request-ID",
    "Content-Encoding",
]


@pytest.mark.parametrize(
    "method,endpoint,args,json_dict,expected_response",
    [
        (
            "GET",
            "records",
            "p6qjf-y6074",
            "",
            {"text": "", "headers": ""},
        )
    ],
)
def test_api_request(
    app, admin, method, endpoint, args, json_dict, expected_response
):
    """ """
    server = app.config.get("MIGRATION_SERVER_DOMAIN")
    token = admin.allowed_token
    other_args = {}
    if json_dict:
        other_args["json_dict"] = json_dict
    actual = api_request(
        method=method,
        endpoint=endpoint,
        server=server,
        args=args,
        token=token,
        **other_args,
    )
    assert actual["status_code"] == 200
    assert all(
        k in top_level_record_keys
        for k in list(json.loads(actual["text"]).keys())
    )
    assert all(k in top_level_record_keys for k in list(actual["json"].keys()))
    assert all(
        k in request_header_keys for k in list(actual["headers"].keys())
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
                                {"id": "01ggx4157", "name": "Entity One"}
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
                                    "id": "01ggx4157",
                                    "name": (
                                        "European Organization for Nuclear "
                                        "Research"
                                    ),
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
                    "resource_type": {"id": "image-photograph"},
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
        (rec42615["input"], 201, rec42615["expected_serialized"]),
        (rec22625["input"], 201, rec22625["expected_serialized"]),
        (rec45177["input"], 201, rec45177["expected_serialized"]),
        (rec44881["input"], 201, rec44881["expected_serialized"]),
        (rec22647["input"], 201, rec22647["expected_serialized"]),
        (rec11451["input"], 201, rec11451["expected_serialized"]),
        (rec34031["input"], 201, rec34031["expected_serialized"]),
        (rec16079["input"], 201, rec16079["expected_serialized"]),
        (rec33383["input"], 201, rec33383["expected_serialized"]),
        (rec38367["input"], 201, rec38367["expected_serialized"]),
        (rec48799["input"], 201, rec48799["expected_serialized"]),
        (rec583["input"], 201, rec583["expected_serialized"]),
        (rec28491["input"], 201, rec28491["expected_serialized"]),
    ],
)
def test_create_invenio_record(
    app, admin, json_payload, expected_status_code, expected_json
):
    """ """
    # Send everything from test JSON fixtures except
    #  - created
    #  - updated
    #  - parent
    #  - pids
    #  -
    TESTING_SERVER_DOMAIN = app.config.get("MIGRATION_SERVER_DOMAIN")

    expected_headers = {
        "Server": "nginx/1.23.4",
        "Date": "Tue, 30 May 2023 19:07:31 GMT",
        "Content-Type": "application/json",
        "Content-Length": "182",
        "Connection": "keep-alive",
        "Set-Cookie": (
            "csrftoken=eyJhbGciOiJIUzUxMiIsImlhdCI6MTY4NTQ3MzY1MSwiZXhwIjoxNjg1NTYwMDUxfQ.IkZIODNHR0h2bThxZHdmRVMwaE9JRzgzaE9OaHJhaDFzIg.Te5wJA-7cO-jc29ydK-b2NvEkF17jZNclMIhpGfBou77Ib-I50Qiy4XCBxgttNGGBhkcbeYBRWOm_-2K7YsEBg;"
            " Expires=Tue, 06 Jun 2023 19:07:31 GMT; Max-Age=604800; Secure;"
            " Path=/; SameSite=Lax"
        ),
        "X-RateLimit-Limit": "500",
        "X-RateLimit-Remaining": "499",
        "X-RateLimit-Reset": "1685473712",
        "Retry-After": "60",
        "Permissions-Policy": "interest-cohort=()",
        "X-Frame-Options": "sameorigin",
        "X-XSS-Protection": "1; mode=block",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": (
            "default-src 'self' data: 'unsafe-inline' blob:"
        ),
        "Strict-Transport-Security": "max-age=31556926; includeSubDomains",
        "Referrer-Policy": "strict-origin-when-cross-origin",
    }
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
    actual = create_invenio_record(
        json_payload,
        record_source="knowledgeCommons",
        token=admin.allowed_token,
    )
    actual_id = actual["json"]["id"]
    actual_parent = actual["json"]["parent"]["id"]
    actual["json"]["metadata"]["resource_type"] = {
        "id": actual["json"]["metadata"]["resource_type"]["id"]
    }
    for idx, c in enumerate(actual["json"]["metadata"]["creators"]):
        if "role" in c.keys() and "de" in c["role"]["title"].keys():
            del actual["json"]["metadata"]["creators"][idx]["role"]["title"][
                "de"
            ]
    if "contributors" in actual["json"]["metadata"].keys():
        for idx, c in enumerate(actual["json"]["metadata"]["contributors"]):
            if "role" in c.keys() and "de" in c["role"]["title"].keys():
                del actual["json"]["metadata"]["contributors"][idx]["role"][
                    "title"
                ]["de"]
    if "description" in actual["json"]["metadata"].keys():
        actual["json"]["metadata"]["description"] = _normalize_punctuation(
            _clean_backslashes_and_spaces(
                actual["json"]["metadata"]["description"]
            )
        )
    if "additional_descriptions" in actual["json"]["metadata"].keys():
        for idx, d in enumerate(
            actual["json"]["metadata"]["additional_descriptions"]
        ):
            if "de" in d["type"]["title"].keys():
                del actual["json"]["metadata"]["additional_descriptions"][idx][
                    "type"
                ]["title"]["de"]
            actual["json"]["metadata"]["additional_descriptions"][idx][
                "description"
            ] = _normalize_punctuation(
                _clean_backslashes_and_spaces(
                    actual["json"]["metadata"]["additional_descriptions"][idx][
                        "description"
                    ]
                )
            )
    # Test response content
    simple_fields = [
        f
        for f in actual["json"].keys()
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
        print(actual["json"][s])
        if s == "errors":
            print("*****")
            pprint(actual["json"]["errors"])
        assert actual["json"][s] == expected_json[s]
    assert actual["json"]["versions"] == {
        "is_latest_draft": True,
        "index": 1,
        "is_latest": False,
    }
    assert actual["json"]["is_draft"] == True
    assert actual["json"]["access"] == {
        "files": "public",
        "embargo": {"active": False, "reason": None},
        "record": "public",
        "status": "metadata-only",
    }
    assert actual["json"]["status"] == "draft"
    assert type(actual["json"]["revision_id"]) == int
    assert actual["json"]["is_published"] == False

    links = {
        "self": f"https://{TESTING_SERVER_DOMAIN}/api/records/###/draft",
        "self_html": f"https://{TESTING_SERVER_DOMAIN}/uploads/###",
        "self_iiif_manifest": (
            f"https://{TESTING_SERVER_DOMAIN}/api/iiif/draft:###/manifest"
        ),
        "self_iiif_sequence": f"https://{TESTING_SERVER_DOMAIN}/api/iiif/draft:###/sequence/default",
        "files": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/draft/files"
        ),
        "archive": f"https://{TESTING_SERVER_DOMAIN}/api/records/###/draft/files-archive",
        "record": f"https://{TESTING_SERVER_DOMAIN}/api/records/###",
        "record_html": f"https://{TESTING_SERVER_DOMAIN}/records/###",
        "publish": f"https://{TESTING_SERVER_DOMAIN}/api/records/###/draft/actions/publish",
        "review": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/draft/review"
        ),
        "versions": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/versions"
        ),
        "access_links": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/access/links"
        ),
        "reserve_doi": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/draft/pids/doi"
        ),
        "communities": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/communities"
        ),
        "communities-suggestions": f"https://{TESTING_SERVER_DOMAIN}/api/records/###/communities-suggestions",
        "requests": (
            f"https://{TESTING_SERVER_DOMAIN}/api/records/###/requests"
        ),
    }
    actual_doi = ""
    if "doi" in actual["json"]["links"].keys():
        actual_doi = actual["json"]["pids"]["doi"]["identifier"]
        links["doi"] = "https://handle.stage.datacite.org/$$$"
    for label, link in actual["json"]["links"].items():
        assert link == links[label].replace("###", actual_id).replace(
            "$$$", actual_doi
        )

    assert actual["json"]["files"] == {
        "enabled": True,
        "entries": {},
        "count": 0,
        "order": [],
        "total_bytes": 0,
    }
    assert valid_date(actual["json"]["created"])
    assert isoparse(actual["json"]["created"]) - pytz.utc.localize(
        datetime.datetime.utcnow()
    ) <= datetime.timedelta(seconds=60)
    assert valid_date(actual["json"]["updated"])
    assert isoparse(actual["json"]["updated"]) - pytz.utc.localize(
        datetime.datetime.utcnow()
    ) <= datetime.timedelta(seconds=60)
    print("ACTUAL &&&&")
    pprint(actual)

    # Confirm the record is retrievable
    confirm_created = api_request(
        "GET", endpoint="records", args=f"{actual_id}/draft"
    )
    pprint(actual_id)
    pprint(confirm_created)
    print("Confirming record was created...")
    assert confirm_created["status_code"] == 200

    # Clean up created record from live db
    deleted = delete_invenio_draft_record(actual_id)
    assert deleted["status_code"] == 204

    # # Confirm it no longer exists
    confirm_deleted = api_request(
        "GET", endpoint="records", args=f"{actual_id}"
    )
    assert confirm_deleted["status_code"] == 404


def test_upload_draft_files(app):
    my_record = rec42615["expected_serialized"]

    json_payload = {
        "custom_fields": my_record["custom_fields"],
        "metadata": my_record["metadata"],
        "pids": my_record["pids"],
    }
    json_payload["access"] = {"record": "public", "files": "public"}
    json_payload["files"] = {"enabled": True}

    actual_draft = create_invenio_record(json_payload)
    actual_draft_id = actual_draft["json"]["id"]

    files_in = {
        "palazzo-vernacular_patterns_in_portugal_and_brazil-2021.pdf": (
            "/srv/www/commons/current/web/app/uploads"
            "/humcore/2021/11/o_1fk563qmpqgs1on0ue"
            "g6mfcf7.pdf.palazzo-vernacular_pa"
            "tterns_in_portugal_and_brazil-2021.pdf"
        )
    }

    actual_upload = upload_draft_files(
        draft_id=actual_draft_id, files_dict=files_in
    )
    pprint(actual_upload)
    for k, v in actual_upload["file_transactions"].items():
        assert k in files_in.keys()
        assert valid_date(v["upload_commit"]["json"]["created"])
        assert valid_date(v["upload_commit"]["json"]["updated"])
        assert v["upload_commit"]["json"]["status"] == "completed"
        assert v["upload_commit"]["json"]["metadata"] == None
    assert actual_upload["confirmation"]["status_code"] == 200


def test_create_invenio_community(app, db, admin):
    slug = "mla"
    actual_community = create_invenio_community(
        slug, token=admin.allowed_token
    )
    actual_community_id = actual_community["json"]["id"]
    assert actual_community["status_code"] == 201
    assert actual_community["json"]["metadata"]["slug"] == slug


@pytest.mark.parametrize("json_in", [(rec42615["expected_serialized"])])
def test_create_full_invenio_record(json_in):
    # random doi necessary because repeat tests with same data
    # can't re-use record's doi
    random_doi = json_in["pids"]["doi"]["identifier"].split("-")[0]
    random_doi = f"{random_doi}-{generate_random_string(5)}"
    json_in["pids"]["doi"]["identifier"] = random_doi
    actual_full_record = create_full_invenio_record(json_in)
    assert (
        actual_full_record["community"]["json"]["metadata"]["website"]
        == f'https://{json_in["custom_fields"]["kcr:commons_domain"]}'
    )
    assert actual_full_record["community"]["status_code"] == 200
    assert (
        actual_full_record["community"]["json"]["access"]["record_policy"]
        == "open"
    )
    assert (
        actual_full_record["community"]["json"]["access"]["review_policy"]
        == "open"
    )

    assert (
        actual_full_record["metadata_record_created"]["json"]["access"][
            "files"
        ]
        == "public"
    )
    assert (
        actual_full_record["metadata_record_created"]["json"]["access"][
            "record"
        ]
        == "public"
    )
    assert (
        actual_full_record["metadata_record_created"]["json"]["access"][
            "status"
        ]
        == "metadata-only"
    )
    assert (
        actual_full_record["metadata_record_created"]["json"]["is_draft"]
        == True
    )
    assert (
        actual_full_record["metadata_record_created"]["json"]["is_published"]
        == False
    )

    afu = actual_full_record["uploaded_files"]
    for k, v in json_in["files"]["entries"].items():
        actual_trans = afu["file_transactions"][k]
        assert actual_trans["content_upload"]["json"]["key"] == k
        assert actual_trans["content_upload"]["status_code"] == 200
        assert actual_trans["upload_commit"]["status_code"] == 200
        assert actual_trans["upload_commit"]["json"]["key"] == k
        assert actual_trans["upload_commit"]["json"]["size"] > 1000
        assert actual_trans["upload_commit"]["json"]["status"] == "completed"
        assert valid_date(actual_trans["upload_commit"]["json"]["created"])
        assert valid_date(actual_trans["upload_commit"]["json"]["updated"])
        assert afu["confirmation"]["status_code"] == 200

    assert type(actual_full_record["created_user"]["new_user"]) == bool
    assert int(actual_full_record["created_user"]["user_id"]) > 2 < 10000

    assert actual_full_record["request_to_community"]["status_code"] == 200
    assert valid_date(
        actual_full_record["request_to_community"]["json"]["created"]
    )
    assert actual_full_record["request_to_community"]["json"][
        "created_by"
    ] == {"user": "3"}
    assert (
        actual_full_record["request_to_community"]["json"]["is_closed"]
        == False
    )
    assert (
        actual_full_record["request_to_community"]["json"]["is_expired"]
        == False
    )
    assert (
        actual_full_record["request_to_community"]["json"]["is_open"] == False
    )
    assert (
        actual_full_record["request_to_community"]["json"]["receiver"][
            "community"
        ]
        == actual_full_record["community"]["json"]["id"]
    )
    assert (
        actual_full_record["request_to_community"]["json"]["status"]
        == "created"
    )
    assert (
        actual_full_record["request_to_community"]["json"]["type"]
        == "community-submission"
    )
    assert actual_full_record["request_to_community"]["json"]["topic"] == {
        "record": actual_full_record["metadata_record_created"]["json"]["id"]
    }

    assert valid_date(
        actual_full_record["review_submitted"]["json"]["created"]
    )
    assert (
        actual_full_record["review_submitted"]["json"]["created_by"]["user"]
        == "3"
    )
    assert actual_full_record["review_submitted"]["json"]["is_closed"] == False
    assert actual_full_record["review_submitted"]["json"]["is_open"] == True
    assert (
        actual_full_record["review_submitted"]["json"]["receiver"]["community"]
        == actual_full_record["community"]["json"]["id"]
    )
    assert (
        actual_full_record["review_submitted"]["json"]["status"] == "submitted"
    )
    assert (
        actual_full_record["review_submitted"]["json"]["topic"]["record"]
        == actual_full_record["metadata_record_created"]["json"]["id"]
    )
    assert (
        actual_full_record["review_submitted"]["json"]["title"]
        == actual_full_record["metadata_record_created"]["json"]["metadata"][
            "title"
        ]
    )
    assert (
        actual_full_record["review_submitted"]["json"]["type"]
        == "community-submission"
    )
    assert actual_full_record["review_submitted"]["status_code"] == 200

    assert actual_full_record["review_accepted"]["json"]["is_closed"] == True
    assert actual_full_record["review_accepted"]["json"]["is_open"] == False
    assert (
        actual_full_record["review_accepted"]["json"]["receiver"]["community"]
        == actual_full_record["community"]["json"]["id"]
    )
    assert (
        actual_full_record["review_accepted"]["json"]["status"] == "accepted"
    )
    assert (
        actual_full_record["review_accepted"]["json"]["topic"]["record"]
        == actual_full_record["metadata_record_created"]["json"]["id"]
    )
    assert (
        actual_full_record["review_accepted"]["json"]["title"]
        == actual_full_record["metadata_record_created"]["json"]["metadata"][
            "title"
        ]
    )
    assert (
        actual_full_record["review_accepted"]["json"]["type"]
        == "community-submission"
    )
    assert actual_full_record["review_accepted"]["status_code"] == 200

    assert actual_full_record["changed_ownership"]["return_code"] == 0
    print(actual_full_record)


@pytest.mark.parametrize(
    "email_in,source_username,full_name,new_user_flag",
    [
        ("myaddress3@somedomain.edu", "myuser", "My User", True),
        ("scottia4@msu.edu", "ianscott", "Ian Scott", False),
    ],
)
def test_create_invenio_user(
    app,
    admin,
    user_factory,
    email_in,
    source_username,
    full_name,
    new_user_flag,
):
    if not new_user_flag:
        preexisting_user = user_factory(email=email_in).user
        assert preexisting_user.id
    actual_user = create_invenio_user(
        email_in,
        source_username=source_username,
        full_name=full_name,
        record_source="knowledgeCommons",
        token=admin.allowed_token,
    )
    print(actual_user)
    assert re.match(r"\d+", actual_user["user_id"])
    assert actual_user["new_user"] == new_user_flag


def test_record_loader(app, admin):
    app.config["MIGRATION_API_TOKEN"] = admin.allowed_token
    runner = CliRunner()
    result = runner.invoke(cli, ["load", "0", "1"])
    assert result.exit_code == 0
    assert "Finished!" in result.output
    assert "Created 1 records in InvenioRDM" in result.output
