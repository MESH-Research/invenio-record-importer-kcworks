#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 MESH Research
#
# core-migrate is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

import arrow
from copy import deepcopy
from click.testing import CliRunner
from invenio_access.permissions import system_identity
from invenio_pidstore.errors import PIDUnregistered
from invenio_rdm_records.proxies import (
    current_rdm_records_service as records_service,
)
from invenio_rdm_records.records.stats.api import Statistics
from invenio_record_importer.main import cli
from invenio_record_importer.services.stats.stats import (
    StatsFabricator,
    AggregationFabricator,
)
from invenio_record_importer.utils import valid_date, generate_random_string
from invenio_record_importer.serializer import add_date_info
from invenio_record_importer.queries import (
    view_events_search,
    download_events_search,
    aggregations_search,
)
from invenio_record_importer.record_loader import (
    api_request,
    create_invenio_record,
    create_invenio_user,
    delete_invenio_draft_record,
    import_record_to_invenio,
)
from invenio_record_importer.services.communities import (
    CommunitiesHelper,
)
from invenio_record_importer.services.files import FilesHelper
from invenio_search import current_search_client
import datetime
import json
import re
from pprint import pprint, pformat
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
def test_serialize_json(app, sample_record, serialized_records):
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
            assert [
                i
                for i in actual_json_item[k].keys()
                if i not in expected_json[k].keys()
            ] == []
            for i in expected_json[k].keys():
                if i == "subjects":
                    assert sorted(
                        actual_json_item[k][i], key=lambda x: x["subject"]
                    ) == sorted(
                        expected_json[k][i], key=lambda x: x["subject"]
                    )
                else:
                    assert actual_json_item[k][i] == expected_json[k][i]
        else:
            print(k)
            print(expected_json[k])
            print(actual_json_item[k])
            assert actual_json_item[k] == expected_json[k]

    assert all(k for k in expected_json.keys() if k in actual_json_item.keys())
    assert not any(
        [k for k in actual_json_item.keys() if k not in expected_json.keys()]
    )


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


@pytest.mark.skip(reason="The use of REST API calls is deprecated.")
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
    server = app.config.get("RECORD_IMPORTER_DOMAIN")
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
    """ """
    # Send everything from test JSON fixtures except
    #  - created
    #  - updated
    #  - parent
    #  - pids
    #  -
    TESTING_SERVER_DOMAIN = (
        app.config.get("SITE_UI_URL")
        # .replace("https://", "")
        # .replace("http://", "")
    )

    # expected_headers = {
    #     "Server": "nginx/1.23.4",
    #     "Date": "Tue, 30 May 2023 19:07:31 GMT",
    #     "Content-Type": "application/json",
    #     "Content-Length": "182",
    #     "Connection": "keep-alive",
    #     "Set-Cookie": (
    #         "csrftoken=eyJhbGciOiJIUzUxMiIsImlhdCI6MTY4NTQ3MzY1MSwiZXhwIjoxNjg1NTYwMDUxfQ.IkZIODNHR0h2bThxZHdmRVMwaE9JRzgzaE9OaHJhaDFzIg.Te5wJA-7cO-jc29ydK-b2NvEkF17jZNclMIhpGfBou77Ib-I50Qiy4XCBxgttNGGBhkcbeYBRWOm_-2K7YsEBg;"  # noqa: E501
    #         " Expires=Tue, 06 Jun 2023 19:07:31 GMT; Max-Age=604800; Secure;"
    #         " Path=/; SameSite=Lax"
    #     ),
    #     "X-RateLimit-Limit": "500",
    #     "X-RateLimit-Remaining": "499",
    #     "X-RateLimit-Reset": "1685473712",
    #     "Retry-After": "60",
    #     "Permissions-Policy": "interest-cohort=()",
    #     "X-Frame-Options": "sameorigin",
    #     "X-XSS-Protection": "1; mode=block",
    #     "X-Content-Type-Options": "nosniff",
    #     "Content-Security-Policy": (
    #         "default-src 'self' data: 'unsafe-inline' blob:"
    #     ),
    #     "Strict-Transport-Security": "max-age=31556926; includeSubDomains",
    #     "Referrer-Policy": "strict-origin-when-cross-origin",
    # }
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
        no_updates=False,
    )
    actual_record = actual["record_data"]
    actual_id = actual_record["id"]
    # actual_parent = actual["json"]["parent"]["id"]
    # actual["json"]["metadata"]["resource_type"] = {
    #     "id": actual["json"]["metadata"]["resource_type"]["id"]
    # }
    # for idx, c in enumerate(actual["json"]["metadata"]["creators"]):
    #     if "role" in c.keys() and "de" in c["role"]["title"].keys():
    #         del actual["json"]["metadata"]["creators"][idx]["role"]["title"][
    #             "de"
    #         ]
    # if "contributors" in actual["json"]["metadata"].keys():
    #     for idx, c in enumerate(actual["json"]["metadata"]["contributors"]):
    #         if "role" in c.keys() and "de" in c["role"]["title"].keys():
    #             del actual["json"]["metadata"]["contributors"][idx]["role"][
    #                 "title"
    #             ]["de"]
    # if "description" in actual["json"]["metadata"].keys():
    #     actual["json"]["metadata"]["description"] = _normalize_punctuation(
    #         _clean_backslashes_and_spaces(
    #             actual["json"]["metadata"]["description"]
    #         )
    #     )
    # if "additional_descriptions" in actual["json"]["metadata"].keys():
    #     for idx, d in enumerate(
    #         actual["json"]["metadata"]["additional_descriptions"]
    #     ):
    #         if "de" in d["type"]["title"].keys():
    #             del actual["json"]["metadata"]["additional_descriptions"]
    #               [idx]["type"]["title"]["de"]
    #         actual["json"]["metadata"]["additional_descriptions"][idx][
    #             "description"
    #         ] = _normalize_punctuation(
    #             _clean_backslashes_and_spaces(
    #                 actual["json"]["metadata"]["additional_descriptions"][idx][
    #                     "description"
    #                 ]
    #             )
    #         )
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
    deleted = delete_invenio_draft_record(actual_id)
    assert deleted is True

    # # Confirm it no longer exists
    confirm_deleted = records_service.search_drafts(
        system_identity, q=f'id:"{actual_id}"'
    ).to_dict()
    pprint(confirm_deleted)
    assert confirm_deleted["hits"]["total"] == 0


def test_upload_draft_files(app, db, search_clear):
    my_record = rec42615["expected_serialized"]

    json_payload = {
        "custom_fields": my_record["custom_fields"],
        "metadata": my_record["metadata"],
        "pids": my_record["pids"],
    }
    json_payload["access"] = {"record": "public", "files": "public"}
    json_payload["files"] = {"enabled": True}

    actual_draft = create_invenio_record(json_payload, no_updates=False)
    actual_draft_id = actual_draft["record_data"]["id"]

    files_dict = {
        "palazzo-vernacular_patterns_in_portugal_and_brazil-2021.pdf": {  # noqa: E501
            "key": (
                "palazzo-vernacular_patterns_in_portugal_and_b"
                "razil-2021.pdf"
            ),
            "mimetype": "application/pdf",
            "size": "17181",
        }
    }
    source_filenames = {
        "palazzo-vernacular_patterns_in_portugal_and_brazil-2021.pdf": (
            "/srv/www/commons/current/web/app/uploads"
            "/humcore/2021/11/o_1fk563qmpqgs1on0ue"
            "g6mfcf7.pdf.palazzo-vernacular_pa"
            "tterns_in_portugal_and_brazil-2021.pdf"
        )
    }

    actual_upload = FilesHelper()._upload_draft_files(
        draft_id=actual_draft_id,
        files_dict=files_dict,
        source_filenames=source_filenames,
    )
    pprint(actual_upload)
    for k, v in actual_upload.items():
        assert k in files_dict.keys()
        assert v == "uploaded"


def test_create_invenio_community(
    app,
    db,
    admin,
    community_type_v,
    search_clear,
):
    slug = "mla"
    actual_community = CommunitiesHelper().create_invenio_community(
        "knowledgeCommons", slug
    )
    # actual_community_id = actual_community["id"]
    assert actual_community["slug"] == slug


@pytest.mark.parametrize(
    "json_in",
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
def test_create_stats_events(
    app,
    db,
    create_stats_indices,
    location,
    search_clear,
    json_in,
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
):

    # Have to first create the record and then publish it
    app.logger.warning("Creating record...")
    data = {**json_in["expected_serialized"]}
    draft_record = create_invenio_record(data, no_updates=False)
    record_id = draft_record["record_data"]["id"]

    filekey = list(json_in["expected_serialized"]["files"]["entries"].keys())[
        0
    ]
    # Then upload files
    app.logger.warning("Uploading files...")
    files_dict = json_in["expected_serialized"]["files"]["entries"]
    source_filenames = {
        filekey: json_in["expected_serialized"]["custom_fields"][
            "hclegacy:file_location"
        ]
    }

    actual_upload = FilesHelper()._upload_draft_files(
        draft_id=record_id,
        files_dict=files_dict,
        source_filenames=source_filenames,
    )
    assert list(actual_upload.keys())[0] == filekey
    assert actual_upload[filekey] == "uploaded"

    # Publish the record
    app.logger.warning("Publishing record...")
    record = records_service.publish(system_identity, record_id)
    parent_rec_id = record.to_dict()["parent"]["id"]
    assert record.to_dict()["status"] == "published"
    assert record.to_dict()["id"] == record_id

    # Create stats eventsinfo
    app.logger.warning("Creating stats events...")
    events = StatsFabricator().create_stats_events(
        record_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        eager=True,
        verbose=True,
    )

    assert [e for e in events if e[0] == "record-view"][0][1][0] == data[
        "custom_fields"
    ]["hclegacy:total_views"]
    assert [e for e in events if e[0] == "file-download"][0][1][0] == data[
        "custom_fields"
    ]["hclegacy:total_downloads"]

    files_request = records_service.files.list_files(
        system_identity, record_id
    ).to_dict()

    app.logger.warning("Refreshing indices...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")
    file_id = files_request["entries"][0]["file_id"]
    app.logger.warning("Searching for view events...")
    view_events = view_events_search(record_id)
    app.logger.warning("Searching for download events...")
    download_events = download_events_search(file_id)
    assert len(view_events) == data["custom_fields"]["hclegacy:total_views"]
    assert (
        len(download_events)
        == data["custom_fields"]["hclegacy:total_downloads"]
    )

    # Run again to test idempotency
    app.logger.warning("Creating stats events again...")
    events = StatsFabricator().create_stats_events(
        record_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        eager=True,
        verbose=True,
    )
    assert [e for e in events if e[0] == "record-view"][0][1][0] == 0
    assert [e for e in events if e[0] == "file-download"][0][1][0] == 0

    app.logger.warning("Refreshing indices again...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")

    app.logger.warning("Searching for view events again...")
    view_events = view_events_search(record_id)
    download_events = download_events_search(file_id)
    assert len(view_events) == data["custom_fields"]["hclegacy:total_views"]
    assert (
        len(download_events)
        == data["custom_fields"]["hclegacy:total_downloads"]
    )

    # Create stats aggregations
    # Since we re-run the aggregations for each test case,
    # this also tests the idempotency of the aggregations
    # agg_fab = AggregationFabricator()
    # app.logger.warning("Creating stats aggregations...")
    # for yr in range(2017, 2024):
    #     app.logger.warning(f"Creating stats aggregations for {yr}...")
    #     app.logger.warning(arrow.get(f"{yr}-01-01").isoformat())
    #     agg_fab.create_stats_aggregations(
    #         start_date=arrow.get(f"{yr}-01-01"),
    #         end_date=arrow.get(f"{yr}-12-31"),
    #         eager=True,
    #         verbose=True,
    #     )
    #     current_search_client.indices.flush(
    #         ["*record-view*", "*file-download*"], wait_if_ongoing=True
    #     )
    #     view_aggs, download_aggs = aggregations_search(record_id)
    #     view_count = sum([agg.to_dict()["count"] for agg in view_aggs])
    #     download_count = sum([agg.to_dict()["count"] for agg in download_aggs])
    #     app.logger.warning(f"VIEW AGGS {yr} {view_count}")
    #     app.logger.warning(f"DOWNLOAD AGGS {yr} {download_count}")
    app.logger.warning(f"Creating all stats aggregations for {record_id}...")
    AggregationFabricator().create_stats_aggregations(
        start_date=arrow.get("2017-01-01"),
        end_date=arrow.get("2025-01-01"),
        eager=True,
        verbose=True,
    )
    app.logger.warning("Refreshing indices...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")
    app.logger.warning("Searching for view aggregations...")
    view_aggs, download_aggs = aggregations_search(record_id)
    if len(view_aggs) > 0:
        print("VIEW AGGS", pformat(view_aggs[0].to_dict()))
        print("DOWNLOAD AGGS", pformat(download_aggs[0].to_dict()))
    view_count = sum([agg.to_dict()["count"] for agg in view_aggs])
    download_count = sum([agg.to_dict()["count"] for agg in download_aggs])
    view_unique_count = sum(
        [agg.to_dict()["unique_count"] for agg in view_aggs]
    )
    download_unique_count = sum(
        [agg.to_dict()["unique_count"] for agg in download_aggs]
    )
    expected_views = json_in["expected_serialized"]["custom_fields"][
        "hclegacy:total_views"
    ]
    expected_downloads = json_in["expected_serialized"]["custom_fields"][
        "hclegacy:total_downloads"
    ]
    assert view_count == expected_views
    assert download_count == expected_downloads
    assert view_unique_count == expected_views
    assert download_unique_count == expected_downloads

    # Compare those stats to the stats returned by the get_record_stats method
    app.logger.warning("Getting record stats...")
    record_stats = Statistics.get_record_stats(record_id, parent_rec_id)
    this_version_stats = record_stats["this_version"]
    all_versions_stats = record_stats["all_versions"]
    assert this_version_stats["views"] == expected_views
    assert this_version_stats["unique_views"] == expected_views
    assert this_version_stats["downloads"] == expected_downloads
    assert this_version_stats["unique_downloads"] == expected_downloads
    assert all_versions_stats["views"] == expected_views
    assert all_versions_stats["unique_views"] == expected_views
    assert all_versions_stats["downloads"] == expected_downloads
    assert all_versions_stats["unique_downloads"] == expected_downloads


@pytest.mark.parametrize(
    "json_in,json_out",
    [(rec42615["expected_serialized"], rec42615["expected_loaded"])],
)
def test_create_full_invenio_record(
    app,
    appctx,
    db,
    json_in,
    json_out,
    search_clear,
    location,
):
    # random doi necessary because repeat tests with same data
    # can't re-use record's doi
    random_doi = json_in["pids"]["doi"]["identifier"].split("-")[0]
    random_doi = f"{random_doi}-{generate_random_string(5)}"
    json_in["pids"]["doi"]["identifier"] = random_doi
    actual_full_record = import_record_to_invenio(
        json_in, record_source="knowledgeCommons"
    )
    assert (
        actual_full_record["community"]["metadata"]["website"]
        == f'https://{json_in["custom_fields"]["kcr:commons_domain"]}'
    )
    assert (
        actual_full_record["community"]["access"]["record_policy"] == "closed"
    )
    assert (
        actual_full_record["community"]["access"]["review_policy"] == "closed"
    )

    actual_metadata = actual_full_record["metadata_record_created"][
        "record_data"
    ]

    print("ACTUAL FULL RECORD", pformat(actual_metadata))
    assert actual_metadata["access"]["files"] == "public"
    assert actual_metadata["access"]["record"] == "public"
    assert actual_metadata["access"]["status"] == "metadata-only"
    assert actual_metadata["is_draft"]
    assert not actual_metadata["is_published"]

    for k, v in json_in["files"]["entries"].items():
        actual_full_record["uploaded_files"][k] == "uploaded"

    acceptance = actual_full_record["community_review_accepted"].to_dict()
    assert acceptance["is_closed"]
    assert not acceptance["is_open"]
    assert (
        acceptance["receiver"]["community"]
        == actual_full_record["community"]["id"]
    )
    assert acceptance["status"] == "accepted"
    assert acceptance["topic"]["record"] == actual_metadata["id"]
    print("ACCEPTANCE", pformat(acceptance))
    assert acceptance["title"] == actual_metadata["metadata"]["title"]
    assert acceptance["type"] == "community-submission"

    owner = actual_full_record["assigned_ownership"]
    assert owner.email == json_in["custom_fields"]["kcr:submitter_email"]
    assert owner.username == (
        f"knowledgeCommons-"
        f"{json_in['custom_fields']['kcr:submitter_username']}"
    )
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
    db,
    search_clear,
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
    )
    print(actual_user)
    assert re.match(r"\d+", actual_user["user_id"])
    assert actual_user["new_user"] == new_user_flag


def test_record_loader(app, admin, script_info):
    # app.config["RECORD_IMPORTER_API_TOKEN"] = admin.allowed_token
    runner = CliRunner()
    result = runner.invoke(cli, ["load", "0", "1"])
    assert result.exit_code == 0
    assert "Finished!" in result.output
    assert "Created 1 records in InvenioRDM" in result.output
