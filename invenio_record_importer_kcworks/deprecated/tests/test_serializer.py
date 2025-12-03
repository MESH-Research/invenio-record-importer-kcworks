# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for serializer functions."""

import sys
from pathlib import Path

import pytest

# Add tests directory to path for imports
_tests_path = Path(__file__).parent.parent.parent / "tests"
if str(_tests_path) not in sys.path:
    sys.path.insert(0, str(_tests_path))

from deprecated.serializer import add_date_info

from helpers.sample_records import (
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
    rec583,
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
    """Test add_date_info function."""
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
    """Test serialize_json function."""
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

