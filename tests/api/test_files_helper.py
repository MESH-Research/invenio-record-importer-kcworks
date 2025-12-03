# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for FilesHelper."""

from pprint import pprint

from invenio_record_importer_kcworks.services.files import FilesHelper
from invenio_record_importer_kcworks.services.records import RecordsHelper
from tests.helpers.sample_records import rec42615


def test_upload_draft_files(app, db, search_clear):
    """Test FilesHelper._upload_draft_files method."""
    my_record = rec42615["expected_serialized"]

    json_payload = {
        "custom_fields": my_record["custom_fields"],
        "metadata": my_record["metadata"],
        "pids": my_record["pids"],
    }
    json_payload["access"] = {"record": "public", "files": "public"}
    json_payload["files"] = {"enabled": True}

    actual_draft = RecordsHelper().create_invenio_record(json_payload, no_updates=False)
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

