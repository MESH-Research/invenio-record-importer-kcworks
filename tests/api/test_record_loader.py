# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for record_loader module.

Unit tests for record_loader-specific functionality.
"""

import json

import pytest

from invenio_record_importer_kcworks.utils.utils import api_request

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
    """Test record_loader.api_request function."""
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

