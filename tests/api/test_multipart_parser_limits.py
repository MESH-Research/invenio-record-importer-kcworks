# Part of invenio-record-importer-kcworks.
# Copyright (C) 2026, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests that import multipart parsing applies Flask/Werkzeug body limits."""

import pytest
from flask import request
from werkzeug.exceptions import RequestEntityTooLarge

from invenio_record_importer_kcworks.parser import RequestMultipartParser

# Werkzeug's default `Request.max_form_memory_size` (non-file multipart parts).
_WERKZEUG_DEFAULT_FORM_MEMORY = 500_000


def test_multipart_parser_applies_config_limits(app, monkeypatch):
    """Parser copies importer config onto the request before reading the body."""
    form_limit = 8 * 1024
    content_limit = 64 * 1024
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_FORM_MEMORY_SIZE", form_limit
    )
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_CONTENT_LENGTH", content_limit
    )

    with app.test_request_context(
        "/api/import/example",
        method="POST",
        data={"metadata": "[]"},
        content_type="multipart/form-data",
    ):
        parsed = RequestMultipartParser().parse()
        assert request.max_form_memory_size == form_limit
        assert request.max_content_length == content_limit
        assert parsed["form"]["metadata"] == "[]"


def test_multipart_parser_rejects_oversized_form_field(app, monkeypatch):
    """Non-file parts above `RECORD_IMPORTER_MAX_FORM_MEMORY_SIZE` raise 413."""
    form_limit = 1024
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_FORM_MEMORY_SIZE", form_limit
    )
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_CONTENT_LENGTH", 10 * 1024 * 1024
    )

    with app.test_request_context(
        "/api/import/example",
        method="POST",
        data={"metadata": "x" * (form_limit + 1)},
        content_type="multipart/form-data",
    ):
        with pytest.raises(RequestEntityTooLarge):
            RequestMultipartParser().parse()


def test_multipart_parser_rejects_oversized_content_length(app, monkeypatch):
    """Bodies above `RECORD_IMPORTER_MAX_CONTENT_LENGTH` raise 413."""
    content_limit = 100
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_CONTENT_LENGTH", content_limit
    )
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_FORM_MEMORY_SIZE", 10 * 1024 * 1024
    )

    with app.test_request_context(
        "/api/import/example",
        method="POST",
        data={"metadata": "y" * 200},
        content_type="multipart/form-data",
    ):
        assert request.content_length > content_limit
        with pytest.raises(RequestEntityTooLarge):
            RequestMultipartParser().parse()


def test_multipart_parser_allows_form_field_above_werkzeug_default(
    app, monkeypatch
):
    """Form parts above Werkzeug's 500 KiB default are accepted under the raise."""
    form_limit = 10 * 1024 * 1024
    monkeypatch.setitem(
        app.config, "RECORD_IMPORTER_MAX_FORM_MEMORY_SIZE", form_limit
    )
    monkeypatch.setitem(
        app.config,
        "RECORD_IMPORTER_MAX_CONTENT_LENGTH",
        500 * (1024**3),
    )
    assert form_limit > _WERKZEUG_DEFAULT_FORM_MEMORY
    payload = "z" * (_WERKZEUG_DEFAULT_FORM_MEMORY + 100_000)

    with app.test_request_context(
        "/api/import/example",
        method="POST",
        data={"metadata": payload},
        content_type="multipart/form-data",
    ):
        parsed = RequestMultipartParser().parse()
        assert request.max_form_memory_size == form_limit
        assert len(parsed["form"]["metadata"]) == len(payload)
