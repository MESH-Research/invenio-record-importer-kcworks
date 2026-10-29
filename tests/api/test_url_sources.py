"""Unit tests for remote URL source helpers."""

import pytest
from flask import Flask

from invenio_record_importer_kcworks.url_sources import (
    FetchURLNotAllowedError,
    entry_source_url,
    split_upload_and_url_entries,
    validate_fetch_url,
)


def test_entry_source_url_variants():
    """Accept ``source_url`` and transfer-style fetch URLs."""
    assert entry_source_url({"source_url": "https://cdn.example/a.pdf"}) == (
        "https://cdn.example/a.pdf"
    )
    assert entry_source_url(
        {"transfer": {"type": "F", "url": "https://cdn.example/b.pdf"}}
    ) == ("https://cdn.example/b.pdf")
    assert entry_source_url({"key": "local.pdf", "size": 1}) is None
    assert entry_source_url(None) is None


def test_split_upload_and_url_entries():
    """Partition upload entries from URL-backed entries."""
    entries = {
        "local.pdf": {"key": "local.pdf", "size": 10},
        "remote.pdf": {
            "key": "remote.pdf",
            "source_url": "https://cdn.example/remote.pdf",
        },
    }
    uploads, urls = split_upload_and_url_entries(entries)
    assert list(uploads.keys()) == ["local.pdf"]
    assert urls == {"remote.pdf": "https://cdn.example/remote.pdf"}


def test_validate_fetch_url_allowlist(monkeypatch):
    """Reject URLs when allowlist is empty or host is not listed."""
    app = Flask("test-url-sources")
    app.config["RECORD_IMPORTER_FETCH_ALLOWED_DOMAINS"] = []
    app.config["RECORD_IMPORTER_FETCH_ALLOWED_BUCKETS"] = []

    with app.app_context():
        with pytest.raises(FetchURLNotAllowedError):
            validate_fetch_url("https://cdn.example/a.pdf")

        app.config["RECORD_IMPORTER_FETCH_ALLOWED_DOMAINS"] = ["cdn.example"]
        validate_fetch_url("https://cdn.example/a.pdf")

        with pytest.raises(FetchURLNotAllowedError):
            validate_fetch_url("https://evil.example/a.pdf")

        with pytest.raises(FetchURLNotAllowedError):
            validate_fetch_url("s3://my-bucket/key")

        app.config["RECORD_IMPORTER_FETCH_ALLOWED_BUCKETS"] = ["my-bucket"]
        validate_fetch_url("s3://my-bucket/key")
