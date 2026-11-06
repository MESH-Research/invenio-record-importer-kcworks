"""Helpers for remote file URL sources on import metadata entries."""

from typing import Any
from urllib.parse import urlparse

from flask import current_app


class FetchURLNotAllowedError(ValueError):
    """Raised when a fetch URL fails allowlist or scheme checks."""


def entry_source_url(entry: dict[str, Any] | None) -> str | None:
    """Return a remote source URL from a ``files.entries`` value if present.

    Supports either ``source_url`` or Invenio-style
    ``transfer: {"type": "F", "url": "..."}``.

    Args:
        entry: One files.entries value dict, or None.

    Returns:
        URL string, or None when the entry is a normal upload.
    """
    if not entry or not isinstance(entry, dict):
        return None
    source_url = entry.get("source_url")
    if isinstance(source_url, str) and source_url.strip():
        return source_url.strip()
    transfer = entry.get("transfer")
    if isinstance(transfer, dict):
        transfer_type = transfer.get("type")
        url = transfer.get("url")
        if (
            transfer_type in ("F", "fetch", "url")
            and isinstance(url, str)
            and url.strip()
        ):
            return url.strip()
    return None


def split_upload_and_url_entries(
    entries: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Partition ``files.entries`` into upload entries vs URL sources.

    Args:
        entries: Record ``files.entries`` mapping (key → entry dict).

    Returns:
        ``(upload_entries, url_by_key)`` where ``url_by_key`` maps entry key
        (or entry ``key`` field) to the remote URL.
    """
    if not entries:
        return {}, {}
    upload_entries: dict[str, Any] = {}
    url_by_key: dict[str, str] = {}
    for key, entry in entries.items():
        url = entry_source_url(entry if isinstance(entry, dict) else None)
        file_key = (
            entry.get("key", key) if isinstance(entry, dict) else key
        )
        if url:
            url_by_key[str(file_key)] = url
        else:
            upload_entries[key] = entry
    return upload_entries, url_by_key


def validate_fetch_url(url: str) -> None:
    """Validate scheme and host/bucket against importer allowlists.

    Args:
        url: Candidate fetch URL (``https://``, ``http://``, or ``s3://``).

    Raises:
        FetchURLNotAllowedError: If the URL is missing, uses a bad scheme,
            or is not on the configured allowlist.
    """
    if not url or not isinstance(url, str):
        raise FetchURLNotAllowedError("Fetch URL is empty")

    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("https", "http", "s3"):
        raise FetchURLNotAllowedError(
            f"Fetch URL scheme not allowed: {scheme or '(none)'}"
        )

    if scheme == "s3":
        bucket = parsed.netloc
        allowed_buckets = current_app.config.get(
            "RECORD_IMPORTER_FETCH_ALLOWED_BUCKETS", []
        )
        if not allowed_buckets:
            raise FetchURLNotAllowedError(
                "s3:// fetch requires RECORD_IMPORTER_FETCH_ALLOWED_BUCKETS"
            )
        if bucket not in allowed_buckets:
            raise FetchURLNotAllowedError(
                f"S3 bucket not allowed for fetch: {bucket}"
            )
        return

    domain = parsed.netloc.lower()
    if "@" in domain:
        domain = domain.rsplit("@", 1)[-1]
    if ":" in domain:
        domain = domain.split(":", 1)[0]

    allowed_domains = current_app.config.get(
        "RECORD_IMPORTER_FETCH_ALLOWED_DOMAINS", []
    )
    if not allowed_domains:
        raise FetchURLNotAllowedError(
            "Fetch requires RECORD_IMPORTER_FETCH_ALLOWED_DOMAINS to be set"
        )
    if domain not in {d.lower() for d in allowed_domains}:
        raise FetchURLNotAllowedError(f"Fetch domain not allowed: {domain}")
