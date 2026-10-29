"""In-memory/cache-backed import job status for async URL fetches."""

from __future__ import annotations

import uuid
from typing import Any

from flask import current_app

_JOB_KEY_PREFIX = "record-importer-job:"
_DEFAULT_TTL_SECONDS = 60 * 60 * 24  # 24h


def _cache():
    """Return the application cache if available, else None."""
    return current_app.extensions.get("invenio-cache")


def _job_key(job_id: str) -> str:
    return f"{_JOB_KEY_PREFIX}{job_id}"


def create_import_job(
    *,
    community_id: str = "",
    pending_fetches: list[dict[str, Any]] | None = None,
) -> str:
    """Create a job record for tracking async file fetches.

    Args:
        community_id: Target community id/slug for context.
        pending_fetches: Initial list of fetch descriptors
            (``record_id``, ``file_key``, ``url``, ``status``).

    Returns:
        New job id string.
    """
    job_id = str(uuid.uuid4())
    payload = {
        "id": job_id,
        "status": "pending" if pending_fetches else "completed",
        "community_id": community_id,
        "fetches": list(pending_fetches or []),
        "message": "",
    }
    _store_job(job_id, payload)
    return job_id


def get_import_job(job_id: str) -> dict[str, Any] | None:
    """Load a job by id.

    Args:
        job_id: Job identifier.

    Returns:
        Job dict, or None if missing.
    """
    cache = _cache()
    if cache is not None:
        data = cache.get(_job_key(job_id))
        if data is not None:
            return dict(data)
    store = current_app.extensions.setdefault("_record_importer_jobs", {})
    job = store.get(job_id)
    return dict(job) if job else None


def update_fetch_status(
    job_id: str,
    *,
    record_id: str,
    file_key: str,
    status: str,
    error: str = "",
) -> dict[str, Any] | None:
    """Update one fetch entry and recompute overall job status.

    Args:
        job_id: Job identifier.
        record_id: Draft/record id for the file.
        file_key: File key.
        status: New status (``pending``, ``completed``, ``failed``).
        error: Optional error message.

    Returns:
        Updated job dict, or None if job missing.
    """
    job = get_import_job(job_id)
    if job is None:
        return None
    for fetch in job.get("fetches", []):
        if fetch.get("record_id") == record_id and fetch.get("file_key") == file_key:
            fetch["status"] = status
            if error:
                fetch["error"] = error
            break
    statuses = [f.get("status") for f in job.get("fetches", [])]
    if not statuses or all(s == "completed" for s in statuses):
        job["status"] = "completed"
    elif any(s == "failed" for s in statuses) and all(
        s in ("completed", "failed") for s in statuses
    ):
        job["status"] = "multi_status"
    elif any(s == "failed" for s in statuses):
        job["status"] = "pending"
    else:
        job["status"] = "pending"
    _store_job(job_id, job)
    return job


def _store_job(job_id: str, payload: dict[str, Any]) -> None:
    ttl = current_app.config.get(
        "RECORD_IMPORTER_JOB_TTL_SECONDS", _DEFAULT_TTL_SECONDS
    )
    cache = _cache()
    if cache is not None:
        cache.set(_job_key(job_id), payload, timeout=ttl)
    store = current_app.extensions.setdefault("_record_importer_jobs", {})
    store[job_id] = payload
