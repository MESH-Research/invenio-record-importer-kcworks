"""S3 staging helpers for community-scoped async imports.

Bytes go client → S3 via presigned URLs under the imports/ prefix on the
existing Invenio bucket. This module only initiates/completes/aborts MPU
(and single-shot PUT) and builds object keys — it does not proxy file bytes.
"""

import base64
import binascii
import math
from datetime import UTC, datetime, timedelta
from typing import Any

from flask import current_app
from werkzeug.utils import secure_filename


def get_s3_client() -> Any:
    """Build a boto3 S3 client from Invenio S3 app config.

    `boto3` is imported only when staging actually talks to S3, so loading this
    module (or the importer extension) does not require it at install time.

    Returns:
        Configured S3 client.

    Raises:
        ImportError: When `boto3` is not installed.
    """
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for async import S3 staging operations"
        ) from exc

    cfg = current_app.config
    return boto3.client(
        "s3",
        endpoint_url=cfg.get("S3_ENDPOINT_URL"),
        region_name=cfg.get("S3_REGION_NAME"),
        aws_access_key_id=cfg.get("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=cfg.get("S3_SECRET_ACCESS_KEY"),
    )


def safe_filename(filename: str) -> str:
    """Sanitize a client filename for use as an object key segment.

    Args:
        filename: Raw client-provided filename.

    Returns:
        Secure basename.

    Raises:
        ValueError: When the filename sanitizes to empty.
    """
    name = secure_filename(filename or "")
    if not name:
        raise ValueError("Invalid or empty filename")
    return name


def build_object_key(community_id: str, import_id: str, filename: str) -> str:
    """Build the full S3 object key under the staging prefix.

    Args:
        community_id: Community UUID string.
        import_id: Import UUID string.
        filename: Already-sanitized filename.

    Returns:
        Object key such as `imports/<community>/<import>/<filename>`.
    """
    prefix = current_app.config["RECORD_IMPORTER_STAGING_PREFIX"]
    return f"{prefix}{community_id}/{import_id}/{filename}"


def compute_part_layout(size: int) -> tuple[int, int]:
    """Suggest MPU part size and count for a file size (client helper).

    The server does not choose parts — the client splits the file, checksums
    each part, and sends that layout in `files[].parts` at create/init. This
    helper mirrors the usual 5 MiB / max-parts guidance so clients can pick a
    layout that stays within S3 limits.

    Args:
        size: Total object size in bytes (>= 0).

    Returns:
        `(part_size, num_parts)`.

    Raises:
        ValueError: When `size` is negative.
    """
    if size < 0:
        raise ValueError("size must be >= 0")
    block = int(current_app.config["RECORD_IMPORTER_STAGING_PART_SIZE"])
    parts_cap = int(current_app.config["RECORD_IMPORTER_STAGING_MAX_PARTS"])
    if size == 0:
        return block, 1
    # Ensure we never exceed max_parts; grow part size if needed.
    part_size = max(block, math.ceil(size / parts_cap))
    # Keep part_size a multiple of the default block when grown.
    if part_size > block and part_size % block:
        part_size = int(math.ceil(part_size / block) * block)
    num_parts = max(1, math.ceil(size / part_size))
    return int(part_size), int(num_parts)


def _expiration_iso(expires_in: int) -> str:
    """UTC expiration timestamp ending in `Z` (AWS-style).

    Args:
        expires_in: Seconds from now.

    Returns:
        ISO-8601 UTC string with `Z` suffix.
    """
    expiration = datetime.now(UTC) + timedelta(seconds=expires_in)
    return expiration.replace(tzinfo=None).isoformat(timespec="seconds") + "Z"


def normalize_md5_as_b64(checksum: str) -> str:
    """Normalize a client MD5 checksum to S3 `ContentMD5` (base64).

    Accepts either a raw base64 digest or `md5:<hex>`.

    Args:
        checksum: Client-provided checksum string.

    Returns:
        Base64-encoded 16-byte MD5 digest for the `ContentMD5` param/header.

    Raises:
        ValueError: When the checksum cannot be parsed.
    """
    value = (checksum or "").strip()
    if not value:
        raise ValueError("checksum must be non-empty")
    if value.lower().startswith("md5:"):
        hex_digest = value.split(":", 1)[1].strip()
        try:
            raw = binascii.unhexlify(hex_digest)
        except binascii.Error as exc:
            raise ValueError(f"invalid md5 hex checksum: {checksum!r}") from exc
        if len(raw) != 16:
            raise ValueError(f"md5 hex checksum must be 32 hex chars: {checksum!r}")
        return base64.b64encode(raw).decode("ascii")
    try:
        raw = base64.b64decode(value, validate=True)
    except binascii.Error as exc:
        raise ValueError(
            f"checksum must be base64 MD5 or md5:<hex>: {checksum!r}"
        ) from exc
    if len(raw) != 16:
        raise ValueError(f"base64 checksum must decode to 16 bytes: {checksum!r}")
    return value


def create_multipart_upload(
    key: str,
    *,
    content_type: str | None = None,
    client: Any | None = None,
) -> str:
    """Start an S3 multipart upload for a staging key.

    Args:
        key: Full object key.
        content_type: Optional ContentType for the object.
        client: Optional boto3 client (injected in tests).

    Returns:
        S3 upload id.
    """
    s3 = client or get_s3_client()
    params: dict[str, Any] = {
        "Bucket": current_app.config["RECORD_IMPORTER_STAGING_BUCKET"],
        "Key": key,
    }
    if content_type:
        params["ContentType"] = content_type
    response = s3.create_multipart_upload(**params)
    return response["UploadId"]


def presign_parts(
    key: str,
    upload_id: str,
    part_specs: list[dict[str, Any]],
    *,
    client: Any | None = None,
    expires_in: int | None = None,
) -> list[dict[str, Any]]:
    """Generate presigned `upload_part` URLs for client-declared parts.

    Each spec must include `part_number` and `checksum` (base64 MD5 or
    `md5:<hex>`). `ContentMD5` is included in the signed params so the client
    must send the same header on the PUT.

    Args:
        key: Full object key.
        upload_id: MPU upload id.
        part_specs: `[{part_number, checksum, size?}, …]` from the client.
        client: Optional boto3 client.
        expires_in: Optional override for URL lifetime.

    Returns:
        List of `{part_number, url, expiration, checksum, size?}` dicts.

    Raises:
        ValueError: When a part spec is missing number/checksum.
    """
    s3 = client or get_s3_client()
    ttl = (
        expires_in
        if expires_in is not None
        else int(current_app.config["RECORD_IMPORTER_STAGING_URL_EXPIRATION"])
    )
    expiration = _expiration_iso(ttl)
    bucket = current_app.config["RECORD_IMPORTER_STAGING_BUCKET"]
    parts: list[dict[str, Any]] = []
    for spec in part_specs:
        part_number = spec.get("part_number")
        checksum = spec.get("checksum")
        if part_number is None or checksum is None:
            raise ValueError(
                "each part requires part_number and checksum (Content-MD5)"
            )
        content_md5 = normalize_md5_as_b64(str(checksum))
        url = s3.generate_presigned_url(
            "upload_part",
            Params={
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": int(part_number),
                "ContentMD5": content_md5,
            },
            ExpiresIn=ttl,
        )
        part_out: dict[str, Any] = {
            "part_number": int(part_number),
            "url": url,
            "expiration": expiration,
            "checksum": content_md5,
        }
        if "size" in spec:
            part_out["size"] = int(spec["size"])
        parts.append(part_out)
    return parts


def presign_put_object(
    key: str,
    *,
    content_type: str | None = None,
    content_md5: str | None = None,
    client: Any | None = None,
    expires_in: int | None = None,
) -> dict[str, Any]:
    """Generate a single-shot presigned `put_object` URL (small files).

    Args:
        key: Full object key.
        content_type: Optional ContentType included in the signed params.
        content_md5: Optional checksum (base64 or `md5:<hex>`); signed as
            `ContentMD5` when provided.
        client: Optional boto3 client.
        expires_in: Optional override for URL lifetime.

    Returns:
        `{part_number: 1, url, expiration, checksum?}` shaped like an MPU
        part entry.
    """
    s3 = client or get_s3_client()
    ttl = (
        expires_in
        if expires_in is not None
        else int(current_app.config["RECORD_IMPORTER_STAGING_URL_EXPIRATION"])
    )
    params: dict[str, Any] = {
        "Bucket": current_app.config["RECORD_IMPORTER_STAGING_BUCKET"],
        "Key": key,
    }
    if content_type:
        params["ContentType"] = content_type
    normalized_md5 = None
    if content_md5:
        normalized_md5 = normalize_md5_as_b64(content_md5)
        params["ContentMD5"] = normalized_md5
    url = s3.generate_presigned_url(
        "put_object",
        Params=params,
        ExpiresIn=ttl,
    )
    out: dict[str, Any] = {
        "part_number": 1,
        "url": url,
        "expiration": _expiration_iso(ttl),
    }
    if normalized_md5:
        out["checksum"] = normalized_md5
    return out


def complete_multipart_upload(
    key: str,
    upload_id: str,
    parts: list[dict[str, Any]],
    *,
    client: Any | None = None,
) -> dict[str, Any]:
    """Complete an S3 multipart upload.

    Args:
        key: Full object key.
        upload_id: MPU upload id.
        parts: Part dicts with `part_number` / `etag` (mapped to S3
            `PartNumber` / `ETag` for the complete call).
        client: Optional boto3 client.

    Returns:
        S3 complete response (includes `ETag` when available).

    Raises:
        ValueError: When a part is missing number or etag.
    """
    s3 = client or get_s3_client()
    normalized: list[dict[str, Any]] = []
    for part in parts:
        part_number = part.get("part_number")
        etag = part.get("etag")
        if part_number is None or etag is None:
            raise ValueError("Each part must include part_number and etag")
        entry: dict[str, Any] = {"PartNumber": int(part_number), "ETag": etag}
        for checksum_key in (
            "ChecksumCRC32",
            "ChecksumCRC32C",
            "ChecksumSHA1",
            "ChecksumSHA256",
        ):
            if checksum_key in part:
                entry[checksum_key] = part[checksum_key]
        normalized.append(entry)

    response = s3.complete_multipart_upload(
        Bucket=current_app.config["RECORD_IMPORTER_STAGING_BUCKET"],
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": normalized},
    )
    return response


def abort_multipart_upload(
    key: str,
    upload_id: str,
    *,
    client: Any | None = None,
) -> None:
    """Abort an in-progress multipart upload.

    Args:
        key: Full object key.
        upload_id: MPU upload id.
        client: Optional boto3 client.
    """
    s3 = client or get_s3_client()
    s3.abort_multipart_upload(
        Bucket=current_app.config["RECORD_IMPORTER_STAGING_BUCKET"],
        Key=key,
        UploadId=upload_id,
    )


def delete_object(key: str, *, client: Any | None = None) -> None:
    """Delete a staged object (best-effort cleanup).

    Args:
        key: Full object key.
        client: Optional boto3 client.
    """
    s3 = client or get_s3_client()
    s3.delete_object(
        Bucket=current_app.config["RECORD_IMPORTER_STAGING_BUCKET"],
        Key=key,
    )


def head_object(key: str, *, client: Any | None = None) -> dict[str, Any]:
    """HEAD a staged object (size / etag after single-shot PUT).

    Args:
        key: Full object key.
        client: Optional boto3 client.

    Returns:
        S3 head_object response.
    """
    s3 = client or get_s3_client()
    return s3.head_object(
        Bucket=current_app.config["RECORD_IMPORTER_STAGING_BUCKET"],
        Key=key,
    )
