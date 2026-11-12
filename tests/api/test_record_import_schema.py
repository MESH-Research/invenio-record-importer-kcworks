"""Behavioral tests for `RecordImportSchema` create-payload validation."""

import base64
import binascii
from copy import deepcopy

import pytest
from invenio_rdm_records.services.schemas import RDMRecordSchema
from marshmallow import ValidationError

from invenio_record_importer_kcworks.schemas import RecordImportSchema
from tests.fixtures.files import file_md5

_PART_BYTES = b"hello-part"
_MD5_HEX = file_md5(_PART_BYTES)
_MD5_B64 = base64.b64encode(binascii.unhexlify(_MD5_HEX)).decode("ascii")


def _with_import_recid(record_meta, source_id: str):
    """Attach an `import-recid` identifier to a `TestRecordMetadata` instance.

    Returns:
        The same metadata object after update.
    """
    record_meta.update_metadata(
        {
            "metadata|identifiers": [
                {"identifier": source_id, "scheme": "import-recid"},
            ]
        }
    )
    return record_meta


def _create_payload(metadata=None, **overrides) -> dict:
    """Build a v2 create body for `RecordImportSchema.load`.

    Returns:
        Create payload dict.
    """
    body = {
        "api_version": 2,
        "metadata": metadata if metadata is not None else [],
        "id_scheme": "import-recid",
        "alternate_id_scheme": "",
    }
    body.update(overrides)
    return body


def _load(payload: dict, identity) -> dict:
    """Load a create payload with service identity context.

    Returns:
        Normalized create dict from `RecordImportSchema`.
    """
    return RecordImportSchema(context={"identity": identity}).load(payload)


def _staging_file(filename: str, size: int, parts: list[dict]) -> dict:
    """One create-body `files[]` declaration.

    Returns:
        Staging file dict with `filename`, `size`, and `parts`.
    """
    return {
        "filename": filename,
        "size": size,
        "parts": parts,
    }


def test_load_accepts_minimal_create_payload(
    running_app, superuser_identity, record_metadata
):
    """Valid metadata-only create body loads with defaults applied."""
    meta = _with_import_recid(record_metadata(), "src-1")
    result = _load(_create_payload(metadata=[meta.metadata_in]), superuser_identity)

    assert result["api_version"] == 2
    assert result["id_scheme"] == "import-recid"
    assert result["files"] == []
    assert result["metadata"][0]["metadata"]["title"] == "A Romans story"
    assert result["metadata"][0]["metadata"]["identifiers"] == [
        {"identifier": "src-1", "scheme": "import-recid"},
    ]


def test_load_requires_import_source_id(
    running_app, superuser_identity, record_metadata
):
    """Records without an identifier for the create id_scheme fail."""
    meta = record_metadata()
    meta.update_metadata(
        {
            "metadata|identifiers": [
                {"identifier": "10.1/xyz", "scheme": "doi"},
            ]
        }
    )

    with pytest.raises(ValidationError) as exc_info:
        _load(_create_payload(metadata=[meta.metadata_in]), superuser_identity)

    metadata_errors = str(exc_info.value.messages["metadata"])
    assert "import identifier for scheme" in metadata_errors
    assert "import-recid" in metadata_errors


def test_load_accepts_alternate_id_scheme(
    running_app, superuser_identity, record_metadata
):
    """Alternate scheme satisfies the required import source id."""
    meta = record_metadata()
    meta.update_metadata(
        {
            "metadata|identifiers": [
                {"identifier": "legacy-99", "scheme": "hclegacy-record-id"},
            ]
        }
    )
    payload = _create_payload(
        metadata=[meta.metadata_in],
        alternate_id_scheme="hclegacy-record-id",
    )

    result = _load(payload, superuser_identity)
    assert result["metadata"][0]["metadata"]["identifiers"][0]["identifier"] == (
        "legacy-99"
    )


def test_load_rejects_duplicate_source_ids(
    running_app, superuser_identity, record_metadata
):
    """Two metadata items with the same import source id fail."""
    first = _with_import_recid(record_metadata(), "dup").metadata_in
    second = _with_import_recid(record_metadata(), "dup").metadata_in

    with pytest.raises(ValidationError) as exc_info:
        _load(_create_payload(metadata=[first, second]), superuser_identity)

    metadata_errors = str(exc_info.value.messages["metadata"])
    assert "duplicate import source id" in metadata_errors
    assert "dup" in metadata_errors


def test_load_rejects_invalid_rdm_metadata(
    running_app, superuser_identity, record_metadata
):
    """Nested RDM validation fails for an incomplete metadata record."""
    meta = _with_import_recid(record_metadata(), "bad-meta")
    meta.update_metadata({"metadata|title": None})
    # Drop required nested fields so RDM schema rejects the record.
    payload_record = meta.metadata_in
    payload_record["metadata"].pop("creators", None)
    payload_record["metadata"].pop("resource_type", None)
    payload_record["metadata"]["title"] = ""

    with pytest.raises(ValidationError) as exc_info:
        _load(_create_payload(metadata=[payload_record]), superuser_identity)

    metadata_errors = exc_info.value.messages["metadata"]
    # Nested under metadata[0].metadata (RDM nested field errors).
    nested = str(metadata_errors)
    assert "creators" in nested
    assert "resource_type" in nested or "title" in nested


def test_stock_rdm_schema_drops_dump_only_import_fields(
    running_app, superuser_identity, record_metadata_with_files
):
    """Upstream `RDMRecordSchema.load` strips dump-only import fields."""
    meta = _with_import_recid(
        record_metadata_with_files(
            file_entries={
                "paper.pdf": {"mimetype": "application/pdf", "size": 100},
            }
        ),
        "drop-check",
    )
    meta.update_metadata(
        {
            "parent|access": {"owned_by": [{"user": "1011375"}]},
            "parent|communities": {"ids": ["comm-1"], "default": "comm-1"},
            "created": "2022-09-29T14:34:36Z",
        }
    )
    raw = deepcopy(meta.metadata_in)

    loaded = RDMRecordSchema(context={"identity": superuser_identity}).load(raw)

    assert "created" not in loaded
    assert "entries" not in loaded.get("files", {})
    parent = loaded.get("parent") or {}
    assert "access" not in parent
    assert "communities" not in parent


def test_load_restores_dump_only_import_fields(
    running_app, superuser_identity, record_metadata_with_files
):
    """`RecordImportSchema` restores dump-only fields after RDM load."""
    meta = _with_import_recid(
        record_metadata_with_files(
            file_entries={
                "paper.pdf": {"mimetype": "application/pdf", "size": 100},
            }
        ),
        "src-restore",
    )
    meta.update_metadata(
        {
            "parent|access": {"owned_by": [{"user": "1011375"}]},
            "parent|communities": {"ids": ["comm-1"], "default": "comm-1"},
            "created": "2022-09-29T14:34:36Z",
        }
    )
    payload = _create_payload(
        metadata=[meta.metadata_in],
        files=[
            _staging_file(
                "paper.pdf",
                100,
                [{"part_number": 1, "size": 100, "checksum": f"md5:{_MD5_HEX}"}],
            )
        ],
    )

    result = _load(payload, superuser_identity)
    loaded = result["metadata"][0]

    assert loaded["files"]["entries"]["paper.pdf"] == {
        "mimetype": "application/pdf",
        "size": 100,
    }
    assert loaded["parent"]["access"]["owned_by"] == [{"user": "1011375"}]
    assert loaded["parent"]["communities"]["ids"] == ["comm-1"]
    assert loaded["created"] == "2022-09-29T14:34:36Z"


def _import_recid_from_loaded(record: dict) -> str:
    """Read `import-recid` from a loaded metadata item.

    Returns:
        The identifier string for scheme `import-recid`.

    Raises:
        AssertionError: When the loaded record has no `import-recid`.
    """
    for item in record["metadata"]["identifiers"]:
        if item.get("scheme") == "import-recid":
            return item["identifier"]
    raise AssertionError("missing import-recid on loaded record")


def test_load_restores_dump_only_fields_per_source_id(
    running_app, superuser_identity, record_metadata_with_files
):
    """Dump-only restore is keyed by source id across multiple records."""
    first = _with_import_recid(
        record_metadata_with_files(
            file_entries={
                "a.pdf": {"mimetype": "application/pdf", "size": 10},
            }
        ),
        "src-a",
    )
    first.update_metadata(
        {
            "parent|access": {"owned_by": [{"user": "1"}]},
            "created": "2020-01-01T00:00:00Z",
        }
    )
    second = _with_import_recid(
        record_metadata_with_files(
            file_entries={
                "b.pdf": {"mimetype": "application/pdf", "size": 20},
            }
        ),
        "src-b",
    )
    second.update_metadata(
        {
            "parent|access": {"owned_by": [{"user": "2"}]},
            "created": "2021-01-01T00:00:00Z",
        }
    )
    payload = _create_payload(
        metadata=[first.metadata_in, second.metadata_in],
        files=[
            _staging_file(
                "a.pdf",
                10,
                [{"part_number": 1, "size": 10, "checksum": _MD5_B64}],
            ),
            _staging_file(
                "b.pdf",
                20,
                [{"part_number": 1, "size": 20, "checksum": _MD5_B64}],
            ),
        ],
    )

    result = _load(payload, superuser_identity)
    by_id = {_import_recid_from_loaded(rec): rec for rec in result["metadata"]}

    assert by_id["src-a"]["files"]["entries"]["a.pdf"] == {
        "mimetype": "application/pdf",
        "size": 10,
    }
    assert by_id["src-a"]["parent"]["access"]["owned_by"] == [{"user": "1"}]
    assert by_id["src-a"]["created"] == "2020-01-01T00:00:00Z"
    assert by_id["src-b"]["files"]["entries"]["b.pdf"] == {
        "mimetype": "application/pdf",
        "size": 20,
    }
    assert by_id["src-b"]["parent"]["access"]["owned_by"] == [{"user": "2"}]
    assert by_id["src-b"]["created"] == "2021-01-01T00:00:00Z"


def test_load_requires_create_files_when_metadata_has_entries(
    running_app, superuser_identity, record_metadata_with_files
):
    """Top-level files is required when a metadata record has files.entries."""
    meta = _with_import_recid(
        record_metadata_with_files(
            file_entries={
                "paper.pdf": {"mimetype": "application/pdf", "size": 100},
            }
        ),
        "needs-files",
    )

    with pytest.raises(ValidationError) as exc_info:
        _load(_create_payload(metadata=[meta.metadata_in]), superuser_identity)

    assert "files" in exc_info.value.messages
    assert "needs-files" in str(exc_info.value.messages["files"])


def test_load_normalizes_part_checksums_to_content_md5(
    running_app, superuser_identity, record_metadata
):
    """Part checksums accept md5:<hex> and normalize to base64 ContentMD5."""
    meta = _with_import_recid(record_metadata(), "src-checksum")
    payload = _create_payload(
        metadata=[meta.metadata_in],
        files=[
            _staging_file(
                "hello.bin",
                len(_PART_BYTES),
                [
                    {
                        "part_number": 1,
                        "size": len(_PART_BYTES),
                        "checksum": f"md5:{_MD5_HEX}",
                    }
                ],
            )
        ],
    )

    result = _load(payload, superuser_identity)
    assert result["files"][0]["parts"][0]["checksum"] == _MD5_B64


def test_load_rejects_invalid_part_checksum(
    running_app, superuser_identity, record_metadata
):
    """Malformed part checksums fail validation."""
    meta = _with_import_recid(record_metadata(), "src-bad-checksum")
    payload = _create_payload(
        metadata=[meta.metadata_in],
        files=[
            _staging_file(
                "hello.bin",
                5,
                [{"part_number": 1, "size": 5, "checksum": "not-a-checksum"}],
            )
        ],
    )

    with pytest.raises(ValidationError) as exc_info:
        _load(payload, superuser_identity)

    files_errors = str(exc_info.value.messages["files"])
    assert "checksum" in files_errors
    assert "not-a-checksum" in files_errors


def test_load_accepts_multipart_layout(
    running_app, superuser_identity, record_metadata
):
    """Contiguous multi-part declarations load with normalized checksums."""
    meta = _with_import_recid(record_metadata(), "src-mpu")
    payload = _create_payload(
        metadata=[meta.metadata_in],
        files=[
            _staging_file(
                "big.bin",
                15,
                [
                    {"part_number": 1, "size": 5, "checksum": f"md5:{_MD5_HEX}"},
                    {"part_number": 2, "size": 5, "checksum": _MD5_B64},
                    {"part_number": 3, "size": 5, "checksum": f"md5:{_MD5_HEX}"},
                ],
            )
        ],
    )

    result = _load(payload, superuser_identity)
    parts = result["files"][0]["parts"]
    assert [p["part_number"] for p in parts] == [1, 2, 3]
    assert sum(p["size"] for p in parts) == 15
    assert all(p["checksum"] == _MD5_B64 for p in parts)


def test_load_rejects_noncontiguous_part_numbers(
    running_app, superuser_identity, record_metadata
):
    """Part numbers must be contiguous 1..N."""
    meta = _with_import_recid(record_metadata(), "src-gap")
    payload = _create_payload(
        metadata=[meta.metadata_in],
        files=[
            _staging_file(
                "big.bin",
                10,
                [
                    {"part_number": 1, "size": 5, "checksum": _MD5_B64},
                    {"part_number": 3, "size": 5, "checksum": _MD5_B64},
                ],
            )
        ],
    )

    with pytest.raises(ValidationError) as exc_info:
        _load(payload, superuser_identity)

    files_errors = str(exc_info.value.messages["files"])
    assert "contiguous" in files_errors


def test_load_rejects_inconsistent_parts_layout(
    running_app, superuser_identity, record_metadata
):
    """Part sizes must sum to the declared file size."""
    meta = _with_import_recid(record_metadata(), "src-parts")
    payload = _create_payload(
        metadata=[meta.metadata_in],
        files=[
            _staging_file(
                "hello.bin",
                10,
                [{"part_number": 1, "size": 5, "checksum": _MD5_B64}],
            )
        ],
    )

    with pytest.raises(ValidationError) as exc_info:
        _load(payload, superuser_identity)

    files_errors = str(exc_info.value.messages["files"])
    assert "sum of part sizes" in files_errors
    assert "does not match size" in files_errors


def test_load_rejects_non_v2_api_version(
    running_app, superuser_identity, record_metadata
):
    """Only api_version 2 is accepted on the async create path."""
    meta = _with_import_recid(record_metadata(), "src-v1")
    with pytest.raises(ValidationError) as exc_info:
        _load(
            _create_payload(metadata=[meta.metadata_in], api_version=1),
            superuser_identity,
        )

    api_version_errors = str(exc_info.value.messages["api_version"])
    assert "must be 2" in api_version_errors


def test_load_rejects_unknown_top_level_keys(
    running_app, superuser_identity, record_metadata
):
    """Unknown create-body keys raise rather than being dropped."""
    meta = _with_import_recid(record_metadata(), "src-unknown")
    payload = _create_payload(metadata=[meta.metadata_in])
    payload["not_a_field"] = True

    with pytest.raises(ValidationError) as exc_info:
        _load(payload, superuser_identity)

    assert "not_a_field" in exc_info.value.messages
