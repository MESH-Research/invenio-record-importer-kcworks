"""Marshmallow schemas for the async import create payload.

Validates and normalizes the JSON create body via `load()`. Nested `files` /
`parts` schemas cover staging declarations. Each `metadata[]` item uses
`ImportRecordSchema` (`RDMRecordSchema` + stash/restore for dump-only import
fields: `files.entries`, `parent.access`, `parent.communities`, `created`).
"""

from copy import deepcopy
from typing import Any

from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.services.schemas import RDMRecordSchema
from marshmallow import (
    RAISE,
    Schema,
    ValidationError,
    fields,
    post_load,
    pre_load,
    validate,
    validates,
    validates_schema,
)

from invenio_record_importer_kcworks.services import staging as staging_helper


def record_source_id(
    metadata: dict,
    *,
    id_scheme: str = "import-recid",
    alternate_id_scheme: str = "",
) -> str:
    """Return the required import source id from `metadata.identifiers`.

    Looks up identifier for `id_scheme` (default `import-recid`) first, 
    and then falls back to optional `alternate_id_scheme` (same order as 
    `RecordLoader`).

    Args:
        metadata: The record's `metadata` object (not the whole record).
        id_scheme: Primary identifier scheme (default `import-recid`).
        alternate_id_scheme: Optional secondary scheme.

    Returns:
        Identifier string.

    Raises:
        ValidationError: When neither scheme yields an identifier.
    """
    schemes = [id_scheme] + ([alternate_id_scheme] if alternate_id_scheme else [])
    scheme_order = {s: i for i, s in enumerate(schemes)}
    identifiers = [
        i
        for i in metadata.get("identifiers") or []
        if i.get("identifier") and i.get("scheme") in schemes
    ]
    for item in sorted(identifiers, key=lambda x: scheme_order[x.get("scheme")]):
        return str(item["identifier"])
    
    raise ValidationError(
        "metadata.identifiers must include an import identifier for scheme "
        f"{', '.join(s for s in schemes)}"
    )


class ContentMD5Field(fields.String):
    """Normalize client checksums to S3 `ContentMD5` (base64 MD5)."""

    def _deserialize(
        self, value: Any, attr: str | None, data: dict | None, **kwargs: Any
    ) -> str:
        text = super()._deserialize(value, attr, data, **kwargs)
        try:
            return staging_helper.normalize_md5_as_b64(text)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc


class ImportFilePartSchema(Schema):
    """One client-declared MPU/PUT part with size and Content-MD5."""

    class Meta:
        """Fail validation on unknown part keys."""

        unknown = RAISE

    part_number = fields.Integer(required=True, validate=validate.Range(min=1))
    size = fields.Integer(required=True, validate=validate.Range(min=0))
    checksum = ContentMD5Field(required=True)


class ImportFileDeclarationSchema(Schema):
    """One staging file declaration (`files[]` item) with client part layout."""

    class Meta:
        """Fail validation on unknown file keys."""

        unknown = RAISE

    filename = fields.String(required=True)
    size = fields.Integer(required=True, validate=validate.Range(min=0))
    content_type = fields.String(load_default="application/octet-stream")
    is_zip = fields.Boolean(load_default=False)
    parts = fields.List(
        fields.Nested(ImportFilePartSchema),
        required=True,
        validate=validate.Length(min=1),
    )

    @validates_schema
    def _validate_parts_layout(self, data: dict, **kwargs: Any) -> None:
        """Require contiguous part numbers and sizes that sum to `size`.

        Raises:
            ValidationError: When part numbers or sizes are inconsistent.
        """
        parts = list(data.get("parts") or [])
        numbers = sorted(int(part["part_number"]) for part in parts)
        expected = list(range(1, len(parts) + 1))
        if numbers != expected:
            raise ValidationError(
                f"part_number values must be contiguous 1..{len(parts)}",
                field_name="parts",
            )
        total = sum(int(part["size"]) for part in parts)
        size = int(data["size"])
        if total != size:
            raise ValidationError(
                f"sum of part sizes ({total}) does not match size ({size})",
                field_name="parts",
            )


class ImportRecordSchemaMixin:
    """Stash dump-only import fields before RDM load; restore them after.

    Upstream RDM schemas strip dump-only keys on `pre_load` (e.g.
    `files.entries`, `parent.access`, `parent.communities`). Import payloads
    need those values later, so we copy them aside here and merge them back on
    `post_load`, keyed by the record's import source id
    (`metadata.identifiers` / create `id_scheme`).
    """

    def _source_id_schemes(self) -> tuple[str, str]:
        """Return `(id_scheme, alternate_id_scheme)` from load context."""
        return (
            str(self.context.get("id_scheme") or "import-recid"),
            str(self.context.get("alternate_id_scheme") or ""),
        )

    @pre_load
    def clean(self, input_data: Any, **kwargs: Any) -> Any:
        """Stash import-only fields, then drop dump_only keys like upstream.

        Overrides `BaseRecordSchema.clean` so stash runs on the raw payload
        before dump-only keys (e.g. `created`) are removed. Hook order among
        separate `@pre_load` methods is not guaranteed.

        Returns:
            Copy of input with dump-only schema fields removed.

        Raises:
            ValidationError: When the record has no import source id, or the
                source id is duplicated in this load.
        """
        if not isinstance(input_data, dict):
            return input_data

        metadata = input_data.get("metadata")
        if not isinstance(metadata, dict):
            raise ValidationError(
                "Missing data for required field.",
                field_name="metadata",
            )

        id_scheme, alternate_id_scheme = self._source_id_schemes()
        source_id = record_source_id(
            metadata,
            id_scheme=id_scheme,
            alternate_id_scheme=alternate_id_scheme,
        )
        seen = self.context.setdefault("_import_record_source_ids", set())
        if source_id in seen:
            raise ValidationError(
                f"duplicate import source id {source_id!r} in metadata[]"
            )
        seen.add(source_id)

        stash: dict[str, Any] = {}
        files = input_data.get("files")
        if isinstance(files, dict) and "entries" in files:
            stash["files_entries"] = deepcopy(files["entries"])
        parent = input_data.get("parent")
        if isinstance(parent, dict):
            if "access" in parent:
                stash["parent_access"] = deepcopy(parent["access"])
            if "communities" in parent:
                stash["parent_communities"] = deepcopy(parent["communities"])
        if "created" in input_data:
            stash["created"] = input_data["created"]
        if stash:
            by_id = self.context.setdefault("_import_record_stash", {})
            by_id[source_id] = stash

        data = deepcopy(input_data)
        for name, field in self.fields.items():
            if field.dump_only:
                data.pop(name, None)
        return data

    @post_load
    def restore_import_only_fields(self, data: dict, **kwargs: Any) -> dict:
        """Reattach stashed dump-only fields for this record's source id.

        Returns:
            Loaded record dict with import-only fields restored when present.
        """
        by_id = self.context.get("_import_record_stash") or {}
        if not by_id:
            return data

        id_scheme, alternate_id_scheme = self._source_id_schemes()
        source_id = record_source_id(
            data["metadata"],
            id_scheme=id_scheme,
            alternate_id_scheme=alternate_id_scheme,
        )
        stash = by_id.pop(source_id, None)
        if stash is None:
            return data
        if "files_entries" in stash:
            files = data.setdefault("files", {})
            if not isinstance(files, dict):
                files = {}
                data["files"] = files
            files["entries"] = stash["files_entries"]
        if "parent_access" in stash or "parent_communities" in stash:
            parent = data.setdefault("parent", {})
            if not isinstance(parent, dict):
                parent = {}
                data["parent"] = parent
            if "parent_access" in stash:
                parent["access"] = stash["parent_access"]
            if "parent_communities" in stash:
                parent["communities"] = stash["parent_communities"]
        if "created" in stash:
            data["created"] = stash["created"]
        return data


class ImportRecordSchema(ImportRecordSchemaMixin, RDMRecordSchema):
    """RDM record schema plus stash/restore of dump-only import fields."""


class RecordImportSchema(Schema):
    """Create-payload schema for async community imports (api_version 2).

    `load()` both validates and normalizes. Nested metadata items use
    `ImportRecordSchema`. Nested `files[]` normalize part checksums to
    Content-MD5. Persist the load result. Server-only row fields are not
    part of this schema.
    """

    class Meta:
        """Fail validation on unknown top-level keys."""

        unknown = RAISE

    api_version = fields.Integer(load_default=2)
    metadata = fields.List(fields.Nested(ImportRecordSchema), load_default=list)
    files = fields.List(
        fields.Nested(ImportFileDeclarationSchema),
        load_default=list,
    )
    review_required = fields.Boolean(load_default=True)
    strict_validation = fields.Boolean(load_default=True)
    all_or_none = fields.Boolean(load_default=True)
    no_updates = fields.Boolean(load_default=False)
    notify_record_owners = fields.Boolean(load_default=False)
    id_scheme = fields.String(load_default="import-recid")
    alternate_id_scheme = fields.String(load_default="")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Attach RDM service field-permission context when identity is set.

        Args:
            *args: Positional args for `Schema`.
            **kwargs: Keyword args for `Schema` (including `context`).
        """
        super().__init__(*args, **kwargs)
        identity = self.context.get("identity")
        if identity is not None and "field_permission_check" not in self.context:
            # Same permission context Nested RDM schemas get via the service.
            self.context.update(
                records_service.schema._build_context({"identity": identity})
            )

    @pre_load
    def _seed_source_id_schemes(self, data: Any, **kwargs: Any) -> Any:
        """Put create `id_scheme` / `alternate_id_scheme` on shared context.

        Nested record stash/restore keys dump-only fields by the matching
        `metadata.identifiers` value.

        Returns:
            Unchanged input data.
        """
        if isinstance(data, dict):
            self.context["id_scheme"] = data.get("id_scheme") or "import-recid"
            self.context["alternate_id_scheme"] = (
                data.get("alternate_id_scheme") or ""
            )
        return data

    @validates("api_version")
    def _validate_api_version(self, value: int) -> None:
        """Only v2 is accepted on the JSON create path.

        Raises:
            ValidationError: When `api_version` is not 2.
        """
        if value != 2:
            raise ValidationError(
                "api_version must be 2 to use the async import API. To use the "
                "legacy multipart/form-data synchronous import API, set api_version to 1."
            )

    @validates_schema(pass_original=True)
    def _validate_metadata_files_consistency(
        self, data: dict, original_data: dict, **kwargs: Any
    ) -> None:
        """Require create `files[]` when any metadata record has `files.entries`.

        Uses the original payload because nested load strips dump-only
        `entries` before restore; consistency must see the client input.

        Raises:
            ValidationError: On top-level `files` when metadata declares
                `files.entries` without create `files[]`.
        """
        if original_data.get("files") or data.get("files"):
            return
        id_scheme = str(self.context.get("id_scheme") or "import-recid")
        alternate_id_scheme = str(self.context.get("alternate_id_scheme") or "")
        offending_ids: list[str] = []
        for record in original_data.get("metadata") or []:
            if not isinstance(record, dict):
                continue
            entries = (record.get("files") or {}).get("entries")
            if not entries:
                continue
            offending_ids.append(
                record_source_id(
                    record["metadata"],
                    id_scheme=id_scheme,
                    alternate_id_scheme=alternate_id_scheme,
                )
            )
        if offending_ids:
            ids = ", ".join(repr(i) for i in offending_ids)
            raise ValidationError(
                {
                    "files": [
                        "create files[] is required when metadata records "
                        f"declare files.entries (source ids: {ids})"
                    ]
                }
            )
