# Part of Invenio Record Importer KCWorks
# Copyright (C) 2023-2026, MESH Research
#
# Invenio Record Importer KCWorks is free software; you can redistribute and/or
# modify it under the terms of the MIT License; see LICENSE file for more details.


"""High-level service entry points for record import."""

from datetime import UTC, datetime
from typing import Any, cast

from flask import current_app as app
from flask_principal import Identity
from invenio_db import db
from invenio_records_resources.services.base import Service
from sqlalchemy.orm.attributes import flag_modified

from .errors import (
    ActiveImportExistsError,
    ImportCommunityMismatchError,
    ImportFileNotFoundError,
    ImportInvalidStateError,
    ImportNotFoundError,
    ImportStagingIncompleteError,
)
from .models import ACTIVE_STATUSES, ImportStatusEnum, RecordImport
from .record_loader import RecordLoader
from .schemas import ImportFileDeclarationSchema, RecordImportSchema
from .services import staging as staging_helper
from .services.communities import CommunitiesHelper
from .types import APIResponsePayload, FileData

# Statuses that allow file init / complete / delete before commit.
_UPLOADABLE_STATUSES = frozenset(
    {
        ImportStatusEnum.AWAITING_FILES,
        ImportStatusEnum.UPLOADING,
        ImportStatusEnum.STAGED,
    }
)


class RecordImporterService(Service):
    """Service for record importer."""

    def __init__(self, config):
        """Initialize the service."""
        super().__init__(config)

    def check_permission(self, identity, action_name, **kwargs):
        """Check a permission, passing `identity` into generator kwargs.

        Needed so `And` (and any similar generators) can test child
        satisfaction against the caller.

        Args:
            identity: Caller identity.
            action_name: Policy action name (e.g. `import_records`).
            **kwargs: Extra policy context (`record`, etc.).

        Returns:
            Whether the identity is allowed.
        """
        return self.permission_policy(action_name, identity=identity, **kwargs).allows(
            identity
        )

    def import_records(
        self,
        identity: Identity,
        file_data: list[FileData] | None = None,
        metadata: list[dict] | None = None,
        community_id: str = "",
        id_scheme: str = "import-recid",
        alternate_id_scheme: str = "",
        review_required: bool = True,
        strict_validation: bool = True,
        all_or_none: bool = True,
        no_updates: bool = False,
        notify_record_owners: bool = True,
        views_field: str = "",
        downloads_field: str = "",
    ) -> dict:
        """Import records.

        Parameters
        ----------
        file_data : list[FileData]
            The file data to import. Each FileData object contains the following
            following keys/properties:
            - filename: the name of the file
            - content_type: the MIME type of the file
            - mimetype: the MIME type of the file
            - mimetype_params: the MIME type parameters of the file (if any)
            - stream: the file stream (a SpooledTemporaryFile object)
        metadata : list[dict]
            The metadata to attach to the records. Must be a list of dictionaries
            that satisfy the KCWorks metadata schema requirements.
        community_id : str
            The UUID or slug of the community to import the records into. If none
            is provided, the records will not be associated with any community.
        id_scheme : str
            The identifier scheme provided in the import source metadata to
            differentiate between records before they are created in InvenioRDM
            or assigned a DOI. Defaults to `import_id`.
        alternate_id_scheme : str
            An optional alternate identifier scheme to use for the records. Defaults
            to an empty string.
        review_required : bool
            If provided, overrides the collection's review policy. Only applied if
            the request is made by the collection's owner or a user with the
            `manager` role.
        strict_validation : bool
            Whether to strictly validate each records and only create a record if
            any of the provided metadata fields are not valid. If `False`, the
            records will each be created provided that the minimum required
            metadata fields are valid.
        all_or_none : bool
            Whether to import a partial set of records in the case that some of
            the records fail. If it is `True`, no records will be imported if
            any of the records fail.
        no_updates : bool
            When `True`, refuse to change an existing matched record if its
            metadata differs from the import payload. When `False` (default),
            differing metadata is applied to the existing draft/record.
        notify_record_owners : bool
            Whether to notify the owners of the records of the work's creation.
            Defaults to `True`.
        views_field : str
            The field to use for the views count. Defaults to ""
        downloads_field : str
            The field to use for the downloads count. Defaults to ""

        Returns:
        -------
        dict
            The import result. This is an APIResponsePayload object dumped to a
            dictionary.
        """
        # load_community_needs(identity)

        if metadata is None:
            metadata = []
        if file_data is None:
            file_data = []
        community_item = CommunitiesHelper().look_up_community(community_id)
        community = community_item._record
        self.require_permission(
            identity,
            "import_records",
            record=community,
        )
        # Resolve community_id to UUID if a slug was provided
        resolved_community_id = community_item.id
        metadata_keys = (
            sorted(str(k) for k in metadata.keys())
            if isinstance(metadata, dict)
            else type(metadata).__name__
        )
        app.logger.debug(
            "Importing records with metadata type=%s keys=%s",
            type(metadata).__name__,
            metadata_keys,
        )
        app.logger.debug(
            "Importing records with file_data type=%s",
            type(file_data).__name__,
        )
        app.logger.debug(
            f"Importing records with community id: {community_id} "
            f"(resolved to: {resolved_community_id})"
        )
        app.logger.debug(f"Importing records with review required: {review_required}")
        app.logger.debug(
            f"Importing records with strict validation: {strict_validation}"
        )
        app.logger.debug(f"Importing records with all or none: {all_or_none}")
        # user added to identity by flask_security
        import_result: APIResponsePayload = RecordLoader(
            user_id=cast(Any, identity).user.id,
            community_id=resolved_community_id,
            views_field=views_field,
            downloads_field=downloads_field,
            sourceid_schemes=[id_scheme, alternate_id_scheme],
        ).load_all(
            files=file_data,
            metadata=metadata,
            no_updates=no_updates,
            review_required=review_required,
            strict_validation=strict_validation,
            all_or_none=all_or_none,
            notify_record_owners=notify_record_owners,
        )

        return import_result.model_dump()

    def _require_community_permission(
        self,
        identity: Identity,
        action_name: str,
        community_id: str,
    ):
        """Resolve community and require a permission against it.

        Args:
            identity: Caller identity.
            action_name: Policy action name.
            community_id: Community UUID or slug.

        Returns:
            The resolved community service item.
        """
        community_item = CommunitiesHelper().look_up_community(community_id)
        self.require_permission(
            identity,
            action_name,
            record=community_item._record,
        )
        return community_item

    def _active_import_for_community(self, community_id) -> RecordImport | None:
        """Return the active import row for a community, if any.

        Args:
            community_id: Resolved community UUID (or uuid-shaped string).

        Returns:
            The active `RecordImport`, or `None`.
        """
        return (
            RecordImport.query
            .filter(
                RecordImport.community_id == community_id,
                RecordImport.status.in_(ACTIVE_STATUSES),
            )
            .order_by(RecordImport.created.desc())
            .first()
        )

    def _get_import_row(self, community_id, import_id: str) -> RecordImport:
        """Load an import scoped to a community.

        Args:
            community_id: Resolved community UUID (or uuid-shaped string).
            import_id: Import UUID (or uuid-shaped string).

        Returns:
            The matching `RecordImport` row.

        Raises:
            ImportNotFoundError: When no row exists for `import_id`.
            ImportCommunityMismatchError: When the row belongs to another community.
        """
        row = db.session.get(RecordImport, import_id)
        if row is None:
            raise ImportNotFoundError(import_id=import_id)
        if str(row.community_id) != str(community_id):
            raise ImportCommunityMismatchError(
                import_id=import_id,
                community_id=str(community_id),
                actual_community_id=str(row.community_id),
            )
        return row

    def _build_staging_upload(
        self,
        row: RecordImport,
        *,
        filename: str,
        size: int,
        parts: list,
        content_type: str = "application/octet-stream",
        is_zip: bool = False,
    ) -> tuple[dict, dict]:
        """Create S3 MPU/PUT instructions and a manifest entry for one file.

        Callers must pass schema-validated `parts` (contiguous numbers, sizes
        summing to `size`, checksums as Content-MD5). One part → single PUT;
        multiple parts → MPU.

        Does not mutate `row` or commit. Caller appends the entry and persists.

        Args:
            row: Import row (must already have `id` and `community_id`).
            filename: Client filename (sanitized for the object key).
            size: Declared byte size (>= 0).
            parts: Schema-validated part list
                (`part_number`, `size`, `checksum`).
            content_type: MIME type.
            is_zip: Whether this object is a zip to extract at run time.

        Returns:
            `(manifest_entry, upload_payload)` where `upload_payload` has
            `filename`, `key`, `size`, `content_type`, `is_zip`, `transfer`,
            `upload_id`, `part_size`, and `parts` (presigned).
        """
        size = int(size)

        safe_name = staging_helper.safe_filename(filename)
        object_key = staging_helper.build_object_key(
            str(row.community_id), str(row.id), safe_name
        )
        declared_parts = list(parts)

        if len(declared_parts) == 1:
            part = declared_parts[0]
            upload_id = None
            transfer = "put"
            part_size = part["size"] if part["size"] > 0 else size
            signed_parts = [
                staging_helper.presign_put_object(
                    object_key,
                    content_type=content_type,
                    content_md5=part["checksum"],
                )
            ]
            signed_parts[0]["size"] = part["size"]
        else:
            part_size = declared_parts[0]["size"]
            upload_id = staging_helper.create_multipart_upload(
                object_key, content_type=content_type
            )
            signed_parts = staging_helper.presign_parts(
                object_key, upload_id, declared_parts
            )
            transfer = "multipart"

        shared = {
            "filename": safe_name,
            "key": object_key,
            "size": size,
            "content_type": content_type,
            "is_zip": bool(is_zip),
            "transfer": transfer,
            "part_size": part_size,
        }
        entry = {
            **shared,
            "checksum": None,
            "etag": None,
            "s3_upload_id": upload_id,
            "s3_deleted": False,
            "status": "uploading",
            "parts_expected": len(declared_parts),
            "declared_parts": declared_parts,
        }
        upload = {
            **shared,
            "upload_id": upload_id,
            "parts": signed_parts,
        }
        return entry, upload

    def create_import(
        self,
        identity: Identity,
        community_id: str,
        *,
        records_metadata: list | None = None,
        options: dict | None = None,
        files: list | None = None,
    ) -> dict:
        """Create a community-scoped import and optionally stage file uploads.

        When `files` is non-empty, starts MPU/PUT for each declaration and
        returns presigned URLs in `uploads` (`uploading`). When `files` is
        empty, enqueues `run_import` immediately (`queued`) — nothing to stage.

        Enforces one active import per community.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            records_metadata: Record metadata list to ingest after commit.
            options: Import flags including `api_version`.
            files: Optional list of
                `{filename, size, content_type?, is_zip?, parts: [{part_number,
                size, checksum}, …]}`. The client splits each file and supplies
                per-part MD5 (`checksum` as base64 or `md5:<hex>`); URLs are
                signed with `ContentMD5`. Built into `staging_manifest` on the
                new row. Create `load()` validates and normalizes these (and
                metadata) before persistence.

        Returns:
            Serialized import dict plus `uploads` (list of per-file upload
            instruction payloads; empty when no `files`).

        Raises:
            ActiveImportExistsError: When an active import already exists.

        Note:
            Invalid create payloads raise Marshmallow `ValidationError`
            (files layout, RDM metadata, or files/metadata consistency).
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        existing = self._active_import_for_community(community_item.id)
        if existing is not None:
            raise ActiveImportExistsError(
                community_id=str(community_item.id),
                existing_import_id=str(existing.id),
            )

        # Validate + normalize. Wrapper restores dump-only import fields on
        # metadata items; part checksums are normalized to Content-MD5.
        validated = RecordImportSchema(context={"identity": identity}).load(
            {
                **(options or {}),
                "metadata": list(records_metadata or []),
                "files": list(files or []),
            }
        )
        records_metadata = list(validated.get("metadata") or [])
        files = list(validated.get("files") or [])
        options = {
            key: value
            for key, value in validated.items()
            if key not in ("metadata", "files")
        }

        row = RecordImport.create(
            community_id=community_item.id,
            created_by=cast(Any, identity).user.id,
            records_metadata=records_metadata,
            options=options,
            staging_manifest=[],
            status=(
                ImportStatusEnum.UPLOADING
                if files
                else ImportStatusEnum.QUEUED
            ),
        )
        # Need PK before building `imports/<community>/<import>/…` keys.
        db.session.flush()

        uploads: list[dict] = []
        if files:
            manifest: list[dict] = []
            for file_entry in files:
                entry, upload = self._build_staging_upload(
                    row,
                    filename=file_entry["filename"],
                    size=file_entry["size"],
                    parts=file_entry["parts"],
                    content_type=file_entry.get("content_type")
                    or "application/octet-stream",
                    is_zip=bool(file_entry.get("is_zip", False)),
                )
                manifest.append(entry)
                uploads.append(upload)
            row.staging_manifest = manifest
            flag_modified(row, "staging_manifest")
            db.session.commit()
        else:
            db.session.commit()
            self._enqueue_run_import(row)

        payload = row.to_dict()
        payload["uploads"] = uploads
        return payload

    def _enqueue_run_import(self, row: RecordImport) -> None:
        """Set `queued`, enqueue Celery `run_record_import`, store `task_id`.

        Args:
            row: Import row to enqueue (must be persisted).
        """
        row.status = ImportStatusEnum.QUEUED
        db.session.commit()

        from invenio_record_importer_kcworks.tasks import run_record_import

        async_result = run_record_import.delay(str(row.id))
        row.task_id = async_result.id
        db.session.commit()

    def get_import(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
    ) -> dict:
        """Return one import for a community.

        Args:
            identity: Caller identity (must pass `read_import_job`).
            community_id: Community UUID or slug.
            import_id: Import UUID.

        Returns:
            Serialized import dict.
        """
        community_item = self._require_community_permission(
            identity, "read_import_job", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        return row.to_dict()

    def get_current_import(
        self,
        identity: Identity,
        community_id: str,
    ) -> dict:
        """Return the active import, else the latest terminal import.

        Args:
            identity: Caller identity (must pass `read_import_job`).
            community_id: Community UUID or slug.

        Returns:
            Serialized import dict.

        Raises:
            ImportNotFoundError: When the community has no imports.
        """
        community_item = self._require_community_permission(
            identity, "read_import_job", community_id
        )
        active = self._active_import_for_community(community_item.id)
        if active is not None:
            return active.to_dict()

        latest = (
            RecordImport.query
            .filter(RecordImport.community_id == community_item.id)
            .order_by(RecordImport.created.desc())
            .first()
        )
        if latest is None:
            raise ImportNotFoundError(community_id=str(community_item.id))
        return latest.to_dict()

    def list_imports(
        self,
        identity: Identity,
        community_id: str,
    ) -> dict:
        """List imports for a community (newest first).

        Args:
            identity: Caller identity (must pass `read_import_job`).
            community_id: Community UUID or slug.

        Returns:
            Dict with `imports` (list of serialized rows) and `total`.
        """
        community_item = self._require_community_permission(
            identity, "read_import_job", community_id
        )
        rows = (
            RecordImport.query
            .filter(RecordImport.community_id == community_item.id)
            .order_by(RecordImport.created.desc())
            .all()
        )
        imports = [row.to_dict() for row in rows]
        return {"imports": imports, "total": len(imports)}

    def _manifest_entry(
        self, row: RecordImport, filename: str
    ) -> tuple[int, dict]:
        """Find a staging manifest entry by sanitized filename.

        Args:
            row: Import row.
            filename: Sanitized filename key.

        Returns:
            `(index, entry)` into `row.staging_manifest`.

        Raises:
            ImportFileNotFoundError: When no entry matches.
        """
        manifest = list(row.staging_manifest or [])
        for index, entry in enumerate(manifest):
            if entry.get("filename") == filename:
                return index, entry
        raise ImportFileNotFoundError(import_id=str(row.id), filename=filename)

    def _abort_open_uploads(self, row: RecordImport) -> None:
        """Best-effort abort of in-progress MPU uploads on the manifest.

        Args:
            row: Import whose manifest may contain open `s3_upload_id`s.
        """
        for entry in list(row.staging_manifest or []):
            upload_id = entry.get("s3_upload_id")
            key = entry.get("key")
            status = entry.get("status")
            if not upload_id or not key or status == "complete":
                continue
            try:
                staging_helper.abort_multipart_upload(key, upload_id)
            except Exception:  # noqa: BLE001
                app.logger.warning(
                    "Failed to abort MPU for import %s key %s",
                    row.id,
                    key,
                    exc_info=True,
                )
            entry["s3_upload_id"] = None
            if status != "complete":
                entry["status"] = "aborted"

    def _cancel_import_row(self, row: RecordImport) -> dict:
        """Mark an active import row cancelled and commit.

        Aborts open MPUs and revokes a queued Celery task when present.

        Args:
            row: Active `RecordImport` to cancel.

        Returns:
            Serialized import dict.

        Raises:
            ValueError: When the import is not active.
        """
        if not row.is_active:
            raise ValueError(f"Import {row.id} is not active and cannot be cancelled")

        self._abort_open_uploads(row)

        if row.task_id is not None:
            try:
                from invenio_record_importer_kcworks.tasks import run_record_import

                run_record_import.AsyncResult(str(row.task_id)).revoke(terminate=False)
            except Exception:  # noqa: BLE001
                app.logger.warning(
                    "Failed to revoke task %s for import %s",
                    row.task_id,
                    row.id,
                    exc_info=True,
                )

        now = datetime.now(UTC)
        row.status = ImportStatusEnum.CANCELLED
        row.cancelled_at = now
        row.completed_at = now
        # Touch JSON column so SQLAlchemy persists in-place mutations.
        row.staging_manifest = list(row.staging_manifest or [])
        flag_modified(row, "staging_manifest")
        db.session.commit()
        return row.to_dict()

    def cancel_import(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
    ) -> dict:
        """Cancel an active import by id.

        Sets `cancelled` status plus `cancelled_at` and `completed_at`.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            import_id: Import UUID.

        Returns:
            Serialized import dict.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        return self._cancel_import_row(row)

    def cancel_current_import(
        self,
        identity: Identity,
        community_id: str,
    ) -> dict:
        """Cancel the active import for a community.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.

        Returns:
            Serialized cancelled import dict.

        Raises:
            ImportNotFoundError: When there is no active import.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        active = self._active_import_for_community(community_item.id)
        if active is None:
            raise ImportNotFoundError(community_id=str(community_item.id))
        return self._cancel_import_row(active)

    def init_import_file(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
        *,
        filename: str,
        size: int,
        parts: list | None,
        content_type: str = "application/octet-stream",
        is_zip: bool = False,
    ) -> dict:
        """Late-add a file with client-declared parts and return signed URLs.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            import_id: Import UUID.
            filename: Client filename (sanitized for the object key).
            size: Declared byte size (>= 0).
            parts: Client part list (`part_number`, `size`, `checksum`).
            content_type: MIME type.
            is_zip: Whether this object is a zip to extract at run time.

        Returns:
            Dict with `filename`, `key`, `size`, `content_type`, `is_zip`,
            `transfer` (`multipart` | `put`), `upload_id`, `part_size`,
            `parts` (presigned), and `import`.

        Raises:
            ImportInvalidStateError: When the import is not uploadable.

        Note:
            Invalid file declarations raise Marshmallow `ValidationError`.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        if row.status not in _UPLOADABLE_STATUSES:
            raise ImportInvalidStateError(
                import_id=str(row.id),
                status=row.status.value,
            )

        declared = ImportFileDeclarationSchema().load(
            {
                "filename": filename,
                "size": size,
                "parts": parts,
                "content_type": content_type,
                "is_zip": is_zip,
            }
        )
        entry, upload = self._build_staging_upload(
            row,
            filename=declared["filename"],
            size=declared["size"],
            parts=declared["parts"],
            content_type=declared.get("content_type") or "application/octet-stream",
            is_zip=bool(declared.get("is_zip", False)),
        )
        safe_name = entry["filename"]
        manifest = [
            e
            for e in list(row.staging_manifest or [])
            if e.get("filename") != safe_name
        ]
        manifest.append(entry)
        row.staging_manifest = manifest
        flag_modified(row, "staging_manifest")
        row.status = ImportStatusEnum.UPLOADING
        db.session.commit()

        return {**upload, "import": row.to_dict()}

    def refresh_import_file(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
        filename: str,
    ) -> dict:
        """Re-presign upload URLs for an in-progress staging file.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            import_id: Import UUID.
            filename: Sanitized filename / URL key.

        Returns:
            Same shape as `init_import_file` (without mutating size/type).

        Raises:
            ImportInvalidStateError: When the entry is already complete or
                the import is not uploadable.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        if row.status not in _UPLOADABLE_STATUSES:
            raise ImportInvalidStateError(
                import_id=str(row.id),
                status=row.status.value,
            )

        safe_name = staging_helper.safe_filename(filename)
        _index, entry = self._manifest_entry(row, safe_name)
        if entry.get("status") == "complete":
            raise ImportInvalidStateError(
                import_id=str(row.id),
                status=entry.get("status"),
                message=f"File {safe_name} is already complete",
            )

        object_key = entry["key"]
        content_type = entry.get("content_type") or "application/octet-stream"
        size = int(entry.get("size") or 0)
        transfer = entry.get("transfer") or "multipart"
        declared_parts = list(entry.get("declared_parts") or [])
        if not declared_parts:
            raise ImportInvalidStateError(
                import_id=str(row.id),
                message=(
                    f"File {safe_name} has no declared_parts to re-presign; "
                    "re-init the file with parts[] and checksums"
                ),
            )
        part_size = int(
            entry.get("part_size")
            or (declared_parts[0].get("size") if declared_parts else size)
            or size
        )

        if transfer == "put" or not entry.get("s3_upload_id"):
            part = declared_parts[0]
            parts = [
                staging_helper.presign_put_object(
                    object_key,
                    content_type=content_type,
                    content_md5=part.get("checksum"),
                )
            ]
            parts[0]["size"] = int(part.get("size") or size)
            upload_id = None
            transfer = "put"
        else:
            upload_id = entry["s3_upload_id"]
            parts = staging_helper.presign_parts(
                object_key, upload_id, declared_parts
            )

        return {
            "filename": safe_name,
            "key": object_key,
            "size": size,
            "content_type": content_type,
            "is_zip": bool(entry.get("is_zip")),
            "transfer": transfer,
            "part_size": part_size,
            "upload_id": upload_id,
            "parts": parts,
            "import": row.to_dict(),
        }

    def complete_import_file(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
        filename: str,
        *,
        parts: list | None = None,
        checksum: str | None = None,
    ) -> dict:
        """Complete staging for one file (MPU complete or verify single PUT).

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            import_id: Import UUID.
            filename: Sanitized filename / URL key.
            parts: MPU part ETags (`[{part_number, etag}, …]`); required for
                multipart transfers.
            checksum: Optional client checksum stored on the manifest.

        Returns:
            Dict with updated `entry` and `import`.

        Raises:
            ImportInvalidStateError: When the import/file cannot be completed.
            ValueError: When multipart parts are missing.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        if row.status not in _UPLOADABLE_STATUSES:
            raise ImportInvalidStateError(
                import_id=str(row.id),
                status=row.status.value,
            )

        safe_name = staging_helper.safe_filename(filename)
        index, entry = self._manifest_entry(row, safe_name)
        if entry.get("status") == "complete":
            return {"entry": entry, "import": row.to_dict()}

        object_key = entry["key"]
        transfer = entry.get("transfer") or "multipart"
        etag = None

        if transfer == "multipart":
            upload_id = entry.get("s3_upload_id")
            if not upload_id:
                raise ImportInvalidStateError(
                    import_id=str(row.id),
                    message=f"File {safe_name} has no multipart upload id",
                )
            if not parts:
                raise ValueError(
                    "parts with part_number/etag are required to complete multipart"
                )
            response = staging_helper.complete_multipart_upload(
                object_key, upload_id, parts
            )
            etag = (response.get("ETag") or "").strip('"') or None
        else:
            head = staging_helper.head_object(object_key)
            etag = (head.get("ETag") or "").strip('"') or None
            declared = int(entry.get("size") or 0)
            actual = int(head.get("ContentLength") or 0)
            if declared and actual and actual != declared:
                raise ValueError(
                    f"Uploaded size {actual} does not match declared size {declared}"
                )

        entry["status"] = "complete"
        entry["etag"] = etag
        entry["s3_upload_id"] = None
        if checksum:
            entry["checksum"] = checksum

        manifest = list(row.staging_manifest or [])
        manifest[index] = entry
        row.staging_manifest = manifest
        flag_modified(row, "staging_manifest")

        if all(e.get("status") == "complete" for e in manifest):
            row.status = ImportStatusEnum.STAGED
        else:
            row.status = ImportStatusEnum.UPLOADING
        db.session.commit()
        return {"entry": entry, "import": row.to_dict()}

    def delete_import_file(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
        filename: str,
    ) -> dict:
        """Abort MPU / delete a staged file before commit.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            import_id: Import UUID.
            filename: Sanitized filename / URL key.

        Returns:
            Updated serialized import dict.

        Raises:
            ImportInvalidStateError: When the import is not uploadable.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        if row.status not in _UPLOADABLE_STATUSES:
            raise ImportInvalidStateError(
                import_id=str(row.id),
                status=row.status.value,
            )

        safe_name = staging_helper.safe_filename(filename)
        _index, entry = self._manifest_entry(row, safe_name)
        object_key = entry.get("key")
        upload_id = entry.get("s3_upload_id")

        if upload_id and object_key and entry.get("status") != "complete":
            try:
                staging_helper.abort_multipart_upload(object_key, upload_id)
            except Exception:  # noqa: BLE001
                app.logger.warning(
                    "Failed to abort MPU on delete for import %s key %s",
                    row.id,
                    object_key,
                    exc_info=True,
                )
        elif object_key and entry.get("status") == "complete":
            try:
                staging_helper.delete_object(object_key)
            except Exception:  # noqa: BLE001
                app.logger.warning(
                    "Failed to delete staged object for import %s key %s",
                    row.id,
                    object_key,
                    exc_info=True,
                )

        manifest = [
            e
            for e in list(row.staging_manifest or [])
            if e.get("filename") != safe_name
        ]
        row.staging_manifest = manifest
        flag_modified(row, "staging_manifest")
        if not manifest or all(e.get("status") == "complete" for e in manifest):
            row.status = ImportStatusEnum.STAGED
        else:
            row.status = ImportStatusEnum.UPLOADING
        db.session.commit()
        return row.to_dict()

    def commit_import(
        self,
        identity: Identity,
        community_id: str,
        import_id: str,
    ) -> dict:
        """Validate staged files, set `queued`, and enqueue `run_import`.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            import_id: Import UUID.

        Returns:
            Serialized import dict (`queued`, with `task_id` when enqueued).

        Raises:
            ImportInvalidStateError: When status is not staged/uploading-complete.
            ImportStagingIncompleteError: When any manifest entry is incomplete.
        """
        community_item = self._require_community_permission(
            identity, "import_records", community_id
        )
        row = self._get_import_row(community_item.id, import_id)
        if row.status not in {
            ImportStatusEnum.STAGED,
            ImportStatusEnum.UPLOADING,
            ImportStatusEnum.AWAITING_FILES,
        }:
            raise ImportInvalidStateError(
                import_id=str(row.id),
                status=row.status.value,
            )

        manifest = list(row.staging_manifest or [])
        incomplete = [
            e.get("filename") or e.get("key") or "?"
            for e in manifest
            if e.get("status") != "complete"
        ]
        # Allow metadata-only imports (empty manifest).
        if incomplete:
            raise ImportStagingIncompleteError(
                import_id=str(row.id), incomplete=incomplete
            )

        self._enqueue_run_import(row)
        return row.to_dict()

    def run_import(self, identity: Identity, import_id: str) -> dict:
        """Thin stub for async ingestion (Layer 3 will flesh this out).

        Loads the import, checks permission, and transitions
        `queued` → `running` when applicable. Does not extract or ingest.

        Args:
            identity: Caller identity (must pass `import_records`).
            import_id: Import UUID.

        Returns:
            Serialized import dict.

        Raises:
            ImportNotFoundError: When the import id is unknown.
        """
        row = db.session.get(RecordImport, import_id)
        if row is None:
            raise ImportNotFoundError(import_id=import_id)

        community_item = CommunitiesHelper().look_up_community(str(row.community_id))
        self.require_permission(
            identity,
            "import_records",
            record=community_item._record,
        )

        if row.status == ImportStatusEnum.QUEUED:
            row.status = ImportStatusEnum.RUNNING
            db.session.commit()

        return row.to_dict()
