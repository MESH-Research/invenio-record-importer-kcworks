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

from .errors import (
    ActiveImportExistsError,
    ImportCommunityMismatchError,
    ImportNotFoundError,
)
from .models import ACTIVE_STATUSES, ImportStatusEnum, RecordImport
from .record_loader import RecordLoader
from .services.communities import CommunitiesHelper
from .types import APIResponsePayload, FileData


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

    def create_import(
        self,
        identity: Identity,
        community_id: str,
        *,
        records_metadata: list | None = None,
        options: dict | None = None,
        staging_manifest: list | None = None,
    ) -> dict:
        """Create a community-scoped import in `awaiting_files` status.

        Enforces one active import per community.

        Args:
            identity: Caller identity (must pass `import_records`).
            community_id: Community UUID or slug.
            records_metadata: Record metadata list to ingest after commit.
            options: Import flags including `api_version`.
            staging_manifest: Optional pre-declared file entries.

        Returns:
            Serialized import dict.

        Raises:
            ActiveImportExistsError: When an active import already exists.
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

        row = RecordImport.create(
            community_id=community_item.id,
            created_by=cast(Any, identity).user.id,
            records_metadata=records_metadata,
            options=options,
            staging_manifest=staging_manifest,
            status=ImportStatusEnum.AWAITING_FILES,
        )
        db.session.commit()
        return row.to_dict()

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

    def _cancel_import_row(self, row: RecordImport) -> dict:
        """Mark an active import row cancelled and commit.

        Args:
            row: Active `RecordImport` to cancel.

        Returns:
            Serialized import dict.

        Raises:
            ValueError: When the import is not active.
        """
        if not row.is_active:
            raise ValueError(f"Import {row.id} is not active and cannot be cancelled")

        now = datetime.now(UTC)
        row.status = ImportStatusEnum.CANCELLED
        row.cancelled_at = now
        row.completed_at = now
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
