"""Database models for community-scoped record imports."""

import enum
import uuid
from typing import Any, ClassVar

from invenio_accounts.models import User
from invenio_communities.communities.records.models import CommunityMetadata
from invenio_db import db
from sqlalchemy.dialects import postgresql
from sqlalchemy_utils import Timestamp
from sqlalchemy_utils.types import ChoiceType, JSONType, UUIDType

# JSON column that uses JSONB on PostgreSQL (same pattern as invenio-jobs).
JSON = (
    db
    .JSON()
    .with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    .with_variant(JSONType(), "sqlite")
    .with_variant(JSONType(), "mysql")
)


class ImportStatusEnum(enum.Enum):
    """Lifecycle states for a community-scoped record import."""

    AWAITING_FILES = "awaiting_files"
    UPLOADING = "uploading"
    STAGED = "staged"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Statuses that block creating another import for the same community.
ACTIVE_STATUSES: frozenset[ImportStatusEnum] = frozenset({
    ImportStatusEnum.AWAITING_FILES,
    ImportStatusEnum.UPLOADING,
    ImportStatusEnum.STAGED,
    ImportStatusEnum.QUEUED,
    ImportStatusEnum.RUNNING,
})

TERMINAL_STATUSES: frozenset[ImportStatusEnum] = frozenset({
    ImportStatusEnum.SUCCEEDED,
    ImportStatusEnum.PARTIAL_SUCCESS,
    ImportStatusEnum.FAILED,
    ImportStatusEnum.CANCELLED,
})


class RecordImport(db.Model, Timestamp):
    """One attempt to import a batch of records into a community.

    Domain noun is **import** (not product “job”). `created_by` is audit
    only; authorization is community-scoped `import_records` + OAuth.
    """

    __tablename__ = "record_imports"

    __table_args__: ClassVar[tuple] = (
        db.Index("ix_record_imports_community_status", "community_id", "status"),
    )

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    """Import identifier."""

    community_id = db.Column(
        UUIDType,
        db.ForeignKey(CommunityMetadata.id),
        nullable=False,
    )
    """Resolved community UUID this import belongs to."""

    created_by = db.Column(
        db.Integer,
        db.ForeignKey(User.id),
        nullable=False,
    )
    """User id that created the import (audit only)."""

    status = db.Column(
        ChoiceType(ImportStatusEnum, impl=db.String(32)),
        nullable=False,
        default=ImportStatusEnum.STAGED.value,
    )
    """Lifecycle status (see `ACTIVE_STATUSES` / `TERMINAL_STATUSES`)."""

    # Avoid naming this `metadata` — clashes with SQLAlchemy `MetaData`.
    records_metadata = db.Column(JSON, nullable=False, default=lambda: [])
    """List of record metadata objects to ingest after commit."""

    options = db.Column(JSON, nullable=False, default=lambda: {})
    """Import flags (review_required, all_or_none, api_version, id schemes, …)."""

    staging_manifest = db.Column(JSON, nullable=False, default=lambda: [])
    """Declared/staged file entries under the imports/ S3 prefix."""

    progress = db.Column(JSON, nullable=False, default=lambda: {})
    """Live progress (phase, counters) while running."""

    result = db.Column(JSON, nullable=True)
    """Terminal payload shaped like the sync import response body."""

    task_id = db.Column(UUIDType, nullable=True)
    """Celery task id once queued (for revoke on cancel)."""

    cancelled_at = db.Column(db.DateTime(timezone=True), nullable=True)
    """When cancel was requested/applied (UTC-aware)."""

    completed_at = db.Column(db.DateTime(timezone=True), nullable=True)
    """Terminal time (UTC-aware); also anchors staging cleanup."""

    @property
    def is_active(self) -> bool:
        """Whether this import blocks another active import for the community.

        Returns:
            True if `status` is in `ACTIVE_STATUSES`.
        """
        return self.status in ACTIVE_STATUSES

    @property
    def is_terminal(self) -> bool:
        """Whether the import has finished (success, failure, or cancel).

        Returns:
            True if `status` is in `TERMINAL_STATUSES`.
        """
        return self.status in TERMINAL_STATUSES

    def to_dict(self) -> dict[str, Any]:
        """Serialize the import for API responses.

        Returns:
            Dict with keys: `id`, `community_id` (UUID strings), `created_by`,
            `status` (string value), `records_metadata`, `options`,
            `staging_manifest`, `progress`, `result`, `task_id` (string or
            null), `cancelled_at` / `completed_at` / `created` / `updated`
            (ISO-8601 strings or null).
        """
        return {
            "id": str(self.id) if self.id is not None else None,
            "community_id": (
                str(self.community_id) if self.community_id is not None else None
            ),
            "created_by": self.created_by,
            "status": self.status.value,
            "records_metadata": self.records_metadata or [],
            "options": self.options or {},
            "staging_manifest": self.staging_manifest or [],
            "progress": self.progress or {},
            "result": self.result,
            "task_id": str(self.task_id) if self.task_id is not None else None,
            "cancelled_at": (
                self.cancelled_at.isoformat() if self.cancelled_at else None
            ),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "created": self.created.isoformat() if self.created else None,
            "updated": self.updated.isoformat() if self.updated else None,
        }

    @classmethod
    def create(
        cls,
        *,
        community_id: uuid.UUID | str,
        created_by: int,
        records_metadata: list | None = None,
        options: dict | None = None,
        staging_manifest: list | None = None,
        status: ImportStatusEnum = ImportStatusEnum.STAGED,
    ) -> "RecordImport":
        """Create and session-add a new import row.

        Args:
            community_id: Resolved community UUID.
            created_by: Creating user id (audit).
            records_metadata: Record metadata list to ingest later.
            options: Import flags / api_version / schemes.
            staging_manifest: Optional pre-declared file entries.
            status: Initial status enum (default `STAGED`).

        Returns:
            The new `RecordImport` instance (not yet committed).
        """
        if not isinstance(community_id, uuid.UUID):
            community_id = uuid.UUID(str(community_id))

        row = cls(
            community_id=community_id,
            created_by=created_by,
            status=status,
            records_metadata=list(records_metadata or []),
            options=dict(options or {}),
            staging_manifest=list(staging_manifest or []),
            progress={},
        )
        db.session.add(row)
        return row
