# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025 MESH Research.
#
# Invenio Record Importer is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

"""Helpers for draft community review / inclusion requests."""

from __future__ import annotations

from typing import Any

from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.services.errors import ReviewNotFoundError, ReviewStateError
from invenio_records_resources.services.uow import RecordIndexOp
from invenio_requests.errors import CannotExecuteActionError
from invenio_requests.proxies import current_requests_service
from opensearchpy.exceptions import NotFoundError
from sqlalchemy.exc import NoResultFound


class ReviewsHelper:
    """Helpers for draft review / community-submission requests."""

    def clear_draft_review(
        self, draft_id: str, uow: Any | None = None
    ) -> str | None:
        """Cancel and/or delete any review linked to a draft.

        Open reviews cannot be deleted until cancelled. Closed or cancelled
        reviews are deleted. If delete fails because the request is already
        gone but still referenced on the parent, and `uow` is provided, the
        stale `parent.review` link is cleared.

        Args:
            draft_id: Draft record id (PID value).
            uow: Optional unit of work for clearing a stale parent review link.

        Returns:
            The cancelled open request id as a string, or `None`.
        """
        try:
            existing_review = records_service.review.read(
                system_identity, id_=draft_id
            )
        except (ReviewNotFoundError, NoResultFound):
            app.logger.info(
                f"no review requests found for draft record {draft_id}..."
            )
            return None

        app.logger.info(
            f"clearing review request for draft {draft_id}: {existing_review.id}"
        )
        cancelled_request_id: str | None = None

        if existing_review.data["is_open"]:
            request_id = existing_review.id
            current_requests_service.execute_action(
                system_identity, request_id, "cancel"
            )
            cancelled_request_id = str(request_id)

        try:
            records_service.review.delete(system_identity, id_=draft_id)
        except ReviewStateError:
            # Still open after cancel attempt (or cancel was skipped):
            # cancel then delete.
            request_id = existing_review.id
            current_requests_service.execute_action(
                system_identity, request_id, "cancel"
            )
            cancelled_request_id = str(request_id)
            records_service.review.delete(system_identity, id_=draft_id)
        except (NotFoundError, CannotExecuteActionError, ReviewNotFoundError):
            if uow is not None:
                draft_record = records_service.read_draft(
                    system_identity, id_=draft_id
                )._record
                draft_record.parent.review = None
                uow.register(ParentRecordCommitOp(draft_record.parent))
                uow.register(RecordIndexOp(draft_record))

        return cancelled_request_id
