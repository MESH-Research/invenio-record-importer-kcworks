"""Custom exceptions for invenio-record-importer-kcworks."""

from __future__ import annotations

from typing import Any

from invenio_records_resources.errors import validation_error_to_list_errors
from marshmallow import ValidationError


def _stringify_messages(messages: Any) -> list[str]:
    """Coerce validation messages to plain strings (e.g. Babel LazyString)."""
    if isinstance(messages, (list, tuple)):
        return [str(m) for m in messages]
    if messages is None:
        return []
    return [str(messages)]


def _as_field_messages_item(item: dict) -> dict:
    """Return a JSON-safe `{field, messages}` dict."""
    return {
        "field": str(item.get("field", "")),
        "messages": _stringify_messages(item.get("messages")),
    }


def normalize_validation_errors(payload: Any) -> list[dict]:
    """Normalize validation payloads to Invenio `{field, messages}` list form.

    Accepts marshmallow nested dicts, an already-normalized list of
    `{field, messages}` dicts, a single such dict, or a scalar message.
    Message values are coerced with `str()` so Babel `LazyString` and similar
    lazy text objects are JSON-serializable.

    Args:
        payload: Nested marshmallow messages, PID-style error list, or scalar.

    Returns:
        A list of `{"field": "...", "messages": [...]}` dicts.
    """
    if payload is None or payload == "" or payload == [] or payload == {}:
        return []

    if isinstance(payload, list):
        if all(
            isinstance(item, dict) and "field" in item and "messages" in item
            for item in payload
        ):
            return [_as_field_messages_item(item) for item in payload]
        normalized: list[dict] = []
        for item in payload:
            normalized.extend(normalize_validation_errors(item))
        return normalized

    if isinstance(payload, dict):
        if "field" in payload and "messages" in payload:
            return [_as_field_messages_item(payload)]
        return [
            _as_field_messages_item(item)
            for item in validation_error_to_list_errors(ValidationError(payload))
        ]

    return [{"field": "", "messages": [str(payload)]}]


class CollectionDoesNotExistError(Exception):
    """Collection does not exist error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class CommonsGroupServiceError(Exception):
    """Commons group service error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class DraftDeletionFailedError(Exception):
    """Draft deletion failed error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class DraftValidationError(Exception):
    """Draft validation error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class DeletedRecordExistsError(Exception):
    """A soft-deleted record already matches the import identifier."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class ExistingRecordNotUpdatedError(Exception):
    """Existing record not updated error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class FailedCreatingUsageEventsError(Exception):
    """Failed creating usage events error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class FileUploadError(Exception):
    """File upload error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class InvalidParametersError(Exception):
    """Invalid parameters error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class MissingNewUserEmailError(Exception):
    """Missing new user email error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class MissingParentMetadataError(Exception):
    """Missing parent metadata error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class MultipleActiveCollectionsError(Exception):
    """Multiple active collections error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class NoUpdates(Exception):
    """No updates error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class NoAvailableRecordsError(Exception):
    """No available records error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class OwnershipChangeFailedError(Exception):
    """Ownership change failed error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class PublicationValidationError(Exception):
    """Publication validation error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class RestrictedRecordPublicationError(Exception):
    """Restricted record publication error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class SkipRecord(Exception):
    """Skip record exception."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class TooManyDownloadEventsError(Exception):
    """Too many download events error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class TooManyViewEventsError(Exception):
    """Too many view events error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class UpdateValidationError(Exception):
    """Update validation error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message


class UploadFileNotFoundError(Exception):
    """Upload file not found error."""

    def __init__(self, message):
        """Initialize the exception."""
        super().__init__(message)
        self.message = message
