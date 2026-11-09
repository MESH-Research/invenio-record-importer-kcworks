"""Custom exceptions for invenio-record-importer-kcworks."""


class ActiveImportExistsError(Exception):
    """Raised when creating an import while one is already active for the community."""

    def __init__(
        self,
        community_id: str,
        existing_import_id: str | None = None,
        message: str | None = None,
    ):
        """Initialize the exception.

        Args:
            community_id: Community UUID (string) that already has an active import.
            existing_import_id: Id of the blocking import, if known.
            message: Optional override message.
        """
        self.community_id = community_id
        self.existing_import_id = existing_import_id
        if message is None:
            message = (
                f"An active import is already running for community {community_id}"
                + (
                    f" (import_id={existing_import_id})"
                    if existing_import_id
                    else ""
                )
            )
        super().__init__(message)
        self.message = message


class ImportNotFoundError(Exception):
    """Raised when a `RecordImport` row does not exist."""

    def __init__(
        self,
        import_id: str | None = None,
        community_id: str | None = None,
        message: str | None = None,
    ):
        """Initialize the exception.

        Args:
            import_id: Missing import id, if known.
            community_id: Community id when looking up current/latest, if known.
            message: Optional override message.
        """
        self.import_id = import_id
        self.community_id = community_id
        if message is None:
            if import_id is not None:
                message = f"Import not found: {import_id}"
            elif community_id is not None:
                message = f"No import found for community: {community_id}"
            else:
                message = "Import not found"
        super().__init__(message)
        self.message = message


class ImportCommunityMismatchError(Exception):
    """Raised when an import exists but belongs to a different community."""

    def __init__(
        self,
        import_id: str,
        community_id: str,
        actual_community_id: str | None = None,
        message: str | None = None,
    ):
        """Initialize the exception.

        Args:
            import_id: Import id that was requested.
            community_id: Community id from the request path.
            actual_community_id: Community id on the import row, if known.
            message: Optional override message.
        """
        self.import_id = import_id
        self.community_id = community_id
        self.actual_community_id = actual_community_id
        if message is None:
            message = (
                f"Import {import_id} does not belong to community {community_id}"
            )
        super().__init__(message)
        self.message = message


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
