"""Custom exceptions for invenio-record-importer-kcworks."""


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
