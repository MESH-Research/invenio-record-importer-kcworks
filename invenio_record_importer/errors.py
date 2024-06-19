"""Custom exceptions for invenio-record-importer."""


class FileUploadError(Exception):
    """File upload error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(FileUploadError, self).__init__(message)
        self.message = message


class UploadFileNotFoundError(Exception):
    """Upload file not found error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(UploadFileNotFoundError, self).__init__(message)
        self.message = message


class TooManyViewEventsError(Exception):
    """Too many view events error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(TooManyViewEventsError, self).__init__(message)
        self.message = message


class TooManyDownloadEventsError(Exception):
    """Too many download events error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(TooManyDownloadEventsError, self).__init__(message)
        self.message = message


class ExistingRecordNotUpdatedError(Exception):
    """Existing record not updated error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(ExistingRecordNotUpdatedError, self).__init__(message)
        self.message = message


class PublicationValidationError(Exception):
    """Publication validation error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(PublicationValidationError, self).__init__(message)
        self.message = message


class MissingNewUserEmailError(Exception):
    """Missing new user email error."""

    def __init__(self, message):
        """Initialize the exception."""
        super(MissingNewUserEmailError, self).__init__(message)
        self.message = message
