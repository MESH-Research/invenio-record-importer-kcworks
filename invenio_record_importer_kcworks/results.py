"""Result types for record importer operations."""


class ImportResult:
    """ImportResult."""

    def __init__(self, status: str, data: list):
        """Initialize the instance."""
        self.status = status
        self.data = data

    def to_dict(self):
        """To dict.

        Returns:
            Description of the return value.
        """
        return {"status": self.status, "data": self.data}


class ImportResultsList:
    """ImportResultsList."""

    def __init__(self, status: str, results: list[ImportResult]):
        """Initialize the instance."""
        self.status = status
        self.results = results

    def to_dict(self):
        """To dict.

        Returns:
            Description of the return value.
        """
        return {
            "status": self.status,
            "data": [result.to_dict() for result in self.results],
        }
