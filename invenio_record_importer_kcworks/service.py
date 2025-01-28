from invenio_records_resources.services.base import Service


class RecordImporterService(Service):
    """Service for record importer."""

    def __init__(self, config):
        """Initialize the service."""
        super().__init__(config)

    def import_records(
        self,
        file_data,
        community_id,
        review_required=True,
        strict_validation=True,
        all_or_none=True,
    ):
        """Import records."""
        return {"status": "success", "data": []}
