from flask import current_app as app
from invenio_records_resources.services.base import Service
from pprint import pformat
from tempfile import SpooledTemporaryFile
from .record_loader import RecordLoader


class RecordImporterService(Service):
    """Service for record importer."""

    def __init__(self, config):
        """Initialize the service."""
        super().__init__(config)

    def import_records(
        self,
        file_data: list[dict] = [],
        metadata: list[dict] = [],
        user_id: str = "",
        community_id: str = "",
        id_scheme: str = "import_id",
        alternate_id_scheme: str = "",
        review_required: bool = True,
        strict_validation: bool = True,
        all_or_none: bool = True,
    ):
        """Import records.

        Parameters
        ----------
        file_data : list[dict]
            The file data to import. Each dictionary in the list contains the
            following keys:
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
        """
        app.logger.debug(f"Importing records with metadata: {pformat(metadata)}")
        app.logger.debug(f"Importing records with metadata: {type(metadata)}")
        app.logger.debug(f"Importing records with form: {pformat(type(file_data))}")
        app.logger.debug(f"Importing records with file data: " f"{pformat(file_data)}")
        app.logger.debug(f"Importing records with community id: {community_id}")
        app.logger.debug(f"Importing records with review required: {review_required}")
        app.logger.debug(
            f"Importing records with strict validation: {strict_validation}"
        )
        app.logger.debug(f"Importing records with all or none: {all_or_none}")
        import_result = RecordLoader(
            user_id=user_id,
            community_id=community_id,
        ).load_all(
            file_data=file_data,
            metadata=metadata,
            no_updates=True,
        )
        app.logger.debug(import_result)

        return {
            "status": "success",
            "data": [
                {
                    "record_id": "",
                    "record_url": "",
                    "files": [],
                    "collection_id": "",
                    "errors": [],
                    "metadata": {},
                },
            ],
        }
