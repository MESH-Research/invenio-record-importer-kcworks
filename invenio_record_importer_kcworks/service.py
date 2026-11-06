"""High-level service entry points for record import."""

from typing import Any, cast

from flask import current_app as app
from flask_principal import Identity
from invenio_records_resources.services.base import Service

from .jobs import load_import_job
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
        return self.permission_policy(
            action_name, identity=identity, **kwargs
        ).allows(identity)

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
            When ``True``, refuse to change an existing matched record if its
            metadata differs from the import payload. When ``False`` (default),
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

        pending = list(import_result.pending_fetches or [])
        if pending:
            from .jobs import create_import_job
            from .tasks import fetch_import_file

            job_id = create_import_job(
                community_id=str(resolved_community_id),
                pending_fetches=pending,
            )
            import_result.job_id = job_id
            for fetch in pending:
                fetch_import_file.delay(
                    fetch["record_id"],
                    fetch["file_key"],
                    fetch["url"],
                    job_id,
                )

        return import_result.model_dump()

    def get_import_job(self, identity: Identity, job_id: str) -> dict:
        """Return status for an async import file-fetch job.

        Loads the job first so permission can use its `community_id` with the
        same community ∧ import-action gate as `import_records`.

        Args:
            identity: Caller identity (must pass `read_import_job`).
            job_id: Job identifier from a prior import response.

        Returns:
            Job status dict.

        Raises:
            KeyError: When the job id is unknown.
            PermissionDeniedError: When the identity fails `read_import_job`.
        """
        job = load_import_job(job_id)
        if job is None:
            raise KeyError(job_id)

        community_item = CommunitiesHelper().look_up_community(
            job.get("community_id") or ""
        )
        self.require_permission(
            identity,
            "read_import_job",
            record=community_item._record,
        )
        return job
