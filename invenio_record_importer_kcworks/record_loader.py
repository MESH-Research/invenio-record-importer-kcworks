#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

import copy
from typing import Optional

import arrow
from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_drafts_resources.resources.records.errors import DraftNotCreatedError
from invenio_pidstore.errors import PIDDoesNotExistError
from invenio_rdm_records.proxies import (
    current_rdm_records_service as records_service,
)
from invenio_record_importer_kcworks.errors import (
    PublicationValidationError,
    SkipRecord,
)
from invenio_record_importer_kcworks.services.communities import (
    CommunitiesHelper,
)
from invenio_record_importer_kcworks.services.files import FilesHelper
from invenio_record_importer_kcworks.services.records import RecordsHelper
from invenio_record_importer_kcworks.services.stats.stats import (
    StatsFabricator,
    AggregationFabricator,
)
from invenio_record_importer_kcworks.errors import (
    FileUploadError,
    DraftValidationError,
)
from invenio_record_importer_kcworks.utils.utils import (
    replace_value_in_nested_dict,
)
from invenio_records_resources.services.uow import (
    unit_of_work,
    UnitOfWork,
    RecordCommitOp,
)
import itertools
import json
import jsonlines
from pathlib import Path
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.exc import NoResultFound
from traceback import print_exc, format_exc
from typing import Union
from pprint import pformat
from invenio_record_importer_kcworks.errors import (
    InvalidParametersError,
    NoAvailableRecordsError,
)
from invenio_record_importer_kcworks.types import (
    APIResponsePayload,
    LoaderResult,
    FileData,
    ImportedRecord,
)


class RecordLoader:
    """Record loader."""

    def __init__(
        self,
        user_id: int,
        community_id: str = "",
        sourceid_schemes: list[str] = ["import-recid", "neh-recid"],
        views_field: str = "",
        downloads_field: str = "",
    ):
        self.user_id = user_id
        self.community_id = community_id
        self.import_identifier = community_id or user_id
        self.metadata_overrides_folder = Path(
            f"{app.config.get('RECORD_IMPORTER_OVERRIDES_FOLDER', '')}/"
            f"{self.import_identifier}"
        )
        self.metadata_overrides_file = Path(
            f"{self.metadata_overrides_folder}/"
            f"record-importer-overrides_{self.import_identifier}.jsonl"
        )
        self.failed_log_path = Path(
            f"{app.config.get('RECORD_IMPORTER_LOGS_LOCATION')}/"
            f"record_importer_failed_log_{self.import_identifier}.jsonl"
        )
        self.created_log_path = Path(
            f"{app.config.get('RECORD_IMPORTER_LOGS_LOCATION')}/"
            f"record_importer_created_log_{self.import_identifier}.jsonl"
        )
        self.sourceid_scheme, self.sourceid_scheme2 = sourceid_schemes

        self.created_records = self._get_created_records()
        (
            self.existing_failed_records,
            self.residual_failed_records,
            self.existing_failed_indices,
            self.existing_failed_sourceids,
            self.existing_failed_invenioids,
        ) = self._load_prior_failed_records()

        if not self.user_id:
            raise ValueError("user_id is required")
        if not self.community_id:
            raise ValueError("community_id is required")
        self.views_field = views_field
        self.downloads_field = downloads_field

    @unit_of_work()
    def _override_created_timestamp(
        self,
        draft_id: str,
        created_timestamp_override: str,
        uow: Optional[UnitOfWork] = None,
    ) -> None:
        """
        Override the created timestamp of a draft record and update related events.

        If we want to override the created timestamp, we need to do it
        manually here because normal record api objects operations don't
        have access to that model field. We also update the stats-community-events
        index to reflect the artificial created date.
        """
        if (
            created_timestamp_override
            and RecordsHelper._validate_timestamp(created_timestamp_override)
            and uow
        ):
            try:
                record = records_service.read_draft(
                    system_identity, id_=draft_id
                )._record
            except (NoResultFound, DraftNotCreatedError):
                record = records_service.read(system_identity, id_=draft_id)._record

            record.model.created = created_timestamp_override
            uow.register(RecordCommitOp(record))

            # Update events in the stats-community-events index
            try:
                from invenio_search.utils import prefix_index
                from invenio_search import current_search_client

                # Force a refresh to make sure events are searchable
                current_search_client.indices.refresh(
                    index=prefix_index("stats-community-events")
                )

                # Get the community ID from the record
                if hasattr(record.parent, "communities"):
                    record_communities = record.parent.communities.ids
                    app.logger.error(f"Record communities: {record_communities}")
                else:
                    record_communities = []

                if record_communities:
                    for community_id in record_communities:
                        app.logger.error(f"Community ID: {community_id}")
                        app.logger.error(f"Record PID value: {record.pid.pid_value}")
                        search_query = {
                            "query": {
                                "bool": {
                                    "must": [
                                        {
                                            "term": {
                                                "record_id": str(record.pid.pid_value)
                                            }
                                        },
                                        {"term": {"community_id": community_id}},
                                    ]
                                }
                            }
                        }
                        app.logger.error(f"Search query: {pformat(search_query)}")
                        search_result = current_search_client.search(
                            index=prefix_index("stats-community-events"),
                            body=search_query,
                        )
                        app.logger.error(f"Search result: {pformat(search_result)}")

                        for hit in search_result["hits"]["hits"]:
                            event_id = hit["_id"]
                            event_source = hit["_source"]
                            update_body = {
                                "doc": {
                                    "record_created_date": created_timestamp_override
                                }
                            }
                            updated_event = current_search_client.update(
                                index=prefix_index("stats-community-events"),
                                id=event_id,
                                body=update_body,
                            )
                            app.logger.error(f"Updated event: {pformat(updated_event)}")
                            app.logger.error(
                                f"Updating event {event_id}: "
                                f"{pformat(event_source)}"
                            )
                            app.logger.error(
                                f"Updated event {event_id} with created_date: "
                                f"{created_timestamp_override}"
                            )

                    # Refresh the index after all updates to ensure they're searchable
                    current_search_client.indices.refresh(
                        index=prefix_index("stats-community-events")
                    )

                    app.logger.info(
                        f"Updated stats-community-events index for record {record.id} "
                        f"to use original creation date: {created_timestamp_override}"
                    )
                else:
                    app.logger.warning(f"No communities found for record {record.id}")
            except Exception as e:
                app.logger.warning(
                    f"Failed to update stats-community-events index for record {record.id}: {e}"
                )

    def load(
        self,
        index: int = 0,
        log_object: dict = {},
        import_data: dict = {},
        files: list[FileData] = [],
        no_updates: bool = False,
        user_system: str = "knowledgeCommons",
        overrides: dict = {},
        strict_validation: bool = True,
        notify_record_owners: bool = True,
    ) -> LoaderResult:
        """
        Create an invenio record with file uploads, ownership, communities.

        Note that the owners list identified in the submitted data's
        parent.access.owned_by list will not be included in the record metadata
        during record creation. It will be removed and the record ownership
        updated after record creation.

        Likewise, any file data in the submitted data's files.entries list will be
        removed and the files will be uploaded after record creation.

        Note that if an owner does not already have a Knowledge Commons account,
        KCWorks will create an internal KCWorks account for them. This KCWorks
        account is distinct from the Knowledge Commons account they will need
        to create in order to manage their works on Knowledge Commons. The accounts
        will be automatically linked after the owner creates their Knowledge Commons
        account, provided that the owner provides an identifier in their Knowledge
        Commons account that matches an identifier provided for them in the
        `owned_by` property of the work's metadata object.

        Owners will also be added to the collection's membership with the "reader"
        role, which allows them access to any records restricted to the collection's
        membership, but does not afford them any additional permissions. What it does
        mean is that collection managers will be able to see all of the work owners
        in the list of collection members on the collection's landing page.

        Parameters
        ----------
        import_data : dict
            The data to import into Invenio. This should be a dictionary
            with the following keys:
            - custom_fields: a dictionary of custom metadata fields
            - metadata: a dictionary of standard metadata fields
            - pids: a dictionary of PID values
            - files: a dictionary of file uploads
        files : list[FileData]
            A list of FileData objects. Each FileData object should have
            the following properties:
            - filename: the name of the file
            - content_type: the MIME type of the file
            - mimetype: the MIME type of the file
            - mimetype_params: the MIME type parameters of the file
            - stream: the file stream (a file-like object)
        no_updates : bool
            If True, do not update existing records
        user_system : str
            The name of the system in which the user is/will be registered.
            Defaults to "knowledgeCommons".
        overrides : dict
            A dictionary of metadata fields to override in the import data
            if manual corrections are necessary
        strict_validation : bool
            If True, raise a DraftValidationError if there are validation
            errors in the record metadata, even if they do not prevent record creation.
            If False, add the errors to the errors list and continue with the import.
        notify_record_owners : bool
            If True, notify the owners of the records by email when their records
            are created. If False, do not notify the owners of the records.
            Defaults to True.

        Returns
        -------
        LoaderResult
            A LoaderResult object with the results of the import. It has the
            following properties:
            - index: the index of the record in the source file
            - source_id: the ID of the record in the source system, using the
                first sourceid_scheme specified in the RecordLoader constructor
            - primary_community: the community data dictionary for the record's
                primary community
            - record_created: the metadata record creation result.
                This is not just the metadata record, but the dictionary
                returned by the create_invenio_record method of RecordsHelper.
                It contains the following keys:
                - record_data: the metadata record
                - record_uuid: the UUID of the metadata record
                - status: the status of the metadata record
            - existing_record: the existing metadata record if it exists
                (after updating)
            - uploaded_files: the file upload results
            - community_review_result: the community review acceptance result
                as a dictionary
            - assigned_ownership: the record ownership assignment result
            - added_to_collections: the group collection addition
            - submitted: the submitted data
                - data: the submitted data
                - files: the submitted files
                - owners: the submitted owners
        """
        result = LoaderResult(
            index=index,
            source_id="",
            log_object=log_object,
            primary_community={},
            record_created={},
            uploaded_files={},
            community_review_result={},
            assigned_owners={},
            added_to_collections=[],
            existing_record={},
            errors=[],
            submitted={},
        )
        result.source_id = next(
            (
                i.get("identifier")
                for i in import_data.get("metadata", {}).get("identifiers", [])
                if i.get("scheme") == self.sourceid_scheme
            ),
            "",
        )
        for key, val in overrides.items():
            app.logger.debug(f"updating metadata key {key} with value {val}")
            updated_data = replace_value_in_nested_dict(import_data, key, val)
            if isinstance(updated_data, dict):
                import_data = updated_data
            else:
                raise ValueError(
                    f"failed to update metadata key {key} with value {val}"
                )

        # Build the initial metadata to be submitted
        result.submitted["files"] = copy.deepcopy(import_data["files"])

        created_timestamp_override = import_data.get("created", None)
        submitted_data = {
            "access": import_data.get("access", {}),
            "custom_fields": import_data.get("custom_fields", {}),
            "metadata": import_data["metadata"],
            "pids": import_data.get("pids", {}),
            "parent": import_data.get("parent", {}),
        }
        result.submitted["data"] = submitted_data

        # Remove the owned_by field from the access dictionary because we
        # will be adding it back in later
        result.submitted["owners"] = copy.deepcopy(
            submitted_data["parent"].get("access", {}).get("owned_by", [])
        )
        if isinstance(result.submitted["owners"], dict):
            result.submitted["owners"] = [result.submitted["owners"]]
        if "access" in result.submitted["data"]["parent"].keys():
            result.submitted["data"]["parent"]["access"] = {
                k: v
                for k, v in result.submitted["data"]["parent"]["access"].items()
                if k != "owned_by"
            }
        # else:
        #     result.submitted["data"]["access"] = {"record": "public",
        # "files": "public"}

        # Keep file data separate from other metadata, but enable files if
        # there are files in the metadata
        if len(result.submitted["files"].get("entries", [])) > 0:
            result.submitted["data"]["files"] = {"enabled": True}
        else:
            result.submitted["data"]["files"] = {"enabled": False}

        try:
            # Create/find the necessary primary community
            # We aren't yet placing the record in the community, just making sure
            # the community exists
            app.logger.info("    finding or creating primary community...")
            if self.community_id:
                result.primary_community = (
                    CommunitiesHelper().prepare_invenio_community(
                        record_source=user_system,
                        community_string=self.community_id,
                    )
                )

            # Create the basic metadata record
            # This tries to recover (or if necessary, delete) existing draft or
            # published records *unless* the no_updates flag is set
            app.logger.info("    finding or creating draft metadata record...")
            app.logger.debug(f"    submitted_data: {pformat(submitted_data)}")
            result.record_created = RecordsHelper().create_invenio_record(
                submitted_data, no_updates, created_timestamp_override
            )
            validation_errors = (
                result.record_created["record_data"]
                .get("metadata", {})
                .get("errors", [])
            )
            if strict_validation and len(validation_errors) > 0:
                raise DraftValidationError(validation_errors)
            else:
                result.errors.extend(
                    [{"validation_error": e} for e in validation_errors]
                )
            result.status = result.record_created["status"]
            if result.record_created["status"] in [
                "updated_published",
                "updated_draft",
                "unchanged_existing_draft",
                "unchanged_existing_published",
            ]:
                result.existing_record = result.record_created["record_data"]
            # NOTE: this "is_draft" logic will be True for new drafts for unpublished
            # versions of published records, but *not* for drafts editing
            # already published versions
            is_draft = (
                False
                if result.existing_record
                and result.existing_record["is_published"]
                and not result.existing_record["is_draft"]
                else True
            )
            draft_id = result.record_created["record_data"]["id"]

            # Upload the files
            if len(result.submitted["files"].get("entries", [])) > 0:
                app.logger.info("    uploading files for draft...")
                result.uploaded_files = FilesHelper(
                    is_draft=is_draft
                ).handle_record_files(
                    result.record_created["record_data"],
                    result.submitted["files"]["entries"],
                    files=files,
                    existing_record=result.existing_record,
                )
            else:
                FilesHelper(is_draft=is_draft).set_to_metadata_only(draft_id)
                result.record_created["record_data"]["files"]["enabled"] = False
                if result.existing_record:
                    result.existing_record["files"]["enabled"] = False
            failed_files = [
                k for k, f in result.uploaded_files.items() if f[0] == "failed"
            ]
            if any(failed_files):
                app.logger.error(f"failed files: {pformat(result.uploaded_files)}")
                raise FileUploadError(
                    {
                        "file upload failures": {
                            k: result.uploaded_files[k] for k in failed_files
                        }
                    }
                )

            # Attach the record to the communities
            result.community_review_result = (
                CommunitiesHelper().publish_record_to_community(
                    draft_id,
                    community_id=result.primary_community["id"],
                )
            )
            # Publishing the record happens during community acceptance
            # If the record already belongs to the community...
            # - if the record has no changes, we do nothing
            # - if the metadata was not changed, but there are new files, we
            #   create and publish a new draft of the record with the new files
            # - if the metadata and/or files were changed, a new draft of the
            #   record should already exist and we publish that
            if result.community_review_result["status"] == "already_published":
                if result.uploaded_files.get("status") != "skipped":
                    app.logger.info("    publishing new draft record version...")
                    try:
                        check_rec = records_service.read_draft(
                            system_identity, id_=draft_id
                        )._record
                        print(check_rec.files.bucket)
                        print(check_rec.files.entries)
                    except Exception:
                        # edit method creates a new draft of the published record
                        # if one doesn't already exist
                        records_service.edit(system_identity, id_=draft_id)
                    publish = records_service.publish(system_identity, id_=draft_id)
                    assert publish.data["status"] == "published"

            self._override_created_timestamp(draft_id, created_timestamp_override)

            # Assign ownership of the record
            result.assigned_owners = RecordsHelper.assign_record_ownership(
                draft_id=draft_id,
                submitted_data=result.submitted["data"],
                user_id=self.user_id,
                submitted_owners=result.submitted["owners"],
                user_system=user_system,
                collection_id=self.community_id,
                existing_record=result.existing_record,
                notify_record_owners=notify_record_owners,
            )

            # Add the record to the appropriate group collections
            result.added_to_collections = (
                CommunitiesHelper().add_record_to_group_collections(
                    result.record_created["record_data"],
                    record_source=user_system,
                )
            )

            # Retrieve final metadata record
            final_metadata_record = records_service.read(system_identity, id_=draft_id)
            result.record_created["record_data"] = final_metadata_record.to_dict()

            # Create fictional usage events to generate correct usage stats
            # FIXME: Change the stats fields or make them configurable
            if self.views_field or self.downloads_field:
                StatsFabricator().create_stats_events(
                    draft_id,
                    downloads_field=self.downloads_field,
                    views_field=self.views_field,
                    date_field="metadata.publication_date",
                    views_count=result.submitted["data"]["custom_fields"].get(
                        self.views_field, 0
                    ),
                    downloads_count=result.submitted["data"]["custom_fields"].get(
                        self.downloads_field, 0
                    ),
                    publication_date=result.submitted["data"]["metadata"].get(
                        "publication_date"
                    ),
                    eager=True,
                    verbose=True,
                )
        except (PublicationValidationError, DraftValidationError) as e:
            app.logger.error(f"PublicationValidationError: {e}")
            result.errors.append({"validation_error": e.message})
            result.status = "error"

            # Try to delete any draft records that were created
            # FIXME: Why are some draft records no longer found when we try
            # to delete them?
            if result.record_created:
                try:
                    record_id = result.record_created["record_data"]["id"]
                    RecordsHelper().delete_invenio_record(record_id)
                    app.logger.error(f"Deleted draft record {record_id}")
                except (NoResultFound, PIDDoesNotExistError):
                    app.logger.error(f"No draft record found to delete: {record_id}")
                result.record_created["record_data"] = {}
                result.record_created["record_uuid"] = (
                    ""  # FIXME: Can we search by UUID?
                )
                result.record_created["status"] = "deleted"
            else:
                result.record_created["status"] = "not_created"
        except FileUploadError as e:
            app.logger.error(f"FileUploadError: {e}")
            result.errors.append({"file_upload_error": e.message})
            result.status = "error"
            if result.record_created:
                try:
                    record_id = result.record_created["record_data"]["id"]
                    RecordsHelper().delete_invenio_record(record_id)
                    app.logger.error(f"Deleted draft record {record_id}")
                except (NoResultFound, PIDDoesNotExistError):
                    app.logger.error(f"No draft record found to delete: {record_id}")
                result.record_created["record_data"] = {}
                result.record_created["record_uuid"] = (
                    ""  # FIXME: Can we search by UUID?
                )
                result.record_created["status"] = "deleted"

        result.log_object = self._update_record_log_object(result)

        return result

    def _log_success(
        self,
        load_result: LoaderResult,
        lists: dict[str, list[LoaderResult]],
    ) -> dict[str, list[LoaderResult]]:
        """
        Log a created record to the created records log file.

        This does not update the log file if the record has already been
        created. If the record does not appear in the log file, it is added at
        the end.

        :param index: the index of the record in the source file
        :param record_log_object: the record log object
        :param load_result: The result object returned from the load operation
        :param successful_records: The list of successful records (LoaderResult objects)

        :returns: the updated list of successful records (LoaderResult objects)
        """
        success_list = lists["successful_records"]
        rec_log_object = load_result.log_object
        rec_log_object["timestamp"] = arrow.utcnow().format()
        existing_lines = [
            (idx, t)
            for idx, t in enumerate(self.created_records)
            if t["source_id"]
            and t["source_id"] == rec_log_object["source_id"]
            and t["invenio_id"]
            and t["invenio_id"] == rec_log_object["invenio_id"]
        ]
        if not existing_lines:
            self.created_records.append(rec_log_object)
            with jsonlines.open(self.created_log_path, "a") as created_writer:
                created_writer.write(rec_log_object)
        elif (
            existing_lines
            and existing_lines[0][1]["invenio_recid"] != rec_log_object["invenio_recid"]
        ):
            i = existing_lines[0][0]
            self.created_records = [
                *self.created_records[:i],
                *self.created_records[i + 1 :],  # noqa: E203
                rec_log_object,
            ]
            with jsonlines.open(self.created_log_path, "w") as created_writer:
                for t in self.created_records:
                    created_writer.write(t)

        # Append the full result object to the successful records
        success_list.append(load_result)
        lists["successful_records"] = success_list

        return lists

    def _log_failed_record(
        self,
        result: LoaderResult,
        lists: dict,
        reason: str = "",
    ) -> dict[str, list[Union[LoaderResult, dict]]]:
        """
        Log a failed record to the failed records log file.
        """
        failed_list = lists["failed_records"]
        index = result.log_object.get("index", -1)
        failed_obj = result.log_object.copy()
        failed_obj.update(
            {
                "reason": reason,
                "datestamp": arrow.now().format(),
            }
        )

        if index > -1:
            failed_list.append(result)
        self._update_failed_logfile(lists)

        lists["failed_records"] = failed_list

        return lists

    def _update_failed_logfile(self, lists: dict) -> None:
        """
        Update the failed records logfile.
        """
        failed_list = lists["failed_records"]
        skipped_ids = []
        if len(lists["skipped_records"]) > 0:
            skipped_ids = [r["source_id"] for r in lists["skipped_records"] if r]
        with jsonlines.open(
            self.failed_log_path,
            "w",
        ) as failed_writer:
            total_failed = [
                r.log_object for r in failed_list if r.source_id not in skipped_ids
            ]
            failed_ids = [r.source_id for r in failed_list if r]
            for e in [r for r in self.residual_failed_records if isinstance(r, dict)]:
                if e["source_id"] not in failed_ids and e not in total_failed:
                    total_failed.append(e)
            ordered_failed_records = sorted(total_failed, key=lambda r: r["index"])
            for o in ordered_failed_records:
                failed_writer.write(o)

    def _log_repaired_record(
        self,
        result: LoaderResult,
        lists: dict,
    ) -> dict[str, list[dict]]:
        """
        Log a repaired record.
        """
        app.logger.info("repaired previously failed record...")
        app.logger.info(
            f"    {result.log_object.get('doi')} {result.log_object.get('source_id')}"
            f" {result.log_object.get('source_id_2')}"
        )
        self.residual_failed_records = [
            d
            for d in self.residual_failed_records
            if isinstance(d, dict) and d["source_id"] != result.log_object["source_id"]
        ]
        lists["repaired_failed"].append(result.log_object)
        self._update_failed_logfile(lists)
        return lists

    def _load_prior_failed_records(
        self,
    ) -> tuple[list[dict], list[dict], list[int], list[str], list[str]]:
        """
        Load the prior failed records.

        :returns: a tuple of lists which contain:
            - the existing failed records
            - the residual failed records (these will be removed as repaired)
            - the existing failed indices
            - the existing failed source ids (using the import identifier scheme)
            - the existing failed invenio ids (using the InvenioRDM record ID scheme)
        """
        existing_failed_records: list[dict] = []
        try:
            with jsonlines.open(
                self.failed_log_path,
                "r",
            ) as reader:
                existing_failed_records = [obj for obj in reader]
        except FileNotFoundError:
            app.logger.info("**no existing failed records log file found...**")
        existing_failed_indices = [r["index"] for r in existing_failed_records]
        existing_failed_sourceids = [r["source_id"] for r in existing_failed_records]
        existing_failed_invenioids = [
            r["invenio_recid"] for r in existing_failed_records
        ]
        residual_failed_records = [*existing_failed_records]

        return (
            existing_failed_records,
            residual_failed_records,
            existing_failed_indices,
            existing_failed_sourceids,
            existing_failed_invenioids,
        )

    def _get_record_set(
        self,
        metadata: list[dict] = [],
        flags: dict[str, bool] = {},
        range_args: list[int] = [],
        nonconsecutive: list[int] = [],
    ) -> list[dict]:
        """
        Get the record set from the metadata.
        """
        retry_failed = flags.get("retry_failed")
        no_updates = flags.get("no_updates")
        use_sourceids = flags.get("use_sourceids")

        if range_args[1] == -1:
            range_args[1] = len(metadata)

        if no_updates:
            app.logger.info(
                "    **no-updates flag is set, so skipping updating existing"
                " records...**"
            )
        if not nonconsecutive:
            stop_string = "" if len(range_args) == 1 else f" to {range_args[1]}"
            app.logger.info(
                f"Loading records from {str(range_args[0]) + stop_string}..."
            )
        else:
            id_type = "source record id" if use_sourceids else "index in import file"
            app.logger.info(
                f"Loading records {' '.join([str(s) for s in nonconsecutive])}"
                f" (by {id_type})..."
            )
        app.logger.info("Loading records from json data: ")

        record_data_source = metadata
        record_set = []
        if not record_data_source:
            pathstring = app.config.get(
                "RECORD_IMPORTER_SERIALIZED_PATH",
                "record_importer_source_data_{}.jsonl",
            ).format(self.import_identifier)
            serialized_path = Path(pathstring)
            if not serialized_path:
                raise ValueError(
                    "RECORD_IMPORTER_SERIALIZED_PATH config value is required"
                )
            record_data_source = jsonlines.open(serialized_path, mode="r")
            app.logger.info(f"Loading records from file in {serialized_path}...")

        try:
            # decide how to determine the record set
            if retry_failed:
                if no_updates:
                    raise InvalidParametersError(
                        "Cannot retry failed records with no-updates flag set."
                    )
                if not self.existing_failed_records:
                    raise NoAvailableRecordsError(
                        "No previously failed records to retry."
                    )
                line_num = 1
                for j in record_data_source:
                    if line_num in self.existing_failed_indices:
                        j["jsonl_index"] = line_num
                        record_set.append(j)
                    line_num += 1
            elif nonconsecutive:
                record_set = []
                if use_sourceids:
                    for j in record_data_source:
                        if [
                            i["identifier"]
                            for i in j["metadata"]["identifiers"]
                            if i["identifier"] in nonconsecutive
                            and i["scheme"] == self.sourceid_scheme
                        ]:
                            record_set.append(j)
                else:
                    line_num = 1
                    for j in record_data_source:
                        if line_num in nonconsecutive:
                            j["jsonl_index"] = line_num
                            record_set.append(j)
                        line_num += 1
        finally:
            record_set = list(itertools.islice(record_data_source, *range_args))

        if isinstance(record_data_source, jsonlines.Reader):
            record_data_source.close()

        if len(record_set) == 0:
            raise NoAvailableRecordsError("No records found to load.")

        return record_set

    def _get_log_object(
        self,
        current_record_index: int,
        record: dict,
    ) -> dict[str, str]:
        """
        Get the record ids from the record.
        """

        rec_doi = record.get("pids", {}).get("doi", {}).get("identifier", "")
        scheme_ids = []
        for scheme in [self.sourceid_scheme, self.sourceid_scheme2]:
            scheme_matches = [
                r
                for r in record["metadata"].get("identifiers", [])
                if r["scheme"] == scheme
            ]
            if scheme_matches:
                scheme_ids.append(scheme_matches[0].get("identifier", ""))
            else:
                scheme_ids.append("")
        rec_invenioid = record.get("id", "")
        app.logger.info(f"....starting to load record {current_record_index}")

        rec_log_object = {
            "index": current_record_index,
            "invenio_recid": rec_invenioid,
            "invenio_id": rec_doi,
            "source_id": scheme_ids[0],
            "source_id_2": scheme_ids[1],
        }

        return rec_log_object

    def _handle_raised_exception(
        self,
        e: Exception,
        result: LoaderResult,
        lists: dict[str, list[Union[LoaderResult, dict]]],
    ) -> dict[str, list[Union[LoaderResult, dict]]]:
        """
        Handle a raised exception and log the error.

        :param e: the raised exception
        :param current_index: the index of the record in the source file
        :param current_record: the record
        :param record_log_object: the record log object
        :param failed_records: the list of previously failed records
        :param skipped_records: the list of skipped records
        :param no_updates_records: the list of no-updates records

        :returns: the updated lists of failed, and no-updates records
        """
        print_exc()
        app.logger.error(f"ERROR: {e}")
        msg = str(e)

        if hasattr(e, "messages"):
            msg = e.messages  # type: ignore
        elif hasattr(e, "message"):
            msg = e.message  # type: ignore

        error_reasons = {
            "CommonsGroupNotFoundError": msg,
            "CommonsGroupServiceError": msg,
            "DraftDeletionFailedError": msg,
            "ExistingRecordNotUpdatedError": msg,
            "FileKeyNotFoundError": msg,
            "FailedCreatingUsageEventsError": msg,
            "FileUploadError": msg,
            "UploadFileNotFoundError": msg,
            "InvalidKeyError": msg,
            "InvalidParametersError": msg,
            "MissingNewUserEmailError": msg,
            "MissingParentMetadataError": msg,
            "MultipleActiveCollectionsError": msg,
            "NoAvailableRecordsError": msg,
            "PublicationValidationError": msg,
            "RestrictedRecordPublicationError": msg,
            "StaleDataError": msg,
            "TooManyViewEventsError": msg,
            "TooManyDownloadEventsError": msg,
            "UpdateValidationError": msg,
            "NoUpdatesError": msg,
            "ValidationError": (
                f"There was a problem validating the record related to these "
                f"fields: {msg}",
            ),
        }
        if e.__class__.__name__ in error_reasons.keys():
            result.log_object.update({"reason": error_reasons[e.__class__.__name__]})
        else:
            app.logger.error(format_exc())
            app.logger.error(e)
            raise e

        if e.__class__.__name__ not in ["SkipRecord", "NoUpdates"]:
            result.errors.append(
                {
                    "message": error_reasons[e.__class__.__name__],
                }
            )
            lists = self._log_failed_record(result=result, lists=lists)
        elif e.__class__.__name__ == "NoUpdates":
            lists["no_updates_records"].append(result.log_object)

        return lists

    def _get_created_records(self) -> list[dict]:
        created_records = []
        try:
            with jsonlines.open(self.created_log_path, "r") as reader:
                created_records = [obj for obj in reader]
        except FileNotFoundError:
            app.logger.info("**no existing created records log file found...**")
        return created_records

    def _get_overrides(self, record: dict) -> tuple[bool, dict]:
        """
        Get any metadata overrides for a record.

        :param record: the record to get overrides for
        :returns: a tuple containing a boolean indicating whether the record
            should be skipped and a dictionary containing any overrides
        """
        overrides = {}
        skip = False  # allow skipping records in the source record list
        try:
            with jsonlines.open(self.metadata_overrides_file, "r") as override_reader:
                for o in override_reader:
                    if o["source_id"] in [
                        i["identifier"]
                        for i in record["metadata"]["identifiers"]
                        if i["scheme"] == self.sourceid_scheme
                    ]:
                        overrides = o.get("overrides")
                        skip = (
                            True
                            if o.get("skip") in [True, "True", "true", 1, "1"]
                            else False
                        )
        except FileNotFoundError:
            app.logger.info("**no existing metadata overrides file found...**")
        return skip, overrides

    def _update_counts(self, counts: dict, result: LoaderResult) -> dict:
        """
        Update the counts based on the result of the load operation.
        """
        if not result.existing_record:
            counts["new_records"] += 1
        if "unchanged_existing" in result.status:
            counts["unchanged_existing"] += 1
        if result.status == "updated_published":
            counts["updated_published"] += 1
        if result.status == "updated_draft":
            counts["updated_drafts"] += 1

        return counts

    def _report_counts(
        self,
        counts: dict = {},
        lists: dict[str, list[dict]] = {},
        nonconsecutive: list[int] = [],
        start_index: int = 0,
    ) -> None:
        """
        Log and report the final counts of the load operation.
        """
        counter = counts["record_counter"]
        app.logger.info("All done loading records into InvenioRDM")
        set_string = ""
        if nonconsecutive:
            set_string = f"{' '.join([str(n) for n in nonconsecutive])}"
        else:
            target_string = f" to {start_index + counter - 1}" if counter > 1 else ""
            set_string = f"{start_index}{target_string}"

        counts["unchanged_existing"] += len(lists["no_updates_records"])
        successes = len(lists["successful_records"]) + len(lists["no_updates_records"])
        message = (
            f"Processed {str(counter)} records in "
            f"InvenioRDM ({set_string})\n"
            f"    {str(successes)} successful \n"
            f"    {str(counts['new_records'])} new records created \n"
            f"    {str(successes - counts['new_records'])} already "
            f"existed \n"
            f"        {str(counts['updated_published'])} updated published "
            f"records \n"
            f"        {str(counts['updated_drafts'])} updated existing draft records \n"
            f"        {str(counts['unchanged_existing'])} unchanged existing records \n"
            f"        {str(len(lists['repaired_failed']))} previously failed records "
            f"repaired \n"
            f"   {str(len(lists['failed_records']))} failed \n"
            f"   {str(len(lists['skipped_records']))} records skipped (as marked in "
            f"overrides)"
            f"\n"
        )
        if len(lists["no_updates_records"]) > 0:
            message += (
                f"   {str(len(lists['no_updates_records']))} records not updated "
                f"because 'no updates' flag was set \n"
            )
        app.logger.info(message)

        if lists["repaired_failed"] or (
            lists["failed_records"] and not self.residual_failed_records
        ):
            app.logger.info("Previously failed records repaired:")
            for r in lists["repaired_failed"]:
                app.logger.info(r)

        if lists["failed_records"]:
            app.logger.info("Failed records:")
            for r in lists["failed_records"]:
                app.logger.info(r)
            app.logger.info(f"Failed records written to {self.failed_log_path}")

    def _update_record_log_object(self, result: LoaderResult) -> dict:
        """
        Update the record log object with the result of the load operation.
        """
        result.log_object["invenio_recid"] = result.record_created.get(
            "record_data", {}
        ).get("id")
        result.log_object["doi"] = (
            result.record_created.get("record_data", {})
            .get("pids", {})
            .get("doi", {})
            .get("identifier", "")
        )
        return result.log_object

    def _roll_back_created_records(self, records: list[LoaderResult]) -> None:
        """
        Roll back the created records.
        """
        for record in records:
            record_id = record.record_created.get("record_data", {}).get("id")
            record_status = record.record_created.get("record_data", {}).get("status")
            if record_id:
                assert RecordsHelper().delete_invenio_record(
                    record_id, record_type=record_status
                )

    def _aggregate_stats(
        self,
        start_date: str = "",
        end_date: str = "",
    ) -> Union[list, bool]:
        """
        Aggregate the stats for the load operation.
        """
        start_date = (
            start_date
            if start_date
            else arrow.utcnow().shift(days=-1).naive.date().isoformat()
        )
        end_date = (
            end_date
            if end_date
            else arrow.utcnow().shift(days=1).naive.date().isoformat()
        )
        aggregations = AggregationFabricator().create_stats_aggregations(
            start_date=arrow.get(start_date).naive,
            end_date=arrow.get(end_date).naive,
            bookmark_override=arrow.get(start_date).naive,
            eager=True,
        )
        app.logger.debug("    created usage aggregations...")
        app.logger.debug(pformat(aggregations))

        return aggregations

    def load_all(
        self,
        start_index: int = 0,
        stop_index: int = -1,
        nonconsecutive: list = [],
        no_updates: bool = False,
        use_sourceids: bool = False,
        retry_failed: bool = False,
        aggregate: bool = False,
        start_date: str = "",
        end_date: str = "",
        clean_filenames: bool = False,
        verbose: bool = False,
        stop_on_error: bool = False,
        files: list[FileData] = [],
        metadata: list[dict] = [],
        review_required: bool = True,
        strict_validation: bool = True,
        all_or_none: bool = True,
        notify_record_owners: bool = True,
    ) -> APIResponsePayload:
        """
        Create new InvenioRDM records and upload files for serialized deposits.

        params:
            start_index (int): the starting index of the records to load in the
                source jsonl file
            stop_index (int): the stopping index of the records to load in the
                source jsonl file (inclusive)
            nonconsecutive (list): a list of nonconsecutive indices to load
                from the source jsonl file
            no_updates (bool): whether to update existing records
            use_sourceids (bool): whether to use ids from the record source's
                id system for identification of records to load
            retry_failed (bool): whether to retry failed records from a prior
                run
            aggregate (bool): whether to aggregate usage stats for the records
                after loading. This may take a long time.
            start_date (str): the starting date of usage events to aggregate if
                aggregate is True
            end_date (str): the ending date of usage events to aggregate if
                aggregate is True
            clean_filenames (bool): whether to sanitize the filenames of the
                files to upload
            verbose (bool): whether to print and log verbose output during the
                loading process
            stop_on_error (bool): whether to stop the loading process if an
                error is encountered is encountered
            files (list[dict]): the list of files to upload
            metadata (list[dict]): the list of metadata objects for the records
                to load
            review_required (bool): whether to require review of the records
                before they are published
            strict_validation (bool): whether to strictly validate the records
                against the InvenioRDM metadata schema
            all_or_none (bool): whether to stop the loading process if an
                error is encountered
            notify_record_owners (bool): whether to notify the owners of the records
                by email when their records are created

            files (list[dict]): the list of files to upload
            metadata (list[dict]): the list of metadata objects for the records
                to load
        returns:
            None
        """
        counts: dict[str, int] = {
            "record_counter": 0,
            "updated_drafts": 0,
            "updated_published": 0,
            "unchanged_existing": 0,
            "new_records": 0,
        }
        lists: dict = {
            "successful_records": [],  # type: list[LoaderResult]
            "failed_records": [],  # type: list[LoaderResult]
            "skipped_records": [],  # type: list[dict]
            "no_updates_records": [],  # type: list[dict]
            "repaired_failed": [],  # type: list[dict]
        }
        flags: dict[str, bool] = {
            "no_updates": no_updates,
            "retry_failed": retry_failed,
            "use_sourceids": use_sourceids,
            "aggregate": aggregate,
            "verbose": verbose,
            "stop_on_error": stop_on_error,
            "clean_filenames": clean_filenames,
            "review_required": review_required,
            "strict_validation": strict_validation,
            "all_or_none": all_or_none,
            "notify_record_owners": notify_record_owners,
        }

        # sanitize the names of files before upload to avoid
        # issues with special characters
        # FIXME: Move this to the record file actions
        if clean_filenames:
            app.logger.info("Sanitizing file names...")
            FilesHelper.sanitize_filenames(app.config["RECORD_IMPORTER_FILES_LOCATION"])

        app.logger.info("Starting to load records into Invenio...")
        record_set = self._get_record_set(
            metadata=metadata,
            flags=flags,
            range_args=[start_index, stop_index],
            nonconsecutive=nonconsecutive,
        )

        for record_metadata in record_set:
            current_record_index = (
                record_metadata.get("jsonl_index")
                or start_index + counts["record_counter"]
            )

            skip, overrides = self._get_overrides(record_metadata)
            rec_log_object = self._get_log_object(current_record_index, record_metadata)
            current_files = [
                f
                for f in files
                if f.filename.split("/")[-1]
                in record_metadata.get("files", {}).get("entries", {}).keys()
            ]
            app.logger.debug(
                f"current files for record {current_record_index}: "
                f"{pformat(current_files)}"
            )
            app.logger.debug(f"all files: {pformat(files)}")
            app.logger.debug(
                f"entries: {pformat(record_metadata.get('files', {}).get('entries', {}))}"  # noqa: E501
            )

            try:
                result = LoaderResult(
                    index=current_record_index
                )  # initialize empty result object
                if skip:
                    lists["skipped_records"].append(rec_log_object)
                    raise SkipRecord("Record marked for skipping in override file")
                try:
                    result: LoaderResult = self.load(
                        index=current_record_index,
                        log_object=rec_log_object,
                        import_data=record_metadata,
                        files=current_files,
                        no_updates=flags["no_updates"],
                        overrides=overrides,
                        strict_validation=flags["strict_validation"],
                        notify_record_owners=flags["notify_record_owners"],
                    )
                # FIXME: This is a hack to handle StaleDataError which
                # is consistently resolved on a second attempt -- seems
                # to arise when a record is being added to several
                # communities at once
                except StaleDataError:
                    result: LoaderResult = self.load(
                        index=current_record_index,
                        log_object=rec_log_object,
                        import_data=record_metadata,
                        files=files,
                        no_updates=flags["no_updates"],
                        overrides=overrides,
                        strict_validation=flags["strict_validation"],
                        notify_record_owners=flags["notify_record_owners"],
                    )
                if result.status in [
                    "new_record",
                    "updated_published",
                    "updated_draft",
                    "unchanged_existing_draft",
                    "unchanged_existing_published",
                ]:
                    lists = self._log_success(result, lists)
                    counts = self._update_counts(counts, result)
                    if (
                        result.record_created.get("record_data", {}).get("id")
                        in self.existing_failed_invenioids
                    ):
                        lists = self._log_repaired_record(result, lists)
                else:
                    reason = "|".join(json.dumps(e) for e in result.errors)
                    lists = self._log_failed_record(result, lists, reason)
                    if flags["all_or_none"]:
                        self._roll_back_created_records(lists["successful_records"])
                        break
                    elif stop_on_error:
                        break
            except Exception as e:
                lists = self._handle_raised_exception(e, result, lists)
                if flags["all_or_none"] and lists["failed_records"]:
                    self._roll_back_created_records(lists["successful_records"])
                    break
                elif stop_on_error and lists["failed_records"]:
                    break

            app.logger.info(
                f"....done with record {current_record_index}, {rec_log_object}"
            )
            counts["record_counter"] += 1

        # Report the overall counts to the log
        self._report_counts(
            counts=counts,
            lists=lists,
            nonconsecutive=nonconsecutive,
            start_index=start_index,
        )

        # Aggregate the imported records stats if requested
        if aggregate:
            self._aggregate_stats(start_date=start_date, end_date=end_date)
        else:
            app.logger.warning(
                "    Skipping usage stats aggregation. Usage stats "
                "for the imported records will not be visible "
                "until an aggregation is performed."
            )

        success_list = [
            ImportedRecord(
                item_index=r.index,
                record_id=r.record_created.get("record_data", {}).get("id", ""),
                source_id=r.source_id,
                record_url=r.record_created.get("record_data", {})
                .get("links", {})
                .get("self_html", ""),
                metadata=r.record_created.get("record_data", {}),
                files=r.uploaded_files,
                collection_id=r.primary_community["id"],
                errors=r.errors,
            ).model_dump()
            for r in lists["successful_records"]
        ]
        error_list = [
            ImportedRecord(
                item_index=r.index,
                record_id=r.record_created.get("record_data", {}).get("id", ""),
                source_id=r.source_id,
                record_url=r.record_created.get("record_data", {})
                .get("links", {})
                .get("self_html", ""),
                metadata=r.record_created.get("record_data", {}),
                files=r.uploaded_files,
                collection_id=r.primary_community["id"],
                errors=r.errors,
            ).model_dump()
            for r in lists["failed_records"]
        ]

        # Determine the overall status of the import operation
        if success_list and not error_list:
            overall_status = "success"
            message = "All records were successfully imported"
        elif success_list and not flags["all_or_none"]:
            overall_status = "partial_success"
            message = "Some records were successfully imported, but some failed"
        elif error_list and flags["all_or_none"]:
            overall_status = "error"
            message = (
                "Some records could not be imported, and the 'all_or_none' flag "
                "was set to True, so the import was aborted and no records "
                "were created. Please check the list of failed records in the "
                "'errors' field for more information. Each failed item should have "
                "its own list of specific errors."
            )
        else:
            overall_status = "error"
            message = (
                "No records were successfully imported. Please check the list "
                "of failed records in the 'errors' field for more information. "
                "Each failed item should have its own list of specific errors."
            )

        return APIResponsePayload(
            status=overall_status,
            data=success_list,
            errors=error_list,
            message=message,
        )
