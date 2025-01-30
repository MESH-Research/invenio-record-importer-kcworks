#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

import arrow
from halo import Halo
from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import (
    current_rdm_records_service as records_service,
)
from invenio_record_importer_kcworks.errors import (
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
from invenio_record_importer_kcworks.utils.utils import (
    replace_value_in_nested_dict,
)
import itertools
import jsonlines
from pathlib import Path
from sqlalchemy.orm.exc import StaleDataError
from traceback import print_exc
from typing import Optional, Union
from pprint import pformat
from invenio_record_importer_kcworks.errors import (
    InvalidParametersError,
    NoAvailableRecordsError,
)


class RecordLoader:
    """Record loader."""

    def __init__(
        self,
        user_id: str = "",
        community_id: str = "",
        sourceid_schemes: list[str] = ["hclegacy-pid", "hclegacy-record-id"],
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
        ) = self._load_prior_failed_records()

    def load(
        self,
        import_data: dict = {},
        user_id: str = "",
        files: list = [],
        no_updates: bool = False,
        record_source: Optional[str] = None,
        overrides: dict = {},
    ) -> dict:
        """
        Create an invenio record with file uploads, ownership, communities.

        Parameters
        ----------
        import_data : dict
            The data to import into Invenio. This should be a dictionary
            with the following keys:
            - custom_fields: a dictionary of custom metadata fields
            - metadata: a dictionary of standard metadata fields
            - pids: a dictionary of PID values
            - files: a dictionary of file uploads
        files : list
            A list of file upload dictionaries. Each dictionary should have
            the following keys:
            - filename: the name of the file
            - content_type: the MIME type of the file
            - mimetype: the MIME type of the file
            - mimetype_params: the MIME type parameters of the file
            - stream: the file stream (a file-like object)
        no_updates : bool
            If True, do not update existing records
        record_source : str
            The name of the source service for the record
        overrides : dict
            A dictionary of metadata fields to override in the import data
            if manual corrections are necessary

        Returns
        -------
        dict
            A dictionary with the results of the import. It has the following
            keys:
            - community: the community data dictionary for the record's
                primary community
            - metadata_record_created: the metadata record creation result.
                This is not just the metadata record, but the dictionary
                returned by the create_invenio_record method of RecordsHelper.
                It contains the following keys:
                - record_data: the metadata record
                - record_uuid: the UUID of the metadata record
                - status: the status of the metadata record
            - uploaded_files: the file upload results
            - community_review_accepted: the community review acceptance result
            - assigned_ownership: the record ownership assignment result
            - added_to_collections: the group collection addition
        """
        existing_record = None
        result = {}

        for key, val in overrides.items():
            app.logger.debug(f"updating metadata key {key} with value {val}")
            import_data = replace_value_in_nested_dict(import_data, key, val)

        file_data = import_data["files"]

        # Build the initial metadata to be submitted
        submitted_data = {
            "custom_fields": import_data.get("custom_fields", {}),
            "metadata": import_data["metadata"],
            "pids": import_data.get("pids", {}),
        }

        # Remove the owned_by field from the access dictionary because we
        # will be adding it back in later
        submitted_owners = submitted_data.get("access", {}).get("owned_by", [])
        if "access" in submitted_data.keys():
            submitted_data["access"] = {
                k: v for k, v in submitted_data["access"].items() if k != "owned_by"
            }
        else:
            submitted_data["access"] = {"record": "public", "files": "public"}
        if len(file_data.get("entries", [])) > 0:
            submitted_data["files"] = {"enabled": True}
        else:
            submitted_data["files"] = {"enabled": False}

        # Create/find the necessary domain communities
        app.logger.info("    finding or creating community...")
        if self.community_id:
            # FIXME: allow for drawing community labels from other fields
            # for other data sources
            result["community"] = CommunitiesHelper().prepare_invenio_community(
                record_source=record_source or "",
                community_string=self.community_id,
            )
            community_id = result["community"]["id"]

        # Create the basic metadata record
        app.logger.info("    finding or creating draft metadata record...")
        app.logger.debug(f"    submitted_data: {pformat(submitted_data)}")
        record_created = RecordsHelper().create_invenio_record(
            submitted_data, no_updates
        )
        result["metadata_record_created"] = record_created
        result["status"] = record_created["status"]
        app.logger.info(f"    record status: {record_created['status']}")
        if record_created["status"] in [
            "updated_published",
            "updated_draft",
            "unchanged_existing_draft",
            "unchanged_existing_published",
        ]:
            existing_record = result["existing_record"] = record_created["record_data"]
        is_draft = (
            False
            if existing_record
            and existing_record["is_published"]
            and not existing_record["is_draft"]
            else True
        )
        metadata_record = record_created["record_data"]
        draft_id = metadata_record["id"]
        app.logger.info(f"    metadata record id: {draft_id}")

        print(
            f"import_record_to_invenio metadata_record: " f"{pformat(metadata_record)}"
        )

        # Upload the files
        if len(import_data["files"].get("entries", [])) > 0:
            app.logger.info("    uploading files for draft...")
            result["uploaded_files"] = FilesHelper(
                is_draft=is_draft
            ).handle_record_files(
                metadata_record,
                file_data,
                existing_record=existing_record,
            )
        else:
            app.logger.warning(
                f"files in metadata: {pformat(metadata_record.get('files'))}"
            )
            FilesHelper(is_draft=is_draft).set_to_metadata_only(draft_id)
            metadata_record["files"]["enabled"] = False
            if existing_record:
                existing_record["files"]["enabled"] = False

        # Attach the record to the communities
        result[
            "community_review_accepted"
        ] = CommunitiesHelper().publish_record_to_community(
            draft_id,
            community_id=self.community_id,
        )
        # Publishing the record happens during community acceptance
        # If the record already belongs to the community...
        # - if the record has no changes, we do nothing
        # - if the metadata was not changed, but there are new files, we
        #   create and publish a new draft of the record with the new files
        # - if the metadata and/or files were changed, a new draft of the
        #   record should already exist and we publish that
        if result["community_review_accepted"]["status"] == "already_published":
            if result["uploaded_files"].get("status") != "skipped":
                app.logger.info("    publishing new draft record version...")
                app.logger.debug(
                    records_service.read(
                        system_identity, id_=draft_id
                    )._record.files.entries
                )
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

        # Assign ownership of the record
        result["assigned_ownership"] = RecordsHelper.assign_record_ownership(
            draft_id=draft_id,
            submitted_data=submitted_data,
            submitted_owners=submitted_owners,
            record_source=record_source or "",
            existing_record=existing_record,
        )

        # Add the record to the appropriate group collections
        result[
            "added_to_collections"
        ] = CommunitiesHelper().add_record_to_group_collections(
            metadata_record, record_source=record_source or ""
        )

        # Create fictional usage events to generate correct usage stats
        events = StatsFabricator().create_stats_events(
            draft_id,
            downloads_field="custom_fields.hclegacy:total_downloads",
            views_field="custom_fields.hclegacy:total_views",
            date_field="metadata.publication_date",
            views_count=import_data["custom_fields"].get("hclegacy:total_views"),
            downloads_count=import_data["custom_fields"].get(
                "hclegacy:total_downloads"
            ),
            publication_date=import_data["metadata"].get("publication_date"),
            eager=True,
            verbose=True,
        )
        for e in events:
            app.logger.debug(f"    created {e[1][0]} usage events ({e[0]})...")
            # app.logger.debug(pformat(events))

        return result

    def _log_created_record(
        self,
        record_log_object: dict = {},
        successful_records: list = [],
    ) -> list:
        """
        Log a created record to the created records log file.

        This does not update the log file if the record has already been
        created. If the record does not appear in the log file, it is added at
        the end.

        :param index: the index of the record in the source file
        :param record_log_object: the record log object

        :returns: the updated list of created records
        """
        record_log_object["timestamp"] = arrow.utcnow().format()
        record_log_line = {
            k: v for k, v in record_log_object.items() if k != "metadata"
        }
        existing_lines = [
            (idx, t)
            for idx, t in enumerate(self.created_records)
            if t["source_id"]
            and t["source_id"] == record_log_object["source_id"]
            and t["invenio_id"]
            and t["invenio_id"] == record_log_object["invenio_id"]
        ]
        if not existing_lines:
            self.created_records.append(record_log_line)
            with jsonlines.open(self.created_log_path, "a") as created_writer:
                created_writer.write(record_log_line)
        elif (
            existing_lines
            and existing_lines[0][1]["invenio_recid"]
            != record_log_object["invenio_recid"]
        ):
            i = existing_lines[0][0]
            self.created_records = [
                *self.created_records[:i],
                *self.created_records[i + 1 :],  # noqa: E203
                record_log_line,
            ]
            with jsonlines.open(self.created_log_path, "w") as created_writer:
                for t in self.created_records:
                    created_writer.write(t)

        # Append the logging data *with* full metadata to the successful records
        successful_records.append(record_log_object)

        return successful_records

    def _log_failed_record(
        self,
        index: int = -1,
        record_log_object: dict = {},
        failed_records: list = [],
        reason: str = "",
        skipped_records: list = [],
    ) -> list:
        """
        Log a failed record to the failed records log file.
        """

        failed_obj = record_log_object.copy()
        failed_obj.update(
            {
                "reason": reason,
                "datestamp": arrow.now().format(),
            }
        )
        if index > -1:
            failed_records.append(failed_obj)
        skipped_ids = []
        if len(skipped_records) > 0:
            skipped_ids = [r["commons_id"] for r in skipped_records if r]
        with jsonlines.open(
            self.failed_log_path,
            "w",
        ) as failed_writer:
            total_failed = [
                r for r in failed_records if r["commons_id"] not in skipped_ids
            ]
            failed_ids = [r["commons_id"] for r in failed_records if r]
            for e in self.residual_failed_records:
                if e["commons_id"] not in failed_ids and e not in total_failed:
                    total_failed.append(e)
            ordered_failed_records = sorted(total_failed, key=lambda r: r["index"])
            for o in ordered_failed_records:
                failed_writer.write(o)

        return failed_records

    def _load_prior_failed_records(self) -> tuple[list, list, list, list]:
        existing_failed_records = []
        try:
            with jsonlines.open(
                self.failed_log_path,
                "r",
            ) as reader:
                existing_failed_records = [obj for obj in reader]
        except FileNotFoundError:
            app.logger.info("**no existing failed records log file found...**")
        existing_failed_indices = [r["index"] for r in existing_failed_records]
        existing_failed_hcids = [r["commons_id"] for r in existing_failed_records]
        residual_failed_records = [*existing_failed_records]

        return (
            existing_failed_records,
            residual_failed_records,
            existing_failed_indices,
            existing_failed_hcids,
        )

    def _get_record_set(
        self,
        metadata: list[dict] = [],
        retry_failed: bool = False,
        no_updates: bool = False,
        use_sourceids: bool = False,
        range_args: list[int] = [],
        nonconsecutive: list[int] = [],
    ) -> list[dict]:
        """
        Get the record set from the metadata.
        """

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

    def _get_record_ids(
        self,
        current_record_index: int,
        record: dict,
    ) -> tuple[str, str, str, str]:
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
        app.logger.info(
            f"    DOI:{rec_doi} {rec_invenioid} {' '.join(scheme_ids)} " f"{record}"
        )
        return rec_doi, *scheme_ids, rec_invenioid

    def _handle_raised_exception(
        self,
        e: Exception,
        current_index: int,
        current_record: dict,
        record_log_object: dict,
        failed_records: list[dict],
        skipped_records: list[dict],
        no_updates_records: list[dict],
    ) -> tuple[list[dict], list[dict]]:
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
        ("ERROR:", e)
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
            "ValidationError": f"There was a problem validating the record related to these fields: {msg}",
        }
        if e.__class__.__name__ in error_reasons.keys():
            record_log_object.update({"reason": error_reasons[e.__class__.__name__]})
        if e.__class__.__name__ not in ["SkipRecord", "NoUpdates"]:
            failed_records = self._log_failed_record(
                record_log_object=record_log_object,
                failed_records=failed_records,
                skipped_records=skipped_records,
            )
        elif e.__class__.__name__ == "NoUpdates":
            no_updates_records.append(record_log_object)

        return failed_records, no_updates_records

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

    def _update_counts(self, counts: dict, result: dict) -> dict:
        """
        Update the counts based on the result of the load operation.
        """
        counts["successful_records"] += 1
        if not result.get("existing_record"):
            counts["new_records"] += 1
        if "unchanged_existing" in result["status"]:
            counts["unchanged_existing"] += 1
        if result["status"] == "updated_published":
            counts["updated_published"] += 1
        if result["status"] == "updated_draft":
            counts["updated_drafts"] += 1

        return counts

    def _report_counts(
        self,
        counts: dict = {},
        successful_records: list[dict] = [],
        failed_records: list[dict] = [],
        repaired_failed: list[dict] = [],
        skipped_records: list[dict] = [],
        no_updates_records: list[dict] = [],
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

        counts["unchanged_existing"] += len(no_updates_records)
        message = (
            f"Processed {str(counter)} records in "
            f"InvenioRDM ({set_string})\n"
            f"    {str(len(successful_records) + len(no_updates_records))} "
            "successful \n"
            f"    {str(counts['new_records'])} new records created \n"
            f"    {str(len(successful_records) - counts['new_records'])} already "
            f"existed \n"
            f"        {str(counts['updated_published'])} updated published "
            f"records \n"
            f"        {str(counts['updated_drafts'])} updated existing draft records \n"
            f"        {str(counts['unchanged_existing'])} unchanged existing records \n"
            f"        {str(len(repaired_failed))} previously failed records "
            f"repaired \n"
            f"   {str(len(failed_records))} failed \n"
            f"   {str(len(skipped_records))} records skipped (as marked in "
            f"overrides)"
            f"\n"
        )
        if len(no_updates_records) > 0:
            message += (
                f"   {str(len(no_updates_records))} records not updated "
                f"because 'no updates' flag was set \n"
            )
        app.logger.info(message)

        if repaired_failed or (failed_records and not self.residual_failed_records):
            app.logger.info("Previously failed records repaired:")
            for r in repaired_failed:
                app.logger.info(r)

        if failed_records:
            app.logger.info("Failed records:")
            for r in failed_records:
                app.logger.info(r)
            app.logger.info(f"Failed records written to {self.failed_log_path}")

    def load_all(
        self,
        start_index: int = 1,
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
        file_data: list[dict] = [],
        metadata: list[dict] = [],
    ) -> dict[str, Union[list[dict], str]]:
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
            verbose (bool): whether to print and log verbose output during the
                loading process
            stop_on_error (bool): whether to stop the loading process if an
                error is encountered is encountered

            file_data (list[dict]): the list of files to upload
            metadata (list[dict]): the list of metadata objects for the records
                to load
        returns:
            None
        """
        counts = {
            "record_counter": 0,
            "updated_drafts": 0,
            "updated_published": 0,
            "unchanged_existing": 0,
            "new_records": 0,
        }
        successful_records: list[dict] = []
        failed_records: list[dict] = []
        skipped_records: list[dict] = []
        no_updates_records: list[dict] = []
        repaired_failed: list[dict] = []
        range_args = [start_index - 1]
        if stop_index > -1 and stop_index >= start_index:
            range_args.append(stop_index)
        else:
            range_args.append(start_index)

        # sanitize the names of files before upload to avoid
        # issues with special characters
        # FIXME: Move this to the record file actions
        if clean_filenames:
            app.logger.info("Sanitizing file names...")
            FilesHelper.sanitize_filenames(app.config["RECORD_IMPORTER_FILES_LOCATION"])

        app.logger.info("Starting to load records into Invenio...")
        record_set = self._get_record_set(
            metadata=metadata,
            retry_failed=retry_failed,
            no_updates=no_updates,
            use_sourceids=use_sourceids,
            range_args=range_args,
            nonconsecutive=nonconsecutive,
        )
        app.logger.debug(f"Record set: {pformat(record_set)}")

        for record_metadata in record_set:
            current_record_index = (
                record_metadata.get("jsonl_index")
                or start_index + counts["record_counter"]
            )
            spinner = Halo(
                text=f"    Loading record {current_record_index}", spinner="dots"
            )
            spinner.start()

            skip, overrides = self._get_overrides(record_metadata)
            doi, sourceid, sourceid2, invenioid = self._get_record_ids(
                current_record_index, record_metadata
            )
            rec_log_object = {
                "index": current_record_index,
                "invenio_recid": invenioid,
                "invenio_id": doi,
                "commons_id": sourceid,
                "core_record_id": sourceid2,
            }

            try:
                result = {}
                if skip:
                    skipped_records.append(rec_log_object)
                    raise SkipRecord("Record marked for skipping in override file")
                try:
                    result = self.load(
                        import_data=record_metadata,
                        files=file_data,
                        no_updates=no_updates,
                        overrides=overrides,
                        user_id=self.user_id,
                    )
                # FIXME: This is a hack to handle StaleDataError which
                # is consistently resolved on a second attempt -- seems
                # to arise when a record is being added to several
                # communities at once
                except StaleDataError:
                    result = self.load(
                        import_data=record_metadata,
                        files=file_data,
                        no_updates=no_updates,
                        overrides=overrides,
                        user_id=self.user_id,
                    )
                rec_log_object["invenio_recid"] = (
                    result.get("metadata_record_created", {})
                    .get("record_data", {})
                    .get("id")
                )
                rec_log_object["doi"] = (
                    result.get("metadata_record_created", {})
                    .get("record_data", {})
                    .get("pids", {})
                    .get("doi", {})
                    .get("identifier", "")
                )
                rec_log_object["metadata"] = result.get(
                    "metadata_record_created", {}
                ).get("record_data", {})
                successful_records = self._log_created_record(
                    record_log_object=rec_log_object,
                    successful_records=successful_records,
                )
                counts = self._update_counts(counts, result)
                if sourceid in self.existing_failed_sourceids:
                    app.logger.info("    repaired previously failed record...")
                    app.logger.info(f"    {doi} {sourceid} {sourceid2}")
                    self.residual_failed_records = [
                        d
                        for d in self.residual_failed_records
                        if d["source_id"] != sourceid
                    ]
                    repaired_failed.append(rec_log_object)
                    failed_records, self.residual_failed_records = (
                        self._log_failed_record(
                            failed_records=failed_records,
                            skipped_records=skipped_records,
                        )
                    )
                app.logger.debug("result status: %s", result.get("status"))
            except Exception as e:
                failed_records, no_updates_records = self._handle_raised_exception(
                    e,
                    current_index=current_record_index,
                    current_record=record_metadata,
                    record_log_object=rec_log_object,
                    failed_records=failed_records,
                    skipped_records=skipped_records,
                    no_updates_records=no_updates_records,
                )
                if stop_on_error and failed_records:
                    break

            spinner.stop()
            app.logger.info(f"....done with record {current_record_index}")
            counts["record_counter"] += 1

        self._report_counts(
            counts=counts,
            failed_records=failed_records,
            repaired_failed=repaired_failed,
            skipped_records=skipped_records,
            no_updates_records=no_updates_records,
            nonconsecutive=nonconsecutive,
            start_index=start_index,
        )

        # Aggregate the stats again now
        if aggregate:
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
        else:
            app.logger.warning(
                "    Skipping usage stats aggregation. Usage stats "
                "for the imported records will not be visible "
                "until an aggregation is performed."
            )

        success_list = [
            {
                "item_index": r.get("index"),
                "record_id": r.get("invenio_id"),
                "record_url": r.get("metadata", {})
                .get("links", {})
                .get("self_html", ""),
                "metadata": r.get("metadata", {}),
                "files": r.get("files", {}),
                "collection_ids": r.get("parent", {})
                .get("communities", [])[0]
                .get("id", ""),
                "errors": r.get("errors", []),
            }
            for r in successful_records
            if r.get("invenio_recid") is not None
        ]

        return {
            "status": "success",
            "data": success_list,
            "errors": failed_records,
            "message": "",
        }
