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
    current_records_service as records_service,
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
from typing import Optional
from pprint import pformat


class RecordLoader:
    """Record loader."""

    def import_record_to_invenio(
        self,
        import_data: dict,
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
            "custom_fields": import_data["custom_fields"],
            "metadata": import_data["metadata"],
            "pids": import_data["pids"],
        }

        submitted_data["access"] = {"records": "public", "files": "public"}
        if len(file_data.get("entries", [])) > 0:
            submitted_data["files"] = {"enabled": True}
        else:
            submitted_data["files"] = {"enabled": False}

        # Create/find the necessary domain communities
        app.logger.info("    finding or creating community...")
        if (
            "kcr:commons_domain" in import_data["custom_fields"].keys()
            and import_data["custom_fields"]["kcr:commons_domain"]
        ):
            # FIXME: allow for drawing community labels from other fields
            # for other data sources
            result[
                "community"
            ] = CommunitiesHelper().prepare_invenio_community(
                record_source,
                import_data["custom_fields"]["kcr:commons_domain"],
            )
            community_id = result["community"]["id"]

        # Create the basic metadata record
        app.logger.info("    finding or creating draft metadata record...")
        record_created = RecordsHelper().create_invenio_record(
            import_data, no_updates
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
            existing_record = result["existing_record"] = record_created[
                "record_data"
            ]
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
            f"import_record_to_invenio metadata_record: "
            f"{pformat(metadata_record)}"
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
            existing_record["files"]["enabled"] = False

        # Attach the record to the communities
        result[
            "community_review_accepted"
        ] = CommunitiesHelper().publish_record_to_community(
            draft_id,
            community_id,
        )
        # Publishing the record happens during community acceptance
        # If the record already belongs to the community...
        # - if the record has no changes, we do nothing
        # - if the metadata was not changed, but there are new files, we
        #   create and publish a new draft of the record with the new files
        # - if the metadata and/or files were changed, a new draft of the
        #   record should already exist and we publish that
        if (
            result["community_review_accepted"]["status"]
            == "already_published"
        ):
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
                publish = records_service.publish(
                    system_identity, id_=draft_id
                )
                assert publish.data["status"] == "published"

        # Assign ownership of the record
        result["assigned_ownership"] = RecordsHelper.assign_record_ownership(
            draft_id,
            import_data,
            record_source,
            existing_record=existing_record,
        )

        # Add the record to the appropriate group collections
        result[
            "added_to_collections"
        ] = CommunitiesHelper().add_record_to_group_collections(
            metadata_record, record_source
        )

        # Create fictional usage events to generate correct usage stats
        events = StatsFabricator().create_stats_events(
            draft_id,
            downloads_field="custom_fields.hclegacy:total_downloads",
            views_field="custom_fields.hclegacy:total_views",
            date_field="metadata.publication_date",
            views_count=import_data["custom_fields"].get(
                "hclegacy:total_views"
            ),
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
        index: int = 0,
        invenio_id: str = "",
        invenio_recid: str = "",
        commons_id: str = "",
        core_record_id: str = "",
        created_records: list = [],
    ) -> list:
        """
        Log a created record to the created records log file.

        This does not update the log file if the record has already been
        created. If the record does not appear in the log file, it is added at
        the end.

        :param index: the index of the record in the source file
        :param invenio_id: the doi of the record in Invenio
        :param invenio_recid: the recid of the record in Invenio
        :param commons_id: the user-facing id of the record in the source
            system
        :param core_record_id: the Fedora system id of the record in the
            source database
        :param created_records: the list of previously created records in this
            run

        :returns: the updated list of created records
        """
        created_log_path = app.config["RECORD_IMPORTER_CREATED_LOG_PATH"]
        created_rec = {
            "index": index,
            "invenio_id": invenio_id,
            "invenio_recid": invenio_recid,
            "commons_id": commons_id,
            "core_record_id": core_record_id,
            "timestamp": arrow.now().format(),
        }
        existing_lines = [
            (idx, t)
            for idx, t in enumerate(created_records)
            if t["commons_id"] == commons_id and t["invenio_id"] == invenio_id
        ]
        if not existing_lines:
            created_records.append(created_rec)

            with jsonlines.open(
                created_log_path,
                "a",
            ) as created_writer:
                created_writer.write(created_rec)
        elif (
            existing_lines
            and existing_lines[0][1]["invenio_recid"] != invenio_recid
        ):
            i = existing_lines[0][0]
            created_records = [
                *created_records[:i],
                *created_records[i + 1 :],  # noqa: E203
                created_rec,
            ]
            with jsonlines.open(
                created_log_path,
                "w",
            ) as created_writer:
                for t in created_records:
                    created_writer.write(t)

        return created_records

    def _log_failed_record(
        self,
        index=-1,
        invenio_id=None,
        commons_id=None,
        core_record_id=None,
        failed_records=None,
        residual_failed_records=None,
        reason=None,
        skipped_records=None,
    ) -> None:
        """
        Log a failed record to the failed records log file.
        """
        failed_log_path = Path(app.config["RECORD_IMPORTER_FAILED_LOG_PATH"])

        failed_obj = {
            "index": index,
            "invenio_id": invenio_id,
            "commons_id": commons_id,
            "core_record_id": core_record_id,
            "reason": reason,
            "datestamp": arrow.now().format(),
        }
        if index > -1:
            failed_records.append(failed_obj)
        skipped_ids = []
        if len(skipped_records) > 0:
            skipped_ids = [r["commons_id"] for r in skipped_records if r]
        with jsonlines.open(
            failed_log_path,
            "w",
        ) as failed_writer:
            total_failed = [
                r for r in failed_records if r["commons_id"] not in skipped_ids
            ]
            failed_ids = [r["commons_id"] for r in failed_records if r]
            for e in residual_failed_records:
                if e["commons_id"] not in failed_ids and e not in total_failed:
                    total_failed.append(e)
            ordered_failed_records = sorted(
                total_failed, key=lambda r: r["index"]
            )
            for o in ordered_failed_records:
                failed_writer.write(o)

        return failed_records, residual_failed_records

    def _load_prior_failed_records(self) -> tuple[list, list, list, list]:
        failed_log_path = Path(app.config["RECORD_IMPORTER_FAILED_LOG_PATH"])
        existing_failed_records = []
        try:
            with jsonlines.open(
                failed_log_path,
                "r",
            ) as reader:
                existing_failed_records = [obj for obj in reader]
        except FileNotFoundError:
            app.logger.info("**no existing failed records log file found...**")
        existing_failed_indices = [r["index"] for r in existing_failed_records]
        existing_failed_hcids = [
            r["commons_id"] for r in existing_failed_records
        ]
        residual_failed_records = [*existing_failed_records]

        return (
            existing_failed_records,
            residual_failed_records,
            existing_failed_indices,
            existing_failed_hcids,
        )

    def load_records_into_invenio(
        self,
        start_index: int = 1,
        stop_index: int = -1,
        nonconsecutive: list = [],
        no_updates: bool = False,
        use_sourceids: bool = False,
        sourceid_scheme: str = "hclegacy-pid",
        retry_failed: bool = False,
        aggregate: bool = False,
        start_date: str = "",
        end_date: str = "",
        clean_filenames: bool = False,
        verbose: bool = False,
        stop_on_error: bool = False,
    ) -> None:
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
            sourceid_scheme (str): the scheme to use for the source ids if
                records are identified by source ids
                are identified by source ids
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

        returns:
            None
        """
        record_counter = 0
        failed_records = []
        created_records = []
        skipped_records = []
        no_updates_records = []
        successful_records = 0
        updated_drafts = 0
        updated_published = 0
        unchanged_existing = 0
        new_records = 0
        repaired_failed = []
        range_args = [start_index - 1]
        if stop_index > -1 and stop_index >= start_index:
            range_args.append(stop_index)
        else:
            range_args.append(start_index)

        metadata_overrides_folder = Path(
            app.config["RECORD_IMPORTER_OVERRIDES_FOLDER"]
        )

        created_log_path = Path(
            app.config.get(
                "RECORD_IMPORTER_CREATED_LOG_PATH",
                "record_importer_created_records.jsonl",
            )
        )

        # sanitize the names of files before upload to avoid
        # issues with special characters
        if clean_filenames:
            app.logger.info("Sanitizing file names...")
            FilesHelper.sanitize_filenames(
                app.config["RECORD_IMPORTER_FILES_LOCATION"]
            )

        # Load list of previously created records
        created_records = []
        try:
            with jsonlines.open(
                created_log_path,
                "r",
            ) as reader:
                created_records = [obj for obj in reader]
        except FileNotFoundError:
            app.logger.info(
                "**no existing created records log file found...**"
            )

        # Load list of failed records from prior runs
        (
            existing_failed_records,
            residual_failed_records,
            existing_failed_indices,
            existing_failed_hcids,
        ) = self._load_prior_failed_records()

        app.logger.info("Starting to load records into Invenio...")
        if no_updates:
            app.logger.info(
                "    **no-updates flag is set, so skipping updating existing"
                " records...**"
            )
        if not nonconsecutive:
            stop_string = "" if stop_index == -1 else f" to {stop_index}"
            app.logger.info(
                f"Loading records from {str(start_index) + stop_string}..."
            )
        else:
            id_type = (
                "source record id" if use_sourceids else "index in import file"
            )
            app.logger.info(
                f"Loading records {' '.join([str(s) for s in nonconsecutive])}"
                f" (by {id_type})..."
            )

        app.logger.info(
            f"Loading records from serialized data: "
            f"{app.config.get('RECORD_IMPORTER_SERIALIZED_PATH')}..."
        )
        with jsonlines.open(
            Path(app.config.get("RECORD_IMPORTER_SERIALIZED_PATH")), "r"
        ) as json_source:
            # decide how to determine the record set
            if retry_failed:
                if no_updates:
                    print(
                        "Cannot retry failed records with no-updates flag set."
                    )
                    app.logger.error(
                        "Cannot retry failed records with no-updates flag set."
                    )
                    return
                if not existing_failed_records:
                    print("No previously failed records to retry.")
                    app.logger.info("No previously failed records to retry.")
                    return
                line_num = 1
                record_set = []
                for j in json_source:
                    if line_num in existing_failed_indices:
                        j["jsonl_index"] = line_num
                        record_set.append(j)
                    line_num += 1
            elif nonconsecutive:
                record_set = []
                if use_sourceids:
                    for j in json_source:
                        if [
                            i["identifier"]
                            for i in j["metadata"]["identifiers"]
                            if i["identifier"] in nonconsecutive
                            and i["scheme"] == sourceid_scheme
                        ]:
                            record_set.append(j)
                else:
                    line_num = 1
                    for j in json_source:
                        if line_num in nonconsecutive:
                            j["jsonl_index"] = line_num
                            record_set.append(j)
                        line_num += 1
            else:
                record_set = list(itertools.islice(json_source, *range_args))

            if len(record_set) == 0:
                print("No records found to load.")
                app.logger.info("No records found to load.")
                return

            for rec in record_set:
                record_source = rec.pop("record_source")
                # get metadata overrides for the record (for manual fixing
                # of inport data after serialization)
                overrides = {}
                skip = False  # allow skipping records in the source file
                with jsonlines.open(
                    metadata_overrides_folder
                    / f"record-importer-overrides_{record_source}.jsonl",
                    "r",
                ) as override_reader:
                    for o in override_reader:
                        if o["source_id"] in [
                            i["identifier"]
                            for i in rec["metadata"]["identifiers"]
                            if i["scheme"] == "hclegacy-pid"
                        ]:
                            overrides = o.get("overrides")
                            skip = (
                                True
                                if o.get("skip")
                                in [True, "True", "true", 1, "1"]
                                else False
                            )
                if "jsonl_index" in rec.keys():
                    current_record = rec["jsonl_index"]
                else:
                    current_record = start_index + record_counter
                rec_doi = (
                    rec["pids"]["doi"]["identifier"]
                    if "pids" in rec.keys()
                    else ""
                )
                rec_hcid = [
                    r
                    for r in rec["metadata"]["identifiers"]
                    if r["scheme"] == "hclegacy-pid"
                ][0]["identifier"]
                rec_recid = [
                    r
                    for r in rec["metadata"]["identifiers"]
                    if r["scheme"] == "hclegacy-record-id"
                ][0]["identifier"]
                rec_invenioid = None
                app.logger.info(
                    f"....starting to load record {current_record}"
                )
                app.logger.info(
                    f"    DOI:{rec_doi} {rec_invenioid} {rec_hcid} {rec_recid}"
                    f"{record_source}"
                )
                spinner = Halo(
                    text=f"    Loading record {current_record}", spinner="dots"
                )
                spinner.start()
                rec_log_object = {
                    "index": current_record,
                    "invenio_recid": rec_invenioid,
                    "invenio_id": rec_doi,
                    "commons_id": rec_hcid,
                    "core_record_id": rec_recid,
                }
                try:
                    result = {}
                    # FIXME: This is a hack to handle StaleDataError which
                    # is consistently resolved on a second attempt -- seems
                    # to arise when a record is being added to several
                    # communities at once
                    if skip:
                        skipped_records.append(rec_log_object)
                        raise SkipRecord(
                            "Record marked for skipping in override file"
                        )
                    try:
                        result = self.import_record_to_invenio(
                            rec, no_updates, record_source, overrides
                        )
                    except StaleDataError:
                        result = self.import_record_to_invenio(
                            rec, no_updates, record_source, overrides
                        )
                    created_records = self._log_created_record(
                        index=current_record,
                        invenio_id=rec_doi,
                        invenio_recid=result.get("metadata_record_created")
                        .get("record_data")
                        .get("id"),
                        commons_id=rec_hcid,
                        core_record_id=rec_recid,
                        created_records=created_records,
                    )
                    successful_records += 1
                    if not result.get("existing_record"):
                        new_records += 1
                    if "unchanged_existing" in result["status"]:
                        unchanged_existing += 1
                    if result["status"] == "updated_published":
                        updated_published += 1
                    if result["status"] == "updated_draft":
                        updated_drafts += 1
                    if rec_hcid in existing_failed_hcids:
                        app.logger.info(
                            "    repaired previously failed record..."
                        )
                        app.logger.info(
                            f"    {rec_doi} {rec_hcid} {rec_recid}"
                        )
                        residual_failed_records = [
                            d
                            for d in residual_failed_records
                            if d["commons_id"] != rec_hcid
                        ]
                        repaired_failed.append(rec_log_object)
                        failed_records, residual_failed_records = (
                            self._log_failed_record(
                                failed_records=failed_records,
                                residual_failed_records=residual_failed_records,  # noqa: E501
                                skipped_records=skipped_records,
                            )
                        )
                    app.logger.debug("result status: %s", result.get("status"))
                except Exception as e:
                    print("ERROR:", e)
                    print_exc()
                    app.logger.error(f"ERROR: {e}")
                    msg = str(e)
                    try:
                        msg = e.messages
                    except AttributeError:
                        try:
                            msg = e.messages
                        except AttributeError:
                            pass
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
                        "MissingNewUserEmailError": msg,
                        "MissingParentMetadataError": msg,
                        "MultipleActiveCollectionsError": msg,
                        "PublicationValidationError": msg,
                        "RestrictedRecordPublicationError": msg,
                        "StaleDataError": msg,
                        "TooManyViewEventsError": msg,
                        "TooManyDownloadEventsError": msg,
                        "UpdateValidationError": msg,
                        "NoUpdatesError": msg,
                    }
                    log_object = {
                        "index": current_record,
                        "invenio_id": rec_doi,
                        "commons_id": rec_hcid,
                        "core_record_id": rec_recid,
                        "failed_records": failed_records,
                        "residual_failed_records": residual_failed_records,
                    }
                    if e.__class__.__name__ in error_reasons.keys():
                        log_object.update(
                            {"reason": error_reasons[e.__class__.__name__]}
                        )
                    if e.__class__.__name__ not in ["SkipRecord", "NoUpdates"]:
                        failed_records, residual_failed_records = (
                            self._log_failed_record(
                                **log_object, skipped_records=skipped_records
                            )
                        )
                    elif e.__class__.__name__ == "NoUpdates":
                        no_updates_records.append(log_object)
                    if stop_on_error and failed_records:
                        break

                spinner.stop()
                app.logger.info(f"....done with record {current_record}")
                record_counter += 1

        print("Finished!")
        app.logger.info("All done loading records into InvenioRDM")
        set_string = ""
        if nonconsecutive:
            set_string = f"{' '.join([str(n) for n in nonconsecutive])}"
        else:
            target_string = (
                f" to {start_index + record_counter - 1}"
                if record_counter > 1
                else ""
            )
            set_string = f"{start_index}{target_string}"
        successful_records += len(no_updates_records)
        unchanged_existing += len(no_updates_records)
        message = (
            f"Processed {str(record_counter)} records in "
            f"InvenioRDM ({set_string})\n"
            f"    {str(successful_records)} successful \n"
            f"    {str(new_records)} new records created \n"
            f"    {str(successful_records - new_records)} already "
            f"existed \n"
            f"        {str(updated_published)} updated published "
            f"records \n"
            f"        {str(updated_drafts)} updated existing draft records \n"
            f"        {str(unchanged_existing)} unchanged existing records \n"
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

        # Aggregate the stats again now
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
        if aggregate:
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

        # Report
        if verbose and (
            repaired_failed
            or (existing_failed_records and not residual_failed_records)
        ):
            app.logger.info("Previously failed records repaired:")
            for r in repaired_failed:
                print(r)
                app.logger.info(r)

        # Report and log failed records
        if failed_records:
            if verbose:
                app.logger.info("Failed records:")
                for r in failed_records:
                    app.logger.info(r)
            app.logger.info(
                "Failed records written to"
                f" {app.config['RECORD_IMPORTER_FAILED_LOG_PATH']}"
            )
