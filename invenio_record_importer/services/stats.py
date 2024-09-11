# -*- coding: utf-8 -*-
#
# This file is part of the invenio_record_importer package.
# Copyright (C) 2024, MESH Research.
#
# invenio_record_importer is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

import arrow
from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import (
    current_rdm_records_service as records_service,
)
import json
from pathlib import Path

# from invenio_rdm_records.services.tasks import reindex_stats

from invenio_record_importer.errors import (
    TooManyViewEventsError,
    TooManyDownloadEventsError,
    FailedCreatingUsageEventsError,
)
from invenio_record_importer.queries import (
    view_events_search,
    download_events_search,
)
from invenio_record_importer.tasks import aggregate_events
from invenio_stats.contrib.event_builders import (
    build_file_unique_id,
    # build_record_unique_id,
)
from invenio_stats.processors import anonymize_user
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import process_events
from pprint import pformat
from typing import Union, Optional, List


class StatsFabricator:
    """Service for creating usage events to fit pre-existing stats for records.

    This class "backfills" pre-existing statistics events for records that
    are being imported. The invenio_stats module records view and
    download events only in the search index, not in the database records.
    This class creates the necessary events in the `events-stats-record-view`
    and `events-stats-file-download` indices to fit the pre-existing stats.
    This allows legacy usage statistics to appear on record detail pages.
    """

    def __init__(self):
        """Initialize the service."""

    def generate_datetimes(self, start, n):
        """
        Generate evenly distributed datetimes between the record creation
        date and the current date.
        """
        # Use a relatively recent start time to avoid issues with
        # creating too many monthly indices and running out of
        # available open shards
        if start < arrow.get("2019-01-01"):
            start = arrow.get("2019-01-01")
        total_seconds = arrow.utcnow().timestamp() - start.timestamp()
        interval = total_seconds / n
        datetimes = [start.shift(seconds=i * interval) for i in range(n)]

        return datetimes

    def fabricate_events_from_file(
        self,
        record_source: str = "knowledgeCommons",
        downloads_field: str = "hclegacy:total_downloads",
        views_field: str = "hclegacy:total_views",
        date_field: str = "metadata.publication_date",
        verbose: bool = False,
    ):
        """
        Create statistics events for the migrated records already in the db.

        This method reads a JSONL file of records and creates statistics events
        for each record. The JSONL file should be a list of dictionaries, where
        each dictionary represents a record and has an "id" key that should
        match the Invenio record id.

        This file should be located in the directory specified by the
        `RECORD_IMPORTER_USAGE_STATS_PATH` configuration variable. The file
        name should be `{record_source}.jsonl`.

        The method creates view and download events for each record and
        publishes them to the stats indices.

        params:
            record_source (str): the source of the records
            downloads_field (str): the dot notation field name for the number
                of downloads
            views_field (str): the dot notation field name for the number of
                views
            date_field (str): the dot notation field name for the record
                creation date
            verbose (bool): whether to print debug information

        returns:
            None
        """
        filename = Path(
            app.config["RECORD_IMPORTER_USAGE_STATS_PATH"],
            f"{record_source}.jsonl",
        )
        with open(filename, "r") as f:
            records = [json.loads(line) for line in f]

        for record in records:
            try:
                self.create_stats_events(
                    record["id"],
                    downloads_field=downloads_field,
                    views_field=views_field,
                    date_field=date_field,
                    eager=True,
                    verbose=verbose,
                )
            except TooManyViewEventsError as e:
                app.logger.error(
                    f"Error creating view events for record {record['id']}:"
                    f"{e}"
                )
                print(
                    f"Error creating view events for record {record['id']}:"
                    f"{e}"
                )

    def fabricate_events_from_db(
        self,
        record_ids: Optional[List[int]] = None,
        record_source: str = "knowledgeCommons",
        downloads_field: str = "hclegacy:total_downloads",
        views_field: str = "hclegacy:total_views",
        date_field: str = "metadata.publication_date",
        verbose: bool = False,
    ):
        """
        Create statistics events for the migrated records already in the db.
        """
        if record_ids:
            record_ids = records_service.read_many(
                system_identity, ids=record_ids
            )
        else:
            # NOTE: if we iterate over the generator directly, OpenSearch
            # tries to use a context pointer that expires before the request
            # can complete. It produces a "no search context found for id XXX"
            # error.
            record_ids = [
                r["id"] for r in records_service.scan(identity=system_identity)
            ]

        for record_id in record_ids:
            try:
                self.create_stats_events(
                    record_id,
                    downloads_field=downloads_field,
                    views_field=views_field,
                    date_field=date_field,
                    eager=True,
                    verbose=verbose,
                )
            except TooManyViewEventsError as e:
                app.logger.error(
                    f"Error creating view events for record {record_id}:"
                    f"{e}"
                )
                print(
                    f"Error creating view events for record {record_id['id']}:"
                    f"{e}"
                )

    def create_stats_events(
        self,
        record_id: str,  # the UUID of the record
        downloads_field: str = "custom_fields.hclegacy:total_downloads",
        views_field: str = "custom_fields.hclegacy:total_views",
        date_field: str = "metadata.publication_date",
        eager=False,
        verbose=False,
    ) -> Union[bool, list]:
        """
        Create artificial statistics events for a migrated record.

        Since Invenio stores statistics as aggregated events in the
        search index, we need to create the individual events that
        will be aggregated. This function creates the individual
        events for a record, simulating views and downloads. It creates
        a given number of events for each type, with the timestamps
        evenly distributed between the record creation date and the
        current date.

        params:
            record_id (str): the record id
            downloads_field (str): the dot notation field name for the number
                of downloads
            views_field (str): the dot notation field name for the number of
                views
            date_field (str): the dot notation field name for the record
                creation date
            eager (bool): whether to process the events immediately or
                queue them for processing in a background task
            verbose (bool): whether to print debug information

        returns:
            Either bool or list, depending on the value of eager. If eager
            is True, the function returns a list of the events. If eager
            is False, the function returns True. In either case, if the
            required fields are not present from which to derive the number
            of views and downloads, the function will print an error message
            and return False.

        """
        if verbose:
            app.logger.info(f"Creating stats events for record {record_id}...")
            print(f"Creating stats events for record {record_id}...")
        rec_search = records_service.read(system_identity, id_=record_id)
        record = rec_search._record

        def get_field_value(record: dict, field: str) -> int:
            field_parts = field.split(".")
            if len(field_parts) > 1:
                return get_field_value(
                    record[field_parts[0]], ".".join(field_parts[1:])
                )
            else:
                return record[field_parts[0]]

        metadata_record = rec_search.to_dict()

        # FIXME: this all assumes a single uploaded file per record on import
        try:
            views = get_field_value(metadata_record, views_field)
            downloads = get_field_value(metadata_record, downloads_field)
            record_creation = arrow.get(
                get_field_value(metadata_record, date_field)
            )
        except KeyError as e:
            app.logger.info(f"Required fields not found for {record_id}: {e}")
            print(f"Required fields not found for {record_id}: {e}")
            return False

        if verbose:
            app.logger.info(f"views: {views}")
            app.logger.info(f"downloads: {downloads}")
            app.logger.info(f"record creation date: {record_creation}")
            print(f"views: {views}")
            print(f"downloads: {downloads}")
            print(f"record creation date: {record_creation}")

        files_request = records_service.files.list_files(
            system_identity, record_id
        ).to_dict()

        first_file = files_request["entries"][0]
        file_id = first_file["file_id"]
        file_key = first_file["key"]
        size = first_file["size"]
        bucket_id = first_file["bucket_id"]

        pid = record.pid

        # Check for existing view and download events
        # imported events are flagged with country: "imported"
        # this is a hack
        existing_view_events = view_events_search(record_id)
        existing_download_events = download_events_search(file_id)
        if verbose:
            app.logger.info(
                "existing view events: "
                f"{pformat(existing_view_events['hits']['total']['value'])}"
            )
            print(
                "existing view events: "
                f"{pformat(existing_view_events['hits']['total']['value'])}"
            )

        existing_view_count = existing_view_events["hits"]["total"]["value"]
        if existing_view_count == views:
            app.logger.info(
                "    skipping view events creation. "
                f"{existing_view_count} "
                "view events already exist."
            )
        else:
            if existing_view_count > views:
                raise TooManyViewEventsError(
                    "    existing imported view events exceed expected count."
                )
            else:
                if verbose:
                    app.logger.info(
                        "    creating view events. "
                        f"{views - existing_view_count} "
                        "view events to create."
                    )
                    print(
                        "    creating view events. "
                        f"{views - existing_view_count} "
                        "view events to create."
                    )
                # count
                if existing_view_count > 0:
                    views -= existing_view_count
                view_events = []
                for dt in self.generate_datetimes(record_creation, views):
                    doc = {
                        "timestamp": dt.naive.isoformat(),
                        "recid": record_id,
                        "parent_recid": metadata_record["parent"]["id"],
                        "unique_id": f"ui_{pid.pid_value}",
                        "is_robot": False,
                        "user_id": "1",
                        "country": "imported",
                        "via_api": False,
                        # FIXME: above is hack to mark synthetic data
                    }
                    doc = anonymize_user(doc)
                    # we can safely pass the doc through
                    # processor.anonymize_user with null/dummy values for
                    # user_id, session_id, user_agent, ip_address
                    # it adds visitor_id and unique_session_id
                    view_events.append(doc)
                current_stats.publish("record-view", view_events)

        existing_download_count = existing_download_events["hits"]["total"][
            "value"
        ]
        if existing_download_count == downloads:
            app.logger.info(
                "    skipping download events creation. "
                f"{existing_download_count} "
                "download events already exist."
            )
        else:
            if existing_download_count > downloads:
                raise TooManyDownloadEventsError(
                    "    existing imported download events exceed expected "
                    "count."
                )
            else:
                # only create enough new download events to reach the expected
                # count
                if existing_download_count > 0:
                    downloads -= existing_download_count
                download_events = []
                for dt in self.generate_datetimes(record_creation, downloads):
                    doc = {
                        "timestamp": dt.naive.isoformat(),
                        "bucket_id": str(bucket_id),  # UUID
                        "file_id": str(file_id),  # UUID
                        "file_key": file_key,
                        "size": size,
                        "recid": record_id,
                        "parent_recid": metadata_record["parent"]["id"],
                        "is_robot": False,
                        "user_id": "1",
                        "country": "imported",
                        "unique_id": f"{str(bucket_id)}_{str(file_id)}",
                        "via_api": False,
                    }
                    doc = anonymize_user(doc)
                    # we can safely pass the doc through
                    # processor.anonymize_user with null/dummy values for
                    # user_id, session_id, user_agent, ip_address
                    # it adds visitor_id and unique_session_id
                    download_events.append(build_file_unique_id(doc))
                current_stats.publish("file-download", download_events)

        try:
            if eager:
                # process_task.apply(throw=True)
                # events = process_events.delay(
                #     ["record-view", "file-download"]
                # ).get()
                events = process_events(["record-view", "file-download"])
                app.logger.info(
                    f"Events processed successfully. {pformat(events)}"
                )
                return events
            else:
                process_task = process_events.si(
                    ["record-view", "file-download"]
                )
                process_task.delay()
                app.logger.info("Event processing task sent...")
                return True
        except Exception as e:
            app.logger.error("Error creating usage events:")
            app.logger.error(str(e))
            raise FailedCreatingUsageEventsError(
                "Error creating usage events: {str(e)}"
            )


class AggregationFabricator:
    """Service for creating statistics aggregations for records.

    This class creates the necessary aggregations for records that are being
    imported. The invenio_stats module records aggregated statistics in the
    `stats-record-view` and `stats-file-download` indices. Normally, these
    aggregations are created by a periodic background task, but that task
    only aggregates events that have occurred since the last aggregation
    (i.e., since the last aggregation bookmark recorded in the
    `stats-bookmarks` index). So it will not pick up newly created events
    that have been back-filled by the StatsFabricator class.

    This class creates the necessary aggregations by bypassing the
    bookmark mechanism and aggregating all events in the chosen time period.
    This class should be run after the StatsFabricator class has been run.
    """

    def __init__(self):
        """Initialize the service."""

    def create_stats_aggregations(
        self,
        start_date: str = None,
        end_date: str = None,
        bookmark_override=None,
        eager=False,
    ) -> Union[bool, list]:
        """
        Create statistics aggregations for the migrated records.

        This function triggers the creation of statistics aggregations
        for the migrated records. It is intended to be run after all
        the individual statistics events have been created for the
        migrated records.

        This method is idempotent and can be run multiple times. It will
        recreate aggregations based on the current cumulative totals for
        each record, including any that were created by the StatsFabricator.

        params:
            start_date (str): the start date for the aggregations
            end_date (str): the end date for the aggregations
            eager (bool): whether to process the aggregations immediately
                or queue them for processing in a background task

        returns:
            Either bool or list, depending on the value of eager. If eager
            is True, the function returns a list of the aggregations. If
            eager is False, the function returns True.

        """

        aggregation_types = list(current_stats.aggregations)
        agg_task = aggregate_events.si(
            aggregation_types,
            start_date=(
                arrow.get(start_date).naive.isoformat() if start_date else None
            ),
            end_date=(
                arrow.get(end_date).naive.isoformat() if end_date else None
            ),
            update_bookmark=True,  # is this right?
            bookmark_override=bookmark_override,
        )
        if eager:
            aggs = agg_task.apply(throw=True)
            app.logger.info("Aggregations processed successfully.")

            # implement invenio_rdm_records.services.tasks.reindex_stats here
            # so that stats show up in records
            # but call current_rdm_records.records_service.reindex directly
            # on a list of imported record ids
            return aggs
        else:
            agg_task.delay()
            app.logger.info("Aggregations processing task sent...")
            return True
