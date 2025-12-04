# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for StatsFabricator and AggregationFabricator."""

from pprint import pformat

import arrow
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.records.stats.api import Statistics
from invenio_search import current_search_client

from invenio_record_importer_kcworks.queries import (
    aggregations_search,
    download_events_search,
    view_events_search,
)
from invenio_record_importer_kcworks.services.files import FilesHelper
from invenio_record_importer_kcworks.services.records import RecordsHelper
from invenio_record_importer_kcworks.services.stats.stats import (
    AggregationFabricator,
    StatsFabricator,
)
from tests.helpers.sample_records import rec42615


def test_create_stats_events(  # noqa: D103
    running_app,
    db,
    create_stats_indices,
    search_clear,
    test_sample_files_folder,
    set_app_config_fn_scoped,
    reindex_languages,
):
    # Set RECORD_IMPORTER_FILES_LOCATION to point to the sample_files directory
    set_app_config_fn_scoped({
        "RECORD_IMPORTER_FILES_LOCATION": str(test_sample_files_folder)
    })

    # Use a single test record
    json_in = rec42615

    # Have to first create the record and then publish it
    # Use a narrow, fixed date range for tests to minimize OpenSearch operations
    # Set the publication date to the current month so events are within our range
    test_date = arrow.utcnow().floor("month")
    test_end_date = min(test_date.shift(months=1), arrow.utcnow().ceil("day"))
    test_publication_date = test_date.format("YYYY-MM-DD")

    running_app.app.logger.warning("Creating record...")
    data = {**json_in["expected_serialized"]}
    # Override publication date to be within our test range
    data["metadata"]["publication_date"] = test_publication_date
    draft_record = RecordsHelper().create_invenio_record(data, no_updates=False)
    record_id = draft_record["record_data"]["id"]

    filekey = list(json_in["expected_serialized"]["files"]["entries"].keys())[0]
    # Then upload files
    running_app.app.logger.warning("Uploading files...")
    # Get the actual file size from the test file
    test_file_path = test_sample_files_folder / filekey
    test_file_size = test_file_path.stat().st_size
    # Use the files_dict from the sample record, but update the size to match
    # the actual file
    files_dict = json_in["expected_serialized"]["files"]["entries"].copy()
    files_dict[filekey]["size"] = test_file_size
    # Use a simplified production-style path that will be sanitized to just the
    # filename. The sanitize_filename method strips the production prefix.
    production_file_path = f"/srv/www/commons/current/web/app/uploads/humcore/{filekey}"
    source_filenames = {filekey: production_file_path}

    actual_upload = FilesHelper(is_draft=True)._upload_draft_files(
        draft_id=record_id,
        files_dict=files_dict,
        source_filenames=source_filenames,
    )
    assert list(actual_upload.keys())[0] == filekey
    assert actual_upload[filekey][0] == "uploaded"

    # Publish the record
    running_app.app.logger.warning("Publishing record...")
    record = records_service.publish(system_identity, record_id)
    parent_rec_id = record.to_dict()["parent"]["id"]
    assert record.to_dict()["status"] == "published"
    assert record.to_dict()["id"] == record_id

    # Create stats eventsinfo
    # Events will be distributed from the publication date (which we set to test_date)
    # to test_end_date, keeping them within our narrow test range
    running_app.app.logger.warning("Creating stats events...")
    events = StatsFabricator().create_stats_events(
        record_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        end_date=test_end_date,
        eager=True,
        verbose=True,
    )

    # Note: events return value includes all processed events, not just this record
    # So we check the actual events in the search index instead
    files_request = records_service.files.list_files(
        system_identity, record_id
    ).to_dict()

    running_app.app.logger.warning("Refreshing indices...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")
    file_id = files_request["entries"][0]["file_id"]
    running_app.app.logger.warning("Searching for view events...")
    view_events = view_events_search(record_id)
    running_app.app.logger.warning("Searching for download events...")
    download_events = download_events_search(file_id)
    
    # Check actual event counts for this record
    assert len(view_events) == data["custom_fields"]["hclegacy:total_views"]
    assert len(download_events) == data["custom_fields"]["hclegacy:total_downloads"]

    # Run again to test idempotency
    running_app.app.logger.warning("Creating stats events again...")
    events = StatsFabricator().create_stats_events(
        record_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        end_date=test_end_date,
        eager=True,
        verbose=True,
    )
    assert [e for e in events if e[0] == "record-view"][0][1][0] == 0
    assert [e for e in events if e[0] == "file-download"][0][1][0] == 0

    running_app.app.logger.warning("Refreshing indices again...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")

    running_app.app.logger.warning("Searching for view events again...")
    view_events = view_events_search(record_id)
    running_app.app.logger.error(f"View event sample: {pformat(view_events[0])}")
    running_app.app.logger.error(
        f"View events dates: {pformat([e['timestamp'] for e in view_events])}"
    )
    download_events = download_events_search(file_id)
    assert len(view_events) == data["custom_fields"]["hclegacy:total_views"]
    assert len(download_events) == data["custom_fields"]["hclegacy:total_downloads"]

    # Create stats aggregations
    # Events are now distributed over just one day (test_date to test_end_date)
    # Aggregate just that day to catch all events efficiently
    start_date = test_date
    end_date = test_end_date

    # Ensure event indices are refreshed before aggregation
    running_app.app.logger.warning("Refreshing event indices before aggregation...")
    current_search_client.indices.refresh(index="*events-stats-record-view*")
    current_search_client.indices.refresh(index="*events-stats-file-download*")

    running_app.app.logger.warning(
        f"Creating stats aggregations for {record_id} ({start_date} to {end_date})..."
    )
    AggregationFabricator().create_stats_aggregations(
        start_date=start_date,
        end_date=end_date,
        eager=True,
        verbose=True,
    )
    running_app.app.logger.warning("Refreshing indices...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")
    running_app.app.logger.warning("Searching for view aggregations...")
    view_aggs, download_aggs = aggregations_search(record_id)
    if len(view_aggs) > 0:
        print("VIEW AGGS", pformat(view_aggs[0].to_dict()))
        print("DOWNLOAD AGGS", pformat(download_aggs[0].to_dict()))
    view_count = sum([agg.to_dict()["count"] for agg in view_aggs])
    download_count = sum([agg.to_dict()["count"] for agg in download_aggs])
    view_unique_count = sum([agg.to_dict()["unique_count"] for agg in view_aggs])
    download_unique_count = sum([
        agg.to_dict()["unique_count"] for agg in download_aggs
    ])
    expected_views = data["custom_fields"]["hclegacy:total_views"]
    expected_downloads = data["custom_fields"]["hclegacy:total_downloads"]
    assert view_count == expected_views
    assert download_count == expected_downloads
    assert view_unique_count == expected_views
    assert download_unique_count == expected_downloads

    # Compare those stats to the stats returned by the get_record_stats method
    running_app.app.logger.warning("Getting record stats...")
    record_stats = Statistics.get_record_stats(record_id, parent_rec_id)
    this_version_stats = record_stats["this_version"]
    all_versions_stats = record_stats["all_versions"]
    assert this_version_stats["views"] == expected_views
    assert this_version_stats["unique_views"] == expected_views
    assert this_version_stats["downloads"] == expected_downloads
    assert this_version_stats["unique_downloads"] == expected_downloads
    assert all_versions_stats["views"] == expected_views
    assert all_versions_stats["unique_views"] == expected_views
    assert all_versions_stats["downloads"] == expected_downloads
    assert all_versions_stats["unique_downloads"] == expected_downloads
