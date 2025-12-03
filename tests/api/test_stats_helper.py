# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for StatsFabricator and AggregationFabricator."""

from pprint import pformat

import arrow
import pytest
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
from tests.helpers.sample_records import (
    rec583,
    rec11451,
    rec16079,
    rec22625,
    rec22647,
    rec28491,
    rec33383,
    rec34031,
    rec38367,
    rec42615,
    rec44881,
    rec45177,
    rec48799,
)


@pytest.mark.parametrize(
    "json_in",
    [
        (rec42615),
        (rec22625),
        (rec45177),
        (rec44881),
        (rec22647),
        (rec11451),
        (rec34031),
        (rec16079),
        (rec33383),
        (rec38367),
        (rec48799),
        (rec583),
        (rec28491),
    ],
)
def test_create_stats_events(  # noqa: D103
    app,
    db,
    create_stats_indices,
    location,
    search_clear,
    json_in,
    affiliations_v,
    contributors_role_v,
    date_type_v,
    creators_role_v,
    licenses_v,
    subject_v,
    community_type_v,
    resource_type_v,
    description_type_v,
    language_v,
):

    # Have to first create the record and then publish it
    app.logger.warning("Creating record...")
    data = {**json_in["expected_serialized"]}
    draft_record = RecordsHelper().create_invenio_record(data, no_updates=False)
    record_id = draft_record["record_data"]["id"]

    filekey = list(json_in["expected_serialized"]["files"]["entries"].keys())[
        0
    ]
    # Then upload files
    app.logger.warning("Uploading files...")
    files_dict = json_in["expected_serialized"]["files"]["entries"]
    source_filenames = {
        filekey: json_in["expected_serialized"]["custom_fields"][
            "hclegacy:file_location"
        ]
    }

    actual_upload = FilesHelper()._upload_draft_files(
        draft_id=record_id,
        files_dict=files_dict,
        source_filenames=source_filenames,
    )
    assert list(actual_upload.keys())[0] == filekey
    assert actual_upload[filekey] == "uploaded"

    # Publish the record
    app.logger.warning("Publishing record...")
    record = records_service.publish(system_identity, record_id)
    parent_rec_id = record.to_dict()["parent"]["id"]
    assert record.to_dict()["status"] == "published"
    assert record.to_dict()["id"] == record_id

    # Create stats eventsinfo
    app.logger.warning("Creating stats events...")
    events = StatsFabricator().create_stats_events(
        record_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        eager=True,
        verbose=True,
    )

    assert [e for e in events if e[0] == "record-view"][0][1][0] == data[
        "custom_fields"
    ]["hclegacy:total_views"]
    assert [e for e in events if e[0] == "file-download"][0][1][0] == data[
        "custom_fields"
    ]["hclegacy:total_downloads"]

    files_request = records_service.files.list_files(
        system_identity, record_id
    ).to_dict()

    app.logger.warning("Refreshing indices...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")
    file_id = files_request["entries"][0]["file_id"]
    app.logger.warning("Searching for view events...")
    view_events = view_events_search(record_id)
    app.logger.warning("Searching for download events...")
    download_events = download_events_search(file_id)
    assert len(view_events) == data["custom_fields"]["hclegacy:total_views"]
    assert (
        len(download_events)
        == data["custom_fields"]["hclegacy:total_downloads"]
    )

    # Run again to test idempotency
    app.logger.warning("Creating stats events again...")
    events = StatsFabricator().create_stats_events(
        record_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        eager=True,
        verbose=True,
    )
    assert [e for e in events if e[0] == "record-view"][0][1][0] == 0
    assert [e for e in events if e[0] == "file-download"][0][1][0] == 0

    app.logger.warning("Refreshing indices again...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")

    app.logger.warning("Searching for view events again...")
    view_events = view_events_search(record_id)
    download_events = download_events_search(file_id)
    assert len(view_events) == data["custom_fields"]["hclegacy:total_views"]
    assert (
        len(download_events)
        == data["custom_fields"]["hclegacy:total_downloads"]
    )

    # Create stats aggregations
    # Since we re-run the aggregations for each test case,
    # this also tests the idempotency of the aggregations
    # agg_fab = AggregationFabricator()
    # app.logger.warning("Creating stats aggregations...")
    # for yr in range(2017, 2024):
    #     app.logger.warning(f"Creating stats aggregations for {yr}...")
    #     app.logger.warning(arrow.get(f"{yr}-01-01").isoformat())
    #     agg_fab.create_stats_aggregations(
    #         start_date=arrow.get(f"{yr}-01-01"),
    #         end_date=arrow.get(f"{yr}-12-31"),
    #         eager=True,
    #         verbose=True,
    #     )
    #     current_search_client.indices.flush(
    #         ["*record-view*", "*file-download*"], wait_if_ongoing=True
    #     )
    #     view_aggs, download_aggs = aggregations_search(record_id)
    #     view_count = sum([agg.to_dict()["count"] for agg in view_aggs])
    #     download_count = sum([agg.to_dict()["count"] for agg in download_aggs])
    #     app.logger.warning(f"VIEW AGGS {yr} {view_count}")
    #     app.logger.warning(f"DOWNLOAD AGGS {yr} {download_count}")
    app.logger.warning(f"Creating all stats aggregations for {record_id}...")
    AggregationFabricator().create_stats_aggregations(
        start_date=arrow.get("2017-01-01"),
        end_date=arrow.get("2025-01-01"),
        eager=True,
        verbose=True,
    )
    app.logger.warning("Refreshing indices...")
    current_search_client.indices.refresh(index="*record-view*")
    current_search_client.indices.refresh(index="*file-download*")
    app.logger.warning("Searching for view aggregations...")
    view_aggs, download_aggs = aggregations_search(record_id)
    if len(view_aggs) > 0:
        print("VIEW AGGS", pformat(view_aggs[0].to_dict()))
        print("DOWNLOAD AGGS", pformat(download_aggs[0].to_dict()))
    view_count = sum([agg.to_dict()["count"] for agg in view_aggs])
    download_count = sum([agg.to_dict()["count"] for agg in download_aggs])
    view_unique_count = sum(
        [agg.to_dict()["unique_count"] for agg in view_aggs]
    )
    download_unique_count = sum(
        [agg.to_dict()["unique_count"] for agg in download_aggs]
    )
    expected_views = json_in["expected_serialized"]["custom_fields"][
        "hclegacy:total_views"
    ]
    expected_downloads = json_in["expected_serialized"]["custom_fields"][
        "hclegacy:total_downloads"
    ]
    assert view_count == expected_views
    assert download_count == expected_downloads
    assert view_unique_count == expected_views
    assert download_unique_count == expected_downloads

    # Compare those stats to the stats returned by the get_record_stats method
    app.logger.warning("Getting record stats...")
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

