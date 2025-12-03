#! /usr/bin/env python
#
# Copyright (C) 2024 MESH Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

"""Tests for created date update functionality."""

import arrow
import pytest
from click.testing import CliRunner
from invenio_access.permissions import system_identity
from invenio_communities.communities.records.api import Community
from invenio_communities.proxies import current_communities
from invenio_rdm_records.proxies import (
    current_rdm_records_service as records_service,
)
from invenio_rdm_records.records.api import RDMRecord
from invenio_search.proxies import current_search_client

from invenio_record_importer_kcworks.cli import update_created_dates
from invenio_record_importer_kcworks.services.communities import CommunitiesHelper
from invenio_record_importer_kcworks.services.records import RecordsHelper


@pytest.fixture(scope="function")
def sample_record_with_legacy_date(
    app,
    db,
    search_clear,
    create_records_custom_fields,
    minimal_published_record_factory,
):
    """Create a sample record with hclegacy:record_creation_date.

    Yields:
        dict: Dictionary containing:
            - id (str): The record ID
            - legacy_date (str): The legacy creation date
            - current_date (str): The current date
    """
    # Create a record with a legacy creation date
    legacy_date = "2020-01-15T10:30:00Z"
    current_date = arrow.utcnow().isoformat()

    # Create record using factory
    record = minimal_published_record_factory(
        metadata={
            "custom_fields": {
                "hclegacy:record_creation_date": legacy_date,
            }
        }
    )
    record_id = record.id

    # Ensure the record is committed to the database and indexed
    db.session.commit()
    RDMRecord.index.refresh()

    yield {
        "id": record_id,
        "legacy_date": legacy_date,
        "current_date": current_date,
    }


# Tests for RecordsHelper created date methods


def test_check_opensearch_health_records(running_app):
    """Test OpenSearch health check."""
    helper = RecordsHelper()
    health = helper.check_opensearch_health()

    assert "is_healthy" in health
    assert "reason" in health
    assert "status" in health
    assert isinstance(health["is_healthy"], bool)


def test_find_records_needing_update(running_app, sample_record_with_legacy_date):
    """Test finding records that need created date updates."""
    helper = RecordsHelper()
    records = helper.find_records_needing_created_date_update()

    assert len(records) >= 1
    record_ids = [r["id"] for r in records]
    assert sample_record_with_legacy_date["id"] in record_ids

    # Check record structure
    matching_record = next(
        r for r in records if r["id"] == sample_record_with_legacy_date["id"]
    )
    assert "current_created" in matching_record
    assert "new_created" in matching_record
    assert "pid" in matching_record


def test_find_records_with_date_filter(running_app, sample_record_with_legacy_date):
    """Test finding records with date range filters."""
    helper = RecordsHelper()

    # Filter to include the record
    start_date = arrow.utcnow().shift(days=-1).format("YYYY-MM-DD")
    records = helper.find_records_needing_created_date_update(start_date=start_date)
    record_ids = [r["id"] for r in records]
    assert sample_record_with_legacy_date["id"] in record_ids

    # Filter to exclude the record
    end_date = arrow.utcnow().shift(days=-2).format("YYYY-MM-DD")
    records = helper.find_records_needing_created_date_update(end_date=end_date)
    record_ids = [r["id"] for r in records]
    assert sample_record_with_legacy_date["id"] not in record_ids


def test_update_single_record_created_date(running_app, sample_record_with_legacy_date):
    """Test updating a single record's created date."""
    helper = RecordsHelper()
    record_id = sample_record_with_legacy_date["id"]
    new_date = sample_record_with_legacy_date["legacy_date"]

    # Update the record
    updated = helper.update_single_record_created_date(record_id, new_date)
    assert updated is True

    # Verify the update
    record = records_service.read(system_identity, id_=record_id)._record
    assert arrow.get(record.model.created) == arrow.get(new_date)

    # Try updating again with same date - should skip
    updated = helper.update_single_record_created_date(record_id, new_date)
    assert updated is False


def test_update_single_record_invalid_date(running_app, sample_record_with_legacy_date):
    """Test updating with invalid date format raises error."""
    helper = RecordsHelper()
    record_id = sample_record_with_legacy_date["id"]

    with pytest.raises(ValueError, match="Invalid timestamp format"):
        helper.update_single_record_created_date(record_id, "not-a-valid-date")


def test_update_record_created_dates_dry_run(
    running_app, sample_record_with_legacy_date
):
    """Test dry run mode doesn't make changes."""
    helper = RecordsHelper()

    # Get original created date
    record_id = sample_record_with_legacy_date["id"]
    original_record = records_service.read(system_identity, id_=record_id)._record
    original_created = original_record.model.created.isoformat()

    # Run in dry-run mode
    stats = helper.update_record_created_dates(
        batch_size=10, dry_run=True, verbose=True
    )

    assert stats["total_found"] >= 1
    assert stats["updated"] >= 1
    assert "errors" in stats

    # Verify no actual changes were made
    record = records_service.read(system_identity, id_=record_id)._record
    assert arrow.get(record.model.created) == arrow.get(original_created)


def test_update_record_created_dates(running_app, sample_record_with_legacy_date):
    """Test updating all records' created dates."""
    helper = RecordsHelper()

    stats = helper.update_record_created_dates(batch_size=10, verbose=True)

    assert stats["total_found"] >= 1
    assert stats["updated"] >= 1
    assert isinstance(stats["errors"], list)

    # Verify the update
    record_id = sample_record_with_legacy_date["id"]
    record = records_service.read(system_identity, id_=record_id)._record
    expected_date = sample_record_with_legacy_date["legacy_date"]
    # Compare as arrow objects to handle timezone differences
    assert arrow.get(record.model.created) == arrow.get(expected_date)


# Tests for CommunitiesHelper created date methods


def test_check_opensearch_health_communities(running_app):
    """Test OpenSearch health check."""
    helper = CommunitiesHelper()
    health = helper.check_opensearch_health()

    assert "is_healthy" in health
    assert "reason" in health
    assert "status" in health
    assert isinstance(health["is_healthy"], bool)


def test_find_communities_needing_update(running_app, sample_community_with_group_id):
    """Test finding communities that need created date updates."""
    helper = CommunitiesHelper()
    communities = helper.find_communities_needing_created_date_update()

    assert len(communities) >= 1
    community_ids = [c["id"] for c in communities]
    assert sample_community_with_group_id["id"] in community_ids

    # Check community structure
    matching_community = next(
        c for c in communities if c["id"] == sample_community_with_group_id["id"]
    )
    assert "slug" in matching_community
    assert "group_id" in matching_community
    assert "current_created" in matching_community
    assert matching_community["group_id"] == sample_community_with_group_id["group_id"]


def test_find_oldest_record_no_records(running_app, sample_community_with_group_id):
    """Test finding oldest record when no records exist."""
    helper = CommunitiesHelper()
    group_id = sample_community_with_group_id["group_id"]

    oldest = helper.find_oldest_record_for_community(group_id)
    assert oldest is None


def test_find_oldest_record_with_records(
    running_app,
    sample_community_with_group_id,
    minimal_published_record_factory,
):
    """Test finding oldest record when records exist."""
    group_id = sample_community_with_group_id["group_id"]

    # Create records with different creation dates using factory
    dates = [
        "2020-03-15T10:00:00Z",
        "2019-06-20T14:30:00Z",  # This should be the oldest
        "2021-01-10T08:15:00Z",
    ]

    created_records = []
    for date in dates:
        record = minimal_published_record_factory(
            metadata={
                "custom_fields": {
                    "hclegacy:record_creation_date": date,
                    "hclegacy:groups_for_deposit": [
                        {
                            "group_identifier": group_id,
                            "group_name": "Test Group",
                        }
                    ],
                }
            }
        )

        # Explicitly index the record since UnitOfWork isn't doing it in tests
        records_service.indexer.index(record._record)
        # Refresh immediately after indexing
        current_search_client.indices.refresh(index="*rdmrecords*")

        created_records.append({
            'pid': record['id'],
            'date': date,
        })

    # Final refresh to be sure
    current_search_client.indices.refresh(index="*rdmrecords*")

    # Find oldest record
    helper = CommunitiesHelper()
    oldest = helper.find_oldest_record_for_community(group_id)

    assert oldest is not None
    assert isinstance(oldest, arrow.Arrow)
    # Should be floored to start of day
    expected = arrow.get("2019-06-20T00:00:00Z")
    assert oldest == expected


def test_update_single_community_created_date(
    running_app, sample_community_with_group_id
):
    """Test updating a single community's created date."""
    helper = CommunitiesHelper()
    community_id = sample_community_with_group_id["id"]
    current_date = sample_community_with_group_id["current_date"]

    # Update to an earlier date
    new_date = current_date.shift(years=-2).floor("day")
    updated = helper.update_single_community_created_date(community_id, new_date)
    assert updated is True

    # Verify the update
    community = current_communities.service.read(
        system_identity, id_=community_id
    )._record
    assert arrow.get(community.model.created) == new_date

    # Try updating with a later date - should skip
    later_date = current_date.shift(years=1)
    updated = helper.update_single_community_created_date(community_id, later_date)
    assert updated is False


def test_update_community_created_dates_dry_run(
    running_app, sample_community_with_group_id
):
    """Test dry run mode doesn't make changes."""
    helper = CommunitiesHelper()

    # Get original created date
    community_id = sample_community_with_group_id["id"]
    original_community = current_communities.service.read(
        system_identity, id_=community_id
    )._record
    original_created = arrow.get(original_community.model.created)

    # Run in dry-run mode
    stats = helper.update_community_created_dates(
        batch_size=10, dry_run=True, verbose=True
    )

    assert stats["total_found"] >= 1
    # May be skipped if no records found
    assert "errors" in stats
    assert "no_records" in stats

    # Verify no actual changes were made
    community = current_communities.service.read(
        system_identity, id_=community_id
    )._record
    assert arrow.get(community.model.created) == original_created


def test_update_community_created_dates_no_records(
    running_app, sample_community_with_group_id
):
    """Test updating communities when no matching records exist."""
    helper = CommunitiesHelper()

    stats = helper.update_community_created_dates(batch_size=10, verbose=True)

    assert stats["total_found"] >= 1
    assert stats["no_records"] >= 1  # Our test community has no records
    assert isinstance(stats["errors"], list)


# Tests for CLI command


def test_cli_help(running_app):
    """Test CLI help text."""
    runner = CliRunner()
    result = runner.invoke(update_created_dates, ["--help"])

    assert result.exit_code == 0
    assert "Update created dates" in result.output
    assert "--records-only" in result.output
    assert "--communities-only" in result.output
    assert "--dry-run" in result.output
    assert "--background" in result.output


def test_cli_mutually_exclusive_flags(running_app):
    """Test that records-only and communities-only are mutually exclusive."""
    runner = CliRunner()
    result = runner.invoke(
        update_created_dates, ["--records-only", "--communities-only"]
    )

    assert result.exit_code == 0
    assert "Cannot use both" in result.output


def test_cli_invalid_start_date(running_app):
    """Test CLI with invalid start date format."""
    runner = CliRunner()
    result = runner.invoke(update_created_dates, ["--start-date", "not-a-date"])

    assert result.exit_code == 0
    assert "Invalid start-date format" in result.output


def test_cli_invalid_end_date(running_app):
    """Test CLI with invalid end date format."""
    runner = CliRunner()
    result = runner.invoke(update_created_dates, ["--end-date", "invalid-date"])

    assert result.exit_code == 0
    assert "Invalid end-date format" in result.output


def test_cli_dry_run_records_only(running_app, sample_record_with_legacy_date):
    """Test CLI dry run for records only."""
    runner = CliRunner()
    result = runner.invoke(
        update_created_dates,
        ["--records-only", "--dry-run", "--verbose", "--batch-size", "10"],
    )

    assert result.exit_code == 0
    assert "Updating record created dates" in result.output
    assert "Record Update Results" in result.output
    assert "Total found:" in result.output


def test_cli_dry_run_communities_only(running_app, sample_community_with_group_id):
    """Test CLI dry run for communities only."""
    runner = CliRunner()
    result = runner.invoke(
        update_created_dates,
        [
            "--communities-only",
            "--dry-run",
            "--verbose",
            "--batch-size",
            "10",
        ],
    )

    assert result.exit_code == 0
    assert "Updating community created dates" in result.output
    assert "Community Update Results" in result.output
    assert "Total found:" in result.output


def test_cli_with_date_range(running_app, sample_record_with_legacy_date):
    """Test CLI with date range filters."""
    runner = CliRunner()
    start_date = arrow.utcnow().shift(days=-1).format("YYYY-MM-DD")
    end_date = arrow.utcnow().shift(days=1).format("YYYY-MM-DD")

    result = runner.invoke(
        update_created_dates,
        [
            "--records-only",
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "Record Update Results" in result.output


# Integration tests


def test_full_workflow_records_then_communities(
    running_app,
    db,
    search_clear,
    sample_community_with_group_id,
    minimal_published_record_factory,
):
    """Test complete workflow: update records, then communities."""
    group_id = sample_community_with_group_id["group_id"]
    community_id = sample_community_with_group_id["id"]

    # Create a record with legacy date in the community using factory
    legacy_date = "2019-05-20T10:00:00Z"
    record = minimal_published_record_factory(
        metadata={
            "custom_fields": {
                "hclegacy:record_creation_date": legacy_date,
                "hclegacy:groups_for_deposit": [
                    {
                        "group_identifier": group_id,
                        "group_name": "Test Group",
                    }
                ],
            }
        }
    )
    record_id = record.id

    # Refresh indices
    RDMRecord.index.refresh()
    Community.index.refresh()

    # Step 1: Update record created dates
    records_helper = RecordsHelper()
    record_stats = records_helper.update_record_created_dates(batch_size=10)

    assert record_stats["updated"] >= 1

    # Verify record was updated
    updated_record = records_service.read(system_identity, id_=record_id)._record
    assert arrow.get(updated_record.model.created) == arrow.get(legacy_date)

    # Refresh indices again
    RDMRecord.index.refresh()

    # Step 2: Update community created dates
    communities_helper = CommunitiesHelper()
    community_stats = communities_helper.update_community_created_dates(batch_size=10)

    assert community_stats["total_found"] >= 1

    # Verify community was updated to start of day
    updated_community = current_communities.service.read(
        system_identity, id_=community_id
    )._record
    expected_date = arrow.get("2019-05-20T00:00:00Z")
    assert arrow.get(updated_community.model.created) == expected_date
