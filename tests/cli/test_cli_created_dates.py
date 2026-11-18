#! /usr/bin/env python
#
# Copyright (C) 2024 MESH Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

"""Tests for created date update CLI functionality."""

from invenio_record_importer_kcworks.cli import update_created_dates


def test_cli_help(running_app, cli_runner):
    """Test CLI help text."""
    result = cli_runner(update_created_dates, "--help")

    assert result.exit_code == 0
    assert "Update created dates" in result.output
    assert "--records-only" in result.output
    assert "--communities-only" in result.output
    assert "--dry-run" in result.output
    assert "--background" in result.output


def test_cli_mutually_exclusive_flags(running_app, cli_runner):
    """Test that records-only and communities-only are mutually exclusive."""
    result = cli_runner(update_created_dates, "--records-only", "--communities-only")

    assert result.exit_code == 0
    assert "Cannot use both" in result.output


def test_cli_invalid_start_date(running_app, cli_runner):
    """Test CLI with invalid start date format."""
    result = cli_runner(update_created_dates, "--start-date", "not-a-date")

    assert result.exit_code == 0
    assert "Invalid start-date format" in result.output


def test_cli_invalid_end_date(running_app, cli_runner):
    """Test CLI with invalid end date format."""
    result = cli_runner(update_created_dates, "--end-date", "invalid-date")

    assert result.exit_code == 0
    assert "Invalid end-date format" in result.output


def test_cli_dry_run_communities_only(
    running_app, sample_community_with_group_id, cli_runner
):
    """Test CLI dry run for communities only."""
    result = cli_runner(
        update_created_dates,
        "--communities-only",
        "--dry-run",
        "--verbose",
        "--batch-size",
        "10",
    )

    assert result.exit_code == 0
    assert "Updating community created dates" in result.output
    assert "Community Update Results" in result.output
    assert "Total found:" in result.output
