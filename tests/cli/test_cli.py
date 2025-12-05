# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for CLI commands."""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from invenio_record_importer_kcworks.cli import cli
from invenio_record_importer_kcworks.types import APIResponsePayload


def test_record_loader(running_app, admin, minimal_community_factory):
    """Test the CLI load command."""
    # Create a community for the records to be imported into
    community_record = minimal_community_factory(slug="test-community")
    community_id = community_record.to_dict()["id"]

    # Mock the RecordLoader.load_all method to return a successful response
    mock_response = APIResponsePayload(
        status="success",
        data=[
            {
                "item_index": 1,
                "record_id": "test-record-id",
                "source_id": "test-source-id",
                "record_url": "https://example.com/records/test-record-id",
                "files": {},
                "collection_id": community_id,
                "errors": [],
                "metadata": {},
            }
        ],
        errors=[],
        message="All records were successfully imported",
    )

    with patch("invenio_record_importer_kcworks.cli.RecordLoader") as mock_loader_class:
        mock_loader_instance = MagicMock()
        mock_loader_instance.load_all.return_value = mock_response
        mock_loader_class.return_value = mock_loader_instance

        runner = CliRunner()
        # Use 1-indexed record numbers (1 = first record in JSONL file)
        # Pass the community_id via --community-id argument
        result = runner.invoke(cli, ["load", "1", "--community-id", community_id])
        assert result.exit_code == 0
        # Verify that load_all was called
        mock_loader_instance.load_all.assert_called_once()
