# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for CLI commands."""

from click.testing import CliRunner

from invenio_record_importer_kcworks.cli import cli


def test_record_loader(app, admin):
    """Test the CLI load command."""
    # app.config["RECORD_IMPORTER_API_TOKEN"] = admin.allowed_token
    runner = CliRunner()
    result = runner.invoke(cli, ["load", "0", "1"])
    assert result.exit_code == 0
    assert "Finished!" in result.output
    assert "Created 1 records in InvenioRDM" in result.output

