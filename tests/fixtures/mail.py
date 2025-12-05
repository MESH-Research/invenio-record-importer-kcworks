# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Fixtures for mail-related testing."""

import pytest


@pytest.fixture
def enable_mail_sending(running_app):
    """Temporarily enable mail sending for a test.

    This fixture:
    1. Stores the original value of MAIL_SUPPRESS_SEND
    2. Sets it to False for the test
    3. Restores the original value after the test
    """
    original_value = running_app.app.config["MAIL_SUPPRESS_SEND"]
    running_app.app.config["MAIL_SUPPRESS_SEND"] = False

    yield  # This is where the test runs

    # After the test, restore the original value
    running_app.app.config["MAIL_SUPPRESS_SEND"] = original_value

