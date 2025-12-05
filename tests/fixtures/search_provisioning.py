# Part of invenio-record-importer-kcworks.
#
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

"""Search provisioning related pytest fixtures for testing."""

import pytest


@pytest.fixture
def mock_send_remote_api_update_fixture(mocker):
    """Dummy fixture for mocking remote API updates.
    
    This is a no-op fixture since invenio-remote-api-provisioner is not a
    dependency of this submodule. Tests that use this fixture will work
    regardless of whether the provisioner is installed or not.
    """
    # No-op: don't actually patch anything since the dependency isn't available
    pass

