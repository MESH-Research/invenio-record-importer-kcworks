# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Pytest configuration for deprecated serializer tests."""

import sys
from pathlib import Path

import pytest

# Add deprecated folder to path so we can import serializer
_deprecated_path = Path(__file__).parent.parent
if str(_deprecated_path) not in sys.path:
    sys.path.insert(0, str(_deprecated_path))

from serializer import serialize_json


@pytest.fixture(scope="module")
def serialized_records(app):
    """Serialized records fixture.

    Returns:
        dict: Dictionary containing serialized records and bad data.
    """
    actual_serialized_json, actual_bad_data = serialize_json()
    return {
        "actual_serialized_json": actual_serialized_json,
        "actual_bad_data": actual_bad_data,
    }

