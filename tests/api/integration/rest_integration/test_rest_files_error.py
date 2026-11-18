# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the v1 API /import endpoint with files errors."""


from ..service_integration.test_service_files_error import (
    TestServiceErrorJArticleMissingFile,
)


class TestAPIErrorJArticleMissingFile(TestServiceErrorJArticleMissingFile):
    """Test importing two journal articles with an error in the file via the API."""

    @property
    def by_api(self):  # noqa: D102
        return True
