# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the RecordImportService.import_records with metadata errors."""

import copy
from typing import Any

from tests.helpers.sample_records import (
    sample_metadata_chapter2_pdf,
    sample_metadata_chapter_pdf,
)

from .test_service_metadata_success import ServiceTest


class TestServiceErrorJArticleTitle(ServiceTest):
    """Test importing two journal articles with an error in the title."""

    @property
    def metadata_sources(self):  # noqa: D102
        meta1: dict[str, Any] = copy.deepcopy(sample_metadata_chapter_pdf)
        meta1["metadata"]["title"] = ""
        meta2: dict[str, Any] = copy.deepcopy(sample_metadata_chapter2_pdf)
        return [meta1, meta2]

    @property
    def expected_errors(self):  # noqa: D102
        return [
            [
                {
                    "validation_error": [
                        {
                            "field": "metadata.title",
                            "messages": ["Missing data for required field."],
                        }
                    ]
                }
            ],
            [],
        ]
