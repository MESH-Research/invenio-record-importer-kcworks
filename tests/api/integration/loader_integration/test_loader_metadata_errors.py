# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the API record import."""

import copy

from invenio_record_importer_kcworks.types import LoaderResult
from tests.fixtures.records import TestRecordMetadata
from tests.helpers.sample_records import (
    sample_metadata_journal_article_pdf,
)

from .test_loader_metadata_success import LoaderTest


class LoaderErrorTest(LoaderTest):
    """Base class for testing record imports with errors."""

    def check_result_status(self, result: LoaderResult):
        """Check the status of the result."""
        assert result.status == "error"

    def check_result_record_created(
        self, result: LoaderResult, test_metadata: TestRecordMetadata
    ):
        """Check the record created of the result."""
        assert result.record_created == {
            "record_data": {},
            "record_uuid": "",
            "status": "deleted",
        }

    def check_result_community_review_result(
        self, result: LoaderResult, community: dict, test_metadata: TestRecordMetadata
    ):
        """Check the community review result of the result."""
        assert result.community_review_result == {}


class TestLoaderErrorJArticleTitle(LoaderErrorTest):
    """Test importing a journal article with an empty title."""

    @property
    def metadata_source(self):  # noqa: D102
        return copy.deepcopy(sample_metadata_journal_article_pdf)

    def modify_metadata(self, test_metadata: TestRecordMetadata):  # noqa: D102
        test_metadata.update_metadata({"metadata|title": ""})

    def check_result_errors(self, result: LoaderResult):  # noqa: D102
        assert result.errors == [
            {
                "validation_error": [
                    {
                        "field": "metadata.title",
                        "messages": ["Missing data for required field."],
                    }
                ]
            }
        ]


class TestLoaderErrorJArticleIDScheme(LoaderErrorTest):
    """Test importing a journal article with an invalid metadata.identifiers scheme."""

    @property
    def metadata_source(self):  # noqa: D102
        return copy.deepcopy(sample_metadata_journal_article_pdf)

    def modify_metadata(self, test_metadata: TestRecordMetadata):  # noqa: D102
        test_metadata.update_metadata({
            "metadata|identifiers": [
                {"identifier": "hc:33383", "scheme": "my-made-up-scheme"},
                {"identifier": "1234567890", "scheme": "import-recid"},
            ]
        })

    def check_result_errors(self, result: LoaderResult):  # noqa: D102
        assert result.errors == [
            {
                "validation_error": [
                    {
                        "field": "metadata.identifiers.0.scheme",
                        "messages": ["Invalid scheme."],
                    }
                ]
            }
        ]
