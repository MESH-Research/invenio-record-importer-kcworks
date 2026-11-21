# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the v1 API /import endpoint with metadata errors."""

import copy

from tests.fixtures.communities import make_community_member
from tests.helpers.sample_records import (
    sample_metadata_journal_article2_pdf,
    sample_metadata_journal_article3_pdf,
    sample_metadata_journal_article_pdf,
)

from ..service_integration.test_service_metadata_error import (
    TestServiceErrorJArticleTitle,
)
from ..service_integration.test_service_metadata_success import ServiceTest
from .test_rest_metadata_success import TestAPIJArticle


class BaseAPIErrorPermissionsTest(TestAPIJArticle):
    """Base class for tests that check the API with insufficient permissions."""

    def check_result_status(self, import_results: dict, status_code: int | None):
        """Check the status code of the import results.

        Returns:
            bool: True if status check passes.
        """
        if self.by_api:
            assert status_code == 403
        assert import_results.get("message") == (
            "The user does not have the necessary permissions to "
            "import records via this endpoint."
        )
        return False  # to stop the test execution from looking for data/errors


class TestAPIErrorPermissionsReader(BaseAPIErrorPermissionsTest):
    """Test importing records via the API with insufficient permissions.

    The community allows direct publishing by curators and managers
    (review policy "open"), but uploader is only a reader.
    """

    @property
    def community_access_override(self):  # noqa: D102
        return {"review_policy": "open", "record_policy": "open"}

    def make_submitter(self, user_factory, community_id):
        """Try using API with a user that is just a "reader" in the community.

        Returns:
            tuple: User ID and token.
        """
        new_user = user_factory(email="another@example.com", token=True, oauth_id=None)
        make_community_member(new_user.user.id, "reader", community_id)
        return new_user.user.id, new_user.allowed_token


class TestAPIErrorPermissionsCurator(BaseAPIErrorPermissionsTest):
    """Test importing records via the API with insufficient permissions.

    The community does not allow direct publishing (review policy "closed"),
    and uploader is only a manager.
    """

    @property
    def community_access_override(self):  # noqa: D102
        return {"review_policy": "closed", "record_policy": "closed"}

    def make_submitter(self, user_factory, community_id):
        """Try using API with a user that is just a "reader" in the community.

        Returns:
            tuple: User ID and token.
        """
        new_user = user_factory(email="another@example.com", token=True, oauth_id=None)
        make_community_member(new_user.user.id, "manager", community_id)
        return new_user.user.id, new_user.allowed_token


class TestAPIErrorPermissionsOwner(BaseAPIErrorPermissionsTest):
    """Test importing records via the API with insufficient permissions.

    The community allows direct publishing (review policy "open"),
    but uploader is not a community member.
    """

    @property
    def community_access_override(self):  # noqa: D102
        return {"review_policy": "open", "record_policy": "open"}

    def make_submitter(self, user_factory, community_id) -> tuple[int, str]:
        """Try using API with a user that is not a community member.

        Returns:
            tuple[int, str]: A tuple of the new User's id [0] and bearer
                token [1]
        """
        new_user = user_factory(email="another@example.com", token=True, oauth_id=None)
        return new_user.user.id, new_user.allowed_token


class TestAPIErrorJArticleTitle(TestServiceErrorJArticleTitle):
    """Test importing two journal articles with an error in the title via the API."""

    @property
    def by_api(self):  # noqa: D102
        return True


def _metadata_only(sample: dict) -> dict:
    """Return a deep copy with files disabled for a metadata-only import."""
    meta = copy.deepcopy(sample)
    meta["files"] = {"enabled": False}
    return meta


class MixedPublishValidationRESTTest(ServiceTest):
    """One REST import with three publish outcomes in a single request.

    Record 0: empty title → marshmallow ``ValidationError`` on publish.
    Record 1: invalid external DOI → ``ValidationErrorWithMessageAsList``.
    Record 2: valid metadata-only record → success when ``all_or_none`` is false.
    """

    @property
    def by_api(self):  # noqa: D102
        return True

    @property
    def with_files(self):  # noqa: D102
        return False

    @property
    def metadata_sources(self):  # noqa: D102
        bad_title = _metadata_only(sample_metadata_journal_article_pdf)
        bad_title["metadata"]["title"] = ""

        bad_pid = _metadata_only(sample_metadata_journal_article2_pdf)
        bad_pid["pids"] = {
            "doi": {
                "identifier": "not-a-valid/doi",
                "provider": "external",
            }
        }

        good = _metadata_only(sample_metadata_journal_article3_pdf)
        good.pop("pids", None)
        good.pop("id", None)

        return [bad_title, bad_pid, good]

    @property
    def title_validation_error(self):
        """Expected per-record errors for the empty-title record."""
        return [
            {
                "validation_error": [
                    {
                        "field": "metadata.title",
                        "messages": ["Missing data for required field."],
                    }
                ]
            }
        ]

    @property
    def pid_validation_error(self):
        """Expected per-record errors for the invalid external DOI record."""
        return [
            {
                "validation_error": [
                    {
                        "field": "pids.doi",
                        "messages": ["Invalid DOI"],
                    }
                ]
            }
        ]


class TestAPIMixedPublishValidationAllOrNoneTrue(MixedPublishValidationRESTTest):
    """Abort on first failure: only the title error; later records are not run."""

    @property
    def all_or_none(self):  # noqa: D102
        return True

    @property
    def expected_errors(self):  # noqa: D102
        # Only the first failure is reported when all_or_none is true.
        return [self.title_validation_error, [], []]


class TestAPIMixedPublishValidationAllOrNoneFalse(MixedPublishValidationRESTTest):
    """Continue on error: both validation shapes plus one success in one response."""

    @property
    def all_or_none(self):  # noqa: D102
        return False

    @property
    def expected_errors(self):  # noqa: D102
        return [
            self.title_validation_error,
            self.pid_validation_error,
            [],
        ]
