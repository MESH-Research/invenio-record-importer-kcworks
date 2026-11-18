# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the v1 API /import endpoint with metadata errors."""


from tests.fixtures.communities import make_community_member

from ..service_integration.test_service_metadata_error import (
    TestServiceErrorJArticleTitle,
)
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
