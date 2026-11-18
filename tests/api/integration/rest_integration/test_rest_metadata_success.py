# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# KCWorks is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Integration tests for the v1 API /import endpoint."""

import copy

from invenio_communities.members.records.api import Member

from tests.fixtures.communities import make_community_member
from tests.fixtures.records import TestRecordMetadataWithFiles
from tests.helpers.sample_records import (
    sample_metadata_journal_article2_pdf,
    sample_metadata_journal_article_pdf,
)

from ..service_integration.test_service_metadata_success import ServiceTest


class TestAPIJArticle(ServiceTest):
    """Test importing two journal articles via the API with no errors."""

    @property
    def by_api(self):  # noqa: D102
        return True

    @property
    def metadata_sources(self):  # noqa: D102
        return [
            copy.deepcopy(sample_metadata_journal_article_pdf),
            copy.deepcopy(sample_metadata_journal_article2_pdf),
        ]


class TestAPIJArticleHiddenOwner(TestAPIJArticle):
    """Import succeeds when the submitting owner's membership is hidden.

    Member Visibility (``visible=False``) must not affect import permissions;
    only the community role matters. The submitter remains a community
    ``owner`` after import.
    """

    @property
    def community_access_override(self):  # noqa: D102
        # Closed review policy requires an owner to import.
        return {"review_policy": "closed", "record_policy": "closed"}

    def make_submitter(self, user_factory, community_id):
        """Use a separate owner whose membership is not publicly listed.

        Returns:
            tuple: User id (truthy so the API path keeps this token) and
                bearer token.
        """
        new_user = user_factory(
            email="hidden-owner@example.com", token=True, oauth_id=None
        )
        make_community_member(new_user.user.id, "owner", community_id, visible=False)
        return new_user.user.id, new_user.allowed_token

    def _check_owners(
        self,
        actual_metadata: dict,
        expected: TestRecordMetadataWithFiles,
        uploader_id: str,
        community_id: str,
        community_slug: str,
        mailbox,
        mocker,
    ):
        """Assert metadata owners, then that the hidden submitter is still owner."""
        super()._check_owners(
            actual_metadata,
            expected,
            uploader_id,
            community_id,
            community_slug,
            mailbox,
            mocker,
        )
        submitter_members = [
            m for m in Member.get_members(community_id) if m.user_id == int(uploader_id)
        ]
        assert submitter_members
        assert submitter_members[0].role == "owner"
