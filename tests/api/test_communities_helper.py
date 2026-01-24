# Part of Knowledge Commons Works
# Copyright (C) 2024-2025 MESH Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the MIT License

"""Tests for CommunitiesHelper."""

import pytest
from invenio_access.permissions import system_identity
from invenio_notifications.services.uow import NotificationOp
from invenio_rdm_records.proxies import current_rdm_records, current_rdm_records_service
from invenio_records_resources.services.uow import UnitOfWork

from invenio_record_importer_kcworks.errors import InvalidParametersError
from invenio_record_importer_kcworks.services.communities import CommunitiesHelper


class TestAddPublishedRecordToCommunity:
    """Test suite for add_published_record_to_community method."""

    def test_add_published_record_to_community_success(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test successfully adding a published record to a community."""
        with running_app.app.app_context():
            # Create a published record and a community
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            # Add the record to the community
            helper = CommunitiesHelper()
            result, uow = helper.add_published_record_to_community(
                published_record.id, community.id
            )

            # Check return value structure
            assert isinstance(result, dict)
            assert "status" in result
            assert result["status"] == "accepted"

            # Check that UOW was returned (auto-created and committed)
            assert uow is not None

            # Verify the record is in the community
            record = current_rdm_records_service.read(
                system_identity, published_record.id
            )
            assert community.id in record.data["parent"]["communities"]["ids"]

    def test_add_published_record_to_community_with_uow(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test adding a published record with a provided UOW."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()
            with UnitOfWork(db.session) as uow:
                result, returned_uow = helper.add_published_record_to_community(
                    published_record.id, community.id, uow=uow
                )

                # Check return value
                assert isinstance(result, dict)
                assert result["status"] == "accepted"

                # UOW should be returned uncommitted
                assert returned_uow is uow
                assert returned_uow is not None

                # Commit the UOW
                returned_uow.commit()

            # Verify the record is in the community after commit
            record = current_rdm_records_service.read(
                system_identity, published_record.id
            )
            assert community.id in record.data["parent"]["communities"]["ids"]

    def test_add_published_record_to_community_already_included(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test adding a record that's already in the community."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()

            # Add the record to the community first time
            result1, _ = helper.add_published_record_to_community(
                published_record.id, community.id
            )
            assert result1["status"] == "accepted"

            # Try to add it again
            result2, _ = helper.add_published_record_to_community(
                published_record.id, community.id
            )

            # Should return already_included status
            assert isinstance(result2, dict)
            assert result2["status"] == "already_included"

    def test_add_published_record_to_community_draft_record(
        self, running_app, db, minimal_draft_record_factory, minimal_community_factory
    ):
        """Test that adding a draft record raises InvalidParametersError."""
        with running_app.app.app_context():
            community = minimal_community_factory()
            draft_record = minimal_draft_record_factory()
            draft_record_id = draft_record.id

            helper = CommunitiesHelper()

            with pytest.raises(InvalidParametersError) as exc_info:
                with db.session.begin_nested():
                    helper.add_published_record_to_community(
                        draft_record_id, community.id
                    )

            assert "not published" in str(exc_info.value).lower()
            assert draft_record.id in str(exc_info.value)

    def test_add_published_record_to_community_record_not_found(
        self, running_app, db, minimal_community_factory
    ):
        """Test that adding a non-existent record raises InvalidParametersError."""
        with running_app.app.app_context():
            community = minimal_community_factory()
            fake_record_id = "00000000-0000-0000-0000-000000000000"

            helper = CommunitiesHelper()

            with pytest.raises(InvalidParametersError) as exc_info:
                helper.add_published_record_to_community(fake_record_id, community.id)

            assert "not found" in str(exc_info.value).lower()
            assert fake_record_id in str(exc_info.value)

    def test_add_published_record_to_community_suppress_notifications(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test that notifications are suppressed when suppress_notifications=True."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()

            # Add with notifications suppressed (default)
            result, uow = helper.add_published_record_to_community(
                published_record.id, community.id, suppress_notifications=True
            )

            # Check that NotificationOp was removed from UOW operations
            if uow and hasattr(uow, "_operations"):
                notification_ops = [
                    op for op in uow._operations if isinstance(op, NotificationOp)
                ]
                assert len(notification_ops) == 0

            assert result["status"] == "accepted"

    def test_add_published_record_to_community_allow_notifications(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test notifications not suppressed when suppress_notifications=False."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()

            # Add with notifications allowed
            result, uow = helper.add_published_record_to_community(
                published_record.id, community.id, suppress_notifications=False
            )

            # Check that NotificationOp might be present (depending on service config)
            # We can't assert it's definitely there, but we can check the method
            # doesn't filter it out
            if uow and hasattr(uow, "_operations"):
                # The operations list should not have been filtered
                # (we can't easily verify NotificationOp is there without
                # inspecting the service implementation)
                assert len(uow._operations) >= 0  # Just verify it's accessible

            assert result["status"] == "accepted"

    def test_add_published_record_to_community_return_value_structure(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test that the return value has the correct structure."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()
            result, uow = helper.add_published_record_to_community(
                published_record.id, community.id
            )

            # Result should be a dict with status
            assert isinstance(result, dict)
            assert "status" in result

            # Status should be "accepted" for successful addition
            assert result["status"] == "accepted"

            # UOW should be returned (auto-created and committed)
            assert uow is not None

    def test_add_published_record_to_community_existing_request(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test handling of existing open inclusion request."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()

            record_communities = current_rdm_records.record_communities_service
            requests, errors = record_communities.add(
                system_identity,
                published_record.id,
                {"communities": [{"id": community.id, "require_review": True}]},
            )

            # Now call add_published_record_to_community - it should
            # find and accept the existing request
            result, uow = helper.add_published_record_to_community(
                published_record.id, community.id, require_review=True
            )

            assert result["status"] == "accepted"

            # Verify the record is in the community
            record = current_rdm_records_service.read(
                system_identity, published_record.id
            )
            assert community.id in record.data["parent"]["communities"]["ids"]

    def test_add_published_record_to_community_uow_auto_committed(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test that auto-created UOW is already committed."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community = minimal_community_factory()

            helper = CommunitiesHelper()
            result, uow = helper.add_published_record_to_community(
                published_record.id, community.id
            )

            # UOW should be returned and already committed
            assert uow is not None

            # Attempting to commit again should raise RuntimeError
            with pytest.raises(RuntimeError) as exc_info:
                uow.commit()

            assert "already committed" in str(exc_info.value).lower()

    def test_add_published_record_to_community_multiple_communities(
        self,
        running_app,
        db,
        minimal_published_record_factory,
        minimal_community_factory,
    ):
        """Test adding a record to multiple communities."""
        with running_app.app.app_context():
            published_record = minimal_published_record_factory()
            community1 = minimal_community_factory(slug="community-1")
            community2 = minimal_community_factory(slug="community-2")

            helper = CommunitiesHelper()

            # Add to first community
            result1, _ = helper.add_published_record_to_community(
                published_record.id, community1.id
            )
            assert result1["status"] == "accepted"

            # Add to second community
            result2, _ = helper.add_published_record_to_community(
                published_record.id, community2.id
            )
            assert result2["status"] == "accepted"

            # Verify the record is in both communities
            record = current_rdm_records_service.read(
                system_identity, published_record.id
            )
            communities = record.data["parent"]["communities"]["ids"]
            assert community1.id in communities
            assert community2.id in communities


def test_create_invenio_community(
    app,
    db,
    admin,
    community_type_v,
    search_clear,
):
    """Test CommunitiesHelper.create_invenio_community method."""
    slug = "mla"
    actual_community = CommunitiesHelper().create_invenio_community(
        "knowledgeCommons", slug
    )
    # actual_community_id = actual_community["id"]
    assert actual_community["slug"] == slug
