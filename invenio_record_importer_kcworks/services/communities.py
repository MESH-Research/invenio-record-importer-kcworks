# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024, MESH Research.
#
# Invenio Record Importer is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Helper module to manage Communities."""

import time
from pprint import pformat

import arrow
from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_communities.communities.services.results import CommunityItem
from invenio_communities.errors import CommunityDeletedError
from invenio_communities.members.records.api import Member
from invenio_communities.proxies import current_communities
from invenio_drafts_resources.resources.records.errors import DraftNotCreatedError
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_group_collections_kcworks.errors import (
    CollectionNotFoundError,
    CommonsGroupNotFoundError,
)
from invenio_group_collections_kcworks.proxies import (
    current_group_collections_service as collections_service,
)
from invenio_pidstore.errors import PIDDoesNotExistError, PIDUnregistered
from invenio_rdm_records.proxies import (
    current_community_records_service,
    current_rdm_records,
)
from invenio_rdm_records.proxies import current_rdm_records_service as records_service
from invenio_rdm_records.services.errors import (
    InvalidAccessRestrictions,
    ReviewNotFoundError,
    ReviewStateError,
)
from invenio_records_resources.services.uow import (
    RecordCommitOp,
    RecordIndexOp,
    unit_of_work,
    UnitOfWork,
)
from invenio_notifications.services.uow import NotificationOp
from invenio_requests.errors import CannotExecuteActionError
from invenio_requests.proxies import current_requests_service
from invenio_search.proxies import current_search_client
from invenio_search.utils import prefix_index
from marshmallow.exceptions import ValidationError
from opensearchpy.exceptions import ConnectionError, ConnectionTimeout, NotFoundError
from opensearchpy.helpers.search import Search
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm.exc import StaleDataError
from werkzeug.exceptions import UnprocessableEntity

from invenio_record_importer_kcworks.errors import (
    CollectionDoesNotExistError,
    CommonsGroupServiceError,
    InvalidParametersError,
    MissingParentMetadataError,
    MultipleActiveCollectionsError,
    PublicationValidationError,
    RestrictedRecordPublicationError,
)


class CommunityRecordHelper:
    """A collection of methods for working with community records."""

    def ___init__(self):
        """Initialize CommunityRecordHelper."""
        pass

    @staticmethod
    def set_record_policy(community_id: str, record_policy: str):
        """Set the record policy for a community.

        If the record policy is set to 'closed', members of the community
        cannot submit records without review. If the record policy is set to
        'open', members of the community can submit records without review.

        Params:
            community_id: str: The id of the community to update
            record_policy: str: The new record policy to set. Must be one of
                                'open' or 'closed'

        Raises:
            AssertionError: If the record policy was not updated successfully

        Returns:
            bool: True if the record policy was updated successfully
        """
        record_data = current_communities.service.read(
            system_identity, community_id
        ).to_dict()
        record_data["access"]["record_policy"] = record_policy
        updated = current_communities.service.update(
            system_identity, community_id, data=record_data
        )
        assert updated["access"]["record_policy"] == record_policy
        return True

    @staticmethod
    def set_member_policy(community_id: str, member_policy: str):
        """Set the member policy for a community.

        If the member policy is set to 'closed', people cannot request
        to become members of the community. If the member policy is
        set to 'open', people can request to become members of the community.

        Params:
            community_id: str: The id of the community to update
            member_policy: str: The new member policy to set. Must be one of
                                'open' or 'closed'

        Raises:
            AssertionError: If the member policy was not updated successfully

        Returns:
            bool: True if the member policy was updated successfully
        """
        record_data = current_communities.service.read(
            system_identity, community_id
        ).to_dict()
        record_data["access"]["member_policy"] = member_policy
        updated = current_communities.service.update(
            system_identity, community_id, data=record_data
        )
        assert updated["access"]["member_policy"] == member_policy
        return True

    @staticmethod
    def set_community_visibility(community_id: str, visibility: str):
        """Set the visibility for a community.

        If the visibility is set to 'public', the community is visible to
        in searches and its landing page is visible to everyone. If the
        visibility is set to 'restricted', the community is not visible in
        searches except to logged-in members and its landing page is not
        visible to everyone.

        Params:
            community_id: str: The id of the community to update
            visibility: str: The new visibility to set. Must be one of
                            'public' or 'restricted'

        Raises:
            AssertionError: If the visibility was not updated successfully

        Returns:
            bool: True if the visibility was updated successfully
        """
        record_data = current_communities.service.read(
            system_identity, community_id
        ).to_dict()
        record_data["access"]["visibility"] = visibility
        updated = current_communities.service.update(
            system_identity, community_id, data=record_data
        )
        assert updated["access"]["visibility"] == visibility
        return True

    @staticmethod
    def set_member_visibility(community_id: str, visibility: str):
        """Set the member visibility for a community.

        Controls whether the members of the community are visible to the
        public or restricted to members of the community. I.e., it
        determines whether the "members" tab of the community landing page
        is visible to the public or restricted to members of the community.

        Params:
            community_id: str: The id of the community to update
            visibility: str: The new member visibility to set. Must be one of
                            'public' or 'restricted'

        Raises:
            AssertionError: If the member visibility was not updated
            successfully

        Returns:
            bool: True if the member visibility was updated successfully
        """
        record_data = current_communities.service.read(
            system_identity, community_id
        ).to_dict()
        record_data["access"]["member_visibility"] = visibility
        updated = current_communities.service.update(
            system_identity, community_id, data=record_data
        )
        assert updated["access"]["member_visibility"] == visibility
        return True

    @staticmethod
    def set_review_policy(community_id: str, review_policy: bool):
        """Set the review policy for a community.

        Params:
            community_id: str: The id of the community to update
            review_policy: bool: The new review policy to set

        Raises:
            AssertionError: If the review policy was not updated successfully

        Returns:
            bool: True if the review policy was updated successfully
        """
        record_data = current_communities.service.read(
            system_identity, community_id
        ).to_dict()
        record_data["access"]["review_policy"] = review_policy
        updated = current_communities.service.update(
            system_identity, community_id, data=record_data
        )
        assert updated["access"]["review_policy"] == review_policy
        return True

    @staticmethod
    def add_member(community_id: str, member_id: int, role: str) -> dict:
        """Add a member to a community.

        Params:
            community_id: str: The id of the community to update. This
                must be a UUID, not the community's slug.
            member_id: int: The id of the user to add as a member
            role: str: The role of the user to add to the community

        Raises:
            CollectionDoesNotExistError: If the community does not exist
            AssertionError: If the member was not added successfully
            ValueError: If the role is invalid

        Returns:
            dict: The member that was added
        """
        try:
            record_data = current_communities.service.read(
                system_identity, community_id
            ).to_dict()
        except Exception as e:
            raise CollectionDoesNotExistError(
                f"Community {community_id} does not exist. Cannot add "
                f"member {member_id}."
            ) from e

        role_order = ["reader", "curator", "manager", "owner"]
        if role not in role_order:
            raise ValueError(f"Invalid role: {role}. Must be one of: {role_order}")

        community_members = Member.get_members(record_data["id"])
        # existing_member = next(
        #     (m for m in community_members if m.user_id == member_id), None
        # )
        existing_members = [m for m in community_members if m.user_id == member_id]
        existing_member = existing_members[0] if existing_members else None
        if existing_member is None or (
            role_order.index(role) > role_order.index(existing_member.role)  # type: ignore
        ):
            current_communities.service.members.add(
                system_identity,
                record_data["id"],
                data={
                    "members": [{"type": "user", "id": str(member_id)}],
                    "role": role,
                },
            )

        community_members_after = Member.get_members(record_data["id"])

        matching_members = [
            {
                "user_id": m.user_id,
                "role": m.role,
                "community_id": m.community_id,
                "community_slug": record_data["slug"],
            }
            for m in community_members_after
            if m.user_id == member_id
        ]
        assert len(matching_members) == 1
        return matching_members[0]

    @staticmethod
    def add_owner(community_id: str, owner_id: int) -> dict:
        """Add an owner to a community.

        Params:
            community_id: str: The id of the community to update
            owner_id: int: The id of the user to add as an owner

        Raises:
            CollectionDoesNotExistError: If the community does not exist
            AssertionError: If the owner was not added successfully

        Returns:
            dict: The owner that was added
        """
        return CommunityRecordHelper.add_member(community_id, owner_id, "owner")


class CommunitiesHelper:
    """Helper class for working with communities during record import."""

    def __init__(self):
        """Initialize CommunitiesHelper."""
        pass

    def look_up_community(
        self, community_string: str, record_source: str = "knowledgeCommons"
    ) -> CommunityItem:
        """Look up a community by its string or UUID.

        The community string may be a slug or a label.
        """
        if not community_string:
            raise ValueError("Community string is required")
        community_check = current_communities.service.read(
            system_identity, id_=community_string
        )
        if not community_check:
            community_check = current_communities.service.search(
                system_identity, q=f"slug:{community_string}"
            )
            if community_check.total == 0:
                # FIXME: use group-collections to create the community
                # so that we import community metadata
                community_check = self.create_invenio_community(
                    record_source, community_string
                )
            else:
                community_check = community_check.to_dict()["hits"]["hits"][0]
        else:
            community_check = community_check

        if isinstance(community_check, dict):
            community_check = current_communities.service.read(
                system_identity, id_=community_check["id"]
            )

        return community_check

    def prepare_invenio_community(
        self, record_source: str = "", community_string: str = ""
    ) -> dict:
        """Ensure that the community exists in Invenio.

        If the community does not exist, it will be created.

        Parameters:
            record_source: str
                The source of the record being imported.
            community_string: str
                The label of the community to prepare. This
                string will be used as the slug for the community.

        Return the community data as a dict. (The result
        of the CommunityItem.to_dict() method.)
        """
        # FIXME: idiosyncratic implementation detail from CORE migration
        community_labels = community_string.split(".")
        if len(community_labels) > 1 and community_labels[1] == "msu":
            community_label: str = community_labels[1]  # type:ignore
        else:
            community_label: str = community_labels[0]  # type:ignore

        # FIXME: remnant of name change from CORE migration
        if community_label == "hcommons":
            community_label = "kcommons"  # type:ignore

        community_check = self.look_up_community(community_label, record_source)
        assert community_check is not None

        return community_check.to_dict()  # type:ignore

    def create_invenio_community(
        self, record_source: str, community_label: str
    ) -> dict:
        """Create a new community in Invenio.

        Parameters:
            record_source: str
                The source of the record being imported.
            community_label: str
                The label of the community to create.

        Return the community data as a dict. (The result
        of the CommunityItem.to_dict() method.)
        """
        my_community_data = app.config.get("RECORD_IMPORTER_COMMUNITIES_DATA")[
            record_source
        ][community_label]
        my_community_data["metadata"]["type"] = {"id": "commons"}
        my_community_data["access"] = {
            "visibility": "public",
            "member_policy": "closed",
            "record_policy": "closed",
            "review_policy": "closed",
            # "owned_by": [{"user": ""}]
        }
        result = current_communities.service.create(
            system_identity, data=my_community_data
        )
        if result.data.get("errors"):
            raise RuntimeError(result)
        return result.to_dict()  # type:ignore

    def _get_record_or_draft(self, draft_id: str) -> dict | None:
        """Get a record as draft or published record.

        Tries to read as draft first, falls back to published record if draft
        doesn't exist.

        Args:
            draft_id: The ID of the draft/record to retrieve

        Returns:
            dict: The record dict if found, None otherwise
        """
        try:
            existing_record = records_service.read_draft(
                system_identity, id_=draft_id
            ).to_dict()
            assert existing_record
            return existing_record  # type:ignore
        except (IndexError, NoResultFound, DraftNotCreatedError, PIDDoesNotExistError):
            try:
                existing_record = records_service.read(
                    system_identity, id_=draft_id
                ).to_dict()
                return existing_record  # type:ignore
            except (AssertionError, PIDUnregistered, PIDDoesNotExistError):
                return None

    def _is_already_published_to_community(
        self, record: dict, community_id: str
    ) -> bool:
        """Check if record is already published to the community.

        Args:
            record: The record dict to check
            community_id: The community ID to check

        Returns:
            bool: True if already published to the community, False otherwise
        """
        return bool(
            record
            and (record["status"] not in ["draft", "draft_with_review"])
            and (
                record["parent"]["communities"]
                and community_id in record["parent"]["communities"]["ids"]
            )
        )

    def _validate_restricted_record_publication(self, record: dict) -> None:
        """Validate that a restricted record can be published.

        DOIs cannot be registered at publication if the record is restricted
        (see datacite provider `validate_restriction_level` method called in
        pid component's `publish` method).

        Args:
            record: The record dict to validate

        Raises:
            RestrictedRecordPublicationError: If the record is restricted
        """
        if record and record["access"]["record"] == "restricted":
            raise RestrictedRecordPublicationError(
                "Record is restricted and cannot be published to "
                "the community because its DOI cannot be registered"
            )

    def _handle_existing_review_request(self, draft_id: str, uow) -> str | None:
        """Handle any existing review request for the record.

        Tries to cancel any existing review request for the record with another
        community, since it will conflict. If the review is closed, it's deleted.
        If it's open, it's cancelled.

        Args:
            draft_id: The ID of the draft record
            uow: The unit of work to use

        Returns:
            str | None: The request ID if an open request was found, None otherwise
        """
        try:
            existing_review = records_service.review.read(system_identity, id_=draft_id)
            app.logger.info(
                "    cancelling existing review request for the record "
                f"to the community...: {existing_review.id}"
            )
            if not existing_review.data["is_open"]:
                try:
                    records_service.review.delete(system_identity, id_=draft_id)
                except (
                    NotFoundError,
                    CannotExecuteActionError,
                ):  # already deleted
                    # Sometimes the review request is already deleted but
                    # hasn't been removed from the record's metadata
                    draft_record = records_service.read_draft(
                        system_identity, id_=draft_id
                    )._record
                    draft_record.parent.review = None
                    uow.register(ParentRecordCommitOp(draft_record.parent))
                    uow.register(RecordIndexOp(draft_record))
                return None
            else:
                request_id = existing_review.id
                current_requests_service.execute_action(
                    system_identity,
                    request_id,
                    "cancel",
                )
                return str(request_id)  # type:ignore
        except (ReviewNotFoundError, NoResultFound):
            app.logger.info(
                "    no existing review request found for the record to "
                "the community..."
            )
            return None

    def _create_and_accept_submission_request(
        self, draft_id: str, community_id: str, uow
    ) -> dict:
        """Create, submit, and accept a community-submission request.

        Creates a 'community-submission' request for an unpublished record
        (record will be published at acceptance). Handles status checks and
        retries as needed.

        Args:
            draft_id: The ID of the draft record
            community_id: The ID of the community
            uow: The unit of work to use

        Returns:
            dict: The accepted request dict

        Raises:
            MissingParentMetadataError: If there's a StaleDataError related to
                missing parent metadata
        """
        review_body = {
            "receiver": {"community": f"{community_id}"},
            "type": "community-submission",
        }
        records_service.review.update(system_identity, draft_id, review_body)

        submitted_body = {
            "payload": {
                "content": "Thank you in advance for the review.",
                "format": "html",
            }
        }
        submitted_request = records_service.review.submit(
            system_identity,
            draft_id,
            data=submitted_body,
            require_review=True,
        )
        if submitted_request.data["status"] not in [
            "submitted",
            "accepted",
        ]:
            app.logger.error(
                f"    initially failed to submit review request: "
                f"{submitted_request.to_dict()}"
            )
            submitted_request = current_requests_service.execute_action(
                system_identity,
                submitted_request.id,
                "submit",
            )

        if submitted_request.data["status"] != "accepted":
            try:
                review_accepted = current_requests_service.execute_action(
                    system_identity,
                    submitted_request.id,
                    "accept",
                )
            except StaleDataError as e:
                if "UPDATE statement on table 'rdm_parents_metadata'" in e.message:
                    raise MissingParentMetadataError(
                        "Missing parent metadata for record during "
                        "community submission acceptance. Original "
                        f"error message: {e.message}"
                    ) from None
                raise
        else:
            review_accepted = submitted_request

        assert review_accepted.data["status"] == "accepted"

        return review_accepted.to_dict()  # type:ignore

    @unit_of_work()
    def publish_record_to_community(
        self,
        draft_id: str,
        community_id: str,
        uow=None,
    ) -> dict:
        """Publish a draft record to a community.

        If the record is already published to the community, the record is
        skipped. If an existing review request for the record to the community
        is found, it is continued and accepted. Otherwise a new review request
        is created and accepted.

        Params:
            draft_id (str): the id of the draft record
            community_id (str): the id of the community to publish the record
                                to (must be a UUID, not the community's slug)

        Returns:
            dict: the result of the review acceptance action
        """
        record = self._get_record_or_draft(draft_id)

        if record and self._is_already_published_to_community(record, community_id):
            app.logger.info(
                "    skipping attaching the record to the community (already"
                " published to it)..."
            )
            return {"status": "already_published"}

        if record:
            self._validate_restricted_record_publication(record)

        self._handle_existing_review_request(draft_id, uow)

        app.logger.info("    attaching the record to the community...")
        try:
            return self._create_and_accept_submission_request(
                draft_id, community_id, uow
            )
        except ValidationError as e:
            app.logger.error(
                f"    failed to validate record for publication: {e.messages}"
            )
            raise PublicationValidationError(e.messages) from None
        except (NoResultFound, ReviewStateError) as e:
            # If the record is already published, we need to create/retrieve
            # and accept a 'community-inclusion' request instead
            app.logger.debug(f"   {pformat(e)}")
            app.logger.debug("   record is already published")
            # add_published_record_to_community always returns a tuple (result, uow)
            review_accepted, _ = self.add_published_record_to_community(
                draft_id, community_id, uow=uow
            )
            return review_accepted  # type:ignore

    @unit_of_work()
    def add_published_record_to_community(
        self,
        draft_id: str,
        community_id: str,
        suppress_notifications: bool = True,
        require_review: bool = False,
        uow: UnitOfWork | None = None,
    ) -> tuple[dict, UnitOfWork | None]:
        """Add a published record to a particular community.

        If suppress_notifications is True, email notifications connected to the request
        will be suppressed before the unit of work is committed.

        **Important:** The returned UnitOfWork's commit status depends on how it was created:
        - If you provide a UnitOfWork, it will be returned **uncommitted** for you to commit.
        - If no UnitOfWork is provided, the decorator creates one, commits it automatically,
          and returns it **already committed**. Attempting to commit it again will raise
          a RuntimeError.

        Parameters:
            draft_id (str): the id of the draft record
            community_id (str): the id of the community to add the record to
            suppress_notifications (bool): if True, suppress email notifications
            require_review (bool): if True, create a request that requires review (stays open).
                If False (default), the request is auto-accepted if permissions allow.
            uow (UnitOfWork, optional): the unit of work to use. If not provided, one will be created.

        Returns:
            tuple[dict, UnitOfWork | None]: the result and the unit of work.
                The UOW will be None if no UOW exists, otherwise it will be the UOW
                (either provided uncommitted, or auto-created and already committed if
                one wasn't provided).

        Raises:
            InvalidParametersError: If the record is not published.
        """
        # Verify that the record is actually published
        record = self._get_record_or_draft(draft_id)
        if not record:
            raise InvalidParametersError(
                f"Record with id {draft_id} not found"
            )
        if record.get("status") in ["draft", "draft_with_review"]:
            raise InvalidParametersError(
                f"Record with id {draft_id} is not published (status: {record.get('status')}). "
                "Use publish_record_to_community() for unpublished records."
            )

        record_communities = current_rdm_records.record_communities_service

        # Try to create and submit a 'community-inclusion' request
        # Pass uow to ensure we use the same unit of work instance so we can
        # filter out notifications before commit
        requests, errors = record_communities.add(
            system_identity,
            draft_id,
            {"communities": [{"id": community_id, "require_review": require_review}]},
            uow=uow,
        )
        submitted_request = requests[0] if requests else None

        app.logger.info(f"errors: {pformat(errors)}")

        # If that failed because the record is already included in the
        # community, skip accepting the request (unnecessary)
        # FIXME: How can we tell if an inclusion request is already
        # open and/or accepted? Without relying on this error message?
        if errors and "already included" in errors[0]["message"]:
            # Suppress email notifications even in early return case
            if uow and hasattr(uow, "_operations") and suppress_notifications:
                uow._operations = [
                    op for op in uow._operations if not isinstance(op, NotificationOp)
                ]
            result = {
                "status": "already_included",
                "submitted_request": (
                    submitted_request.to_dict() if submitted_request else None
                ),
            }
            # Always return the UOW if it exists (may be already committed if decorator created it)
            return result, uow
        # If that failed look for any existing open
        # 'community-inclusion' request and continue with it
        if errors:
            app.logger.debug(f"    inclusion request already open for {draft_id}")
            app.logger.debug(pformat(errors))
            record = record_communities.record_cls.pid.resolve(draft_id)
            request_id = record_communities._exists(community_id, record)
            # Read existing request from database (already committed)
            request_obj = current_requests_service.read(
                system_identity, request_id
            )._record
        # If it succeeded, continue with the new request
        else:
            request_id = (
                submitted_request["id"]  # type:ignore
                if submitted_request.get("id")  # type:ignore
                else submitted_request["request"]["id"]  # type:ignore
            )
            # Note: Even though the UnitOfWork hasn't yet been committed,
            # SQLAlchemy's session identity map will return the uncommitted request
            # object from the same session.
            request_obj = current_requests_service.read(
                system_identity, request_id
            )._record
        community = current_communities.service.record_cls.pid.resolve(community_id)
        # app.logger.debug(f"request_obj: {pformat(request_obj)}  ")

        # Accept the 'community-inclusion' request if it's not already
        # accepted
        if request_obj["status"] != "accepted":
            community_inclusion = current_rdm_records.community_inclusion_service
            try:
                review_accepted = community_inclusion.include(
                    system_identity, community, request_obj, uow
                )
            except InvalidAccessRestrictions:
                # can't add public record to restricted community
                # so set community to public before acceptance
                # TODO: can we change this policy?
                app.logger.warning(f"    setting community {community_id} to public")
                CommunityRecordHelper.set_community_visibility(community_id, "public")
                review_accepted = community_inclusion.include(
                    system_identity, community, request_obj, uow
                )
        else:
            # Request is already accepted, get service result to align
            # type with the return value of the include method
            review_accepted = current_requests_service.read(
                system_identity, request_id
            )

        # Suppress email notifications by stripping NotificationOp instances from uow
        if uow and hasattr(uow, "_operations") and suppress_notifications:
            uow._operations = [
                op for op in uow._operations if not isinstance(op, NotificationOp)
            ]

        # Always return the UOW if it exists (may be already committed if decorator added)
        return review_accepted.to_dict(), uow  # type:ignore

    def add_record_to_group_collections(
        self, metadata_record: dict, record_source: str
    ) -> list:
        """Add a published record to the appropriate group collections.

        These communities/collections are controlled by groups on a
        remote service. The record's metadata includes the group
        identifiers and names for the collections to which it should
        be added.

        If a group collection does not exist, it is created and linked
        to the group on the remote service. Members of the remote group will
        receive role-based membership in the group collection.

        Params:
            metadata_record (dict): the metadata record to add to group
                collections (this is assumed to be a published record)
            record_source (str): the string representation of the record's
                source service, for use by invenio-group-collections-kcworks
                in linking the record to the appropriate group collections

        Returns:
            list: the list of group collections the record was added to
        """
        bad_groups = [
            "1003749",
            "1000743",
            "1004285",
            "1000737",
            "1000754",
            "1003111",
            "1001232",
            "1004181",
            "344",
            "1002956",
            "1002947",
            "1003017",
            "1003436",
            "1003608",
            "1003410",
            "1004047",
        ]
        # FIXME: See whether this can be generalized
        if record_source == "knowledgeCommons":
            added_to_collections = []
            group_list = [
                g
                for g in metadata_record["custom_fields"].get(
                    "hclegacy:groups_for_deposit", []
                )
                if g.get("group_identifier")
                and g.get("group_name")
                and g.get("group_identifier") not in bad_groups
            ]
            for group in group_list:
                group_id = group["group_identifier"]
                group_name = group["group_name"]
                coll_record = None
                index = prefix_index("communities")
                try:
                    coll_search = current_search_client.search(
                        index=index,
                        q=f'custom_fields.kcr\:commons_group_id:"{group_id}"',
                    )
                    coll_record_hits = coll_search["hits"]["hits"]
                    coll_records = []
                    for h in coll_record_hits:
                        try:
                            coll_records.append(
                                current_communities.service.read(
                                    system_identity, id_=h["_source"]["id"]
                                ).to_dict()
                            )
                        except NotFoundError:
                            app.logger.warning(
                                f"    collection {h['_source']['id']} not found"
                            )
                        except CommunityDeletedError:
                            app.logger.warning(
                                f"    collection {h['_source']['id']} deleted"
                            )
                    # NOTE: Don't check for identical group name because
                    # sometimes the group name has changed since the record
                    # was created
                    try:
                        assert len(coll_records) == 1
                    except AssertionError:
                        if len(coll_records) > 1:
                            raise MultipleActiveCollectionsError(
                                f"    multiple active collections found for {group_id}"
                            ) from None
                        else:
                            raise CollectionNotFoundError(
                                f'    no active collections found for "{group_id}"'
                            ) from None
                    coll_record = coll_records[0]
                except CollectionNotFoundError:
                    try:
                        coll_record = collections_service.create(
                            system_identity,
                            group_id,
                            record_source,
                        )
                    except UnprocessableEntity as e:
                        if e.description and (
                            "Something went wrong requesting group" in e.description
                        ):
                            raise CommonsGroupServiceError(
                                f"Failed requesting group collection from API "
                                f"{e.description}"
                            ) from None
                    except CommonsGroupNotFoundError:
                        message = (
                            f"Group {group_id} ({group_name})"
                            f"not found on Knowledge Commons. Could not "
                            f"create a group collection..."
                        )
                        app.logger.warning(message)
                        raise CommonsGroupNotFoundError(message) from None
                    except TimeoutError:
                        raise CommonsGroupServiceError(
                            f"Timed out while creating group collection "
                            f"for group {group_id} ({group_name})"
                        ) from None
                if coll_record:
                    add_result = self.publish_record_to_community(
                        metadata_record["id"],
                        coll_record["id"],
                    )
                    added_to_collections.append(add_result)
            if added_to_collections:
                self._remove_from_extraneous_collections(metadata_record, group_list)
                app.logger.info(
                    f"Record {metadata_record['id']} successfully added "
                    f"to group collections {added_to_collections}"
                )
                return added_to_collections
            else:
                app.logger.info(
                    f"Record {metadata_record['id']} not added to any group collections"
                )
                return []
        else:
            app.logger.info("No group collections to add to")
            return []

    def _remove_from_extraneous_collections(
        self, metadata_record: dict, group_list: list
    ) -> None:
        """Remove a record from any group collections not in the group list."""
        group_ids = [g.get("group_identifier") for g in group_list]
        extraneous_collections = [
            c
            for c in metadata_record["parent"]["communities"].get("entries", [])
            if c.get("custom_fields", {}).get("kcr:commons_group_id")
            and c.get("custom_fields", {}).get("kcr:commons_group_id") not in group_ids
        ]
        if extraneous_collections:
            for c in extraneous_collections:
                removed = current_community_records_service.delete(
                    system_identity,
                    c["id"],
                    {"records": [{"id": metadata_record["id"]}]},
                )

    def check_opensearch_health(self) -> dict:
        """Check OpenSearch cluster health.

        Returns:
            dict: Health check result with keys:
                - is_healthy (bool): Whether the cluster is healthy
                - reason (str): Explanation of health status
                - status (str): Cluster status (green/yellow/red) if available
        """
        try:
            health = current_search_client.cluster.health(timeout=5)
            status = health.get("status", "unknown")

            if status == "red":
                return {
                    "is_healthy": False,
                    "reason": "Cluster status is RED",
                    "status": status,
                }
            elif status == "yellow":
                # Yellow is acceptable but log a warning
                app.logger.warning("OpenSearch cluster status is YELLOW")
                return {
                    "is_healthy": True,
                    "reason": "Cluster status is YELLOW (acceptable)",
                    "status": status,
                }
            else:  # green
                return {
                    "is_healthy": True,
                    "reason": "Cluster status is GREEN",
                    "status": status,
                }

        except (ConnectionTimeout, ConnectionError) as e:
            return {
                "is_healthy": False,
                "reason": f"OpenSearch not responsive: {str(e)}",
                "status": "unreachable",
            }
        except Exception as e:
            app.logger.error(f"Error checking OpenSearch health: {str(e)}")
            return {
                "is_healthy": False,
                "reason": f"Health check error: {str(e)}",
                "status": "error",
            }

    def find_communities_needing_created_date_update(self) -> list[dict]:
        """Find all communities with kcr:commons_group_id that may need updating.

        Queries the search index for all communities that represent migrated
        group collections (identified by having a commons_group_id).

        Returns:
            list[dict]: List of communities to check, each dict contains:
                {
                    'id': str,  # community UUID
                    'slug': str,  # community slug for logging
                    'group_id': str,  # kcr:commons_group_id value
                    'current_created': str  # current created timestamp
                }
        """
        index = prefix_index("communities")

        search = Search(using=current_search_client, index=index)
        search = search.filter("exists", field="custom_fields.kcr:commons_group_id")

        communities = []

        app.logger.info("Scanning communities for created date updates...")
        for hit in search.scan():
            try:
                community_id = hit.meta.id
                slug = hit.slug if hasattr(hit, "slug") else "unknown"

                # Use bracket access for fields with colons
                try:
                    group_id = hit.custom_fields["kcr:commons_group_id"]
                except KeyError:
                    group_id = None

                current_created = hit.created if hasattr(hit, "created") else None

                if group_id and current_created:
                    communities.append({
                        "id": community_id,
                        "slug": slug,
                        "group_id": group_id,
                        "current_created": current_created,
                    })
            except Exception as e:
                app.logger.warning(
                    f"Error processing community {hit.meta.id}: {str(e)}"
                )
                continue

        app.logger.info(f"Found {len(communities)} communities to check")
        return communities

    def find_oldest_record_for_community(self, group_id: str) -> arrow.Arrow | None:
        """Find the oldest record creation date for records in a community.

        Searches for all records that have this group_id in their
        hclegacy:groups_for_deposit array, filters for those with
        hclegacy:record_creation_date, and returns the oldest date.

        Since hclegacy:record_creation_date is mapped as a text field and cannot
        be sorted in OpenSearch, this method fetches all matching records and
        sorts them in application code.

        Args:
            group_id: The kcr:commons_group_id to search for

        Returns:
            arrow.Arrow: The oldest record creation date (floored to start of day),
                        or None if no records found
        """
        if not group_id:
            app.logger.warning(
                "find_oldest_record_for_community called with empty group_id"
            )
            return None

        index = prefix_index("rdmrecords")

        search = Search(using=current_search_client, index=index)

        # Use match filter (not term) for the group_identifier field
        search = search.filter(
            "match",
            **{"custom_fields.hclegacy:groups_for_deposit.group_identifier": group_id},
        )
        search = search.filter(
            "exists", field="custom_fields.hclegacy:record_creation_date"
        )

        # Note: We cannot sort on hclegacy:record_creation_date in OpenSearch
        # because it's a text field. Instead, we fetch all records and sort
        # in Python. Using scan() to handle communities with any number of records.

        try:
            records = []
            for hit in search.scan():
                # Use bracket access for fields with colons
                try:
                    creation_date = hit.custom_fields["hclegacy:record_creation_date"]
                except KeyError:
                    creation_date = None

                if creation_date:
                    records.append({"date": creation_date, "id": hit.meta.id})

            if not records:
                app.logger.info(
                    f"No records with hclegacy:record_creation_date found for group {group_id}"
                )
                return None

            # Sort in Python by date string (ISO format sorts correctly)
            records.sort(key=lambda x: x["date"])

            oldest_date = records[0]["date"]
            # Return start of day
            return arrow.get(oldest_date).floor("day")

        except Exception as e:
            app.logger.error(
                f"Error finding oldest record for group {group_id}: {str(e)}"
            )

        return None

    @unit_of_work()
    def update_single_community_created_date(
        self,
        community_id: str,
        new_created_date: arrow.Arrow,
        uow=None,
    ) -> bool:
        """Update the created date for a single community.

        Uses the Invenio unit of work pattern to ensure the database change
        is committed and the search index is automatically updated.

        Args:
            community_id: UUID of the community to update
            new_created_date: New created date as arrow.Arrow object
            uow: Unit of work instance (injected by decorator)

        Returns:
            bool: True if updated, False if skipped (date already earlier)
        """
        # Read community
        community = current_communities.service.read(
            system_identity, id_=community_id
        )._record

        # Convert arrow date to datetime
        new_dt = new_created_date.datetime

        # Check if update is needed - only update if new date is earlier
        current_created = arrow.get(community.model.created)
        if new_created_date < current_created:
            community.model.created = new_dt
            uow.register(RecordCommitOp(community))
            return True

        return False

    def update_community_created_dates(
        self,
        batch_size: int = 100,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> dict:
        """Update created dates for communities based on their oldest record.

        This is the main orchestration method that:
        1. Finds all communities with kcr:commons_group_id
        2. For each community, finds the oldest record
        3. Updates the community's created date to start of that day
        4. Performs health checks between batches
        5. Tracks statistics and errors

        Args:
            batch_size: Number of communities to process before health check
            dry_run: If True, log what would be done without making changes
            verbose: If True, log detailed progress

        Returns:
            dict: Statistics about the operation
                {
                    'total_found': int,
                    'updated': int,
                    'skipped': int,
                    'no_records': int,
                    'errors': list[dict],
                    'stopped_early': bool (optional),
                    'stopped_at_community': int (optional)
                }
        """
        communities = self.find_communities_needing_created_date_update()
        stats = {
            "total_found": len(communities),
            "updated": 0,
            "skipped": 0,
            "no_records": 0,
            "errors": [],
        }

        if stats["total_found"] == 0:
            app.logger.info("No communities found needing created date updates")
            return stats

        app.logger.info(
            f"Processing {stats['total_found']} communities in batches of {batch_size}"
        )

        # Process in batches
        for i in range(0, len(communities), batch_size):
            batch = communities[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(communities) + batch_size - 1) // batch_size

            app.logger.info(f"Processing batch {batch_num} of {total_batches}")

            # Process each community in the batch
            for community in batch:
                try:
                    # Find oldest record for this community
                    oldest_date = self.find_oldest_record_for_community(
                        community["group_id"]
                    )

                    if oldest_date is None:
                        stats["no_records"] += 1  # type:ignore
                        if verbose:
                            app.logger.info(
                                f"No records found for community {community['slug']}"
                            )
                        continue

                    if dry_run:
                        current = arrow.get(community["current_created"])
                        if oldest_date < current:
                            stats["updated"] += 1  # type:ignore
                            app.logger.info(
                                f"[DRY RUN] Would update {community['slug']} "
                                f"from {current.format('YYYY-MM-DD')} to "
                                f"{oldest_date.format('YYYY-MM-DD')}"
                            )
                        else:
                            stats["skipped"] += 1  # type:ignore
                            if verbose:
                                app.logger.info(
                                    f"[DRY RUN] Would skip {community['slug']} "
                                    f"(current date {current.format('YYYY-MM-DD')} "
                                    "is already earlier)"
                                )
                    else:
                        updated = self.update_single_community_created_date(
                            community["id"], oldest_date
                        )
                        if updated:
                            stats["updated"] += 1  # type:ignore
                            if verbose:
                                current = arrow.get(community["current_created"])
                                app.logger.info(
                                    f"Updated community {community['slug']} "
                                    f"from {current.format('YYYY-MM-DD')} to "
                                    f"{oldest_date.format('YYYY-MM-DD')}"
                                )
                        else:
                            stats["skipped"] += 1  # type:ignore
                            if verbose:
                                app.logger.info(
                                    f"Skipped community {community['slug']} "
                                    f"(current date is already earlier)"
                                )

                except Exception as e:
                    app.logger.error(
                        f"Error updating community {community['slug']}: {str(e)}"
                    )
                    stats["errors"].append({  # type:ignore
                        "community_id": community["id"],
                        "slug": community["slug"],
                        "group_id": community.get("group_id", "unknown"),
                        "error": str(e),
                    })

            # Health check after each batch (except the last one)
            if i + batch_size < len(communities):
                health = self.check_opensearch_health()

                if not health["is_healthy"]:
                    app.logger.warning(
                        f"OpenSearch health check failed: {health['reason']}. "
                        f"Pausing for 30 seconds..."
                    )
                    time.sleep(30)

                    # Check again after pause
                    health = self.check_opensearch_health()
                    if not health["is_healthy"]:
                        app.logger.error(
                            f"OpenSearch still unhealthy after pause: "
                            f"{health['reason']}. Stopping updates. Processed "
                            f"{i + len(batch)} of {len(communities)} communities."
                        )
                        stats["stopped_early"] = True
                        stats["stopped_at_community"] = i + len(batch)
                        break
                elif verbose:
                    app.logger.info(f"OpenSearch health: {health['status']}")

                # Small delay between batches to avoid overwhelming the cluster
                time.sleep(1)

        app.logger.info(
            f"Community created date update complete: "
            f"{stats['updated']} updated, {stats['skipped']} skipped, "
            f"{stats['no_records']} no records, {len(stats['errors'])} errors"  # type:ignore
        )

        return stats
