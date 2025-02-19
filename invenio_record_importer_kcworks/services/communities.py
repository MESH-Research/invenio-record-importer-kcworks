# -*- coding: utf-8 -*-
#
# This file is part of the Invenio Record Importer package.
# Copyright (C) 2024, MESH Research.
#
# Invenio Record Importer is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

from typing import Optional
from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_communities.communities.services.results import CommunityItem
from invenio_communities.errors import CommunityDeletedError
from invenio_communities.members.records.api import Member
from invenio_communities.proxies import current_communities
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_drafts_resources.resources.records.errors import DraftNotCreatedError
from invenio_group_collections_kcworks.errors import (
    CollectionNotFoundError,
    CommonsGroupNotFoundError,
)
from invenio_group_collections_kcworks.proxies import (
    current_group_collections_service as collections_service,
)
from invenio_pidstore.errors import PIDUnregistered
from invenio_rdm_records.proxies import (
    current_rdm_records,
    current_rdm_records_service as records_service,
    current_community_records_service,
)
from invenio_rdm_records.services.errors import (
    ReviewNotFoundError,
    ReviewStateError,
    InvalidAccessRestrictions,
)
from invenio_record_importer_kcworks.errors import (
    CommonsGroupServiceError,
    MissingParentMetadataError,
    MultipleActiveCollectionsError,
    PublicationValidationError,
    RestrictedRecordPublicationError,
)
from invenio_records_resources.services.uow import (
    unit_of_work,
    RecordIndexOp,
)
from invenio_requests.proxies import current_requests_service
from invenio_requests.errors import CannotExecuteActionError
from invenio_search.proxies import current_search_client
from marshmallow.exceptions import ValidationError
from opensearchpy.exceptions import NotFoundError
from pprint import pformat
from sqlalchemy.orm.exc import NoResultFound, StaleDataError
from werkzeug.exceptions import UnprocessableEntity


class CommunityRecordHelper:
    """A collection of methods for working with community records."""

    def ___init__(self):
        pass

    @staticmethod
    def set_record_policy(community_id: str, record_policy: str):
        """Set the record policy for a community.

        If the record policy is set to 'closed', members of the community
        cannot submit records without review. If the record policy is set to
        'open', members of the community can submit records without review.

        params:
            community_id: str: The id of the community to update
            record_policy: str: The new record policy to set. Must be one of
                                'open' or 'closed'

        raises:
            AssertionError: If the record policy was not updated successfully

        returns:
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

        params:
            community_id: str: The id of the community to update
            member_policy: str: The new member policy to set. Must be one of
                                'open' or 'closed'

        raises:
            AssertionError: If the member policy was not updated successfully

        returns:
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

        params:
            community_id: str: The id of the community to update
            visibility: str: The new visibility to set. Must be one of
                            'public' or 'restricted'

        raises:
            AssertionError: If the visibility was not updated successfully

        returns:
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

        params:
            community_id: str: The id of the community to update
            visibility: str: The new member visibility to set. Must be one of
                            'public' or 'restricted'

        raises:
            AssertionError: If the member visibility was not updated
            successfully

        returns:
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

        params:
            community_id: str: The id of the community to update
            review_policy: bool: The new review policy to set

        raises:
            AssertionError: If the review policy was not updated successfully

        returns:
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
    def add_owner(community_id: str, owner_id: int):
        """Add an owner to a community.

        params:
            community_id: str: The id of the community to update
            owner_id: int: The id of the user to add as an owner

        raises:
            AssertionError: If the owner was not added successfully

        returns:
            bool: True if the owner was added successfully
        """
        try:
            record_data = current_communities.service.read(
                system_identity, community_id
            ).to_dict()
        except Exception as e:
            raise e
            community_list = current_communities.service.search(
                identity=system_identity, q=f"slug:{community_id}"
            )
            assert community_list.total == 1
            record_data = next(community_list.hits).to_dict()

        community_members = Member.get_members(record_data["id"])
        owners = [o.user_id for o in community_members if o.role == "owner"]
        if owner_id not in owners:
            owner = current_communities.service.members.add(
                system_identity,
                record_data["id"],
                data={
                    "members": [{"type": "user", "id": str(owner_id)}],
                    "role": "owner",
                },
            )

        community_members_b = Member.get_members(record_data["id"])

        owner = [
            {
                "user_id": o.user_id,
                "role": o.role,
                "community_id": o.community_id,
                "community_slug": record_data["slug"],
            }
            for o in community_members_b
            if o.role == "owner" and o.user_id == owner_id
        ][0]

        return owner


class CommunitiesHelper:
    """Helper class for working with communities during record import."""

    def __init__(self):
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
        app.logger.debug(f"community_check: {pformat(community_check)}")
        if not community_check:
            community_check = current_communities.service.search(
                system_identity, q=f"slug:{community_string}"
            )
            if community_check.total == 0:
                app.logger.debug(
                    "Community", community_string, "does not exist. Creating..."
                )
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
        community_label = community_string.split(".")
        if len(community_label) > 1 and community_label[1] == "msu":
            community_label = community_label[1]
        else:
            community_label = community_label[0]

        # FIXME: remnant of name change from CORE migration
        if community_label == "hcommons":
            community_label = "kcommons"

        community_check = self.look_up_community(community_label, record_source)
        assert community_check is not None

        return community_check.to_dict()

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
        return result.to_dict()

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

        params:
            draft_id (str): the id of the draft record
            community_id (str): the id of the community to publish the record
                                to (must be a UUID, not the community's slug)

        returns:
            dict: the result of the review acceptance action
        """
        # Attachment to community unnecessary if the record is already
        # published or included in it, even if a new draft version
        try:
            # existing_record = (
            #     records_service.search_drafts(system_identity, q=f"id:{draft_id}")
            #     .to_dict()
            #     .get("hits", {})
            #     .get("hits", [])[0]
            # )
            existing_record = records_service.read_draft(
                system_identity, id_=draft_id
            ).to_dict()
            assert existing_record
        except (IndexError, NoResultFound, DraftNotCreatedError):
            try:
                existing_record = records_service.read(
                    system_identity, id_=draft_id
                ).to_dict()
            except (AssertionError, PIDUnregistered):
                existing_record = None
        app.logger.debug(f"existing_record: {pformat(existing_record)}")

        if (
            existing_record
            and (existing_record["status"] not in ["draft", "draft_with_review"])
            and (
                existing_record["parent"]["communities"]
                and community_id in existing_record["parent"]["communities"]["ids"]
            )
        ):
            app.logger.info(
                "    skipping attaching the record to the community (already"
                " published to it)..."
            )
            return {"status": "already_published"}
        # for records that haven't been attached to the community yet
        # submit and accept a review request
        else:
            # DOIs cannot be registered at publication if the record
            # is restricted (see datacite provider `validate_restriction_level`
            # method called in pid component's `publish` method)
            if existing_record and existing_record["access"]["record"] == "restricted":
                app.logger.error(pformat(existing_record))
                raise RestrictedRecordPublicationError(
                    "Record is restricted and cannot be published to "
                    "the community because its DOI cannot be registered"
                )

            request_id = None
            # Try to cancel any existing review request for the record
            # with another community, since it will conflict
            try:
                existing_review = records_service.review.read(
                    system_identity, id_=draft_id
                )
                app.logger.info(
                    "    cancelling existing review request for the record "
                    f"to the community...: {existing_review.id}"
                )
                app.logger.debug(
                    f"existing_review: {pformat(existing_review.to_dict())}"
                )
                # if (
                #     existing_review.to_dict()["receiver"].get("community")
                #     == community_id
                # ):
                #     app.logger.info(
                #         "    skipping cancelling the existing review request"
                #         " (already for the community)..."
                #     )
                if not existing_review.data["is_open"]:
                    app.logger.debug("   existing review request is not open, deleting")
                    try:
                        records_service.review.delete(system_identity, id_=draft_id)
                        app.logger.debug("   existing review request deleted")
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
                        app.logger.debug(
                            "   existing review request was already deleted, "
                            "manually removed from record metadata"
                        )
                else:
                    request_id = existing_review.id
                    cancel_existing_request = current_requests_service.execute_action(
                        system_identity,
                        request_id,
                        "cancel",
                    )
                    app.logger.debug(
                        f"cancel_existing_request: "
                        f"{pformat(cancel_existing_request)}"
                    )
            # If no existing review request, just continue
            except (ReviewNotFoundError, NoResultFound):
                app.logger.info(
                    "    no existing review request found for the record to "
                    "the community..."
                )

            # Create/retrieve and accept a review request
            app.logger.info("    attaching the record to the community...")

            # Try creating/retrieving and accepting a 'community-submission'
            # request for an unpublished record (record will be published at
            # acceptance).
            try:
                review_body = {
                    "receiver": {"community": f"{community_id}"},
                    "type": "community-submission",
                }
                new_request = records_service.review.update(  # noqa: F841
                    system_identity, draft_id, review_body
                )

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
                    app.logger.debug(
                        f"submitted_request: {pformat(submitted_request.to_dict())}"
                    )
                app.logger.debug("submitted to community")

                if submitted_request.data["status"] != "accepted":
                    try:
                        review_accepted = current_requests_service.execute_action(
                            system_identity,
                            submitted_request.id,
                            "accept",
                        )
                    except StaleDataError as e:
                        if (
                            "UPDATE statement on table 'rdm_parents_metadata'"
                            in e.message
                        ):
                            raise MissingParentMetadataError(
                                "Missing parent metadata for record during "
                                "community submission acceptance. Original "
                                f"error message: {e.message}"
                            )
                else:
                    review_accepted = submitted_request

                app.logger.debug("review_accepted")
                assert review_accepted.data["status"] == "accepted"

                return review_accepted.to_dict()

            # Catch validation errors when publishing the record
            except ValidationError as e:
                app.logger.error(
                    f"    failed to validate record for publication: " f"{e.messages}"
                )
                raise PublicationValidationError(e.messages)

            # If the record is already published, we need to create/retrieve
            # and accept a 'community-inclusion' request instead
            except (NoResultFound, ReviewStateError) as e:
                app.logger.debug(f"   {pformat(e)}")
                app.logger.debug("   record is already published")
                record_communities = current_rdm_records.record_communities_service

                app.logger.debug(
                    f"record before inclusion: {pformat(records_service.read(system_identity, id_=draft_id).id)}"
                )

                # Try to create and submit a 'community-inclusion' request
                requests, errors = record_communities.add(
                    system_identity,
                    draft_id,
                    {"communities": [{"id": community_id}]},
                )
                submitted_request = requests[0] if requests else None
                app.logger.debug(pformat(submitted_request))

                # If that failed because the record is already included in the
                # community, skip accepting the request (unnecessary)
                # FIXME: How can we tell if an inclusion request is already
                # open and/or accepted? Without relying on this error message?
                if errors and "already included" in errors[0]["message"]:
                    return {
                        "status": "already_included",
                        "submitted_request": (
                            submitted_request.to_dict() if submitted_request else None
                        ),
                    }
                # If that failed look for any existing open
                # 'community-inclusion' request and continue with it
                if errors:
                    app.logger.debug(
                        f"    inclusion request already open for {draft_id}"
                    )
                    app.logger.debug(pformat(errors))
                    record = record_communities.record_cls.pid.resolve(draft_id)
                    request_id = record_communities._exists(community_id, record)
                    app.logger.debug(
                        f"submitted inclusion request: {pformat(request_id)}"
                    )
                # If it succeeded, continue with the new request
                else:
                    request_id = (
                        submitted_request["id"]
                        if submitted_request.get("id")
                        else submitted_request["request"]["id"]
                    )
                request_obj = current_requests_service.read(
                    system_identity, request_id
                )._record
                community = current_communities.service.record_cls.pid.resolve(
                    community_id
                )
                # app.logger.debug(f"request_obj: {pformat(request_obj)}  ")

                # Accept the 'community-inclusion' request if it's not already
                # accepted
                if request_obj["status"] != "accepted":
                    community_inclusion = (
                        current_rdm_records.community_inclusion_service
                    )
                    try:
                        review_accepted = community_inclusion.include(
                            system_identity, community, request_obj, uow
                        )
                    except InvalidAccessRestrictions:
                        # can't add public record to restricted community
                        # so set community to public before acceptance
                        # TODO: can we change this policy?
                        app.logger.warning(
                            f"    setting community {community_id} to public"
                        )
                        CommunityRecordHelper.set_community_visibility(
                            community_id, "public"
                        )
                        review_accepted = community_inclusion.include(
                            system_identity, community, request_obj, uow
                        )
                else:
                    review_accepted = request_obj
                return review_accepted

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

        params:
            metadata_record (dict): the metadata record to add to group
                collections (this is assumed to be a published record)
            record_source (str): the string representation of the record's
                source service, for use by invenio-group-collections-kcworks
                in linking the record to the appropriate group collections

        returns:
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
                app.logger.debug(f"    linking to group_id: {group_id}")
                app.logger.debug(f"    linking to group_name: {group['group_name']}")
                group_name = group["group_name"]
                coll_record = None
                # TODO: This is a hack to get the index name
                prefix = app.config.get("SEARCH_INDEX_PREFIX")
                if prefix:
                    index = f"{prefix}-communities"
                else:
                    index = "communities"
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
                                f"    collection {h['_source']['id']} not " "found"
                            )
                        except CommunityDeletedError:
                            app.logger.warning(
                                f"    collection {h['_source']['id']} deleted"
                            )
                    # NOTE: Don't check for identical group name because
                    # sometimes the group name has changed since the record
                    # was created
                    #
                    # coll_records = [
                    #     c
                    #     for c in coll_records
                    #     if c["custom_fields"].get("kcr:commons_group_name")
                    #     == group_name
                    # ]
                    # app.logger.debug(
                    #     f"coll_record_search: {pformat(coll_search)}"
                    # )
                    try:
                        assert len(coll_records) == 1
                    except AssertionError:
                        app.logger.debug(pformat(metadata_record))
                        if len(coll_records) > 1:
                            raise MultipleActiveCollectionsError(
                                f"    multiple active collections found "
                                f"for {group_id}"
                            )
                        else:
                            raise CollectionNotFoundError(
                                f"    no active collections found for " f'"{group_id}"'
                            )
                    coll_record = coll_records[0]
                    app.logger.debug(f"    found group collection {coll_record['id']}")
                except CollectionNotFoundError:
                    try:
                        app.logger.debug("    creating group collection...")
                        coll_record = collections_service.create(
                            system_identity,
                            group_id,
                            record_source,
                        )
                        app.logger.debug("   created group collection...")
                    except UnprocessableEntity as e:
                        if "Something went wrong requesting group" in e.description:
                            app.logger.warning(
                                f"Failed requesting group collection from API "
                                f"{e.description}"
                            )
                            raise CommonsGroupServiceError(
                                f"Failed requesting group collection from API "
                                f"{e.description}"
                            )
                    except CommonsGroupNotFoundError:
                        message = (
                            f"    group {group_id} ({group_name})"
                            f"not found on Knowledge Commons. Could not "
                            f"create a group collection..."
                        )
                        app.logger.warning(message)
                        raise CommonsGroupNotFoundError(message)
                if coll_record:
                    app.logger.debug(
                        f"    adding record to group collection "
                        f"{coll_record['id']}..."
                    )
                    add_result = self.publish_record_to_community(
                        metadata_record["id"],
                        coll_record["id"],
                    )
                    added_to_collections.append(add_result)
                    app.logger.debug(
                        f"    added to group collection {coll_record['id']}"
                    )
                    # app.logger.debug(f"    add_result: "
                    #     f"{pformat(add_result)}")
            if added_to_collections:
                self._remove_from_extraneous_collections(metadata_record, group_list)
                app.logger.info(
                    f"    record {metadata_record['id']} successfully added "
                    f"to group collections {added_to_collections}..."
                )
                return added_to_collections
            else:
                app.logger.info(
                    f"    record {metadata_record['id']} not added to any "
                    "group collections..."
                )
                return []
        else:
            app.logger.info("    no group collections to add to...")
            return []

    def _remove_from_extraneous_collections(
        self, metadata_record: dict, group_list: list
    ) -> None:
        """Remove a record from any group collections that are not in the
        group list.
        """
        group_ids = [g.get("group_identifier") for g in group_list]
        app.logger.debug(f"group_ids: {pformat(group_ids)}")
        parent_communities = [
            c.get("custom_fields", {}).get("kcr:commons_group_id")
            for c in metadata_record["parent"]["communities"].get("entries", [])
        ]
        app.logger.debug(f"parent_communities: {pformat(parent_communities)}")
        extraneous_collections = [
            c
            for c in metadata_record["parent"]["communities"].get("entries", [])
            if c.get("custom_fields", {}).get("kcr:commons_group_id")
            and c.get("custom_fields", {}).get("kcr:commons_group_id") not in group_ids
        ]
        if extraneous_collections:
            app.logger.debug(
                f"    removing record from extraneous collections "
                f"{extraneous_collections}..."
            )
            for c in extraneous_collections:
                app.logger.debug(
                    current_community_records_service.community_record_schema.schema  # noqa: E501
                )
                removed = current_community_records_service.delete(
                    system_identity,
                    c["id"],
                    {"records": [{"id": metadata_record["id"]}]},
                )
                app.logger.debug(f"    removed {removed}")
