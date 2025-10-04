# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
# Copyright (C) 2025 MESH Research
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Pytest fixtures for communities."""

from pprint import pformat

import arrow
import pytest
from flask import current_app
from flask_principal import Identity
from flask_sqlalchemy import SQLAlchemy
from invenio_access.permissions import system_identity
from invenio_communities.communities.records.api import Community
from invenio_communities.proxies import current_communities
from invenio_rdm_records.proxies import current_rdm_records, current_rdm_records_service
from invenio_rdm_records.records.api import RDMRecord
from invenio_records_resources.services.uow import RecordCommitOp, UnitOfWork
from invenio_requests.proxies import current_requests_service
from invenio_search.proxies import current_search_client
from sqlalchemy.exc import IntegrityError


def add_community_to_record(
    db: SQLAlchemy,
    record: RDMRecord,
    community_id: str,
    identity: Identity = system_identity,
    default: bool = False,
) -> None:
    """Add a community to a record."""
    current_search_client.indices.refresh(index="*rdmrecords-records*")

    with UnitOfWork(db.session) as uow:
        result = current_rdm_records.record_communities_service.add(
            identity,
            record.pid.pid_value,  # type: ignore
            data={"communities": [{"id": str(community_id)}]},
            uow=uow,
        )

        request = result[0][0]
        if request and "request_id" in request:
            request_id = request["request_id"]
            request_status = request["request"]["status"]

            if request_status == "submitted":
                current_requests_service.execute_action(
                    system_identity, request_id, "accept", uow=uow
                )
                current_app.logger.error(f"Accepted request: {request_id}")

        uow.commit()

    updated_record = current_rdm_records_service.record_cls.get_record(record.id)
    current_app.logger.error(
        f"Updated record communities: {pformat(updated_record.parent.communities.ids)}"
    )

    current_rdm_records_service.indexer.index(
        updated_record, arguments={"refresh": True}
    )


def make_community_member(user_id: int, role: str, community_id: str) -> None:
    """Make a member of a community."""
    service = current_communities.service.members
    with UnitOfWork() as uow:
        try:
            member = service.record_cls.create(
                {},
                community_id=community_id,
                role=role,
                active=True,
                visible=True,
                request_id=None,
                user_id=user_id,
            )
        except IntegrityError as e:
            raise e

        uow.register(
            RecordCommitOp(member, indexer=current_communities.service.members.indexer)
        )

        Community.index.refresh()


@pytest.fixture(scope="function")
def minimal_community_factory(
    app,
    db,
    superuser_identity,
    create_communities_custom_fields,
    requests_mock,
    monkeypatch,
):
    """Create a minimal community for testing.

    Returns a function that can be called to create a minimal community
    for testing. That function returns the created community record.
    """

    def create_minimal_community(
        owner: int | None = None,
        slug: str | None = None,
        metadata: dict | None = None,
        access: dict | None = None,
        custom_fields: dict | None = None,
        members: dict | None = None,
        mock_search_api: bool = True,
        created: str | None = None,
    ) -> Community:
        """Create a minimal community for testing.

        Allows overriding of default metadata, access, and custom fields values.
        Also allows specifying the members of the community with their roles.

        If no owner is specified, a new user is created and used as the owner.
        """
        metadata = metadata or {}
        access = access or {}
        custom_fields = custom_fields or {}
        members = members or {"reader": [], "curator": [], "manager": [], "owner": []}

        slug = slug or "my-community"

        access_data = {
            "visibility": "public",
            "members_visibility": "public",
            "member_policy": "open",
            "record_policy": "open",
            "review_policy": "open",
        }
        access_data.update(access)
        metadata_data = {
            "title": "My Community",
            "description": "A description",
            "type": {
                "id": "event",
            },
            "curation_policy": "Curation policy",
            "page": "Information for my community",
            "website": "https://my-community.com",
            "organizations": [
                {
                    "name": "Organization 1",
                }
            ],
        }
        metadata_data.update(metadata)

        custom_fields_data = {}
        custom_fields_data.update(custom_fields)

        community_data = {
            "slug": slug,
            "access": access_data,
            "metadata": metadata_data,
            "custom_fields": custom_fields_data,
        }

        community_rec = current_communities.service.create(
            identity=system_identity, data=community_data
        )
        community_id = community_rec.id

        if owner:
            members["owner"].append(str(owner))
        for m in members.keys():
            for user_id in members[m]:
                if user_id:
                    make_community_member(user_id, m, community_id)

        # Set creation date if provided
        if created:
            community_record = Community.get_record(community_id)
            community_record.model.created = created  # type: ignore
            uow = UnitOfWork(db.session)
            uow.register(RecordCommitOp(community_record))
            uow.commit()
            current_communities.service.indexer.index(community_record)

        Community.index.refresh()

        community = current_communities.service.read(
            identity=system_identity, id_=community_id
        )
        current_app.logger.error(f"Community: {pformat(community.to_dict())}")
        return community

    return create_minimal_community


@pytest.fixture(scope="function")
def sample_community_with_group_id(
    app, db, search_clear, create_communities_custom_fields, minimal_community_factory
):
    """Create a sample community with kcr:commons_group_id for testing."""
    # Create the community with custom fields
    community = minimal_community_factory(
        metadata={
            "title": "Test Community with Group ID",
            "description": "A test community for created date testing",
            "type": {"id": "organization"},
        },
        access={
            "visibility": "public",
            "member_policy": "open",
            "record_policy": "open",
        },
        custom_fields={
            "kcr:commons_group_id": "test-group-123",
        },
    )
    community_dict = community.to_dict()
    community_id = community_dict["id"]

    # Get the group_id from custom fields (custom_fields is at top level, not under metadata)
    print(f"DEBUG: community_dict keys: {community_dict.keys()}")
    print(f"DEBUG: community_dict custom_fields: {community_dict.get('custom_fields')}")
    group_id = community_dict.get("custom_fields", {}).get("kcr:commons_group_id")
    print(f"DEBUG: extracted group_id: {group_id}")

    # Ensure the community is committed to the database and indexed
    db.session.commit()
    Community.index.refresh()

    # Get current date
    current_date = arrow.utcnow()

    yield {
        "id": community_id,
        "group_id": group_id,
        "current_date": current_date,
    }
