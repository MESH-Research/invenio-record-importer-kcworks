#! /usr/bin/env python
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

import json
import os

import arrow
import requests
from flask import current_app as app
from invenio_access.permissions import system_identity
from invenio_accounts.errors import AlreadyLinkedError
from invenio_accounts.models import User, UserIdentity
from invenio_accounts.proxies import current_accounts
from invenio_communities.proxies import current_communities
from invenio_db import db
from invenio_rdm_records.proxies import current_rdm_records_service as records_service

from invenio_record_importer_kcworks.services.communities import CommunityRecordHelper
from invenio_record_importer_kcworks.tasks import send_security_email
from invenio_remote_user_data_kcworks.utils.auth import CILogonHelpers
from invenio_remote_user_data_kcworks.client import APIResponse, Profile, UserDataAPIClient
from invenio_remote_user_data_kcworks.services.service import RemoteUserDataService


class UsersHelper:
    """A helper class for working with Invenio users during record imports.

    Includes public methods for creating users (including configuring SAML
    login).
    """

    @staticmethod
    def get_admins():
        """Get all users with the role of 'administration'."""
        admin_role = current_accounts.datastore.find_role("administration")
        assert admin_role is not None  # administration role must exist
        admin_role_holders = [u for u in admin_role.users]
        assert len(admin_role_holders) > 0  # should be at least one administration role holder
        return admin_role_holders

    def send_welcome_email(
        self,
        user_email: str,
        user: User,
        community_id: str,
        record_id: str,
    ):
        app.logger.debug(f"Sending welcome email to {user_email}...")
        app.logger.debug(f"community_id: {community_id}")
        record_data = records_service.read(system_identity, id_=record_id).to_dict()
        import_config = app.config.get("RECORD_IMPORTER_COMMUNITIES", {})
        community_record = current_communities.service.read(
            system_identity, id_=community_id
        )
        collection_config = import_config.get(
            community_record.to_dict().get("slug"), None
        )
        if not collection_config:
            raise RuntimeError(
                f"No collection config found for community "
                f"{community_id}. Cannot send welcome "
                f"email to {user_email}."
            )
        else:
            user_dict = {
                "email": user.email,
                "username": user.username,
                "user_profile": user.user_profile,
            }
            app.logger.debug(f"sending welcome email to {user_email}...")
            send_security_email(
                subject=collection_config.get("email_subject_register"),
                recipients=[user_email],
                user=user_dict,
                community_url=community_record.links["self_html"],
                record_data=record_data,
                collection_config=collection_config,
            )  # type: ignore

    def create_invenio_user(
        self,
        user_email: str,
        idp_username: str = "",
        full_name: str = "",
        idp: str = "",
        community_owner: list = [],
        orcid: str = "",
        other_user_ids: list = [],
    ) -> dict:
        """Create a new user account in the Invenio instance.

        Where a user account already exists with the provided email address,
        the existing account is returned. If the user account does not exist,
        a new account is created.

        If the source_username is provided, the user account is configured
        to use SAML login with the provided source service.

        Parameters
        ----------
        user_email : str
            The email address for the new user account
        idp_username : str
            The username of the new user in the source service
        full_name : str
            The full name for the new user account
        idp: str
            The name of the source service for the new user account
            if the user's login will be handled by a SAML identity provider
        community_owner : list
            The list of communities to which the user will be assigned as
            owner. These may be slug strings or community record UUIDs.
        orcid : str
            The ORCID for the new user account
        other_user_ids : list
            A list of other user ids that the new user should be linked to.
            These may be user record UUIDs or other identifiers.

        Returns:
        -------
        dict
            A dictionary with the following keys:

            "user": the user account metadata dictionary for the created or
                existing user
            "new_user": a boolean flag indicating whether the account is new or
                existing ("new_user")
            "communities_owned": a list of the communities to which the user
                was assigned as owner
        """
        new_user_flag = True
        active_user = None
        idps = app.config.get("OAUTHCLIENT_REMOTE_APPS")
        if not idps or idp not in idps.keys():
            app.logger.warning(
                f"During user creation, record_source {idp} not found in "
                "OAUTHCLIENT_REMOTE_APPS"
            )

        remote_service = idp
        if idp in app.config.get("KC_REMOTE_IDPS"):
            remote_service = "knowledgeCommons"

        existing_user = None
        if idp_username or user_email or orcid:
            existing_user = CILogonHelpers.get_user_from_account_info({
                "user": {
                    "email": user_email,
                    "profile": {
                        "identifier_orcid": orcid,
                        "identifier_kc_username": idp_username,
                    },
                },
                "external_method": idp,
            })

        if not user_email and not existing_user:
            raise RuntimeError(
                "No email address found in source data for user. Cannot "
                "create user."
            )

        if existing_user:
            app.logger.info(f"    found existing user {existing_user.id}...")
            new_user_flag = False
            active_user = existing_user
        else:
            # FIXME: make proper password here
            app.logger.debug(f"creating new user for email {user_email}...")
            profile = {} if not full_name else {"full_name": full_name}
            info_args = {
                "email": user_email,
                "active": True,
                "confirmed_at": arrow.utcnow().datetime,
                "user_profile": profile,
            }
            if idp and idp_username:
                info_args["username"] = idp_username
            new_user = current_accounts.datastore.create_user(**info_args)
            current_accounts.datastore.commit()
            assert new_user.id
            app.logger.info(f"    created new user {user_email}...")

            if not new_user.active:
                assert current_accounts.datastore.activate_user(new_user)
                current_accounts.datastore.commit()

            user_confirmed = current_accounts.datastore.get_user_by_email(user_email)
            if user_confirmed:
                user_id = user_confirmed.id
                new_user_flag = True
                app.logger.info(f"    confirmed new user, id {user_id}...")
            else:
                app.logger.error("Failed to create user %s", user_email, exc_info=True)
            active_user = user_confirmed

        new_profile = active_user.user_profile
        if full_name:
            new_profile["full_name"] = full_name
        if idp_username:
            new_profile["identifier_kc_username"] = idp_username
        if orcid:
            new_profile["identifier_orcid"] = orcid
        if other_user_ids:
            new_profile["identifier_other"] = json.dumps(other_user_ids)
        active_user.user_profile = new_profile
        current_accounts.datastore.commit()

        if idp and idp_username:
            remote_data: APIResponse | None = UserDataAPIClient.fetch_user_profile(
                kc_username=idp_username, use_sub_endpoint=True
            )
            sub = None
            if remote_data and remote_data.data and len(remote_data.data) > 0:
                sub = remote_data.data[0].sub
            if sub:
                CILogonHelpers.link_user_to_oauth_identifier(active_user, idp, sub)
                RemoteUserDataService.update_user_from_remote(
                    system_identity, active_user.id, idp, sub, remote_data=remote_data
                )

        communities_owned = []
        for c in community_owner:
            communities_owned.append(CommunityRecordHelper.add_owner(c, active_user.id))

        return {
            "user": active_user,
            "new_user": new_user_flag,
            "communities_owned": communities_owned,
        }
