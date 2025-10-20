#! /usr/bin/env python
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

import json
import os
from traceback import print_exc

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


class UsersHelper:
    """A helper class for working with Invenio users during record imports.

    Includes public methods for creating users (including configuring SAML
    login).
    """

    @staticmethod
    def get_admins():
        """Get all users with the role of 'admin'."""
        admin_role = current_accounts.datastore.find_role_by_id("admin")
        admin_role_holders = [
            u for u in current_accounts.datastore.find_role(admin_role.name).users
        ]
        assert len(admin_role_holders) > 0  # should be at least one admin
        return admin_role_holders

    @staticmethod
    def get_user_by_source_id(
        source_id: str, record_source: str = "knowledgeCommons"
    ) -> dict[str, str]:
        """Get a user by their source id.

        Note that this method depends on the invenio_remote_user_data module
        being installed and configured. The record_source parameter should
        correspond to the name of a remote api in the
        REMOTE_USER_DATA_API_ENDPOINTS config variable.

        :param source_id: The id of the user on the source service from which
            the record is coming (e.g. '1234')
        :param record_source: The name of the source service from which the
            record is coming (e.g. 'knowledgeCommons')

        :returns: A dictionary containing the user data
        """
        endpoint_config = app.config.get("REMOTE_USER_DATA_API_ENDPOINTS", {})[
            record_source
        ]["users"]

        remote_api_token = os.environ[endpoint_config["token_env_variable_label"]]
        api_url = f"{endpoint_config['remote_endpoint']}/{source_id}"
        headers = {"Authorization": f"Bearer {remote_api_token}"}
        response = requests.request(
            endpoint_config["remote_method"],
            url=api_url,
            headers=headers,
            verify=False,
            timeout=10,
        )
        if response.status_code != 200:
            app.logger.error(f"Error fetching user data from remote API: {api_url}")
            app.logger.error("Response status code: " + str(response.status_code))
        try:
            app.logger.debug(response.json())
            return response.json()
        except requests.exceptions.JSONDecodeError:
            app.logger.error(
                "JSONDecodeError: User group data API response was not" " JSON:"
            )
            return {}

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
        """Create a new user account in the Invenio instance

        Where a user account already exists with the provided email address,
        the existing account is returned. If the user account does not exist,
        a new account is created.

        If the source_username is provided, the user account is configured
        to use SAML login with the provided source service.

        Parameters
        ----------
        user_email : str
            The email address for the new user account
        source_username : str
            The username of the new user in the source service
        full_name : str
            The full name for the new user account
        record_source : str
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
        idps = app.config.get("SSO_SAML_IDPS")
        if not idps or idp not in idps.keys():
            app.logger.warning(
                f"During user creation, record_source {idp} not found in SSO_SAML_IDPS"
            )

        if idp_username and idp and not user_email:
            email = UsersHelper.get_user_by_source_id(idp_username, idp).get("email")
            if not email:
                raise RuntimeError(
                    "No email address found in source data for user. Cannot "
                    "create user."
                )
            user_email = email

        existing_user = current_accounts.datastore.get_user_by_email(user_email)
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
                info_args["username"] = f"{idp}-{idp_username}"
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
                app.logger.error(f"    failed to create user {user_email}...")
                print_exc()
            active_user = user_confirmed

        new_profile = active_user.user_profile
        if full_name:
            new_profile["full_name"] = full_name
        if orcid:
            new_profile["identifier_orcid"] = orcid
        if other_user_ids:
            new_profile["identifier_other"] = json.dumps(other_user_ids)
        active_user.user_profile = new_profile
        current_accounts.datastore.commit()

        if idp and idp_username:
            existing_saml = UserIdentity.query.filter_by(
                id_user=active_user.id,
                method=idp,
                id=idp_username,
            ).one_or_none()

            if not existing_saml:
                try:
                    UserIdentity.create(active_user, idp, idp_username)
                    db.session.commit()  # type: ignore
                    app.logger.info(
                        f"    configured SAML login for {user_email} as"
                        f" {idp_username} on {idp}..."
                    )
                    assert UserIdentity.query.filter_by(
                        id_user=active_user.id,
                        method=idp,
                        id=idp_username,
                    ).one_or_none()

                    app.logger.info(active_user.external_identifiers)
                    assert any(
                        [
                            a
                            for a in active_user.external_identifiers
                            if a.method == idp
                            and a.id == idp_username
                            and a.id_user == active_user.id
                        ]
                    )
                except AlreadyLinkedError as e:
                    if idp_username in str(e):
                        app.logger.warning(
                            f"    SAML login already configured for"
                            f" {idp_username} on {idp}..."
                        )
                    else:
                        raise e
            else:
                app.logger.info(
                    f"   found existing SAML login for {user_email},"
                    f" {existing_saml.method}, {existing_saml.id}..."
                )

        communities_owned = []
        for c in community_owner:
            communities_owned.append(CommunityRecordHelper.add_owner(c, active_user.id))

        return {
            "user": active_user,
            "new_user": new_user_flag,
            "communities_owned": communities_owned,
        }
