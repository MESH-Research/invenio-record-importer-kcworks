# Part of the Invenio-Stats-Dashboard extension for InvenioRDM
#
# Copyright (C) 2025 MESH Research.
#
# Invenio-Stats-Dashboard is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""User related pytest fixtures for testing."""

from collections.abc import Callable

import pytest
from flask_login import login_user
from flask_principal import Identity
from flask_security.utils import hash_password
from invenio_access.models import ActionRoles, Role
from invenio_access.permissions import any_user, authenticated_user, superuser_access
from invenio_access.utils import get_identity
from invenio_accounts.models import User
from invenio_accounts.proxies import current_accounts
from invenio_accounts.testutils import login_user_via_session
from invenio_administration.permissions import administration_access_action
from invenio_oauth2server.models import Token
from pytest_invenio.fixtures import UserFixtureBase


def get_authenticated_identity(user: User | Identity) -> Identity:
    """Return an authenticated identity for the given user.

    If an Identity is provided, it is returned with the any_user and authenticated_user
    needs added.
    """
    identity = get_identity(user) if isinstance(user, User) else user
    identity.provides.add(any_user)
    identity.provides.add(authenticated_user)
    return identity


class AugmentedUserFixture(UserFixtureBase):
    """Augmented UserFixtureBase class."""

    def __init__(self, *args, **kwargs):
        """Initialize the AugmentedUserFixture."""
        super().__init__(*args, **kwargs)
        self.allowed_token: str | None = None


@pytest.fixture(scope="function")
def user_factory(
    app,
    db,
    admin_role_need,
) -> Callable:
    """Factory for creating test users.

    Returns:
        a factory function that returns a user.
    """

    def make_user(
        email: str = "myuser@inveniosoftware.org",
        password: str = "password",
        token: bool = False,
        admin: bool = False,
    ) -> AugmentedUserFixture:
        """Create an augmented pytest-invenio user fixture.

        Parameters:
            email: The email address of the user.
            password: The password of the user.
            token: Whether the user should have a token.
            admin: Whether the user should have admin access.

        Returns:
            The created UserFixture object. This has the following attributes:
            - user: The created Invenio User object.
            - identity: The identity of the user.
            - allowed_token: The API auth token of the user.
        """
        u = AugmentedUserFixture(
            email=email,
            password=hash_password(password),
        )
        u.create(app, db)

        if token:
            u.allowed_token = Token.create_personal(
                "webhook", u.id, scopes=[]  # , is_internal=False
            ).access_token

        if admin:
            datastore = app.extensions["security"].datastore
            _, role = datastore._prepare_role_modify_args(
                u.user, "administration-access"
            )
            datastore.add_role_to_user(u.user, role)

        if u.user:
            u.user.username = f"test-{email.split('@')[0]}"
            profile = u.user.user_profile
            u.user.user_profile = profile

        current_accounts.datastore.commit()
        db.session.commit()

        return u

    return make_user


@pytest.fixture(scope="function")
def admin_role_need(db):
    """Store 1 role with 'superuser-access' ActionNeed.

    WHY: This is needed because expansion of ActionNeed is
         done on the basis of a User/Role being associated with that Need.
         If no User/Role is associated with that Need (in the DB), the
         permission is expanded to an empty list.
    """
    role = Role(name="administration-access")
    db.session.add(role)

    action_role = ActionRoles.create(action=administration_access_action, role=role)
    db.session.add(action_role)

    db.session.commit()
    return action_role.need


@pytest.fixture(scope="function")
def admin(user_factory) -> AugmentedUserFixture:
    """Admin user for requests."""
    u: AugmentedUserFixture = user_factory(
        email="admin@inveniosoftware.org",
        password="password",
        admin=True,
        token=True,
    )

    return u


@pytest.fixture(scope="function")
def superuser_role_need(db):
    """Store 1 role with 'superuser-access' ActionNeed.

    WHY: This is needed because expansion of ActionNeed is
         done on the basis of a User/Role being associated with that Need.
         If no User/Role is associated with that Need (in the DB), the
         permission is expanded to an empty list.
    """
    role = Role(name="superuser-access")
    db.session.add(role)

    action_role = ActionRoles.create(action=superuser_access, role=role)
    db.session.add(action_role)

    db.session.commit()

    return action_role.need


@pytest.fixture(scope="function")
def superuser_identity(admin: AugmentedUserFixture, superuser_role_need) -> Identity:
    """Superuser identity fixture."""
    # Recreate identity from a fresh, attached User to avoid detached instance issues
    # in containerized test setups where sessions can be closed between fixture steps.
    user = current_accounts.datastore.get_user(admin.id)
    identity = get_identity(user)
    identity.provides.add(superuser_role_need)
    return identity


@pytest.fixture(scope="module")
def user1_data() -> dict:
    """Data for user1."""
    return {
        "saml_id": "user1",
        "email": "user1@inveniosoftware.org",
        "name": "User Number One",
        "first_name": "User Number",
        "last_name": "One",
        "institutional_affiliation": "Michigan State University",
        "orcid": "0000-0002-1825-0097",  # official dummy orcid
        "preferred_language": "en",
        "time_zone": "UTC",
        "groups": [
            {"id": 12345, "name": "awesome-mock", "role": "administrator"},
            {"id": 67891, "name": "admin", "role": "member"},
        ],
    }


user_data_set = {
    "joanjett": {
        "saml_id": "joanjett",
        "email": "jj@inveniosoftware.com",
        "name": "Joan Jett",
        "first_name": "Joan",
        "last_name": "Jett",
        "institutional_affiliation": "Uc Davis",
        "orcid": "",
        "groups": [],
    },
    "user1": {
        "saml_id": "user1",
        "email": "user1@inveniosoftware.org",
        "name": "User Number One",
        "first_name": "User Number",
        "last_name": "One",
        "institutional_affiliation": "Michigan State University",
        "orcid": "0000-0002-1825-0097",  # official dummy orcid
        "preferred_language": "en",
        "time_zone": "UTC",
        "groups": [
            {"id": 12345, "name": "awesome-mock", "role": "administrator"},
            {"id": 67891, "name": "admin", "role": "member"},
        ],
    },
    "user2": {
        "saml_id": "janedoe",
        "email": "jane.doe@msu.edu",
        "name": "Jane Doe",
        "first_name": "Jane",
        "last_name": "Doe",
        "institutional_affiliation": "College Of Human Medicine",
        "orcid": "0000-0002-1825-0097",  # official dummy orcid
    },
    "user3": {
        "saml_id": "gihctester",
        "email": "ghosthc@lblyoehp.mailosaur.net",
        # FIXME: Unobfuscated email not sent by
        # KC because no email marked as official.
        # Also, different email address than shown in KC profile.
        "name": "Ghost Hc",
        "first_name": "Ghost",
        "last_name": "Hc",
        "groups": [
            {"id": 1004089, "name": "Teaching and Learning", "role": "member"},
            {"id": 1004090, "name": "Humanities, Arts, and Media", "role": "member"},
            {
                "id": 1004091,
                "name": "Technology, Networks, and Sciences",
                "role": "member",
            },
            {"id": 1004092, "name": "Social and Political Issues", "role": "member"},
            {
                "id": 1004093,
                "name": "Educational and Cultural Institutions",
                "role": "member",
            },
            {"id": 1004094, "name": "Publishing and Archives", "role": "member"},
            {
                "id": 1004651,
                "name": "Hidden Testing Group New Name",
                "role": "administrator",
            },
            {
                "id": 1004939,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004940,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004941,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004942,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004943,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004944,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004945,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004946,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004947,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004948,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004949,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004950,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004951,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004952,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
            {
                "id": 1004953,
                "name": "GI Hidden Group for testing",
                "role": "administrator",
            },
        ],
    },
    "user4": {
        "saml_id": "ghostrjtester",
        "email": "jrghosttester@email.ghostinspector.com",
        "name": "Ghost Tester",
        "first_name": "Ghost",
        "last_name": "Tester",
        "institutional_affiliation": "Michigan State University",
        "orcid": "0000-0002-1825-0097",  # official dummy orcid
        "groups": [],
    },
}


@pytest.fixture(scope="function")
def client_with_login(requests_mock, app):
    """Log in a user to the client.

    Returns a factory function that returns a client with a logged in user.
    """

    def log_in_user(
        client,
        user: User,
    ):
        """Log in a user to the client.

        Parameters:
            client: The client to log in with.
            user: The user to log in.
        """
        login_user(user)
        login_user_via_session(client, email=user.email)
        return client

    return log_in_user
