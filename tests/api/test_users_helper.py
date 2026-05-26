# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Tests for UsersHelper."""

import re

import pytest

from invenio_record_importer_kcworks.services.users import UsersHelper
from tests.fixtures.idms import minimal_api_response


@pytest.mark.parametrize(
    "email_in,source_username,full_name,new_user_flag",
    [
        ("myaddress3@somedomain.edu", "newuser", "My User", True),
        ("scottia4@msu.edu", "ianscott", "Ian Scott", False),
    ],
)
def test_create_invenio_user(
    app,
    admin,
    db,
    search_clear,
    user_factory,
    requests_mock,
    email_in,
    source_username,
    full_name,
    new_user_flag,
):
    """Test UsersHelper.create_invenio_user method."""
    if not new_user_flag:
        preexisting_user = user_factory(email=email_in).user
        assert preexisting_user.id

    # Stub the Profiles `subs/{kc_username}/` endpoint so the helper's
    # remote-user lookup resolves to a known sub for this username.
    base_url = app.config["IDMS_BASE_API_URL"]
    profile_sub = f"http://cilogon.org/test/users/{source_username}"
    response = minimal_api_response(
        profile_sub,
        username=source_username,
        email=email_in,
        name=full_name,
    )
    requests_mock.get(
        f"{base_url}subs/{source_username}/",
        json=response.model_dump(mode="json"),
    )

    actual_user = UsersHelper().create_invenio_user(
        user_email=email_in,
        idp_username=source_username,
        full_name=full_name,
        idp="knowledgeCommons",
    )
    assert re.match(r"\d+", str(actual_user["user"].id))
    assert actual_user["new_user"] == new_user_flag
