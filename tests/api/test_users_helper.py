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


@pytest.mark.parametrize(
    "email_in,source_username,full_name,new_user_flag",
    [
        ("myaddress3@somedomain.edu", "myuser", "My User", True),
        ("scottia4@msu.edu", "ianscott", "Ian Scott", False),
    ],
)
def test_create_invenio_user(
    app,
    admin,
    db,
    search_clear,
    user_factory,
    email_in,
    source_username,
    full_name,
    new_user_flag,
):
    """Test UsersHelper.create_invenio_user method."""
    if not new_user_flag:
        preexisting_user = user_factory(email=email_in).user
        assert preexisting_user.id
    actual_user = UsersHelper().create_invenio_user(
        user_email=email_in,
        idp_username=source_username,
        full_name=full_name,
        idp="knowledgeCommons",
    )
    print(actual_user)
    assert re.match(r"\d+", str(actual_user["user"]["id"]))
    assert actual_user["new_user"] == new_user_flag

