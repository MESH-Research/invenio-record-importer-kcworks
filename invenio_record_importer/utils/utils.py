#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 MESH Research
#
# core-migrate is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""
Utility functions for core-migrate
"""

from datetime import datetime
from flask import current_app as app
from flask_security.utils import hash_password
from invenio_access.permissions import system_identity
from invenio_accounts.proxies import current_accounts
from invenio_communities.proxies import current_communities
from invenio_communities.members.records.api import Member
from invenio_search.proxies import current_search_client
from isbnlib import is_isbn10, is_isbn13, clean
import os
import random
import re
import requests
import string
from typing import Any, Union
import unicodedata


class IndexHelper:

    def __init__(self, client=current_search_client):
        self.client = client

    def list_indices(self):
        return self.client.indices.get_alias().keys()

    def delete_index(self, index):
        return self.client.indices.delete(index)

    def drop_event_indices(self, index_strings: list = []):
        if not index_strings:
            index_strings = [
                "kcworks-events-stats-record-view",
                "kcworks-events-stats-file-download",
            ]
        indices = self.list_indices()
        for i in indices:
            if any(s for s in index_strings if s in i):
                app.logger.debug(f"deleting {i}")
                self.delete_index(i)

    def empty_indices():
        views_query = {
            "query": {
                "exists": {
                    "field": "record_id",
                }
            }
        }
        for t in [
            "2011-01",
            "2016-12",
            "2022-12",
            "2019-12",
            "2020-08",
            "2012-06",
            "2015-06",
            "2021-06",
            "2018-06",
            "2024-04",
            "2024-05",
            "2013-01",
            "2013-12",
            "2016-10",
        ]:
            app.logger.debug(f"checking for old views records in index {t}...")
            old_formatted = current_search_client.search(
                index=f"kcworks-events-stats-record-view-{t}", body=views_query
            )
            for hit in old_formatted["hits"]["hits"]:
                app.logger.debug(f"deleting {hit['_id']}")
                current_search_client.delete(
                    index=f"kcworks-events-stats-record-view-{t}",
                    id=hit["_id"],
                )


class UsersHelper:
    """A collection of methods for working with users."""

    def __init__(self):
        pass

    @staticmethod
    def get_admins():
        """Get all users with the role of 'admin'."""
        admin_role = current_accounts.datastore.find_role_by_id("admin")
        admin_role_holders = [
            u
            for u in current_accounts.datastore.find_role(
                admin_role.name
            ).users
        ]
        assert len(admin_role_holders) > 0  # should be at least one admin
        return admin_role_holders

    @staticmethod
    def get_user_by_source_id(source_id: str, record_source: str) -> dict:
        """Get a user by their source id.

        Note that this method depends on the invenio_remote_user_data module
        being installed and configured. The record_source parameter should
        correspond to the name of a remote api in the REMOTE_USER_DATA_API_ENDPOINTS config variable.

        :param source_id: The id of the user on the source service from which
            the record is coming (e.g. '1234')
        :param record_source: The name of the source service from which the
            record is coming (e.g. 'knowledgeCommons')

        :returns: A dictionary containing the user data
        """
        endpoint_config = app.config.get("REMOTE_USER_DATA_API_ENDPOINTS")[
            record_source
        ]["users"]

        remote_api_token = os.environ[
            endpoint_config["token_env_variable_label"]
        ]
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
            app.logger.error(
                f"Error fetching user data from remote API: {api_url}"
            )
            app.logger.error(
                "Response status code: " + str(response.status_code)
            )
        try:
            app.logger.debug(response.json())
            return response.json()
        except requests.exceptions.JSONDecodeError:
            app.logger.error(
                "JSONDecodeError: User group data API response was not"
                " JSON:"
            )
            return None


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
            AssertionError: If the member visibility was not updated successfully

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


def generate_random_string(length):
    """
    Generate a random string of lowercase letters and integer numbers.
    """
    res = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=length)
    )
    return res


def generate_password(length):
    return hash_password(generate_random_string(48))


def flatten_list(list_of_lists, flat_list=[]):
    if not list_of_lists:
        return flat_list
    else:
        for item in list_of_lists:
            if type(item) is list:
                flatten_list(item, flat_list)
            else:
                flat_list.append(item)

    return flat_list


def valid_isbn(isbn: str) -> Union[bool, str]:
    if is_isbn10(isbn) or (is_isbn13(isbn)):
        return isbn
    elif is_isbn10(clean(isbn)) or is_isbn13(clean(isbn)):
        return clean(isbn)
    else:
        return False


def valid_date(datestring: str) -> bool:
    """
    Return true if the supplied string is a valid iso8601 date.

    If it is, then this will also generally be valid for w3c and for LOC's
    Extended Date Time Format Level 0. The latter also requires hyphens
    and colons where appropriate.

    This function allows for truncated dates (just year, year-month,
    year-month-day)
    """
    try:
        datetime.fromisoformat(datestring.replace("Z", "+00:00"))
    except Exception:
        try:
            dtregex = (
                r"^(?P<year>[0-9]{4})(-(?P<month>1[0-2]|0[1-9])"
                r"(-(?P<day>3[0-1]|0[1-9]|[1-2][0-9]))?)?$"
            )
            assert re.search(dtregex, datestring)
        except Exception:
            return False
    return True


def compare_metadata(A: dict, B: dict) -> dict:
    """
    Compare two Invenio records and return a dictionary of differences.

    param A: The first record to compare (typically the existing record
             prior to migration)
    param B: The second record to compare (typically the record being migrated)
    return: A dictionary of differences between the two records
    rtype: dict
    """
    VERBOSE = False
    output = {"A": {}, "B": {}}

    def deep_compare(a, b):
        if type(a) in [str, int, float, bool]:
            return a == b
        elif a is None:
            return a is b
        elif type(a) is list:
            return all(deep_compare(a[i], b[i]) for i in range(len(a)))
        elif type(a) is dict:
            # if the key "en" is present, then we only care about that
            # because Invenio automatically adds other translations
            # to things like titles
            if "en" in list(a.keys()):
                a = {k: v for k, v in a.items() if k == "en"}
            return all(deep_compare(a[k], b[k]) for k in a.keys())

    def obj_list_compare(list_name, key, a, b, comparators):
        VERBOSE = True
        if VERBOSE:
            app.logger.debug(f"comparing {list_name} &&&&&&")
            app.logger.debug(a.get(list_name))
            app.logger.debug(b.get(list_name))
        out = {}
        if list_name not in a.keys():
            a[list_name] = []
        if list_name not in b.keys():
            b[list_name] = []
        existing_items = [
            _normalize_punctuation(i.get(key)) for i in a[list_name]
        ]
        for i in b[list_name]:
            if _normalize_punctuation(i[key]) not in existing_items:
                out.setdefault("A", []).append({})
                out.setdefault("B", []).append(i)
            else:
                same = True
                i_2 = [
                    i2
                    for i2 in a[list_name]
                    if _normalize_punctuation(i2[key])
                    == _normalize_punctuation(i[key])
                ][0]
                for k in comparators:
                    if not (
                        deep_compare(
                            _normalize_punctuation(i[k]),
                            _normalize_punctuation(i_2[k]),
                        )
                    ):
                        if VERBOSE:
                            app.logger.debug(f"{k} is different: {i} != {i_2}")
                        same = False
                if not same:
                    out.setdefault("A", []).append(i_2)
                    out.setdefault("B", []).append(i)
        if len(a[list_name]) != len(b[list_name]):
            out.setdefault("A", []).append(a[list_name])
            out.setdefault("B", []).append(b[list_name])

        if VERBOSE:
            app.logger.debug(f"comparing {list_name} &&&&&&")
        if VERBOSE:
            app.logger.debug(a[list_name])
        if VERBOSE:
            app.logger.debug(b[list_name])
        return out

    def compare_people(list_a, list_b):
        people_diff = {}
        if not list_a:
            if not list_b:
                return {}
            else:
                people_diff["A"] = list_a
                people_diff["B"] = list_b
                return people_diff
        for idx, c in enumerate(list_b):
            same = True
            c_2 = list_a[idx]  # order should be the same
            if _normalize_punctuation(
                c_2["person_or_org"].get("name")
            ) != _normalize_punctuation(c["person_or_org"].get("name")):
                same = False
            for k in c["person_or_org"].keys():
                if k == "identifiers":
                    if (
                        k not in c_2["person_or_org"].keys()
                        or c["person_or_org"][k] != c_2["person_or_org"][k]
                    ):
                        same = False
                else:
                    if k not in c_2[
                        "person_or_org"
                    ].keys() or _normalize_punctuation(
                        c["person_or_org"][k]
                    ) != _normalize_punctuation(
                        c_2["person_or_org"][k]
                    ):
                        same = False
            if (
                "role" not in c_2.keys()
                or c["role"]["id"] != c_2["role"]["id"]
            ):
                same = False
            if not same:
                people_diff.setdefault("A", []).append(c_2)
                people_diff.setdefault("B", []).append(c)
        return people_diff

    if "access" in B.keys():
        if VERBOSE:
            app.logger.debug("comparing access")
            app.logger.debug(A.get("access", {}))
            app.logger.debug(B["access"])
        same_access = deep_compare(A.get("access", {}), B["access"])
        app.logger.debug(same_access)
        if not same_access:
            output["A"]["access"] = A.get("access", {})
            output["B"]["access"] = B["access"]

    if "pids" in B.keys():
        pids_diff = {"A": {}, "B": {}}
        if B["pids"]["doi"] != A["pids"]["doi"]:
            pids_diff["A"] = {"doi": A["pids"]["doi"]}
            pids_diff["B"] = {"doi": B["pids"]["doi"]}
        if pids_diff["A"] or pids_diff["B"]:
            output["A"]["pids"] = pids_diff["A"]
            output["B"]["pids"] = pids_diff["B"]

    if "metadata" in B.keys():
        meta_diff = {"A": {}, "B": {}}
        meta_a = A["metadata"]
        meta_b = B["metadata"]

        simple_fields = [
            "title",
            "publication_date",
            "version",
            "description",
            "publisher",
        ]
        for s in simple_fields:
            if VERBOSE:
                app.logger.debug(f"comparing {s}")
                app.logger.debug(meta_a.get(s))
                app.logger.debug(meta_a.get(s))
            if s in meta_a.keys():
                if s in meta_b.keys():
                    if _normalize_punctuation(
                        meta_b[s]
                    ) != _normalize_punctuation(meta_a[s]):
                        meta_diff["A"][s] = meta_a[s]
                        meta_diff["B"][s] = meta_b[s]
                else:
                    meta_diff["A"][s] = meta_a[s]
                    meta_diff["B"][s] = None
            elif s in meta_b.keys():
                meta_diff["A"][s] = None
                meta_diff["B"][s] = meta_b[s]

        if meta_b["resource_type"]["id"] != meta_a["resource_type"]["id"]:
            meta_diff["A"]["resource_type"] = meta_a["resource_type"]
            meta_diff["B"]["resource_type"] = meta_b["resource_type"]

        creators_comp = compare_people(
            meta_a.get("creators"), meta_b.get("creators")
        )
        if creators_comp:
            meta_diff["A"]["creators"] = creators_comp["A"]
            meta_diff["B"]["creators"] = creators_comp["B"]

        if "contributors" in meta_b.keys():
            if "contributors" not in meta_a.keys():
                meta_a["contributors"] = []
            if meta_b["contributors"] != meta_a["contributors"]:
                comp = compare_people(
                    meta_a["contributors"], meta_b["contributors"]
                )
                if comp:
                    meta_diff["A"]["contributors"] = comp["A"]
                    meta_diff["B"]["contributors"] = comp["B"]

        if "additional_titles" in meta_b.keys():
            if "additional_titles" not in meta_a.keys():
                meta_a["additional_titles"] = []
            existing_titles = [
                _normalize_punctuation(t["title"])
                for t in meta_a["additional_titles"]
            ]
            for t in meta_b["additional_titles"]:
                if _normalize_punctuation(t["title"]) not in existing_titles:
                    meta_diff["A"].setdefault("additional_titles", []).append(
                        {}
                    )
                    meta_diff["B"].setdefault("additional_titles", []).append(
                        t
                    )
                else:
                    same = True
                    t_2 = [
                        t2
                        for t2 in meta_a["additional_titles"]
                        if _normalize_punctuation(t2["title"])
                        == _normalize_punctuation(t["title"])
                    ][0]
                    if (
                        _normalize_punctuation(t["title"])
                        != _normalize_punctuation(t_2["title"])
                        or t["type"]["id"] != t_2["type"]["id"]
                    ):
                        same = False
                    if not same:
                        meta_diff["A"].setdefault(
                            "additional_titles", []
                        ).append(t_2)
                        meta_diff["B"].setdefault(
                            "additional_titles", []
                        ).append(t)

        if "identifiers" in meta_b.keys() or "identifiers" in meta_a.keys():
            comp = obj_list_compare(
                "identifiers",
                "identifier",
                meta_a,
                meta_b,
                ["identifier", "scheme"],
            )
            if comp:
                meta_diff["A"]["identifiers"] = comp["A"]
                meta_diff["B"]["identifiers"] = comp["B"]

        if "dates" in meta_b.keys() or "dates" in meta_a.keys():
            comp = obj_list_compare(
                "dates",
                "date",
                meta_a,
                meta_b,
                ["date", "type"],
            )
            if comp:
                meta_diff["A"]["dates"] = comp["A"]
                meta_diff["B"]["dates"] = comp["B"]

        if "languages" in meta_b.keys() or "languages" in meta_a.keys():
            comp = obj_list_compare("languages", "id", meta_a, meta_b, ["id"])
            if comp:
                meta_diff["A"]["languages"] = comp["A"]
                meta_diff["B"]["languages"] = comp["B"]

        if (
            "additional_descriptions" in meta_b.keys()
            or "additional_descriptions" in meta_a.keys()
        ):
            comp = obj_list_compare(
                "additional_descriptions",
                "description",
                meta_a,
                meta_b,
                ["description"],
            )
            if comp:
                meta_diff["A"]["additional_descriptions"] = comp["A"]
                meta_diff["B"]["additional_descriptions"] = comp["B"]

        if "subjects" in meta_b.keys() or "subjects" in meta_a.keys():
            comp = obj_list_compare(
                "subjects",
                "id",
                meta_a,
                meta_b,
                ["id", "subject", "scheme"],
            )
            if comp:
                meta_diff["A"]["subjects"] = meta_a["subjects"]
                meta_diff["B"]["subjects"] = meta_b["subjects"]

        if meta_diff["A"] or meta_diff["B"]:
            output["A"]["metadata"] = meta_diff["A"]
            output["B"]["metadata"] = meta_diff["B"]

    if "custom_fields" in B.keys():
        custom_a = A["custom_fields"]
        custom_b = B["custom_fields"]
        custom_diff = {"A": {}, "B": {}}

        simple_fields = [
            "hclegacy:collection",
            "hclegacy:file_location",
            "hclegacy:file_pid",
            "hclegacy:previously_published",
            "hclegacy:record_change_date",
            "hclegacy:record_creation_date",
            "hclegacy:submitter_affiliation",
            "hclegacy:submitter_id",
            "hclegacy:submitter_org_memberships",
            "hclegacy:total_downloads",
            "hclegacy:total_views",
            "kcr:ai_usage",
            "kcr:chapter_label",
            "kcr:commons_domain",
            "kcr:content_warning",
            "kcr:course_title",
            "kcr:degree",
            "kcr:discipline",
            "kcr:edition",
            "kcr:media",
            "kcr:meeting_organization",
            "kcr:notes",
            "kcr:project_title",
            "kcr:publication_url",
            "kcr:sponsoring_institution",
            "kcr:submitter_email",
            "kcr:submitter_username",
            "kcr:user_defined_tags",
            "kcr:volumes",
        ]

        for s in simple_fields:
            if s in custom_b.keys():
                same = True
                if s in custom_a.keys():
                    if type(custom_a[s]) is str:
                        if unicodedata.normalize(
                            "NFC", custom_b[s]
                        ) != unicodedata.normalize("NFC", custom_a[s]):
                            same = False
                    elif type(custom_a[s]) is list:
                        if custom_b[s] != custom_a[s]:
                            same = False
                else:
                    same = False
                    custom_a[s] = None
                if not same:
                    custom_diff["A"][s] = custom_a[s]
                    custom_diff["B"][s] = custom_b[s]
            elif s in custom_a.keys():
                custom_diff["A"][s] = custom_a[s]
                custom_diff["B"][s] = None

        if (
            "hclegacy:groups_for_deposit" in custom_b.keys()
            or "hclegacy:groups_for_deposit" in custom_a.keys()
        ):
            comp = obj_list_compare(
                "hclegacy:groups_for_deposit",
                "group_identifier",
                custom_a,
                custom_b,
                ["group_name", "group_identifier"],
            )
            if comp:
                custom_diff["A"]["hclegacy:groups_for_deposit"] = comp["A"]
                custom_diff["B"]["hclegacy:groups_for_deposit"] = comp["B"]

        if "imprint:imprint" in custom_b.keys():
            if "imprint:imprint" not in custom_a.keys():
                custom_a["imprint:imprint"] = {}
            same = True
            for k in ["pages", "isbn", "title"]:
                if k in custom_b["imprint:imprint"].keys():
                    if k in custom_a["imprint:imprint"].keys():
                        if custom_a["imprint:imprint"][
                            k
                        ] != unicodedata.normalize(
                            "NFC", custom_b["imprint:imprint"][k]
                        ):
                            same = False
                    else:
                        same = False
                        custom_a["imprint:imprint"][k] = None

            if "creators" in B["custom_fields"]["imprint:imprint"].keys():
                ci_comp = compare_people(
                    custom_a["imprint:imprint"]["creators"],
                    custom_b["imprint:imprint"]["creators"],
                )
                if ci_comp:
                    same = False

            if not same:
                custom_diff["A"]["imprint:imprint"] = custom_a[
                    "imprint:imprint"
                ]
                custom_diff["B"]["imprint:imprint"] = custom_b[
                    "imprint:imprint"
                ]

        if "journal:journal" in custom_b.keys():
            if "journal:journal" not in custom_a.keys():
                custom_a["journal:journal"] = {}
            same = True
            for k in ["issn", "issue", "pages", "title"]:
                if k in custom_b["journal:journal"].keys():
                    if k in custom_a["journal:journal"].keys():
                        if custom_a["journal:journal"][
                            k
                        ] != unicodedata.normalize(
                            "NFC", custom_b["journal:journal"][k]
                        ):
                            same = False
                    else:
                        same = False
                        custom_a["journal:journal"][k] = None
            if not same:
                custom_diff["A"]["journal:journal"] = custom_a[
                    "journal:journal"
                ]
                custom_diff["B"]["journal:journal"] = custom_b[
                    "journal:journal"
                ]

        if custom_diff["A"] or custom_diff["B"]:
            output["A"]["custom_fields"] = custom_diff["A"]
            output["B"]["custom_fields"] = custom_diff["B"]

    return output if output["A"] or output["B"] else {}


def normalize_string(mystring: str) -> str:
    """Normalize a string for comparison.

    This function produces a normalized string with fancy quotes
    converted to simple characters, combining unicode converted
    to composed characters, multiple slashes and spaces reduced
    to single characters, html escape ampersands converted to
    plain ampersands. It also removes leading and trailing quotes.

    Suitable for cleaning strings for case-insensitive
    comparison but not for display.
    """
    mystring = _clean_backslashes_and_spaces(mystring)
    mystring = _normalize_punctuation(mystring)
    mystring = _strip_surrounding_quotes(mystring)
    return mystring


def normalize_string_lowercase(mystring: str) -> str:
    """Normalize a string for comparison.

    This function produces a normalized *lowercase* string with
    punctuation normalized. It also removes leading and
    trailing quotes.

    Suitable for cleaning strings for case-insensitive
    comparison but not for display.
    """
    mystring = mystring.casefold()
    mystring = _clean_backslashes_and_spaces(mystring)
    mystring = _normalize_punctuation(mystring)
    mystring = _strip_surrounding_quotes(mystring)
    return mystring


def _strip_surrounding_quotes(mystring: str) -> str:
    """Remove surrounding quotes from a string.

    This function removes any leading or trailing single or
    double quotes from a string.
    """
    try:
        if ((mystring[0], mystring[-1]) == ('"', '"')) or (
            (mystring[0], mystring[-1]) == ("'", "'")
        ):
            mystring = mystring[1:-1]
    except IndexError:
        pass
    return mystring


def _normalize_punctuation(mystring) -> str:
    """Normalize the punctuation in a string.

    Converts fancy quotes to simple ones, html escaped
    ampersands to plain ones, and converts any NFD
    (combining) unicode characters to NFC (composed).
    It also converts multiple spaces to single spaces and
    removes any leading or trailing whitespace. Converts
    Windows-style line endings to Unix-style line endings.

    Suitable for cleaning strings for comparison or for
    display.
    """
    if isinstance(mystring, str):
        mystring = mystring.replace("’", "'")
        mystring = mystring.replace("‘", "'")
        mystring = mystring.replace("“", '"')
        mystring = mystring.replace("”", '"')
        mystring = mystring.replace("&amp;", "&")
        mystring = mystring.replace("'", "'")
        mystring = mystring.replace('"', '"')
        mystring = re.sub("[ ]+", " ", mystring)
        mystring = mystring.strip()
        mystring = unicodedata.normalize("NFC", mystring)
        mystring = mystring.replace("\r\n", "\n")
        return mystring
    elif isinstance(mystring, list):
        return [_normalize_punctuation(i) for i in mystring]
    elif isinstance(mystring, dict):
        return {k: _normalize_punctuation(v) for k, v in mystring.items()}
    else:
        return mystring


def _clean_backslashes_and_spaces(mystring: str) -> str:
    """
    Remove unwanted characters from a string and return it.

    Removes backslashes escaping quotation marks, and
    converts multiple spaces to single spaces. Also converts
    multiple backslashes to single backslashes.
    """
    if re.search(r"[\'\"]", mystring):
        mystring = re.sub(r"\\+'", r"'", mystring)
        mystring = re.sub(r'\\+"', r'"', mystring)
    else:
        mystring = re.sub(r"\\+", r"\\", mystring)
    mystring = re.sub(r"[ ]+", " ", mystring)
    return mystring


def update_nested_dict(original, update):
    for key, value in update.items():
        if isinstance(value, dict):
            original[key] = update_nested_dict(original.get(key, {}), value)
        elif isinstance(value, list):
            original.setdefault(key, []).extend(value)
        else:
            original[key] = value
    return original


def replace_value_in_nested_dict(d: dict, path: str, new_value: Any) -> dict:
    """
    Replace a in a nested dictionary based on a bar-separated path string.

    Numbers in the path are treated as list indices.

    Usage examples:

    >>> replace_value_in_nested_dict({"a": {"b": {"c": 1}}}, "a|b|c", 2)
    {'a': {'b': {'c': 2}}}

    >>> e = {"a": {"b": [{"c": 1}, {"d": 2}]}}
    >>> replace_value_in_nested_dict(e, "a|b|1|c", 3)
    {'a': {'b': [{'c': 1}, {'d': 2, 'c': 3}]}}

    >>> f = {"a": {"b": [{"c": 1}, {"d": 2}]}}
    >>> replace_value_in_nested_dict(f, "a|b", {"e": 3})
    {'a': {'b': {'e': 3}}}

    :param d: The dictionary or list to update.
    :param path: The dot-separated path string to the value.
    :param new_value: The new value to set.

    returns: dict: The updated dictionary.
    """
    keys = path.split("|")
    current = d
    for i, key in enumerate(keys):
        if i == len(keys) - 1:  # If this is the last key
            if key.isdigit() and isinstance(
                current, list
            ):  # Handle list index
                current[int(key)] = new_value
            else:  # Handle dictionary key
                current[key] = new_value
        else:
            if key.isdigit():  # Next level is a list
                key = int(key)  # Convert to integer for list access
                if not isinstance(current, list) or key >= len(current):
                    # If current is not a list or index is out of bounds
                    return False
                current = current[key]
            else:  # Next level is a dictionary
                if key not in current or not isinstance(
                    current[key], (dict, list)
                ):
                    # If key not found or next level is not a dict/list
                    return False
                current = current[key]
    return d
