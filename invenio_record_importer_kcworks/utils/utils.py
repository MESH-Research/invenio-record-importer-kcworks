#! /usr/bin/env python
#
# Copyright (C) 2023 MESH Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Utility functions for invenio-record-importer-kcworks."""

import json
import random
import re
import string
import unicodedata
from collections.abc import Callable
from datetime import datetime
from typing import Any

import requests
from flask import current_app as app
from flask_security.utils import hash_password
from invenio_search.proxies import current_search_client
from isbnlib import clean, is_isbn10, is_isbn13
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError


# TODO: Deprecated; remove
def api_request(
    method: str = "GET",
    endpoint: str = "records",
    server: str = "",
    args: str = "",
    token: str = "",
    params: dict[str, str] = {},
    json_dict: dict[str, str] | list[dict] = {},
    file_data: bytes | None = None,
    protocol: str = "",
) -> dict:
    """Make an api request and return the response."""
    if not server:
        server = app.config.get("APP_UI_URL")
    if not token:
        token = app.config.get("RECORD_IMPORTER_API_TOKEN")
    if not protocol:
        protocol = app.config.get("RECORD_IMPORTER_PROTOCOL", "http")

    payload_args: dict[str, str | bytes] = {}

    api_url = f"{protocol}://{server}/api/{endpoint}"
    if args:
        api_url = f"{api_url}/{args}"

    callfuncs: dict[str, Callable[..., requests.Response]] = {
        "GET": requests.get,
        "POST": requests.post,
        "DELETE": requests.delete,
        "PUT": requests.put,
        "PATCH": requests.patch,
    }
    callfunc = callfuncs[method]

    headers = {"Authorization": f"Bearer {token}"}
    if json_dict and method in ["POST", "PUT", "PATCH"]:
        headers["Content-Type"] = "application/json"
        payload_args["data"] = json.dumps(json_dict)
    elif file_data and method in ["POST", "PUT"]:
        headers["content-type"] = "application/octet-stream"
        # headers['content-length'] = str(len(file_data.read()))
        payload_args["data"] = file_data

    response = callfunc(
        api_url, headers=headers, params=params, **payload_args, verify=False
    )

    try:
        json_response = response.json() if method != "DELETE" else None
    except (
        RequestsJSONDecodeError,
        json.decoder.JSONDecodeError,
    ) as e:
        # Create HTTPError with response parameter
        http_error = requests.HTTPError(
            f"Failed to decode JSON response from API request to {api_url}",
            response=response,
        )
        raise http_error from e

    result_dict = {
        "status_code": response.status_code,
        "headers": response.headers,
        "json": json_response,
        "text": response.text,
    }

    if json_response and "errors" in json_response.keys():
        result_dict["errors"] = json_response["errors"]

    return result_dict


class IndexHelper:
    """A helper class for working with OpenSearch domains."""

    def __init__(self, client=current_search_client):
        """Initialize the IndexHelper instance."""
        self.client = client

    def list_indices(self):
        """List indices in search domain."""
        return self.client.indices.get_alias().keys()

    def delete_index(self, index):
        """Delete a search index."""
        return self.client.indices.delete(index)

    def drop_event_indices(self, index_strings: list | None = None):
        """Delete the usage event indices."""
        if not index_strings:
            index_strings = [
                "kcworks-events-stats-record-view",
                "kcworks-events-stats-file-download",
            ]
        indices = self.list_indices()
        for i in indices:
            if any(s for s in index_strings if s in i):
                self.delete_index(i)

    def empty_indices(self):
        """Old utility method.

        # FIXME: deprecate
        """
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
            old_formatted = current_search_client.search(
                index=f"kcworks-events-stats-record-view-{t}", body=views_query
            )
            for hit in old_formatted["hits"]["hits"]:
                current_search_client.delete(
                    index=f"kcworks-events-stats-record-view-{t}",
                    id=hit["_id"],
                )


def generate_random_string(length):
    """Generate a random string of lowercase letters and integer numbers."""
    res = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return res


def generate_password(length):
    """Generate a hashed password."""
    return hash_password(generate_random_string(48))


def flatten_list(list_of_lists, flat_list=None):
    """Flatten a list of lists."""
    if not list_of_lists:
        return flat_list
    else:
        for item in list_of_lists:
            if type(item) is list:
                flatten_list(item, flat_list)
            else:
                flat_list.append(item)

    return flat_list


def valid_isbn(isbn: str) -> bool | str:
    """Check isbn for validity."""
    if is_isbn10(isbn) or (is_isbn13(isbn)):
        return isbn
    elif is_isbn10(clean(isbn)) or is_isbn13(clean(isbn)):
        return clean(isbn)
    else:
        return False


def valid_date(datestring: str) -> bool:
    """Return true if the supplied string is a valid iso8601 date.

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
    """Compare two Invenio records and return a dictionary of differences.

    param A: The first record to compare (typically the existing record
             prior to migration)
    param B: The second record to compare (typically the record being migrated)
    return: A dictionary of differences between the two records
    rtype: dict
    """
    VERBOSE = False
    output: dict[str, dict] = {"A": {}, "B": {}}

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
            # Check if both dictionaries have the same keys
            if set(a.keys()) != set(b.keys()):
                return False
            return all(deep_compare(a[k], b[k]) for k in a.keys())

    def obj_list_compare(list_name, key, a, b, comparators):
        """Compare two lists of objects."""
        out = {}
        if list_name not in a.keys():
            a[list_name] = []
        if list_name not in b.keys():
            b[list_name] = []
        existing_items = [_normalize_punctuation(i.get(key)) for i in a[list_name]]
        for i in b[list_name]:
            if _normalize_punctuation(i[key]) not in existing_items:
                out.setdefault("A", []).append({})
                out.setdefault("B", []).append(i)
            else:
                same = True
                i_2 = [
                    i2
                    for i2 in a[list_name]
                    if _normalize_punctuation(i2[key]) == _normalize_punctuation(i[key])
                ][0]
                for k in comparators:
                    if not (
                        deep_compare(
                            _normalize_punctuation(i.get(k)),
                            _normalize_punctuation(i_2.get(k)),
                        )
                    ):
                        same = False
                if not same:
                    out.setdefault("A", []).append(i_2)
                    out.setdefault("B", []).append(i)
        if len(a[list_name]) != len(b[list_name]):
            out.setdefault("A", []).append(a[list_name])
            out.setdefault("B", []).append(b[list_name])

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
            try:
                c_2 = list_a[idx]  # order should be the same
            except IndexError:
                people_diff.setdefault("A", []).append(c)
                people_diff.setdefault("B", []).append(None)
                break
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
                    if k not in c_2["person_or_org"].keys() or _normalize_punctuation(
                        c["person_or_org"][k]
                    ) != _normalize_punctuation(c_2["person_or_org"][k]):
                        same = False
            if "role" not in c_2.keys() or c["role"]["id"] != c_2["role"]["id"]:
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
        pids_diff: dict[str, dict] = {"A": {}, "B": {}}
        if B["pids"]["doi"] != A["pids"]["doi"]:
            pids_diff["A"] = {"doi": A["pids"]["doi"]}
            pids_diff["B"] = {"doi": B["pids"]["doi"]}
        if pids_diff["A"] or pids_diff["B"]:
            output["A"]["pids"] = pids_diff["A"]
            output["B"]["pids"] = pids_diff["B"]

    if "metadata" in B.keys():
        meta_diff: dict[str, dict] = {"A": {}, "B": {}}
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
                    if _normalize_punctuation(meta_b[s]) != _normalize_punctuation(
                        meta_a[s]
                    ):
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

        creators_comp = compare_people(meta_a.get("creators"), meta_b.get("creators"))
        if creators_comp:
            meta_diff["A"]["creators"] = creators_comp["A"]
            meta_diff["B"]["creators"] = creators_comp["B"]

        if "contributors" in meta_b.keys():
            if "contributors" not in meta_a.keys():
                meta_a["contributors"] = []
            if meta_b["contributors"] != meta_a["contributors"]:
                comp = compare_people(meta_a["contributors"], meta_b["contributors"])
                if comp:
                    meta_diff["A"]["contributors"] = comp["A"]
                    meta_diff["B"]["contributors"] = comp["B"]

        if "additional_titles" in meta_b.keys():
            if "additional_titles" not in meta_a.keys():
                meta_a["additional_titles"] = []
            existing_titles = [
                _normalize_punctuation(t["title"]) for t in meta_a["additional_titles"]
            ]
            for t in meta_b["additional_titles"]:
                if _normalize_punctuation(t["title"]) not in existing_titles:
                    meta_diff["A"].setdefault("additional_titles", []).append({})
                    meta_diff["B"].setdefault("additional_titles", []).append(t)
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
                        meta_diff["A"].setdefault("additional_titles", []).append(t_2)
                        meta_diff["B"].setdefault("additional_titles", []).append(t)

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
        custom_diff: dict[str, dict] = {"A": {}, "B": {}}

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
                    elif type(custom_a[s]) is int:
                        if custom_b[s] != custom_a[s]:
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
                        if custom_a["imprint:imprint"][k] != unicodedata.normalize(
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
                custom_diff["A"]["imprint:imprint"] = custom_a["imprint:imprint"]
                custom_diff["B"]["imprint:imprint"] = custom_b["imprint:imprint"]

        if "journal:journal" in custom_b.keys():
            if "journal:journal" not in custom_a.keys():
                custom_a["journal:journal"] = {}
            same = True
            for k in ["issn", "issue", "pages", "title"]:
                if k in custom_b["journal:journal"].keys():
                    if k in custom_a["journal:journal"].keys():
                        if custom_a["journal:journal"][k] != unicodedata.normalize(
                            "NFC", custom_b["journal:journal"][k]
                        ):
                            same = False
                    else:
                        same = False
                        custom_a["journal:journal"][k] = None
            if not same:
                custom_diff["A"]["journal:journal"] = custom_a["journal:journal"]
                custom_diff["B"]["journal:journal"] = custom_b["journal:journal"]

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
    result = _normalize_punctuation(mystring)
    assert isinstance(result, str), "normalize_string expects string input/output"
    mystring = result
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
    result = _normalize_punctuation(mystring)
    assert isinstance(result, str), "normalize_string_lowercase expects string input/output"
    mystring = result
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


def _normalize_punctuation(mystring: str | list | dict) -> str | list[str] | dict[str, Any]:
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
        return [_normalize_punctuation(i) for i in mystring]  # type: ignore[misc]
    elif isinstance(mystring, dict):
        return {k: _normalize_punctuation(v) for k, v in mystring.items()}
    else:
        return mystring


def _clean_backslashes_and_spaces(mystring: str) -> str:
    """Remove unwanted characters from a string and return it.

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
    """Update a nested dictionary's values."""
    for key, value in update.items():
        if isinstance(value, dict):
            original[key] = update_nested_dict(original.get(key, {}), value)
        elif isinstance(value, list):
            original.setdefault(key, []).extend(value)
        else:
            original[key] = value
    return original


def replace_value_in_nested_dict(d: dict, path: str, new_value: Any) -> dict | bool:
    """Replace a in a nested dictionary based on a bar-separated path string.

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
    :param path: The bar-separated path string to the value.
    :param new_value: The new value to set.

    returns: dict: The updated dictionary.
    """
    keys = path.split("|")
    current = d
    for i, key in enumerate(keys):
        if i == len(keys) - 1:  # If this is the last key
            if key.isdigit() and isinstance(current, list):  # Handle list index
                current[int(key)] = new_value
            else:  # Handle dictionary key
                current[key] = new_value
        else:
            if key.isdigit():  # Next level is a list
                key_int = int(key)  # Convert to integer for list access
                if not isinstance(current, list) or key_int >= len(current):
                    # If current is not a list or index is out of bounds
                    return False
                current = current[key_int]
            else:  # Next level is a dictionary
                if isinstance(current, dict) and key not in current:
                    # Add new dictionary or list at this level to hold deeper keys
                    if keys[i + 1].isdigit():
                        current[key] = []
                    else:
                        current[key] = {}
                elif not isinstance(current[key], (dict, list)):
                    # If key not found or next level is not a dict/list
                    return False
                current = current[key]
    return d
