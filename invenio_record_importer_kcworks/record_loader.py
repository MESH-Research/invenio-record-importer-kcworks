import arrow
from halo import Halo
from flask import current_app as app
from invenio_access.permissions import system_identity

from invenio_access.utils import get_identity
from invenio_accounts import current_accounts
from invenio_accounts.errors import AlreadyLinkedError
from invenio_accounts.models import User
from invenio_db import db
from invenio_oauthclient.models import UserIdentity
from invenio_pidstore.errors import PIDUnregistered, PIDDoesNotExistError
from invenio_rdm_records.records.api import RDMRecord
from invenio_rdm_records.proxies import (
    current_rdm_records,
    current_rdm_records_service as records_service,
)
from invenio_rdm_records.services.errors import (
    ReviewNotFoundError,
)
from invenio_records.systemfields.relations.errors import InvalidRelationValue
from invenio_record_importer_kcworks.errors import (
    DraftDeletionFailedError,
    ExistingRecordNotUpdatedError,
    PublicationValidationError,
    SkipRecord,
    UpdateValidationError,
)
from invenio_record_importer_kcworks.services.communities import (
    CommunitiesHelper,
)
from invenio_record_importer_kcworks.services.files import FilesHelper
from invenio_record_importer_kcworks.services.stats.stats import (
    StatsFabricator,
    AggregationFabricator,
)
from invenio_search.proxies import current_search_client
import itertools
import json
from simplejson.errors import JSONDecodeError as SimpleJSONDecodeError
import jsonlines
from marshmallow.exceptions import ValidationError
from pathlib import Path
import requests
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
from sqlalchemy.orm.exc import NoResultFound, StaleDataError
from traceback import print_exc
from typing import Optional, Union
from pprint import pformat

from invenio_record_importer_kcworks.utils.file_utils import sanitize_filenames
from invenio_record_importer_kcworks.utils.utils import (
    CommunityRecordHelper,
    UsersHelper,
    replace_value_in_nested_dict,
    compare_metadata,
)


def _coerce_types(metadata: dict) -> dict:
    """
    Coerce metadata values to the correct types.

    This is necessary for integer fields, since the metadata
    is stored as a JSON string and so all values are strings.
    """
    # FIXME: Replace this with a proper loader
    if metadata["custom_fields"].get("hclegacy:total_downloads"):
        metadata["custom_fields"]["hclegacy:total_downloads"] = int(
            metadata["custom_fields"]["hclegacy:total_downloads"]
        )
    if metadata["custom_fields"].get("hclegacy:total_views"):
        metadata["custom_fields"]["hclegacy:total_views"] = int(
            metadata["custom_fields"]["hclegacy:total_views"]
        )
    return metadata


# TODO: Deprecated; remove
def api_request(
    method: str = "GET",
    endpoint: str = "records",
    server: str = "",
    args: str = "",
    token: str = "",
    params: dict[str, str] = {},
    json_dict: Optional[Union[dict[str, str], list[dict]]] = {},
    file_data: Optional[bytes] = None,
    protocol: str = "",
) -> dict:
    """
    Make an api request and return the response
    """
    if not server:
        server = app.config.get("APP_UI_URL")
    if not token:
        token = app.config.get("RECORD_IMPORTER_API_TOKEN")
    if not protocol:
        protocol = app.config.get("RECORD_IMPORTER_PROTOCOL", "http")

    payload_args = {}

    api_url = f"{protocol}://{server}/api/{endpoint}"
    if args:
        api_url = f"{api_url}/{args}"

    callfuncs = {
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

    # files = {'file': ('report.xls', open('report.xls', 'rb'),
    # 'application/vnd.ms-excel', {'Expires': '0'})}
    app.logger.debug(f"{method} request to {api_url}")
    # print(f'headers: {headers}')
    app.logger.debug(f"params: {params}")
    app.logger.debug(f"payload_args: {payload_args}")
    response = callfunc(
        api_url, headers=headers, params=params, **payload_args, verify=False
    )

    try:
        json_response = response.json() if method != "DELETE" else None
    except (
        SimpleJSONDecodeError,
        RequestsJSONDecodeError,
        json.decoder.JSONDecodeError,
    ):
        app.logger.error("url for API request:")
        app.logger.error(api_url)
        app.logger.error("response status code:")
        app.logger.error(response.status_code)
        if params:
            app.logger.error("url parameters:")
            app.logger.error(params)
        if payload_args:
            app.logger.error("payload arguments sent:")
            app.logger.error(payload_args)
        app.logger.error(response.text)
        raise requests.HTTPError(
            f"Failed to decode JSON response from API request to {api_url}"
        )

    result_dict = {
        "status_code": response.status_code,
        "headers": response.headers,
        "json": json_response,
        "text": response.text,
    }

    if json_response and "errors" in json_response.keys():
        app.logger.error("API request to {api_url} reported errors:")
        app.logger.error(json_response["errors"])
        result_dict["errors"] = json_response["errors"]

    return result_dict


def create_invenio_record(
    metadata: dict,
    no_updates: bool,
) -> dict:
    """
    Create a new Invenio record from the provided dictionary of metadata

    Values provided in the optional `overrides` dictionary will be used
    to update the metadata before creating the record. This is useful
    for correcting values from a data source at import time.

    params:
        metadata (dict): the metadata for the new record
        no_updates (bool): whether to update an existing record with the
            same DOI if it exists
        overrides (dict): optional dictionary of values to update the
            metadata before creating the record

    returns:
        dict: a dictionary containing the status of the record creation
            and the record data. The keys are

            'status': The kind of record operation that produced the new/
                current metadata record. Possible values: 'new_record',
                'updated_draft', 'updated_published',
                'unchanged_existing_draft',
                'unchanged_existing_published'
            'record_data': The metadata record created or updated
                by the operation.
            'recid': The record internal UUID for the created record
    """
    app.logger.debug("~~~~~~~~")
    metadata = _coerce_types(metadata)
    app.logger.debug("metadata for new record:")
    app.logger.debug(pformat(metadata))
    print(f"metadata for new record: {pformat(metadata['custom_fields'])}")

    # Check for existing record with same DOI
    if "pids" in metadata.keys() and "doi" in metadata["pids"].keys():
        my_doi = metadata["pids"]["doi"]["identifier"]
        doi_for_query = my_doi.split("/")
        # TODO: Can we include deleted records here somehow?
        try:
            print(f"searching for existing record with DOI: {my_doi}")
            print(
                f"query: q=f'pids.doi.identifier:{doi_for_query[0]}/"
                f"{doi_for_query[1]}'"
            )

            same_doi = current_search_client.search(
                index="kcworks-rdmrecords",
                q=f'pids.doi.identifier:"{doi_for_query[0]}/'
                f'{doi_for_query[1]}"',
            )
            # same_doi = records_service.search_drafts(
            #     system_identity,
            #     q=f"pids.doi.identifier:{doi_for_query[0]}/"
            #     f"{doi_for_query[1]}",
            # )
            app.logger.debug(f"same_doi: {pformat(same_doi)}")
        except Exception as e:
            app.logger.error(
                "    error checking for existing record with same DOI:"
            )
            raise e
        if same_doi["hits"]["total"]["value"] > 0:
            draft_recs = []
            published_recs = []
            rec_ids = [r["_source"]["id"] for r in same_doi["hits"]["hits"]]
            for rec_id in rec_ids:
                try:
                    published_recs.append(
                        records_service.read(
                            system_identity, id_=rec_id
                        ).to_dict()
                    )
                except PIDUnregistered:
                    draft_recs.append(
                        records_service.read_draft(
                            system_identity, id_=rec_id
                        ).to_dict()
                    )
            recs = published_recs
            for k, v in draft_recs.items():
                if k not in recs.keys() or v != recs[k]:
                    recs.append({k: v})

            app.logger.info(
                f"    found {same_doi['hits']['total']['value']} existing"
                " records with same DOI..."
            )
            assert len(recs) == same_doi["hits"]["total"]["value"]
            # delete extra records with the same doi
            if len(recs) > 1:
                rec_list = [(r["id"], r["status"]) for r in recs]
                app.logger.info(
                    "    found more than one existing record with same DOI:"
                    f" {rec_list}"
                )
                app.logger.info("   deleting extra records...")
                for i in [r["id"] for r in recs[1:]]:
                    try:
                        delete_invenio_record(i)
                    except PIDUnregistered as e:
                        app.logger.error(
                            "    error deleting extra record with same DOI:"
                            f"{i} was unregistered: {str(e)}"
                        )
                        raise DraftDeletionFailedError(
                            f"Draft deletion failed because PID for record "
                            f"{i} was unregistered: {str(e)}"
                        )
                    except Exception as e:
                        app.logger.error(
                            f"    error deleting extra record {i} with "
                            f"same DOI: {str(e)}"
                        )
                        raise DraftDeletionFailedError(
                            f"Draft deletion failed for record {i} with "
                            f"same DOI: {str(e)}"
                        )
            existing_metadata = published_recs[0] or draft_recs[0]
            # app.logger.debug(
            #     f"existing_metadata: {pformat(existing_metadata)}"
            # )
            # Check for differences in metadata
            differences = compare_metadata(existing_metadata, metadata)
            app.logger.debug(f"differences: {differences}")
            if differences:
                app.logger.info(
                    "    existing record with same DOI has different"
                    f" metadata: existing record: {differences['A']}; new"
                    f" record: {differences['B']}"
                )
                if no_updates:
                    raise RuntimeError(
                        "no_updates flag is set, so not updating existing"
                        " record"
                    )
                update_payload = existing_metadata.copy()
                for key, val in differences["B"].items():
                    if key in [
                        "access",
                        "custom_fields",
                        "files",
                        "metadata",
                        "pids",
                    ]:
                        for k2 in val.keys():
                            if val[k2] is None:
                                update_payload.setdefault(key, {}).pop(k2)
                            else:
                                update_payload.setdefault(key, {})[k2] = (
                                    metadata[key][k2]
                                )
                app.logger.info(
                    "    updating existing record with new metadata..."
                )
                new_comparison = compare_metadata(
                    existing_metadata, update_payload
                )
                if new_comparison:
                    app.logger.debug(
                        f"existing record: {pformat(new_comparison['A'])}"
                        "new record:"
                        f" {pformat(new_comparison['B'])}"
                    )
                    raise ExistingRecordNotUpdatedError(
                        "    metadata still does not match migration source"
                        " after update attempt..."
                    )
                else:
                    update_payload = {
                        k: v
                        for k, v in update_payload.items()
                        if k
                        in [
                            "access",
                            "custom_fields",
                            "files",
                            "metadata",
                            "pids",
                        ]
                    }
                    # TODO: Check whether this is the right way to update
                    if existing_metadata["files"].get("enabled") and (
                        len(existing_metadata["files"]["entries"].keys()) > 0
                    ):
                        app.logger.info(
                            "    existing record has files attached..."
                        )
                        update_payload["files"] = existing_metadata["files"]
                    # Invenio validator will reject other rights metadata
                    # values from existing records
                    if existing_metadata["metadata"].get("rights"):
                        existing_metadata["metadata"]["rights"] = [
                            {"id": r["id"]}
                            for r in existing_metadata["metadata"]["rights"]
                        ]
                    app.logger.info(
                        "    metadata updated to match migration source..."
                    )
                    # if existing_metadata["status"] != "published":
                    try:
                        result = records_service.update_draft(
                            system_identity,
                            id_=existing_metadata,
                            data=update_payload,
                        )
                        app.logger.info(
                            "    continuing with existing draft record"
                            " (new metadata)..."
                        )
                        app.logger.debug(pformat(result))
                        return {
                            "status": "updated_draft",
                            "record_data": result.to_dict(),
                            "recid": result._record.id,
                        }
                    # else:
                    except PIDDoesNotExistError:
                        # TODO: What is status here???
                        app.logger.info(
                            "    creating new draft of published record"
                            " or recovering unsaved draft..."
                        )
                        # app.logger.debug(pprint(existing_metadata))
                        # rec = records_service.read(
                        #     system_identity, id_=existing_metadata["id"]
                        # )
                        create_draft_result = records_service.edit(
                            system_identity, id_=existing_metadata["id"]
                        )
                        app.logger.info(
                            "    updating new draft of published record"
                            " with new metadata..."
                        )
                        result = records_service.update_draft(
                            system_identity,
                            id_=create_draft_result.id,
                            data=update_payload,
                        )
                        result = records_service.update_draft(
                            system_identity,
                            id_=create_draft_result.id,
                            data=update_payload,
                        )
                        if result.to_dict().get("errors"):
                            # NOTE: some validation errors don't prevent
                            # the update and aren't indicative of actual
                            # problems
                            errors = [
                                e
                                for e in result.to_dict()["errors"]
                                if e.get("field") != "metadata.rights.0.icon"
                                and e.get("messages") != ["Unknown field."]
                                and "Missing uploaded files"
                                not in e.get("messages")[0]
                            ]
                            if errors:
                                raise UpdateValidationError(
                                    f"Validation error when trying to update "
                                    f"existing record: {pformat(errors)}"
                                )
                        app.logger.info(
                            f"updated new draft of published: "
                            f"{pformat(result.to_dict())}"
                        )
                        app.logger.debug(
                            f"****title: "
                            f"{result.to_dict()['metadata'].get('title')}"
                        )
                        return {
                            "status": "updated_published",
                            "record_data": result.to_dict(),
                            "recid": result._record.id,
                        }

            if not differences:
                record_type = (
                    "draft"
                    if existing_metadata["status"] != "published"
                    else "published"
                )
                app.logger.info(
                    f"    continuing with existing {record_type} record "
                    "(same metadata)..."
                )
                existing_record_hit = records_service.search_drafts(
                    system_identity,
                    q=f"id:{existing_metadata['id']}",
                )._results[0]
                result = {
                    "record_data": existing_metadata,
                    "status": f"unchanged_existing_{record_type}",
                    "recid": existing_record_hit.to_dict()["uuid"],
                }
                app.logger.debug(
                    f"metadata for existing record: {pformat(result)}"
                )
                return result

    # Make draft and publish
    app.logger.info("    creating new draft record...")
    # app.logger.debug(pformat(metadata.get("access")))
    try:
        result = records_service.create(system_identity, data=metadata)
    except InvalidRelationValue as e:
        raise PublicationValidationError(
            f"Validation error while creating new record: {str(e)}"
        )
    result_recid = result._record.id
    app.logger.debug(f"    new draft record recid: {result_recid}")
    app.logger.debug(f"    new draft record: {pformat(result.to_dict())}")
    print(f"    new draft record: {pformat(result.to_dict())}")

    return {
        "status": "new_record",
        "record_data": result.to_dict(),
        "recid": result_recid,
    }


def delete_invenio_record(record_id: str) -> bool:
    """
    Delete a draft Invenio record with the provided Id

    Since drafts cannot be deleted if they have an associated review request,
    this function first deletes any existing review request for the draft
    record.

    Note: This function only works for draft (unpublished) records.

    :param str record_id:   The id string for the Invenio draft record

    :returns: True if the record was deleted, False otherwise
    """
    result = None
    app.logger.info(f"    deleting draft record {record_id}...")

    try:
        reviews = records_service.review.read(system_identity, id_=record_id)
        if reviews:
            # FIXME: What if there are multiple reviews?
            app.logger.debug(
                f"    deleting review request for draft record {record_id}..."
            )
            records_service.review.delete(system_identity, id_=record_id)
    except ReviewNotFoundError:
        app.logger.info(
            f"    no review requests found for draft record {record_id}..."
        )

    try:  # In case the record is actually published
        result = records_service.delete_record(
            system_identity,
            id_=record_id,
            data={"note": "duplicate record for same DOI"},
        )
    except PIDUnregistered:  # this draft not published
        try:  # no published version exists, so unregistered DOI can be deleted
            result = records_service.delete_draft(
                system_identity, id_=record_id
            )
        # TODO: if published version exists (so DOI registered) or DOI
        # is reserved, the draft can't be manually deleted (involves deleting
        # DOI from PID registry). We let the draft be cleaned up by the
        # system after a period of time.
        except ValidationError as e:
            if (
                "Cannot discard a reserved or registered persistent identifier"
                in str(e)
            ):
                app.logger.warning(
                    f"Cannot delete draft record {record_id} "
                    "immediately because its DOI is reserved "
                    "or registered. It will be left for later "
                    "cleanup."
                )
            else:
                raise e

    return result


def create_invenio_user(
    user_email: str,
    source_username: str = "",
    full_name: str = "",
    record_source: str = "",
    community_owner: list = [],
) -> dict:
    """
    Create a new user account in the Invenio instance

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

    Returns
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
    if not idps or record_source not in idps.keys():
        raise RuntimeError(
            f"record_source {record_source} not found in SSO_SAML_IDPS"
        )

    if source_username and record_source and not user_email:
        user_email = UsersHelper.get_user_by_source_id(
            source_username, record_source
        ).get("email")

    if not user_email:
        user_email = app.config.get("RECORD_IMPORTER_ADMIN_EMAIL")
        source_username = None
        app.logger.warning(
            "No email address provided in source cata for uploader of "
            f"record ({source_username} from {record_source}). Using "
            "default admin account as owner."
        )

    existing_user = current_accounts.datastore.get_user_by_email(user_email)
    if existing_user:
        app.logger.info(f"    found existing user {existing_user.id}...")
        new_user_flag = False
        active_user = existing_user
    else:
        # FIXME: make proper password here
        app.logger.debug(f"creating new user for email {user_email}...")
        profile = {} if not full_name else {"full_name": full_name}
        new_user = current_accounts.datastore.create_user(
            email=user_email,
            # password=generate_password(16),
            active=True,
            confirmed_at=arrow.utcnow().datetime,
            user_profile=profile,
            username=f"{record_source}-{source_username}",
        )
        current_accounts.datastore.commit()
        assert new_user.id
        app.logger.info(f"    created new user {user_email}...")

        if not new_user.active:
            assert current_accounts.datastore.activate_user(new_user)
            current_accounts.datastore.commit()

        user_confirmed = current_accounts.datastore.get_user_by_email(
            user_email
        )
        if user_confirmed:
            user_id = user_confirmed.id
            new_user_flag = True
            app.logger.info(f"    confirmed new user, id {user_id}...")
        else:
            app.logger.error(f"    failed to create user {user_email}...")
            print_exc()
        active_user = user_confirmed
    if full_name:
        active_user.user_profile.full_name = full_name
        current_accounts.datastore.commit()
    if record_source and source_username:
        existing_saml = UserIdentity.query.filter_by(
            id_user=active_user.id,
            method=record_source,
            id=source_username,
        ).one_or_none()

        if not existing_saml:
            try:
                UserIdentity.create(
                    active_user, record_source, source_username
                )
                db.session.commit()
                app.logger.info(
                    f"    configured SAML login for {user_email} as"
                    f" {source_username} on {record_source}..."
                )
                assert UserIdentity.query.filter_by(
                    id_user=active_user.id,
                    method=record_source,
                    id=source_username,
                ).one_or_none()

                app.logger.info(active_user.external_identifiers)
                assert any(
                    [
                        a
                        for a in active_user.external_identifiers
                        if a.method == record_source
                        and a.id == source_username
                        and a.id_user == active_user.id
                    ]
                )
            except AlreadyLinkedError as e:
                if source_username in str(e):
                    app.logger.warning(
                        f"    SAML login already configured for"
                        f" {source_username} on {record_source}..."
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
        communities_owned.append(
            CommunityRecordHelper.add_owner(c, active_user.id)
        )

    return {
        "user": active_user,
        "new_user": new_user_flag,
        "communities_owned": communities_owned,
    }


def change_record_ownership(
    record_id: str,
    new_owner: User,
) -> dict:
    """
    Change the owner of the specified record to a new user.
    """
    app.logger.debug(f"Changing ownership of record {record_id}")

    record = records_service.read(
        id_=record_id, identity=system_identity
    )._record

    parent = record.parent
    parent.access.owned_by = new_owner
    parent.commit()
    db.session.commit()

    if records_service.indexer:
        records_service.indexer.index(record)
    result = records_service.read(
        id_=record_id, identity=system_identity
    )._record

    return result.parent.access.owned_by


def assign_record_ownership(
    draft_id: str,
    core_data: dict,
    record_source: str,
    existing_record: Optional[dict] = None,
):
    # Create/find the necessary user account
    app.logger.info("    creating or finding the user (submitter)...")
    # TODO: Make sure this will be the same email used for SAML login
    new_owner_email = core_data["custom_fields"].get("kcr:submitter_email")
    new_owner_username = core_data["custom_fields"].get(
        "kcr:submitter_username"
    )
    if not new_owner_email and not new_owner_username:
        app.logger.warning(
            "    no submitter email or username found in source metadata. "
            "Assigning ownership to configured admin user..."
        )
        # admin = UsersHelper.get_admins()[0]
        new_owner_email = app.config["RECORD_IMPORTER_ADMIN_EMAIL"]
        new_owner_username = None
    full_name = ""
    for c in [
        *core_data["metadata"].get("creators", []),
        *core_data["metadata"].get("contributors", []),
    ]:
        for i in c["person_or_org"].get("identifiers", []):
            if i["scheme"] == "hc_username":
                full_name = c["person_or_org"]["name"]
    existing_user = current_accounts.datastore.get_user_by_email(
        new_owner_email
    )
    if not existing_user:
        # handle case where user has multiple emails
        try:
            existing_user = current_accounts.datastore.find_user(
                username=f"{record_source.lower()}-{new_owner_username}",
            )
            app.logger.warning(
                f"    finding existing user {new_owner_username} "
                f"({new_owner_email})...{existing_user}"
            )
            print(
                f"    finding existing user {new_owner_username} "
                f"({new_owner_email})...{existing_user}"
            )
            assert existing_user
            idp_slug = (
                "kc" if record_source == "knowledgeCommons" else record_source
            )
            existing_user.user_profile[f"identifier_{idp_slug}_username"] = (
                new_owner_username
            )
            existing_user.user_profile["identifier_email"] = new_owner_email
            current_accounts.datastore.commit()
        except (NoResultFound, AssertionError):
            pass
    if existing_user:
        new_owner = existing_user
        app.logger.debug(
            f"    assigning ownership to existing user: "
            f"{pformat(existing_user)} {existing_user.email}"
        )
    else:
        new_owner_result = create_invenio_user(
            new_owner_email, new_owner_username, full_name, record_source
        )
        new_owner = new_owner_result["user"]
        app.logger.info(f"    new user created: {pformat(new_owner)}")

    # if existing_record:
    #     app.logger.debug("existing record data")
    # app.logger.debug(
    #     existing_record["custom_fields"]["kcr:submitter_email"]
    # )
    # app.logger.debug(existing_record["parent"])
    if (
        existing_record
        and existing_record["custom_fields"].get("kcr:submitter_email")
        == new_owner_email
        and existing_record["parent"]["access"].get("owned_by")
        and str(existing_record["parent"]["access"]["owned_by"]["user"])
        == str(new_owner.id)
    ):
        app.logger.info("    skipping re-assigning ownership of the record ")
        app.logger.info(
            f"    (already belongs to {new_owner_email}, "
            f"user {new_owner.id})..."
        )
    else:
        # Change the ownership of the record
        app.logger.info(
            "    re-assigning ownership of the record to the "
            f"submitter ({new_owner_email}, "
            f"{new_owner.id})..."
        )
        changed_ownership = change_record_ownership(draft_id, new_owner)
        # Remember: changed_ownership is an Owner systemfield object,
        # not User
        assert changed_ownership.owner_id == new_owner.id
    return new_owner


def import_record_to_invenio(
    import_data: dict,
    no_updates: bool = False,
    record_source: Optional[str] = None,
    overrides: dict = {},
) -> dict:
    """
    Create an invenio record with file uploads, ownership, communities.

    Parameters
    ----------
    import_data : dict
        The data to import into Invenio. This should be a dictionary
        with the following keys:
        - custom_fields: a dictionary of custom metadata fields
        - metadata: a dictionary of standard metadata fields
        - pids: a dictionary of PID values
        - files: a dictionary of file uploads
    no_updates : bool
        If True, do not update existing records
    record_source : str
        The name of the source service for the record
    overrides : dict
        A dictionary of metadata fields to override in the import data
        if manual corrections are necessary

    Returns
    -------
    dict
        A dictionary with the results of the import. It has the following
        keys:
        - community: the community data dictionary for the record's
            primary community
        - metadata_record_created: the metadata record creation result.
            This is not just the metadata record, but the dictionary
            returned by the create_invenio_record function. It contains
            the following keys:
            - record_data: the metadata record
            - record_uuid: the UUID of the metadata record
            - status: the status of the metadata record
        - uploaded_files: the file upload results
        - community_review_accepted: the community review acceptance result
        - assigned_ownership: the record ownership assignment result
        - added_to_collections: the group collection addition
    """
    existing_record = None
    result = {}

    for key, val in overrides.items():
        app.logger.debug(f"updating metadata key {key} with value {val}")
        import_data = replace_value_in_nested_dict(import_data, key, val)

    file_data = import_data["files"]

    # Build the initial metadata to be submitted
    submitted_data = {
        "custom_fields": import_data["custom_fields"],
        "metadata": import_data["metadata"],
        "pids": import_data["pids"],
    }

    submitted_data["access"] = {"records": "public", "files": "public"}
    if len(file_data["entries"]) > 0:
        submitted_data["files"] = {"enabled": True}
    else:
        submitted_data["files"] = {"enabled": False}

    # Create/find the necessary domain communities
    app.logger.info("    finding or creating community...")
    if (
        "kcr:commons_domain" in import_data["custom_fields"].keys()
        and import_data["custom_fields"]["kcr:commons_domain"]
    ):
        # FIXME: allow for drawing community labels from other fields
        # for other data sources
        result["community"] = CommunitiesHelper().prepare_invenio_community(
            record_source, import_data["custom_fields"]["kcr:commons_domain"]
        )
        community_id = result["community"]["id"]

    # Create the basic metadata record
    app.logger.info("    finding or creating draft metadata record...")
    record_created = create_invenio_record(import_data, no_updates)
    result["metadata_record_created"] = record_created
    result["status"] = record_created["status"]
    app.logger.info(f"    record status: {record_created['status']}")
    # draft_uuid = record_created["recid"]
    if record_created["status"] in [
        "updated_published",
        "updated_draft",
        "unchanged_existing_draft",
        "unchanged_existing_published",
    ]:
        existing_record = result["existing_record"] = record_created[
            "record_data"
        ]
    is_draft = (
        False
        if existing_record
        and existing_record["is_published"]
        and not existing_record["is_draft"]
        else True
    )
    metadata_record = record_created["record_data"]
    draft_id = metadata_record["id"]
    app.logger.info(f"    metadata record id: {draft_id}")

    print(
        f"import_record_to_invenio metadata_record: {pformat(metadata_record)}"
    )

    # Upload the files
    if len(import_data["files"]["entries"]) > 0:
        app.logger.info("    uploading files for draft...")
        result["uploaded_files"] = FilesHelper(
            is_draft=is_draft
        ).handle_record_files(
            metadata_record,
            file_data,
            existing_record=existing_record,
        )
    else:
        assert metadata_record["files"]["enabled"] is False

    # Attach the record to the communities
    result[
        "community_review_accepted"
    ] = CommunitiesHelper().publish_record_to_community(
        draft_id,
        community_id,
    )
    # Publishing the record happens during community acceptance

    # Assign ownership of the record
    result["assigned_ownership"] = assign_record_ownership(
        draft_id, import_data, record_source, existing_record=existing_record
    )

    # Add the record to the appropriate group collections
    result[
        "added_to_collections"
    ] = CommunitiesHelper().add_record_to_group_collections(
        metadata_record, record_source
    )

    # Create fictural usage events to generate correct usage stats
    events = StatsFabricator().create_stats_events(
        draft_id,
        downloads_field="custom_fields.hclegacy:total_downloads",
        views_field="custom_fields.hclegacy:total_views",
        date_field="metadata.publication_date",
        views_count=import_data["custom_fields"].get("hclegacy:total_views"),
        downloads_count=import_data["custom_fields"].get(
            "hclegacy:total_downloads"
        ),
        publication_date=import_data["metadata"].get("publication_date"),
        eager=True,
        verbose=True,
    )
    for e in events:
        app.logger.debug(f"    created {e[1][0]} usage events ({e[0]})...")
        # app.logger.debug(pformat(events))

    return result


def _log_created_record(
    index: int = 0,
    invenio_id: str = "",
    invenio_recid: str = "",
    commons_id: str = "",
    core_record_id: str = "",
    created_records: list = [],
) -> list:
    """
    Log a created record to the created records log file.

    This does not update the log file if the record has already been created.
    If the record does not appear in the log file, it is added at the end.

    :param index: the index of the record in the source file
    :param invenio_id: the doi of the record in Invenio
    :param invenio_recid: the recid of the record in Invenio
    :param commons_id: the user-facing id of the record in the source system
    :param core_record_id: the Fedora system id of the record in the
        source database
    :param created_records: the list of previously created records in this
        run

    :returns: the updated list of created records
    """
    created_log_path = app.config["RECORD_IMPORTER_CREATED_LOG_PATH"]
    created_rec = {
        "index": index,
        "invenio_id": invenio_id,
        "invenio_recid": invenio_recid,
        "commons_id": commons_id,
        "core_record_id": core_record_id,
        "timestamp": arrow.now().format(),
    }
    existing_lines = [
        (idx, t)
        for idx, t in enumerate(created_records)
        if t["commons_id"] == commons_id and t["invenio_id"] == invenio_id
    ]
    if not existing_lines:
        created_records.append(created_rec)

        with jsonlines.open(
            created_log_path,
            "a",
        ) as created_writer:
            created_writer.write(created_rec)
    elif (
        existing_lines
        and existing_lines[0][1]["invenio_recid"] != invenio_recid
    ):
        i = existing_lines[0][0]
        created_records = [
            *created_records[:i],
            *created_records[i + 1 :],  # noqa: E203
            created_rec,
        ]
        with jsonlines.open(
            created_log_path,
            "w",
        ) as created_writer:
            for t in created_records:
                created_writer.write(t)

    return created_records


def _log_failed_record(
    index=-1,
    invenio_id=None,
    commons_id=None,
    core_record_id=None,
    failed_records=None,
    residual_failed_records=None,
    reason=None,
    skipped_records=None,
) -> None:
    """
    Log a failed record to the failed records log file.
    """
    failed_log_path = Path(app.config["RECORD_IMPORTER_FAILED_LOG_PATH"])

    failed_obj = {
        "index": index,
        "invenio_id": invenio_id,
        "commons_id": commons_id,
        "core_record_id": core_record_id,
        "reason": reason,
        "datestamp": arrow.now().format(),
    }
    if index > -1:
        failed_records.append(failed_obj)
    skipped_ids = []
    if len(skipped_records) > 0:
        skipped_ids = [r["commons_id"] for r in skipped_records if r]
    with jsonlines.open(
        failed_log_path,
        "w",
    ) as failed_writer:
        total_failed = [
            r for r in failed_records if r["commons_id"] not in skipped_ids
        ]
        failed_ids = [r["commons_id"] for r in failed_records if r]
        for e in residual_failed_records:
            if e["commons_id"] not in failed_ids and e not in total_failed:
                total_failed.append(e)
        ordered_failed_records = sorted(total_failed, key=lambda r: r["index"])
        for o in ordered_failed_records:
            failed_writer.write(o)

    return failed_records, residual_failed_records


def _load_prior_failed_records() -> tuple[list, list, list, list]:
    failed_log_path = Path(app.config["RECORD_IMPORTER_FAILED_LOG_PATH"])
    existing_failed_records = []
    try:
        with jsonlines.open(
            failed_log_path,
            "r",
        ) as reader:
            existing_failed_records = [obj for obj in reader]
    except FileNotFoundError:
        app.logger.info("**no existing failed records log file found...**")
    existing_failed_indices = [r["index"] for r in existing_failed_records]
    existing_failed_hcids = [r["commons_id"] for r in existing_failed_records]
    residual_failed_records = [*existing_failed_records]

    return (
        existing_failed_records,
        residual_failed_records,
        existing_failed_indices,
        existing_failed_hcids,
    )


def load_records_into_invenio(
    start_index: int = 1,
    stop_index: int = -1,
    nonconsecutive: list = [],
    no_updates: bool = False,
    use_sourceids: bool = False,
    sourceid_scheme: str = "hclegacy-pid",
    retry_failed: bool = False,
    aggregate: bool = False,
    start_date: str = "",
    end_date: str = "",
    clean_filenames: bool = False,
    verbose: bool = False,
    stop_on_error: bool = False,
) -> None:
    """
    Create new InvenioRDM records and upload files for serialized deposits.

    params:
        start_index (int): the starting index of the records to load in the
            source jsonl file
        stop_index (int): the stopping index of the records to load in the
            source jsonl file (inclusive)
        nonconsecutive (list): a list of nonconsecutive indices to load in the
            source jsonl file
        no_updates (bool): whether to update existing records
        use_sourceids (bool): whether to use ids from the record source's id
            system for identification of records to load
        sourceid_scheme (str): the scheme to use for the source ids if records
            are identified by source ids
        retry_failed (bool): whether to retry failed records from a prior run
        aggregate (bool): whether to aggregate usage stats for the records
            after loading. This may take a long time.
        start_date (str): the starting date of usage events to aggregate if
            aggregate is True
        end_date (str): the ending date of usage events to aggregate if
            aggregate is True
        verbose (bool): whether to print and log verbose output during the
            loading process
        stop_on_error (bool): whether to stop the loading process if an error
            is encountered

    returns:
        None
    """
    record_counter = 0
    failed_records = []
    created_records = []
    skipped_records = []
    successful_records = 0
    updated_drafts = 0
    updated_published = 0
    unchanged_existing = 0
    new_records = 0
    repaired_failed = []
    range_args = [start_index - 1]
    if stop_index > -1 and stop_index >= start_index:
        range_args.append(stop_index)
    else:
        range_args.append(start_index)

    metadata_overrides_folder = Path(
        app.config["RECORD_IMPORTER_OVERRIDES_FOLDER"]
    )

    created_log_path = Path(
        app.config.get(
            "RECORD_IMPORTER_CREATED_LOG_PATH",
            "record_importer_created_records.jsonl",
        )
    )

    # sanitize the names of files before upload to avoid
    # issues with special characters
    if clean_filenames:
        app.logger.info("Sanitizing file names...")
        sanitize_filenames(app.config["RECORD_IMPORTER_FILES_LOCATION"])

    # Load list of previously created records
    created_records = []
    try:
        with jsonlines.open(
            created_log_path,
            "r",
        ) as reader:
            created_records = [obj for obj in reader]
    except FileNotFoundError:
        app.logger.info("**no existing created records log file found...**")

    # Load list of failed records from prior runs
    (
        existing_failed_records,
        residual_failed_records,
        existing_failed_indices,
        existing_failed_hcids,
    ) = _load_prior_failed_records()

    app.logger.info("Starting to load records into Invenio...")
    if no_updates:
        app.logger.info(
            "    **no-updates flag is set, so skipping updating existing"
            " records...**"
        )
    if not nonconsecutive:
        stop_string = "" if stop_index == -1 else f" to {stop_index}"
        app.logger.info(
            f"Loading records from {str(start_index) + stop_string}..."
        )
    else:
        id_type = (
            "source record id" if use_sourceids else "index in import file"
        )
        app.logger.info(
            f"Loading records {' '.join([str(s) for s in nonconsecutive])} "
            f"(by {id_type})..."
        )

    app.logger.info(
        f"Loading records from serialized data: "
        f"{app.config.get('RECORD_IMPORTER_SERIALIZED_PATH')}..."
    )
    with jsonlines.open(
        Path(app.config.get("RECORD_IMPORTER_SERIALIZED_PATH")), "r"
    ) as json_source:
        # decide how to determine the record set
        if retry_failed:
            if no_updates:
                print("Cannot retry failed records with no-updates flag set.")
                app.logger.error(
                    "Cannot retry failed records with no-updates flag set."
                )
                return
            if not existing_failed_records:
                print("No previously failed records to retry.")
                app.logger.info("No previously failed records to retry.")
                return
            line_num = 1
            record_set = []
            for j in json_source:
                if line_num in existing_failed_indices:
                    j["jsonl_index"] = line_num
                    record_set.append(j)
                line_num += 1
        elif nonconsecutive:
            record_set = []
            if use_sourceids:
                for j in json_source:
                    if [
                        i["identifier"]
                        for i in j["metadata"]["identifiers"]
                        if i["identifier"] in nonconsecutive
                        and i["scheme"] == sourceid_scheme
                    ]:
                        record_set.append(j)
            else:
                line_num = 1
                for j in json_source:
                    if line_num in nonconsecutive:
                        j["jsonl_index"] = line_num
                        record_set.append(j)
                    line_num += 1
        else:
            record_set = list(itertools.islice(json_source, *range_args))

        if len(record_set) == 0:
            print("No records found to load.")
            app.logger.info("No records found to load.")
            return

        for rec in record_set:
            record_source = rec.pop("record_source")
            # get metadata overrides for the record (for manual fixing
            # of inport data after serialization)
            overrides = {}
            skip = False  # allow skipping records in the source file
            with jsonlines.open(
                metadata_overrides_folder
                / f"record-importer-overrides_{record_source}.jsonl",
                "r",
            ) as override_reader:
                for o in override_reader:
                    if o["source_id"] in [
                        i["identifier"]
                        for i in rec["metadata"]["identifiers"]
                        if i["scheme"] == "hclegacy-pid"
                    ]:
                        overrides = o.get("overrides")
                        skip = (
                            True
                            if o.get("skip") in [True, "True", "true", 1, "1"]
                            else False
                        )
            if "jsonl_index" in rec.keys():
                current_record = rec["jsonl_index"]
            else:
                current_record = start_index + record_counter
            rec_doi = (
                rec["pids"]["doi"]["identifier"]
                if "pids" in rec.keys()
                else ""
            )
            rec_hcid = [
                r
                for r in rec["metadata"]["identifiers"]
                if r["scheme"] == "hclegacy-pid"
            ][0]["identifier"]
            rec_recid = [
                r
                for r in rec["metadata"]["identifiers"]
                if r["scheme"] == "hclegacy-record-id"
            ][0]["identifier"]
            rec_invenioid = None
            app.logger.info(f"....starting to load record {current_record}")
            app.logger.info(
                f"    DOI:{rec_doi} {rec_invenioid} {rec_hcid} {rec_recid}"
                f"{record_source}"
            )
            spinner = Halo(
                text=f"    Loading record {current_record}", spinner="dots"
            )
            spinner.start()
            rec_log_object = {
                "index": current_record,
                "invenio_recid": rec_invenioid,
                "invenio_id": rec_doi,
                "commons_id": rec_hcid,
                "core_record_id": rec_recid,
            }
            try:
                result = {}
                # FIXME: This is a hack to handle StaleDataError which
                # is consistently resolved on a second attempt -- seems
                # to arise when a record is being added to several
                # communities at once
                if skip:
                    skipped_records.append(rec_log_object)
                    raise SkipRecord(
                        "Record marked for skipping in override file"
                    )
                try:
                    result = import_record_to_invenio(
                        rec, no_updates, record_source, overrides
                    )
                except StaleDataError:
                    result = import_record_to_invenio(
                        rec, no_updates, record_source, overrides
                    )
                created_records = _log_created_record(
                    index=current_record,
                    invenio_id=rec_doi,
                    invenio_recid=result.get("metadata_record_created")
                    .get("record_data")
                    .get("id"),
                    commons_id=rec_hcid,
                    core_record_id=rec_recid,
                    created_records=created_records,
                )
                successful_records += 1
                if not result.get("existing_record"):
                    new_records += 1
                if "unchanged_existing" in result["status"]:
                    unchanged_existing += 1
                if result["status"] == "updated_published":
                    updated_published += 1
                if result["status"] == "updated_draft":
                    updated_drafts += 1
                if rec_hcid in existing_failed_hcids:
                    app.logger.info("    repaired previously failed record...")
                    app.logger.info(f"    {rec_doi} {rec_hcid} {rec_recid}")
                    residual_failed_records = [
                        d
                        for d in residual_failed_records
                        if d["commons_id"] != rec_hcid
                    ]
                    repaired_failed.append(rec_log_object)
                    failed_records, residual_failed_records = (
                        _log_failed_record(
                            failed_records=failed_records,
                            residual_failed_records=residual_failed_records,
                            skipped_records=skipped_records,
                        )
                    )
                app.logger.debug("result status: %s", result.get("status"))
            except Exception as e:
                print("ERROR:", e)
                print_exc()
                app.logger.error(f"ERROR: {e}")
                msg = str(e)
                try:
                    msg = e.messages
                except AttributeError:
                    try:
                        msg = e.messages
                    except AttributeError:
                        pass
                error_reasons = {
                    "CommonsGroupNotFoundError": msg,
                    "CommonsGroupServiceError": msg,
                    "DraftDeletionFailedError": msg,
                    "ExistingRecordNotUpdatedError": msg,
                    "FileKeyNotFoundError": msg,
                    "FailedCreatingUsageEventsError": msg,
                    "FileUploadError": msg,
                    "UploadFileNotFoundError": msg,
                    "InvalidKeyError": msg,
                    "MissingNewUserEmailError": msg,
                    "MissingParentMetadataError": msg,
                    "MultipleActiveCollectionsError": msg,
                    "PublicationValidationError": msg,
                    "RestrictedRecordPublicationError": msg,
                    "StaleDataError": msg,
                    "TooManyViewEventsError": msg,
                    "TooManyDownloadEventsError": msg,
                    "UpdateValidationError": msg,
                }
                log_object = {
                    "index": current_record,
                    "invenio_id": rec_doi,
                    "commons_id": rec_hcid,
                    "core_record_id": rec_recid,
                    "failed_records": failed_records,
                    "residual_failed_records": residual_failed_records,
                }
                if e.__class__.__name__ in error_reasons.keys():
                    log_object.update(
                        {"reason": error_reasons[e.__class__.__name__]}
                    )
                if e.__class__.__name__ != "SkipRecord":
                    failed_records, residual_failed_records = (
                        _log_failed_record(
                            **log_object, skipped_records=skipped_records
                        )
                    )
                if stop_on_error and failed_records:
                    break

            spinner.stop()
            app.logger.info(f"....done with record {current_record}")
            record_counter += 1

    print("Finished!")
    app.logger.info("All done loading records into InvenioRDM")
    set_string = ""
    if nonconsecutive:
        set_string = f"{' '.join([str(n) for n in nonconsecutive])}"
    else:
        target_string = (
            f" to {start_index + record_counter - 1}"
            if record_counter > 1
            else ""
        )
        set_string = f"{start_index}{target_string}"
    message = (
        f"Processed {str(record_counter)} records in InvenioRDM ({set_string})"
        f" \n    {str(successful_records)} successful \n   "
        f" {str(new_records)} new records created \n   "
        f" {str(successful_records - new_records)} already existed \n       "
        f" {str(updated_published)} updated published records \n       "
        f" {str(updated_drafts)} updated existing draft records \n       "
        f" {str(unchanged_existing)} unchanged existing records \n       "
        f" {str(len(repaired_failed))} previously failed records repaired \n "
        f"   {str(len(failed_records))} failed \n"
        f"   {str(len(skipped_records))} records skipped (marked in overrides)"
        f"\n   "
    )
    app.logger.info(message)

    # Aggregate the stats again now
    start_date = (
        start_date
        if start_date
        else arrow.utcnow().shift(days=-1).naive.date().isoformat()
    )
    end_date = (
        end_date
        if end_date
        else arrow.utcnow().shift(days=1).naive.date().isoformat()
    )
    if aggregate:
        aggregations = AggregationFabricator().create_stats_aggregations(
            start_date=arrow.get(start_date).naive,
            end_date=arrow.get(end_date).naive,
            bookmark_override=arrow.get(start_date).naive,
            eager=True,
        )
        app.logger.debug("    created usage aggregations...")
        app.logger.debug(pformat(aggregations))
    else:
        app.logger.warning(
            "    Skipping usage stats aggregation. Usage stats "
            "for the imported records will not be visible "
            "until an aggregation is performed."
        )

    # Report
    if verbose and (
        repaired_failed
        or (existing_failed_records and not residual_failed_records)
    ):
        app.logger.info("Previously failed records repaired:")
        for r in repaired_failed:
            print(r)
            app.logger.info(r)

    # Report and log failed records
    if failed_records:
        if verbose:
            app.logger.info("Failed records:")
            for r in failed_records:
                app.logger.info(r)
        app.logger.info(
            "Failed records written to"
            f" {app.config['RECORD_IMPORTER_FAILED_LOG_PATH']}"
        )


def delete_records_from_invenio(record_ids):
    """
    Delete the selected records from the invenioRDM instance.
    """
    deleted_records = {}
    for record_id in record_ids:
        admin_email = app.config["RECORD_IMPORTER_ADMIN_EMAIL"]
        admin_identity = get_identity(
            current_accounts.datastore.get_user(admin_email)
        )
        service = current_rdm_records.records_service
        record = service.read(id_=record_id, identity=system_identity)._record
        siblings = RDMRecord.get_records_by_parent(record.parent)
        # remove the 0th (latest) version to leave the previous version(s):
        siblings.pop(0)
        # already deleted previous versions will have nothing for metadata
        # (sibling.get('id') will return nothing)
        has_versions = any([sibling.get("id") for sibling in siblings])

        if record.versions.is_latest and has_versions:
            raise Exception(
                "Cannot delete the latest version without first deleting "
                "previous versions"
            )

        deleted = service.delete(id_=record_id, identity=admin_identity)
        deleted_records[record_id] = deleted

    return deleted_records
