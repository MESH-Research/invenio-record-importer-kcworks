from ast import In
import arrow
from halo import Halo
from flask import current_app as app
from invenio_access.permissions import system_identity

# from invenio_access.utils import get_identity
from invenio_accounts import current_accounts
from invenio_accounts.errors import AlreadyLinkedError
from invenio_accounts.models import User
from invenio_communities.proxies import current_communities
from invenio_db import db
from invenio_drafts_resources.services.records.uow import ParentRecordCommitOp
from invenio_files_rest.errors import InvalidKeyError
from regex import P
from invenio_group_collections.errors import (
    CollectionNotFoundError,
    CommonsGroupNotFoundError,
)
from invenio_group_collections.proxies import (
    current_group_collections_service as collections_service,
)
from invenio_oauthclient.models import UserIdentity
from invenio_pidstore.errors import PIDUnregistered, PIDDoesNotExistError
from invenio_rdm_records.proxies import (
    current_rdm_records,
    current_rdm_records_service as records_service,
)
from invenio_rdm_records.services.errors import (
    ReviewNotFoundError,
    ReviewStateError,
    InvalidAccessRestrictions,
)
from invenio_rdm_records.services.tasks import reindex_stats
from invenio_records.systemfields.relations.errors import InvalidRelationValue
from invenio_record_importer.errors import (
    CommonsGroupServiceError,
    DraftDeletionFailedError,
    ExistingRecordNotUpdatedError,
    FailedCreatingUsageEventsError,
    FileUploadError,
    MissingNewUserEmailError,
    MissingParentMetadataError,
    PublicationValidationError,
    RestrictedRecordPublicationError,
    SkipRecord,
    TooManyDownloadEventsError,
    TooManyViewEventsError,
    UpdateValidationError,
    UploadFileNotFoundError,
)
from invenio_record_importer.tasks import aggregate_events
from invenio_records_resources.services.errors import (
    FileKeyNotFoundError,
)
from invenio_records_resources.services.uow import (
    unit_of_work,
    RecordIndexOp,
)
from invenio_requests.proxies import current_requests_service
from invenio_requests.errors import CannotExecuteActionError
from invenio_remote_user_data.service import RemoteUserDataService
from invenio_search import current_search_client
from invenio_search.proxies import current_search
from invenio_stats.contrib.event_builders import (
    build_file_unique_id,
    build_record_unique_id,
)
from invenio_stats.processors import anonymize_user
from invenio_stats.proxies import current_stats
from invenio_stats.tasks import process_events
from invenio_users_resources.proxies import (
    current_users_service as users_service,
)
import itertools
import json
from opensearchpy.exceptions import NotFoundError
from simplejson.errors import JSONDecodeError as SimpleJSONDecodeError
import jsonlines
from marshmallow.exceptions import ValidationError
from pathlib import Path
import requests
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
from sqlalchemy.orm.exc import NoResultFound, StaleDataError
from traceback import format_exception, print_exc
from typing import Optional, Union
from pprint import pformat, pprint
import unicodedata
from urllib.parse import unquote

from invenio_record_importer.queries import (
    view_events_search,
    download_events_search,
)
from invenio_record_importer.utils.utils import (
    CommunityRecordHelper,
    UsersHelper,
    normalize_string,
    update_nested_dict,
    replace_value_in_nested_dict,
    valid_date,
    compare_metadata,
    FilesHelper,
)
from werkzeug.exceptions import UnprocessableEntity


def create_stats_events(
    record_id: str,  # the UUID of the record
    user_id: int = 0,
    visitor_id: int = 0,
    eager=False,
):
    """
    Create artificial statistics events for a migrated record.

    Since Invenio stores statistics as aggregated events in the
    search index, we need to create the individual events that
    will be aggregated. This function creates the individual
    events for a record, simulating views and downloads. It creates
    a given number of events for each type, with the timestamps
    evenly distributed between the record creation date and the
    current date.

    params:
        record_id (str): the record id
        user_id (int): the user id
        visitor_id (int): the visitor id
        eager (bool): whether to process the events immediately or
            queue them for processing in a background task

    """
    rec_search = records_service.read(system_identity, id_=record_id)
    record = rec_search._record

    metadata_record = rec_search.to_dict()
    # FIXME: this all assumes a single file per record on import
    views = metadata_record["custom_fields"]["hclegacy:total_views"]
    downloads = metadata_record["custom_fields"]["hclegacy:total_downloads"]
    record_creation = arrow.get(
        metadata_record["metadata"]["publication_date"]
    )

    files_request = records_service.files.list_files(
        system_identity, record_id
    ).to_dict()
    first_file = files_request["entries"][0]
    file_id = first_file["file_id"]
    file_key = first_file["key"]
    size = first_file["size"]
    bucket_id = first_file["bucket_id"]

    pid = record.pid

    def generate_datetimes(start, n):
        # Use a relatively recent start time to avoid issues with
        # creating too many monthly indices and running out of
        # available open shards
        if start < arrow.get("2019-01-01"):
            start = arrow.get("2019-01-01")
        total_seconds = arrow.utcnow().timestamp() - start.timestamp()
        interval = total_seconds / n
        datetimes = [start.shift(seconds=i * interval) for i in range(n)]

        return datetimes

    # Check for existing view and download events
    # imported events are flagged with country: "imported"
    # this is a hack
    existing_view_events = view_events_search(record_id)
    existing_download_events = download_events_search(file_id)
    app.logger.debug(
        "existing view events: "
        f"{pformat(existing_view_events['hits']['total']['value'])}"
    )

    existing_view_count = existing_view_events["hits"]["total"]["value"]
    if existing_view_count == views:
        app.logger.info(
            "    skipping view events creation. "
            f"{existing_view_count} "
            "view events already exist."
        )
    else:
        if existing_view_count > views:
            raise TooManyViewEventsError(
                "    existing imported view events exceed expected count."
            )
        else:
            # only create enough new view events to reach the expected count
            if existing_view_count > 0:
                views -= existing_view_count
            view_events = []
            for dt in generate_datetimes(record_creation, views):
                doc = {
                    "timestamp": dt.naive.isoformat(),
                    "recid": record_id,
                    "parent_recid": metadata_record["parent"]["id"],
                    "unique_id": f"ui_{pid.pid_value}",
                    "is_robot": False,
                    "user_id": "1",
                    "country": "imported",
                    "via_api": False,
                    # FIXME: above is hack to mark synthetic data
                }
                doc = anonymize_user(doc)
                # we can safely pass the doc through processor.anonymize_user
                # with null/dummy values for user_id, session_id, user_agent,
                # ip_address
                # it adds visitor_id and unique_session_id
                view_events.append(doc)
            current_stats.publish("record-view", view_events)

    existing_download_count = existing_download_events["hits"]["total"][
        "value"
    ]
    if existing_download_count == downloads:
        app.logger.info(
            "    skipping download events creation. "
            f"{existing_download_count} "
            "download events already exist."
        )
    else:
        if existing_download_count > downloads:
            raise TooManyDownloadEventsError(
                "    existing imported download events exceed expected count."
            )
        else:
            # only create enough new download events to reach the expected count
            if existing_download_count > 0:
                downloads -= existing_download_count
            download_events = []
            for dt in generate_datetimes(record_creation, downloads):
                doc = {
                    "timestamp": dt.naive.isoformat(),
                    "bucket_id": str(bucket_id),  # UUID
                    "file_id": str(file_id),  # UUID
                    "file_key": file_key,
                    "size": size,
                    "recid": record_id,
                    "parent_recid": metadata_record["parent"]["id"],
                    "is_robot": False,
                    "user_id": "1",
                    "country": "imported",
                    "unique_id": f"{str(bucket_id)}_{str(file_id)}",
                    "via_api": False,
                }
                doc = anonymize_user(doc)
                # we can safely pass the doc through processor.anonymize_user
                # with null/dummy values for user_id, session_id, user_agent,
                # ip_address
                # it adds visitor_id and unique_session_id
                download_events.append(build_file_unique_id(doc))
            current_stats.publish("file-download", download_events)

    try:
        if eager:
            # process_task.apply(throw=True)
            # events = process_events.delay(
            #     ["record-view", "file-download"]
            # ).get()
            events = process_events(["record-view", "file-download"])
            app.logger.info(
                f"Events processed successfully. {pformat(events)}"
            )
            return events
        else:
            process_task = process_events.si(["record-view", "file-download"])
            process_task.delay()
            app.logger.info("Event processing task sent...")
            return True
    except Exception as e:
        app.logger.error("Error creating usage events:")
        app.logger.error(str(e))
        raise FailedCreatingUsageEventsError(
            "Error creating usage events: {str(e)}"
        )


def create_stats_aggregations(
    start_date: str = None,
    end_date: str = None,
    bookmark_override=None,
    eager=False,
) -> Union[bool, list]:
    """
    Create statistics aggregations for the migrated records.

    This function triggers the creation of statistics aggregations
    for the migrated records. It is intended to be run after all
    the individual statistics events have been created for the
    migrated records.

    params:
        start_date (str): the start date for the aggregations
        end_date (str): the end date for the aggregations
        eager (bool): whether to process the aggregations immediately
            or queue them for processing in a background task

    returns:
        Either bool or list, depending on the value of eager. If eager
        is True, the function returns a list of the aggregations. If
        eager is False, the function returns True.

    """
    # current_search.flush_and_refresh(index="*")

    aggregation_types = list(current_stats.aggregations)
    agg_task = aggregate_events.si(
        aggregation_types,
        start_date=(
            arrow.get(start_date).naive.isoformat() if start_date else None
        ),
        end_date=arrow.get(end_date).naive.isoformat() if end_date else None,
        update_bookmark=True,  # is this right?
        bookmark_override=bookmark_override,
    )
    if eager:
        aggs = agg_task.apply(throw=True)
        app.logger.info("Aggregations processed successfully.")

        # implement invenio_rdm_records.services.tasks.reindex_stats here
        # so that stats show up in records
        # but call current_rdm_records.records_service.reindex directly
        # on a list of imported record ids
        return aggs
    else:
        agg_task.delay()
        app.logger.info("Aggregations processing task sent...")
        return True


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


def create_invenio_record(
    metadata: dict,
    no_updates: bool,
    overrides: dict = {},
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

            'community': The Invenio community to which the record belongs
                if any. This is the primary community to which it was
                initially published, although the record may then have
                been added to other collections as well.
            'status': The kind of record operation that produced the new/
                current metadata record. Possible values: 'new_record',
                'updated_draft', 'updated_published', 'unchanged_existing_draft'
                'unchanged_existing_published'
            'metadata_record_created': The metadata record created or updated
                by the operation. This dictionary has three keys:
                'record_data', 'status', and 'recid' (with the Invenio
                internal UUID for the record object).
            'existing_record': The existing record with the same DOI if
                one was found.
            'uploaded_files': The files uploaded for the record, if any.
            'community_review_accepted': The response object from the
                community review acceptance at publication, if any.
            'assigned_ownership': The response object from the assignment
                of ownership to the record's uploader, if any.
            'added_to_collections': The response object from the addition
                of the record to collections in addition to the publication
                collection, if any.
    """
    app.logger.debug("~~~~~~~~")
    metadata = _coerce_types(metadata)
    app.logger.debug("metadata for new record:")
    app.logger.debug(pformat(metadata))
    for key, val in overrides.items():
        app.logger.debug(f"updating metadata key {key} with value {val}")
        metadata = replace_value_in_nested_dict(metadata, key, val)

    # Check for existing record with same DOI
    if "pids" in metadata.keys() and "doi" in metadata["pids"].keys():
        my_doi = metadata["pids"]["doi"]["identifier"]
        doi_for_query = my_doi.split("/")
        # TODO: Can we include deleted records here somehow?
        try:
            same_doi = records_service.search_drafts(
                system_identity,
                q=f'pids.doi.identifier:"{doi_for_query[0]}/'
                f'{doi_for_query[1]}"',
            )
            # app.logger.debug(f"same_doi: {my_doi}")
            # app.logger.debug(f"same_doi: {pformat(same_doi)}")
        except Exception as e:
            app.logger.error(
                "    error checking for existing record with same DOI:"
            )
            app.logger.error(same_doi.to_dict())
            raise e
        if same_doi.total > 0:
            app.logger.info(
                f"    found {same_doi.total} existing"
                " records with same DOI..."
            )
            # delete extra records with the same doi
            if same_doi.total > 1:
                rec_list = [(j["id"], j["status"]) for j in same_doi.hits]
                app.logger.info(
                    "    found more than one existing record with same DOI:"
                    f" {rec_list}"
                )
                app.logger.info("   deleting extra records...")
                for i in [
                    h["id"]
                    for h in list(same_doi.hits)[1:]
                    if "draft" in h["status"]
                ]:
                    try:
                        delete_invenio_draft_record(i)
                    except PIDUnregistered as e:
                        app.logger.error(
                            "    error deleting extra record with same DOI:"
                        )
                        raise DraftDeletionFailedError(
                            f"Draft deletion failed because PID for record "
                            f"{i} was unregistered: {str(e)}"
                        )
                    except Exception as e:
                        app.logger.error(
                            f"    error deleting extra record {i} with "
                            "same DOI:"
                        )
                        raise DraftDeletionFailedError(
                            f"Draft deletion failed for record {i} with "
                            f"same DOI: {str(e)}"
                        )
            existing_metadata = next(same_doi.hits)
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
                            update_payload.setdefault(key, {})[k2] = metadata[
                                key
                            ][k2]
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
                    if existing_metadata["files"].get("enabled") and (
                        len(existing_metadata["files"]["entries"].keys()) > 0
                    ):
                        app.logger.info(
                            "    existing record has files attached..."
                        )
                        update_payload["files"] = existing_metadata["files"]
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
                        app.logger.info(
                            "    creating new draft of published record..."
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
                        if result.to_dict().get("errors"):
                            errors = [
                                e
                                for e in result.to_dict()["errors"]
                                if e.get("field") != "metadata.rights.0.icon"
                                and e.get("messages") != ["Unknown field."]
                            ]
                            if errors:
                                raise UpdateValidationError(
                                    f"Validation error when trying to update existing record: {pformat(errors)}"
                                )
                        app.logger.info(
                            f"updated new draft of published: {pformat(result.to_dict())}"
                        )
                        app.logger.debug(
                            f"****title: {result.to_dict()['metadata'].get('title')}"
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

    return {
        "status": "new_record",
        "record_data": result.to_dict(),
        "recid": result_recid,
    }


def handle_record_files(
    metadata: dict,
    file_data: dict,
    existing_record: Optional[dict] = None,
):
    assert metadata["files"]["enabled"] is True
    uploaded_files = {}
    same_files = False

    if existing_record:
        # app.logger.debug(
        #     f"    existing metadata record {pformat(existing_record)}"
        # )
        same_files = _compare_existing_files(
            metadata["id"],
            existing_record["is_draft"] is True
            and existing_record["is_published"] is False,
            existing_record["files"]["entries"],
            file_data["entries"],
        )

    if same_files:
        app.logger.info(
            "    skipping uploading files (same already uploaded)..."
        )
    else:
        app.logger.info("    uploading new files...")

        # FIXME: Change upload filename field for other
        # import sources, and make it dict to match ["files"]["entries"]
        uploaded_files = _upload_draft_files(
            metadata["id"],
            file_data["entries"],
            {
                next(iter(file_data["entries"])): metadata["custom_fields"][
                    "hclegacy:file_location"
                ]
            },
        )
        # app.logger.debug("@@@@ uploaded_files")
        # app.logger.debug(pformat(uploaded_files))
    return uploaded_files


def _retry_file_initialization(
    draft_id: str, k: str, files_service: object
) -> bool:
    """Try to recover a failed file upload initialization.

    Handles situation when file key already exists on record but is not
    found in draft metadata retrieved by record service.

    params:
        draft_id (str): the id of the draft record
        k (str): the file key
        files_service (object): the draft files service

    raises:
        InvalidKeyError: if the file key already exists on record but is not
            found in draft metadata retrieved by record service

    returns:
        bool: True if the file key already exists on record but is not found
            in draft metadata retrieved by record service
    """
    existing_record = files_service._get_record(
        draft_id, system_identity, "create_files"
    )
    # FIXME: Why does this occasionally happen?

    if existing_record.files.entries[k] == {"metadata": {}}:
        removed_file = existing_record.files.delete(
            k, softdelete_obj=False, remove_rf=True
        )
        db.session.commit()
        app.logger.debug(
            "...file key existed on record but was empty and was "
            "removed. This probably indicates a prior failed upload."
        )
        app.logger.debug(pformat(removed_file))

        initialization = files_service.init_files(
            system_identity, draft_id, data=[{"key": k}]
        ).to_dict()
        assert (
            len([e["key"] for e in initialization["entries"] if e["key"] == k])
            == 1
        )
        return True
    else:
        app.logger.error(existing_record.files.entries[k].to_dict())
        app.logger.error(
            "    file key already exists on record but is not found in "
            "draft metadata retrieved by record service"
        )
        raise InvalidKeyError(
            f"File key {k} already exists on record but is not found in "
            "draft metadata retrieved by record service"
        )


def _upload_draft_files(
    draft_id: str,
    files_dict: dict[str, dict],
    source_filenames: dict[str, str],
) -> dict:
    """
    Upload the files for one draft record using the REST api.

    This process involves three api calls: one to initialize the
    upload, another to actually send the file content, and a third
    to commit the uploaded data.

    :param str draft_id:    The id number for the Invenio draft record
                            for which the files are to be uploaded.
    :param dict files_dict:     A dictionary whose keys are the filenames to
                                be used for the uploaded files. It is
                                structured like the ["files"]["entries"]
                                dictionary in an Invenio metadata record.
    :param dict source_filenames:  A dictionary whose keys are the filenames
                                to be used for the uploaded files. The values
                                are the corresponding full filenames in the
                                upload source directory.
    """
    files_service = records_service.draft_files
    output = {}

    # FIXME: Collect and upload files as a batch rather than one at a time
    for k, v in files_dict.items():
        source_filename = source_filenames[k]
        # FIXME: implementation detail. Humcore specific
        long_filename = source_filename.replace(
            "/srv/www/commons/current/web/app/uploads/humcore/", ""
        )
        long_filename = long_filename.replace(
            "/srv/www/commons/shared/uploads/humcore/", ""
        )
        # handle @ characters and apostrophes in filenames
        # FIXME: assumes the filename contains the key
        app.logger.debug(k)
        app.logger.debug(source_filename)
        app.logger.debug(normalize_string(k))
        app.logger.debug(normalize_string(unquote(source_filename)))
        try:
            assert normalize_string(k) in normalize_string(
                unquote(source_filename)
            )
        except AssertionError:
            app.logger.error(
                f"    file key {k} does not match source filename"
                f" {source_filename}..."
            )
            raise UploadFileNotFoundError(
                f"File key from metadata {k} not found in source file path"
                f" {source_filename}"
            )
        file_path = (
            Path(app.config["RECORD_IMPORTER_FILES_LOCATION"]) / long_filename
        )
        app.logger.debug(f"    uploading file: {file_path}")
        try:
            assert file_path.is_file()
        except AssertionError:
            raise UploadFileNotFoundError(
                f"    file not found for upload {file_path}..."
            )

        # FIXME: Change the identity throughout the process to the user
        # and permission protect the top-level functions
        try:
            initialization = files_service.init_files(
                system_identity, draft_id, data=[{"key": k}]
            ).to_dict()
            # app.logger.debug(initialization)
            # app.logger.debug(k)
            assert (
                len(
                    [
                        e["key"]
                        for e in initialization["entries"]
                        if e["key"] == k
                    ]
                )
                == 1
            )
        except InvalidKeyError:
            _retry_file_initialization(draft_id, k, files_service)
        except Exception as e:
            app.logger.error(
                f"    failed to initialize file upload for {draft_id}..."
            )
            raise e

        try:
            with open(
                file_path,
                "rb",
            ) as binary_file_data:
                # app.logger.debug(
                #     f"filesize is {len(binary_file_data.read())} bytes"
                # )
                binary_file_data.seek(0)
                files_service.set_file_content(
                    system_identity, draft_id, k, binary_file_data
                )

        except Exception as e:
            app.logger.error(
                f"    failed to upload file content for {draft_id}..."
            )
            raise e

        try:
            files_service.commit_file(system_identity, draft_id, k)
        except Exception as e:
            app.logger.error(
                f"    failed to commit file upload for {draft_id}..."
            )
            raise e

        output[k] = "uploaded"

    result_record = files_service.list_files(
        system_identity, draft_id
    ).to_dict()
    try:

        #   upload_commit["json"]["key"] == unquote(filename)
        assert all(
            r["key"]
            for r in result_record["entries"]
            if r["key"] in files_dict.keys()
        )
        for v in result_record["entries"]:
            # app.logger.debug("key: " + v["key"] + " " + k)
            assert v["key"] == k
            # app.logger.debug("status: " + v["status"])
            assert v["status"] == "completed"
            app.logger.debug(f"size: {v['size']}  {files_dict[k]['size']}")
            if str(v["size"]) != str(files_dict[k]["size"]):
                raise FileUploadError(
                    f"Uploaded file size ({v['size']}) does not match expected size ({files_dict[k]['size']})"
                )
            # TODO: Confirm correct checksum?
            # app.logger.debug(f"checksum: {v['checksum']}")
            # app.logger.debug(f"created: {v['created']}")
            assert valid_date(v["created"])
            # app.logger.debug(f"updated: {v['updated']}")
            assert valid_date(v["updated"])
            # app.logger.debug(f"metadata: {v['metadata']}")
            assert not v["metadata"]
            # TODO: Confirm that links are correct
            # assert (
            #     upload_commit["json"]["links"]["content"]
            #     == f["links"]["content"]
            # )
            # assert upload_commit["json"]["links"]["self"] == f["links"]["self"]
            # assert (
            #     upload_commit["json"]["links"]["commit"]
            #     == f["links"]["commit"]
            # )
    except AssertionError as e:
        app.logger.error(
            "    failed to properly upload file content for"
            f" draft {draft_id}..."
        )
        app.logger.error(f"result is {pformat(result_record['entries'])}")

    return output


def _compare_existing_files(
    draft_id: str,
    is_draft: bool,
    old_files: dict[str, dict],
    new_entries: dict[str, dict],
) -> bool:
    """Compare record's existing files with import record's files.

    Compares the files based on filename and size. If the existing record has
    different files than the import record, the existing files are deleted to
    prepare for uploading the correct files.

    This function also handles prior interrupted uploads, deleting them to
    clear the way for a new upload.

    params:
        draft_id (str): the id of the draft record
        is_draft (bool): whether the record is a draft or published record
        old_files (dict): dictionary of files attached to the existing record
        new_entries (dict): dictionary of files to be attached to the import
                            record

    raises:
        RuntimeError: if existing record has different files than import record
        and the existing files are not deleted.

    returns:
        bool: True if the existing record already has the same files as the
        import record, False if the existing record has different files than
        the import record.
    """

    files_service = (
        records_service.files if not is_draft else records_service.draft_files
    )
    # draft_files_service = records_service.draft_files
    same_files = True

    try:
        files_request = files_service.list_files(
            system_identity, draft_id
        ).to_dict()
    except NoResultFound:
        try:
            files_request = records_service.draft_files.list_files(
                system_identity, draft_id
            ).to_dict()
            # app.logger.debug(pformat(files_request))
        except NoResultFound:
            files_request = None
    existing_files = files_request["entries"] if files_request else []
    # app.logger.debug(pformat(existing_files))
    if len(existing_files) == 0:
        same_files = False
        app.logger.info("    no files attached to existing record")
    else:
        for k, v in new_entries.items():
            wrong_file = False
            existing_file = [
                f
                for f in existing_files
                if unicodedata.normalize("NFC", f["key"])
                == unicodedata.normalize("NFC", k)
            ]
            # app.logger.debug(
            #     [
            #         "normalized existing file: "
            #         f"{unicodedata.normalize('NFC', f['key'])}"
            #         for f in existing_files
            #     ]
            # )

            if len(existing_file) == 0:
                same_files = False

            # handle prior interrupted uploads
            # handle uploads with same name but different size
            elif (existing_file[0]["status"] == "pending") or (
                str(v["size"]) != str(existing_file[0]["size"])
            ):
                same_files = False
                wrong_file = True

            # delete interrupted or different prior uploads
            if wrong_file:
                error_message = (
                    "Existing record with same DOI has different"
                    f" files.\n{pformat(old_files)}\n !=\n {pformat(new_entries)}\n"
                    f"Could not delete existing file {existing_file[0]['key']}."
                )
                try:
                    deleted_file = files_service.delete_file(
                        system_identity, draft_id, existing_file[0]["key"]
                    )
                    # app.logger.debug(pformat(deleted_file))
                    app.logger.info(
                        "    existing record had wrong or partial upload, now"
                        " deleted"
                    )
                except NoResultFound:
                    # interrupted uploads will be in draft files service
                    records_service.draft_files.delete_file(
                        system_identity, draft_id, existing_file[0]["key"]
                    )
                except FileKeyNotFoundError as e:
                    # FIXME: Do we need to create a new draft here?
                    # because the file is not in the draft?
                    # files_delete = files_service.delete_file(
                    #     system_identity, draft_id, existing_file[0]["key"]
                    # )
                    app.logger.info(
                        "    existing record had wrong or partial upload, but"
                        " it could not be found for deletion"
                    )
                    raise e
                except Exception as e:
                    raise e

                # check that the file was actually deleted
                # call produces an error if not found
                try:
                    files_service.list_files(
                        system_identity, draft_id
                    ).to_dict()["entries"]
                except NoResultFound:
                    app.logger.info(
                        "    deleted file is no longer attached to record"
                    )
                else:
                    app.logger.error(error_message)
                    raise RuntimeError(error_message)
        return same_files


def delete_invenio_draft_record(record_id: str) -> Optional[dict]:
    """
    Delete a draft Invenio record with the provided Id

    Since drafts cannot be deleted if they have an associated review request,
    this function first deletes any existing review request for the draft
    record.

    Note: This function only works for draft (unpublished) records.

    :param str record_id:   The id string for the Invenio draft record
    """
    result = None
    app.logger.info(f"    deleting draft record {record_id}...")

    # TODO: Is this read necessary anymore?
    # In case the record is actually published
    try:
        record = records_service.read(system_identity, id_=record_id).to_dict()
    except PIDUnregistered:
        record = records_service.search_drafts(
            system_identity, q=f'id:"{record_id}'
        ).to_dict()

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
            system_identity, id_=record_id, data=record
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

    Returns
    -------
    dict
        A dictionary with the new user account metadata dictionary ("user")
        and a boolean flag indicating whether the account is new or
        existing ("new_user")
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
        )["email"]

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
        new_user = current_accounts.datastore.create_user(
            email=user_email,
            # password=generate_password(16),
            active=True,
            confirmed_at=arrow.utcnow().datetime,
            user_profile={},
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
                UserIdentity.create(active_user, record_source, source_username)
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

    return {"user": active_user, "new_user": new_user_flag}


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


def prepare_invenio_community(community_string: str) -> dict:
    """Ensure that the community exists in Invenio."""
    # FIXME: idiosyncratic implementation detail
    community_label = community_string.split(".")
    if community_label[1] == "msu":
        community_label = community_label[1]
    else:
        community_label = community_label[0]

    # FIXME: remnant of name change
    if community_label == "hcommons":
        community_label = "kcommons"

    app.logger.debug(f"checking for community {community_label}")
    community_check = current_communities.service.search(
        system_identity, q=f"slug:{community_label}"
    ).to_dict()

    if community_check["hits"]["total"] == 0:
        app.logger.debug(
            "Community", community_label, "does not exist. Creating..."
        )
        # FIXME: use group-collections to create the community
        # so that we import community metadata
        community_check = _create_invenio_community(community_label)
    else:
        community_check = community_check["hits"]["hits"][0]

    return community_check


def _create_invenio_community(community_label: str) -> dict:
    """Create a new community in Invenio.

    Return the community data as a dict. (The result
    of the CommunityItem.to_dict() method.)
    """
    # FIXME: Get this from config
    community_data = {
        "hcommons": {
            "slug": "hcommons",
            "metadata": {
                "title": "Humanities Commons",
                "description": (
                    "A collection representing Humanities Commons"
                ),
                "website": "https://hcommons.org",
                "organizations": [{"name": "Humanities Commons"}],
            },
        },
        "msu": {
            "slug": "msu",
            "metadata": {
                "title": "MSU Commons",
                "description": ("A collection representing MSU Commons"),
                "website": "https://commons.msu.edu",
                "organizations": [{"name": "MSU Commons"}],
            },
        },
        "ajs": {
            "slug": "ajs",
            "metadata": {
                "title": "AJS Commons",
                "description": (
                    "AJS is no longer a member of Knowledge Commons"
                ),
                "website": "https://ajs.hcommons.org",
                "organizations": [{"name": "AJS Commons"}],
            },
        },
        "arlisna": {
            "slug": "arlisna",
            "metadata": {
                "title": "ARLIS/NA Commons",
                "description": ("A collection representing ARLIS/NA Commons"),
                "website": "https://arlisna.hcommons.org",
                "organizations": [{"name": "ARLISNA Commons"}],
            },
        },
        "aseees": {
            "slug": "aseees",
            "metadata": {
                "title": "ASEEES Commons",
                "description": ("A collection representing ASEEES Commons"),
                "website": "https://aseees.hcommons.org",
                "organizations": [{"name": "ASEEES Commons"}],
            },
        },
        "hastac": {
            "slug": "hastac",
            "metadata": {
                "title": "HASTAC Commons",
                "description": "A collection representing HASTAC Commons",
                "website": "https://hastac.hcommons.org",
                "organizations": [{"name": "HASTAC Commons"}],
            },
        },
        "caa": {
            "slug": "caa",
            "metadata": {
                "title": "CAA Commons",
                "description": (
                    "CAA is no longer a member of Humanities Commons"
                ),
                "website": "https://caa.hcommons.org",
                "organizations": [{"name": "CAA Commons"}],
            },
        },
        "mla": {
            "slug": "mla",
            "metadata": {
                "title": "MLA Commons",
                "description": ("A collection representing the MLA Commons"),
                "website": "https://mla.hcommons.org",
                "organizations": [{"name": "MLA Commons"}],
            },
        },
        "sah": {
            "slug": "sah",
            "metadata": {
                "title": "SAH Commons",
                "description": (
                    "A community representing the SAH Commons domain"
                ),
                "website": "https://sah.hcommons.org",
                "organizations": [{"name": "SAH Commons"}],
            },
        },
        "up": {
            "access": {
                "visibility": "restricted",
                "member_policy": "closed",
                "record_policy": "closed",
                # "owned_by": [{"user": ""}]
            },
            "slug": "up",
            "metadata": {
                "title": "UP Commons",
                "description": (
                    "A collection representing the UP Commons domain"
                ),
                "website": "https://up.hcommons.org",
                "organizations": [{"name": "UP Commons"}],
            },
        },
    }
    my_community_data = community_data[community_label]
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
        community_id (str): the id of the community to publish the record to
            (must be a UUID, not the community's slug)

    returns:
        dict: the result of the review acceptance action
    """
    # Attachment to community unnecessary if the record is already published
    # or included in it, even if a new draft version
    try:
        existing_record = records_service.read(
            system_identity, id_=draft_id
        ).to_dict()
        assert existing_record
    except (AssertionError, PIDUnregistered):
        try:
            existing_record = (
                records_service.search_drafts(
                    system_identity, q=f"id:{draft_id}"
                )
                .to_dict()
                .get("hits", {})
                .get("hits", [])[0]
            )
        except IndexError:
            existing_record = None
    except NoResultFound:
        existing_record = None

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
        # Publish new draft (otherwise would be published at community
        # review acceptance)
        if existing_record["is_draft"] == True:
            app.logger.info("    publishing new draft record version...")
            app.logger.debug(
                pformat(
                    records_service.search_drafts(
                        system_identity, q=f"id:{draft_id}"
                    ).to_dict()
                )
            )
            # Edit is sometimes necessary if the draft status has become
            # confused
            edit = records_service.edit(system_identity, id_=draft_id)
            publish = records_service.publish(system_identity, id_=edit.id)
            assert publish.data["status"] == "published"
    # for records that haven't been attached to the community yet
    # submit and accept a review request
    else:
        # DOIs cannot be registered at publication if the record
        # is restricted (see datacite provider `validate_restriction_level`
        # method called in pid component's `publish` method)
        if (
            existing_record
            and existing_record["access"]["record"] == "restricted"
        ):
            app.logger.error(pformat(existing_record))
            raise RestrictedRecordPublicationError(
                "Record is restricted and cannot be published to the community"
                " because its DOI cannot be registered"
            )

        request_id = None
        # Try to cancel any existing review request for the record
        # with another community, since it will conflict
        try:
            existing_review = records_service.review.read(
                system_identity, id_=draft_id
            )
            app.logger.info(
                "    cancelling existing review request for the record to the"
                f" community...: {existing_review.id}"
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
                app.logger.debug(
                    "   existing review request is not open, deleting"
                )
                try:
                    records_service.review.delete(
                        system_identity, id_=draft_id
                    )
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
                        "   existing review request was already deleted, manually removed from record metadata"
                    )
            else:
                request_id = existing_review.id
                cancel_existing_request = (
                    current_requests_service.execute_action(
                        system_identity,
                        request_id,
                        "cancel",
                    )
                )
                app.logger.debug(
                    f"cancel_existing_request: "
                    f"{pformat(cancel_existing_request)}"
                )
        # If no existing review request, just continue
        except (ReviewNotFoundError, NoResultFound):
            app.logger.info(
                "    no existing review request found for the record to the"
                " community..."
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
            app.logger.debug(f"new_request: {pformat(new_request.to_dict())}")

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
                    f"    initially failed to submit review request: {submitted_request.to_dict()}"
                )
                submitted_request = current_requests_service.execute_action(
                    system_identity,
                    submitted_request.id,
                    "submit",
                )
            # app.logger.debug(
            #     f"submitted_request: {pformat(new_request.to_dict())}"
            # )

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

            return review_accepted

        # Catch validation errors when publishing the record
        except ValidationError as e:
            app.logger.error(
                f"    failed to validate record for publication: {e.messages}"
            )
            raise PublicationValidationError(e.messages)

        # If the record is already published, we need to create/retrieve
        # and accept a 'community-inclusion' request instead
        except (NoResultFound, ReviewStateError):
            app.logger.debug("   record is already published")
            record_communities = current_rdm_records.record_communities_service

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
                return {submitted_request}
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
    metadata_record: dict, record_source: str
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
            source service, for use by invenio-group-collections in
            linking the record to the appropriate group collections

    returns:
        list: the list of group collections the record was added to
    """
    # FIXME: See whether this can be generalized
    if record_source == "knowledgeCommons":
        added_to_collections = []
        group_list = [
            g
            for g in metadata_record["custom_fields"].get(
                "hclegacy:groups_for_deposit", []
            )
            if g.get("group_identifier") and g.get("group_name")
        ]
        for group in group_list:
            group_id = group["group_identifier"]
            app.logger.debug(f"    linking to group_id: {group_id}")
            app.logger.debug(
                f"    linking to group_name: {group['group_name']}"
            )
            group_name = group["group_name"]
            coll_record = None
            try:
                coll_search = collections_service.search(
                    system_identity,
                    record_source,
                    commons_group_id=group_id,
                )
                coll_records = coll_search.to_dict()["hits"]["hits"]
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
                app.logger.debug(
                    f"coll_record: {pformat(coll_search.to_dict())}"
                )
                assert len(coll_records) == 1
                coll_record = coll_records[0]
                app.logger.debug(
                    f"    found group collection {coll_record['id']}"
                )
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
                    if (
                        "Something went wrong requesting group"
                        in e.description
                    ):
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
                add_result = publish_record_to_community(
                    metadata_record["id"],
                    coll_record["id"],
                )
                added_to_collections.append(add_result)
                app.logger.debug(
                    f"    added to group collection {coll_record['id']}"
                )
                # app.logger.debug(f"    add_result: {pformat(add_result)}")
        if added_to_collections:
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
            assert existing_user
            existing_user.user_profile.setdefault("identifiers", []).extend(
                [
                    {
                        "scheme": f"{record_source}_username",
                        "identifier": new_owner_username,
                    },
                    {
                        "scheme": "email",
                        "identifier": new_owner_email,
                    },
                ]
            )
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
        - metadata_record_created: the metadata record creation result
        - status: the status of the metadata record
        - uploaded_files: the file upload results
        - community_review_accepted: the community review acceptance result
        - assigned_ownership: the record ownership assignment result
        - added_to_collections: the group collection addition
    """
    existing_record = None
    result = {}
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
        result["community"] = prepare_invenio_community(
            import_data["custom_fields"]["kcr:commons_domain"]
        )
        community_id = result["community"]["id"]

    # Create the basic metadata record
    app.logger.info("    finding or creating draft metadata record...")
    record_created = create_invenio_record(import_data, no_updates, overrides)
    result["metadata_record_created"] = record_created
    result["status"] = record_created["status"]
    app.logger.info(f"    record status: {record_created['status']}")
    draft_uuid = record_created["recid"]
    if record_created["status"] in [
        "updated_published",
        "updated_draft",
        "unchanged_existing_draft",
        "unchanged_existing_published",
    ]:
        existing_record = result["existing_record"] = record_created[
            "record_data"
        ]
    metadata_record = record_created["record_data"]
    draft_id = metadata_record["id"]
    app.logger.info(f"    metadata record id: {draft_id}")

    # Upload the files
    if len(import_data["files"]["entries"]) > 0:
        app.logger.info("    uploading files for draft...")
        result["uploaded_files"] = handle_record_files(
            metadata_record,
            file_data,
            existing_record=existing_record,
        )
    else:
        assert metadata_record["files"]["enabled"] is False

    # Attach the record to the communities
    result["community_review_accepted"] = publish_record_to_community(
        draft_id,
        community_id,
    )
    # Publishing the record happens during community acceptance

    # Assign ownership of the record
    result["assigned_ownership"] = assign_record_ownership(
        draft_id, import_data, record_source, existing_record=existing_record
    )

    # Add the record to the appropriate group collections
    result["added_to_collections"] = add_record_to_group_collections(
        metadata_record, record_source
    )

    # Create fictural usage events to generate correct usage stats
    events = create_stats_events(
        draft_id,
        eager=True,
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
        FilesHelper.sanitize_filenames(
            app.config["RECORD_IMPORTER_FILES_LOCATION"]
        )

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
        aggregations = create_stats_aggregations(
            start_date=arrow.get(start_date).naive.isoformat(),
            end_date=arrow.get(end_date).naive.isoformat(),
            bookmark_override=arrow.get(start_date).naive,
            eager=True,
        )
        app.logger.debug(f"    created usage aggregations...")
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
    # FIXME: This is a temporary function for testing purposes
    print("Starting to delete records")
    for r in record_ids:
        print(f"deleting {r}")
        deleted = api_request("DELETE", f"records/{r}")
        pprint(deleted)
    print("finished deleting records")
