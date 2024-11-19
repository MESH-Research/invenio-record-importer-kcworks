#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 Mesh Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.


"""
Functions to convert and migrate legacy CORE deposits to InvenioRDM

Relies on the following environment variables:

RECORD_IMPORTER_DATA_DIR   The full path to the local directory where
                            the source json files (exported from legacy
                            CORE) can be found.
RECORD_IMPORTER_SERIALIZED_PATH   The full path to the local directory where
                            the source json files (exported from legacy
                            CORE) can be found.
RECORD_IMPORTER_EVENTS_PATH   The full path to the local directory where
                            the events json files (exported from legacy
                            CORE) can be found.

Normally these variables can be set in the .env file in your base
knowledge_commons_works directory.
"""

import arrow
import click
from flask import current_app as app
from flask.cli import with_appcontext
from halo import Halo

from invenio_record_importer_kcworks.serializer import serialize_json
from invenio_record_importer_kcworks.services.serialization import (
    SerializationService,
)
from invenio_record_importer_kcworks.services.stats.stats import (
    StatsFabricator,
    AggregationFabricator,
)
from invenio_record_importer_kcworks.record_loader import (
    RecordLoader,
)
from invenio_record_importer_kcworks.services.records import RecordsHelper
from invenio_record_importer_kcworks.services.users import UsersHelper

from pprint import pformat, pprint
from typing import Optional


@click.group()
def cli():
    pass


@cli.command(name="serialize")
@with_appcontext
def serialize_command_wrapper():
    """
    Serialize all exported legacy CORE deposits as JSON that Invenio can ingest
    """
    serialize_json()


@cli.command(name="load")
@click.argument("records", nargs=-1)
@click.option(
    "-n",
    "--no-updates",
    is_flag=True,
    default=False,
    help=(
        "If True, do not update existing records where a record with the same"
        " DOI already exists."
    ),
)
@click.option(
    "-r",
    "--retry-failed",
    is_flag=True,
    default=False,
    help=(
        "If True, try to load in all previously failed records that have not"
        " already been repaired successfully."
    ),
)
@click.option(
    "-s",
    "--use-sourceids",
    is_flag=True,
    default=False,
    help=(
        "If True, the positional arguments are interpreted as ids in the"
        " source system instead of positional indices."
    ),
)
@click.option(
    "--scheme",
    default="hclegacy-pid",
    help=(
        "The identifier scheme to use for the records when the "
        "--use-sourceids flag is True. Defaults to 'hclegacy-pid' for "
        "the ids used by the old Humanities Commons CORE repository."
    ),
)
@click.option(
    "-a",
    "--aggregate",
    is_flag=True,
    default=False,
    help=(
        "If True, aggregate the record view and download statistics for all"
        " records after loading. (This may take a long time.)"
    ),
)
@click.option(
    "--start-date",
    default=None,
    help=(
        "The start date for the record events to aggregate if the --aggregate "
        "flag is True. If not specified, the aggregation will begin from the "
        "earliest creation date of the migrated records. The date should be "
        "formatted in ISO format, i.e. as 'YYYY-MM-DD'."
    ),
)
@click.option(
    "--end-date",
    default=None,
    help=(
        "The end date for the record events to aggregate if the --aggregate "
        "flag is True. If not specified, the aggregation will end with the "
        "current date. The date should be formatted in ISO format, i.e. as "
        "'YYYY-MM-DD'."
    ),
)
@click.option(
    "-c",
    "--clean-filenames",
    is_flag=True,
    default=False,
    help=(
        "If True, clean up the filenames of the uploaded files to remove"
        " special characters and spaces."
    ),
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Print and log verbose output",
)
@click.option(
    "-x",
    "--stop-on-error",
    is_flag=True,
    default=False,
    help="Stop loading records if an error is encountered",
)
@with_appcontext
def load_records(
    records: list,
    no_updates: bool,
    retry_failed: bool,
    use_sourceids: bool,
    scheme: str,
    aggregate: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    clean_filenames: bool,
    verbose: bool,
    stop_on_error: bool,
):
    """
    Load serialized exported records into InvenioRDM.


    If RECORDS is not specified, all records will be loaded. Otherwise,
    RECORDS should be a list of positional arguments specifying which records
    to load.

    Examples:

        To load records 1, 2, 3, and 5, run:

            invenio importer load 1 2 3 5

        A range can be specified in the RECORDS by linking two integers with a
        hyphen. For example, to load only the first 100 records, run:

            invenio importer load 1-100

        If the range ends in a hyphen with no second integer, the program will
        load all records from the start index to the end of the input file. For
        example, to load all records from 100 to the end of the file, run:

            invenio importer load 100-

        Records may be loaded by id in the source system instead of by index.
        For example, to load records with ids hc:4723, hc:8271, and hc:2246,
        run:

            invenio importer load --use-sourceids hc:4723 hc:8271 hc:2246

        To aggregate usage statistics after loading, add the --aggregate flag.
        For example, to load all records and aggregate usage statistics, run:

            invenio importer load --aggregate

    Notes:

        This program must be run from the base knowledge_commons_works
        directory. It will look for the exported records in the directory
        specified by the RECORD_IMPORTER_DATA_DIR environment variable.

        The program must also be run inside the pipenv virtual environment for
        the knowledge_commons_works instance. All of the commands must be
        preceded by `pipenv run` or the pipenv environment must first be
        activated with `pipenv shell`.

        The operations involved require authenitcation as an admin user in the
        knowledge_commons_works instance. This program will look for the
        admin user's api token in the RECORD_IMPORTER_API_TOKEN environment
        variable.

        Where necessary this program will create top-level domain communities,
        assign the records to the correct domain communities, create
        new Invenio users corresponding to the users who uploaded the
        original deposits, and transfer ownership of the Invenio record to
        the correct users.

        If a record with the same DOI already exists in Invenio, the program
        will try to update the existing record with any new metadata and/or
        files, creating a new draft of published records if necessary.
        Unpublished existing drafts will be submitted to the appropriate
        community and published. Alternately, if the --no-updates flag is set,
        the program will skip any records that match DOIs for records that
        already exist in Invenio.

        Since the operations involved are time-consuming, the program should
        be run as a background process (adding & to the end of the command).
        A running log of the program's progress will be written to the file
        invenio_record_importer_kcworks.log in the base
        invenio_record_importer_kcworks/logs
        directory. A record of all records that have been created (a load
        attempt has been made) is recorded in the file
        record_importer_created_records.jsonl in a configurable directory.
        A record of all records that
        have failed to load is recorded in the file
        record_importer_failed_records.json in the
        same directory. If failed records are later
        successfully repaired, they will be removed from the failed records
        file.

    Args:

        records (list, optional): A list of the provided positional arguments
            specifying which records to load. Defaults to [].

            If no positional arguments are provided, all records will be
            loaded.

            If positional arguments are provided, they should be either
            integers specifying the line numbers of the records to load,
            or source ids specifying the ids of the records to load in
            the source system. These will be interpreted as line numbers
            in the jsonl file of records for import (beginning at 1)
            unless the --use-sourceids flag is set.

            If a range is specified in the RECORDS by linking two integers with
            a hyphen, the program will load all records between the two
            indices, inclusive. If the range ends in a hyphen with no second
            integer, the program will load all records from the start index to
            the end of the input file.

        no_updates (bool, optional): If True, do not update existing records
            where a record with the same DOI already exists. Defaults to False.

        retry_failed (bool, optional): If True, try to load in all previously
            failed records that have not already been repaired successfully.
            Defaults to False.

        use_sourceids (bool, optional): If True, the positional arguments
            are interpreted as ids in the source system instead of positional
            indices. Defaults to False.

        scheme (str, optional): The identifier scheme to use for the records
            when the --use-sourceids flag is True. Defaults to 'hclegacy-pid'
            for the ids used by the old Humanities Commons CORE repository.

        aggregate (bool, optional): If True, aggregate the record view and
            download statistics for all records after loading. Defaults to
            False.

        start_date (str, optional): The start date for the record events to
            aggregate if the --aggregate flag is True. If not specified, the
            aggregation will begin from the earliest creation date of the
            migrated records. The date should be formatted in ISO format,
            i.e. as 'YYYY-MM-DD'. Defaults to None.

        end_date (str, optional): The end date for the record events to
            aggregate if the --aggregate flag is True. If not specified, the
            aggregation will end with the current date. The date should be
            formatted in ISO format, i.e. as 'YYYY-MM-DD'. Defaults to None.

        clean_filenames (bool, optional): If True, clean up the filenames of
            the uploaded files to remove special characters and spaces.

        verbose (bool, optional): Print and log verbose output. Defaults to
            False.

        stop_on_error (bool, optional): Stop loading records if an error is
            encountered. Defaults to False.

    Returns:

        None
    """
    named_params = {
        "no_updates": no_updates,
        "retry_failed": retry_failed,
        "use_sourceids": use_sourceids,
        "sourceid_scheme": scheme,
        "aggregate": aggregate,
        "start_date": start_date,
        "end_date": end_date,
        "clean_filenames": clean_filenames,
        "verbose": verbose,
        "stop_on_error": stop_on_error,
    }
    if len(records) > 0 and "-" in records[0]:
        if use_sourceids:
            print("Error: Cannot use source ids with ranges.")
            app.logger.error(
                "Ranges can only be specified using record indices, not source"
                " ids."
            )
            return
        named_params["start_index"], named_params["stop_index"] = records[
            0
        ].split("-")
        named_params["start_index"] = int(named_params["start_index"])
        if named_params["stop_index"] == "":
            named_params["stop_index"] = -1
        else:
            named_params["stop_index"] = int(named_params["stop_index"])
    else:
        if not use_sourceids:
            named_params["nonconsecutive"] = [int(arg) for arg in records]
        else:
            records = [arg.replace("\-", "-") for arg in records]  # noqa
            named_params["nonconsecutive"] = records

    RecordLoader().load_records_into_invenio(**named_params)

    # Make alert sound to signal end of loading process
    print("\a")


@cli.command(name="read")
@click.argument("records", nargs=-1)
@click.option(
    "-s",
    "--use-sourceids",
    is_flag=True,
    default=False,
)
@click.option(
    "--scheme",
    default="doi",
    help=(
        "The identifier scheme to use for the records when the "
        "--use-sourceids flag is True. Defaults to 'doi'."
    ),
)
@click.option(
    "-r",
    "--raw-input",
    is_flag=True,
    default=False,
    help=(
        "If True, the returned records will be in the raw format read from the"
        " source file, rather than the serialized format."
    ),
)
@click.option(
    "-p",
    "--field-path",
    default="",
    help=(
        "A dot-separated string of field names to specify a subfield of the"
        " record(s) to read."
    ),
)
@with_appcontext
def read_records(
    records, scheme, raw_input, use_sourceids, field_path
) -> None:
    """
    Read serialized records or raw input from the source file or original data.

    params:
        records: list
            A list of positional arguments specifying which records to read.
            If positional arguments are provided, they should be either
            integers specifying the line numbers of the records to read,
            or source ids specifying the ids of the records to read in
            the source system. These will be interpreted as line numbers
            in the jsonl file of records for import (beginning at 1)
            unless the --use-sourceids flag is set.

        scheme: str
            The identifier scheme to use for the records when the "
            "--use-sourceids flag is True. Defaults to 'doi'.

        raw_input: bool
            If True, the returned records will be in the raw format read "
            "from the source file, rather than the serialized format.

        use_sourceids: bool
            If True, the positional arguments are interpreted as ids in the
            source system instead of positional indices.

        field_path: str
            A dot-separated string of field names to specify a subfield of the
            record(s) to read.
    """
    service = SerializationService()
    args = {"field_path": field_path}
    if use_sourceids:
        args["id_scheme"] = scheme
        args["identifiers"] = records
    else:
        args["indices"] = [int(r) - 1 for r in records]

    if raw_input:
        raw_records = service.read_raw(**args)
        for r in raw_records:
            print(f"Raw (unserialized) source data for record {r['id']}:")
            pprint(r["record"])
    else:
        pprint(args)
        records = service.read_serialized(**args)
        for c in records:
            print(f"Processed (serialized) input data for record {c['id']}:")
            pprint(c["record"])


@cli.command(name="create_user")
@click.option(
    "-e",
    "--email",
    help=(
        "The email address of the user to create. This must be the same "
        "Required."
    ),
)
@click.option(
    "-o",
    "--origin",
    default="knowledgeCommons",
    help=(
        "The commons instance or id provider where the users are being "
        "created. Defaults to 'knowledgeCommons'."
    ),
)
@click.option(
    "-n",
    "--source-username",
    help=("The username of the user in the source system. Required."),
)
@click.option(
    "-f",
    "--full-name",
    help=("The user's full name for the new account."),
)
@click.option(
    "-c",
    "--community-owner",
    multiple=True,
    help=(
        "The id (slug or UUID) of the community to which the user will be "
        "assigned as owner. To submit multiple values, repeat the flag for "
        "each value."
    ),
)
@with_appcontext
def create_user(
    email: str,
    origin: str,
    source_username: str,
    full_name: str,
    community_owner: list,
) -> None:
    """
    Create a new user in InvenioRDM linked to an external service.

    This function does not just create a new user in the Invenio database,
    but also links the user's account to an external service as a SAML
    identity provider and a source of user data. (Depends on a service
    being configured for the invenio-saml and
    invenio-remote-user-data-kcworks extensions.)

    This operation assumes that usernames on the source system are unique
    and that the email address is the same as the one used in the source
    system.

    If desired, the user can be assigned as the owner of one or more
    communities in the Invenio instance.

    params:
        email: str
            The email address of the user to create. This must be the same
            email address that the user used in the source system.

        origin: str
            The commons instance or id provider where the users are
            being created.

        source_username: str
            The username of the user in the source system.

        full_name: str
            The user's full name for the new account.

        community_owner: list
            The id of the community to which the user will be assigned as
            owner. To submit multiple values, repeat the flag for each value.
    """
    spinner = Halo(
        text=f"Creating user {email}, {source_username}, from {origin}...",
        spinner="dots",
    )
    spinner.start()

    create_response = UsersHelper().create_invenio_user(
        email,
        record_source=origin,
        source_username=source_username,
        full_name=full_name,
        community_owner=community_owner,
    )
    user_data = create_response["user"]

    def print_user_data(user_data, community_owner, communities_owned):
        print(f"username: {pformat(user_data.username)}")
        print(f"email: {pformat(user_data.email)}")
        print(f"profile: {pformat(user_data.user_profile)}")
        print(f"remote accounts: {pformat(user_data.remote_accounts)}")
        print(
            f"external identifiers: {pformat(user_data.external_identifiers)}"
        )
        print(f"domain: {pformat(user_data.domain)}")
        print(f"authenticated: {pformat(user_data.is_authenticated)}")
        print(f"preferences: {pformat(user_data.preferences)}")
        print(f"active: {pformat(user_data.active)}")
        if community_owner:
            print_community_owner(
                community_owner, create_response["communities_owned"]
            )

    def print_community_owner(community_owner, communities_owned):
        print(
            f"User {email} has been assigned as owner of the "
            "following communities:"
        )
        for community_id in community_owner:
            matches = [
                c
                for c in communities_owned
                if community_id in [c["community_id"], c["community_slug"]]
            ]
            if matches[0]:
                print(matches[0]["community_id"], matches[0]["community_slug"])
            else:
                print(f"Error: Not assigned ownership of {community_id}.")

    if create_response["new_user"]:
        spinner.stop()
        print_user_data(
            user_data, community_owner, create_response["communities_owned"]
        )
    else:
        spinner.stop()
        admin_email = app.config.get("RECORD_IMPORTER_ADMIN_EMAIL")
        if user_data.email == admin_email:
            print("Error: The user could not be created.")
        else:
            print("NOTE: The user already exists in the system:")
            print_user_data(
                user_data,
                community_owner,
                create_response["communities_owned"],
            )


@cli.command(name="count")
@with_appcontext
def count_objects():
    """
    Count the number of objects in the JSON file for import.

    The file location is specified by the RECORD_IMPORTER_SERIALIZED_PATH
    config variable.
    """
    serialized_path = app.config.get("RECORD_IMPORTER_SERIALIZED_PATH")
    try:
        with open(serialized_path, "r") as file:
            lines_count = sum(1 for line in file)
            print(f"Total objects in {serialized_path}: {lines_count}")
    except FileNotFoundError:
        print(f"Error: File not found at {serialized_path}")


@cli.command(name="delete")
@click.argument("records", nargs=-1)
@click.option(
    "-v",
    "--visible",
    is_flag=True,
    default=True,
    help=("If True, the record tombstone will be visible after deletion."),
)
@click.option(
    "-r",
    "--reason",
    default="take-down-request",
    help=("The reason for the record deletion."),
)
@click.option(
    "-n",
    "--note",
    default="",
    help=("The note to include in the record deletion."),
)
@with_appcontext
def delete_records(records, visible, reason, note):
    """
    Delete one or more records from InvenioRDM by record id.
    """
    print("Starting to delete records")
    results = RecordsHelper().delete_records_from_invenio(
        records, visible, reason, note
    )
    pprint(results)
    print(f"All done deleting records: {[k for k in results.keys()]}")


@cli.command(name="stats")
@click.option(
    "-r",
    "--record-ids",
    help=(
        "A comma-separated list of record ids to create usage statistics for."
    ),
)
@click.option(
    "-s",
    "--record-source",
    default="knowledgeCommons",
    help=(
        "The source of the records to create usage statistics for. If not "
        "specified, this will default to 'knowledgeCommons'."
    ),
)
@click.option(
    "-d",
    "--from-db",
    is_flag=True,
    default=False,
    help=(
        "If True, the usage statistics will be created from the database. If "
        "False, the usage statistics will be created from the events in the "
        "file specified by the RECORD_IMPORTER_EVENTS_PATH config variable."
    ),
)
@click.option(
    "--downloads-field",
    default="custom_fields.hclegacy:total_downloads",
    help=(
        "The field (in dot notation) in each record to use for the number "
        "of downloads. If the --from-db flag is True, the field should be "
        "found in the database record metadata. If the --from-db flag is "
        "False, the field should be found in the record data object read "
        "from the import file."
    ),
)
@click.option(
    "--views-field",
    default="custom_fields.hclegacy:total_views",
    help=(
        "The field (in dot notation) in each record to use for the number "
        "of record views. If the --from-db flag is True, the field should be "
        "found in the database record metadata. If the --from-db flag is "
        "False, the field should be found in the record data object read "
        "from the import file. Defaults to 'hclegacy:total_views'."
    ),
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help=(
        "If True, information will be printed to the console as the stats "
        "events are created. Otherwise, no information will be printed "
        "until all the stats events are created."
    ),
)
@click.option(
    "--date-field",
    default="metadata.publication_date",
    help=(
        "The field (in dot notation) in each record to use for the record "
        "creation date. If the --from-db flag is True, the field should be "
        "found in the database record metadata. If the --from-db flag is "
        "False, the field should be found in the record data object read "
        "from the import file. Defaults to 'metadata.publication_date'."
    ),
)
@with_appcontext
def create_stats(
    from_db: bool,
    record_ids: list,
    downloads_field: str,
    views_field: str,
    record_source: str,
    date_field: str,
    verbose: bool,
) -> None:
    """
    Create events necessary for legacy usage statistics for imported records.

    This operation is idempotent, so it can be run multiple times without
    causing errors or creating duplicate events. Each time it will simply
    collect the events currently in the system for the given date range.

    params:
        record_ids: list
            A list of record ids to create usage statistics for. If not
            specified, all records from the specified source will be used.

        record_source: str
            The source of the records to create usage statistics for. If not
            specified, this will default to 'knowledgeCommons'. If records are
            being read from a file, the source file should be a JSONL file
            with this string as its name (without the .jsonl extension).

        from_db: bool
            If True, the usage statistics will be created from the database. If
            False, the usage statistics will be created from the events in the
            file specified by the RECORD_IMPORTER_USAGE_STATS_PATH config
            variable.

        downloads_field: str
            The field (in dot notation) in each record to use for the number
            of downloads. If the --from-db flag is True, the field should be
            found in the database record metadata. If the --from-db flag is
            False, the field should be found in the record data object read
            from the import file. Defaults to 'hclegacy:total_downloads'.

        views_field: str
            The field (in dot notation) in each record to use for the number
            of record views. If the --from-db flag is True, the field should be
            found in the database record metadata. If the --from-db flag is
            False, the field should be found in the record data object read
            from the import file. Defaults to 'hclegacy:total_views'.

        date_field: str
            The field (in dot notation) in each record to use for the record
            creation date. If the --from-db flag is True, the field should be
            found in the database record metadata. If the --from-db flag is
            False, the field should be found in the record data object read
            from the import file. Defaults to 'metadata.publication_date'.

        verbose: bool
            If True, information will be printed to the console as the stats
            events are created. Otherwise, no information will be printed
            until all the stats events are created.

    returns:
        None
    """
    print("Creating synthetic stats events from db records...")
    if record_ids:
        print(f"    for records {record_ids}...")
    else:
        print("    for all records...")

    print("    record source: ", record_source)
    print("    downloads field: ", downloads_field)
    print("    views field: ", views_field)
    print("    date field: ", date_field)

    args = {
        "record_ids": record_ids,
        "record_source": record_source,
        "downloads_field": downloads_field,
        "views_field": views_field,
        "date_field": date_field,
        "verbose": verbose,
    }

    if not from_db:
        StatsFabricator().fabricate_events_from_file(**args)
    else:
        StatsFabricator().fabricate_events_from_db(**args)
    print("All done creating stats events!")
    # Make alert sound to signal end of loading process
    print("\a")


@cli.command(name="aggregations")
@click.option(
    "--start-date",
    default=None,
    help=(
        "The start date for the record events to aggregate. If not specified, "
        "the aggregation will begin from the earliest creation date of the "
        "migrated records. The date should be formatted in ISO format, i.e. "
        "as 'YYYY-MM-DD'."
    ),
)
@click.option(
    "--end-date",
    default=None,
    help=(
        "The end date for the record events to aggregate. If not specified, "
        "the aggregation will end with the current date. The date should be "
        "formatted in ISO format, i.e. as 'YYYY-MM-DD'."
    ),
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help=(
        "If True, information will be printed to the console as the stats "
        "aggregations are created. Otherwise, no information will be printed "
        "until all the stats aggregations are created."
    ),
)
# @click.option(
#     "--eager",
#     is_flag=True,
#     default=False,
#     help=(
#         "If True, the aggregations will be created immediately. If False, "
#         "the aggregations will be created in a background task."
#     ),
# )
@with_appcontext
def create_aggregations(start_date, end_date, verbose):
    """
    Create usage stats aggregations for all records.

    This is a very time-consuming operation, but the operations are run as
    background tasks so as not to interfere with other operations. It can,
    however, overwhelm the search index with too many requests. So the
    operation is divided into 1 year chunks.

    The `start-date` and `end-date` parameters allow for specification of a
    range of dates for which to create aggregations. A start date is required.
    If it is not provided in the cli call, the start date will be taken from
    the RECORD_IMPORTER_START_DATE config variable. If that is not set, an
    error will be raised.  An end date is optional. If not end date is
    specified, the current date is used.

    NOTE: Currently this operation is intensive on memory and search index
    resources and can fail as a result. If you are aggregating a large
    number of stats events this can be mitigated by running the operation
    for smaller time ranges sequentially.

    This operation is idempotent, so it can be run multiple times without
    causing errors or creating duplicate aggregations. Each time it will
    simply collect the events currently in the system for the given date range.

    params:
        start_date: str
            The start date for the record events to aggregate. If not
            specified, the aggregation will begin from the earliest
            recorded usage event. The date should be formatted in ISO format,
            i.e. as 'YYYY-MM-DD'.

        end_date: str
            The end date for the record events to aggregate. If not specified,
            the aggregation will end with the current date. The date should be
            formatted in ISO format, i.e. as 'YYYY-MM-DD'.

        verbose: bool
            If True, information will be printed to the console as the stats
            aggregations are created. Otherwise, no information will be printed
            until all the stats aggregations are created.

    returns:
        None
    """
    print("Creating usage stats aggregations...")
    if not end_date:
        end_date = arrow.utcnow().naive.isoformat()
    if not start_date:
        start_date = arrow.get(
            app.config.get("RECORD_IMPORTER_START_DATE")
        ).naive.isoformat()
        if not start_date:
            raise ValueError("No start date specified")
    # If start_date is after end_date, swap them
    # If start_date is more than 1 year before end_date, divide the range
    # into 1 year chunks
    start_dt = arrow.get(start_date)
    end_dt = arrow.get(end_date)

    print("    start date: ", start_dt)
    print("    end date: ", end_dt)

    if start_dt < end_dt.shift(years=-1):
        if verbose:
            print("    start date is more than 1 year before end date")
            print("    dividing the range into 1 year chunks")
        # Divide the range into 1 year chunks
        inner_end_date = start_dt.shift(years=1)
        print("    start date: ", start_dt)
        print("    inner_end_date: ", inner_end_date)
        print(
            "    inner_end_date < arrow.get(end_date): ",
            inner_end_date < end_dt,
        )
        while inner_end_date < end_dt:
            if verbose:
                print(
                    "    creating aggregations for ",
                    start_dt,
                    inner_end_date,
                )
            AggregationFabricator().create_stats_aggregations(
                start_dt.naive,
                inner_end_date.naive,
                bookmark_override=start_dt.naive,
                verbose=verbose,
            )
            start_dt = inner_end_date
            inner_end_date = start_dt.shift(years=1)
    else:
        if verbose:
            print("    creating aggregations for ", start_dt, end_dt)
        AggregationFabricator().create_stats_aggregations(
            start_dt.naive,
            end_dt.naive,
            bookmark_override=start_dt.naive,
            verbose=verbose,
        )

    print("All done creating usage stats aggregations!")
    # Make alert sound to signal end of loading process
    print("\a")


if __name__ == "__main__":
    cli()
