# INVENIO RECORD IMPORTER

This is a command line utility to perform a bulk import of records into Knowledge Commons Works, an InvenioRDM instance. The utility could be adapted to work with other InvenioRDM installations, but in
its current form it assumes the customized metadata schema, database structures, and other configuration employed in Knowledge Commons Works.

This utility is designed to play two functions. (1) The first is to serve as a bulk importer for any batch of records coming from an external service or source. (2) The second is a specific legacy function--to convert legacy Humanities Commons CORE deposits to the metadata schema used in KC Works and prepare a json file for use by the importer.

This module adds a cli command to InvenioRDM's internal via the `console_commands` entry point. The command must be run within a Knowledge Commons Works instance of InvenioRDM. From the command line,
within the instance directory, run

```shell
pipenv run invenio importer
```

## Installation

This module should already be installed in a standard Knowledge Commons Works instance of InvenioRDM.
To install from source, go to your KC Works instance directory and run

```shell
pipenv install {path/to/invenio-record-importer}
```

## Setup

Prior to running the importer, a jsonlines file containing the serialized metadata records named `records-for-import.json` must be placed in the folder identified by the MIGRATION_SERVER_DATA_DIR environment variable. All files for the records to be imported should be placed in the folder identified by the MIGRATION_SERVER_FILES_LOCATION environment variable.

## Configuration

The importer relies on several environment variables:

| Variable name                   | Description                                                                                                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| MIGRATION_SERVER_DATA_DIR       | The folder where the file with serialized metadata records can be found, named `records-for-import.json`                                                           |
| MIGRATION_SERVER_FILES_LOCATION | The folder where the files for upload withe the new deposits may be found.                                                                                         |
| MIGRATION_SERVER_DATA_DIR       | The full path to the local directory where the source json files can be found. This is also where the serializer function will place the newly serialized records. |
| MIGRATION_API_TOKEN             | The authentication token required by the InvenioRDM REST api.                                                                                                      |

## Serializer usage

`pipenv run invenio importer serialize`

## Loader usage

The record loader is run within the knowledge_commons_repository instance directory like this:

```shell
pipenv run invenio importer load {RECORDS} {FLAGS}
```

If RECORDS is not specified, all records will be loaded. Otherwise,
RECORDS should be a list of positional arguments specifying which records
to load.

### Command-line arguments

A list of the provided positional arguments specifying which records to load. Defaults to [].

If no positional arguments are provided, all records will be loaded.

If positional arguments are provided, they should be either integers
specifying the line numbers of the records to load, or source ids
specifying the ids of the records to load in the source system.
These will be interpreted as line numbers in the jsonl file of
records for import (beginning at 1) unless the --use-sourceids flag
is set.

If a range is specified in the RECORDS by linking two integers with
a hyphen, the program will load all records between the two
indices, inclusive. If the range ends in a hyphen with no second
integer, the program will load all records from the start index to
the end of the input file.

### Command-line flags

| Flag                           | Description                                                                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| no_updates (bool, optional)    | If True, do not update existing records where a record with the same DOI already exists. Defaults to False.                     |
| retry_failed (bool, optional)  | If True, try to load in all previously failed records that have not already been repaired successfully. Defaults to False.      |
| use_sourceids (bool, optional) | If True, the positional arguments are interpreted as ids in the source system instead of positional indices. Defaults to False. |

### Examples:

To load records 1, 2, 3, and 5, run:

```shell
pipenv run invenio importer load 1 2 3 5
```

A range can be specified in the RECORDS by linking two integers with a
hyphen. For example, to load only the first 100 records, run:

```shell
pipenv run invenio importer load 1-100
```

If the range ends in a hyphen with no second integer, the program will
load all records from the start index to the end of the input file. For
example, to load all records from 100 to the end of the file, run:

```shell
pipenv run invenio importer load 100-
```

Records may be loaded by id in the source system instead of by index.
For example, to load records with ids hc:4723, hc:8271, and hc:2246,
run:

```shell
pipenv run invenio importer load --use-sourceids hc:4723 hc:8271 hc:2246
```

### Source file locations

The `load` command must be run from the base knowledge_commons_repository directory. It will look for the exported records in the directory specified by the MIGRATION_SERVER_DATA_DIR environment variable. It will send REST api requests to the knowledge_commons_repository instance specified by the MIGRATION_SERVER_DOMAIN environment variable.

### Authentication

The operations involved require authentication as an admin user in the knowledge_commons_repository instance. This program will look for the admin user's api token in the MIGRATION_API_TOKEN environment variable.

### Collections and record owners

Where necessary this program will create top-level domain communities, assign the records to the correct domain communities, create new Invenio users corresponding to the users who uploaded the original deposits, and transfer ownership of the Invenio record to the correct users. If the source of the records is associated with a SAML authentication IDP, these new users will be set to authenticate using their account with that IDP.

### Recovering existing records

If a record with the same DOI already exists in Invenio, the program will try to update the existing record with any new metadata and/or files, creating a new draft of published records if necessary. Unpublished existing drafts will be submitted to the appropriate community and published. Alternately, if the --no-updates flag is set, the program will skip any records that match DOIs for records that already exist in Invenio.

### Logging

Since the operations involved are time-consuming, the program should be run as a background process (adding & to the end of the command). A running log of the program's progress will be written to the file `invenio_record_importer.log` in the base `invenio_record_importer/logs` directory. A record of all records that have been touched (a load attempt has been made) is recorded in the file `invenio_record_importer_touched.json` in the base invenio_record_importer/logs directory. A record of all records that have failed to load is recorded in the file `invenio_record_importer_failed.json` in the `invenio_record_importer/logs` directory. If failed records are later successfully
repaired, they will be removed from the failed records file.

## Copyright

Copyright 2023-24 MESH Research. Released under the MIT license.
