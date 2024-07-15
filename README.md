# INVENIO RECORD IMPORTER

This is a command line utility to perform a bulk import of records into Knowledge Commons Works, an InvenioRDM instance. The utility could be adapted to work with other InvenioRDM installations, but in
its current form it assumes the customized metadata schema, database structures, and other configuration employed in Knowledge Commons Works.

This utility is designed to play two functions.
(1) The first is to serve as a bulk importer for any batch of records coming from an external service or source.
(2) The second is a specific legacy function--to convert legacy Humanities Commons CORE deposits to the metadata schema used in KC Works and prepare a json file for use by the importer.

This module adds a cli command to InvenioRDM's internal via the `console_commands` entry point. The command must be run within a Knowledge Commons Works instance of InvenioRDM. From the command line,
within the instance directory, run

```shell
pipenv run invenio importer
```

## Commands

The importer has three commands: `serialize`, `read`, and `load`. These are described in detail below. The `serialize` command is used to serialize metadata records for import into InvenioRDM. The `read` command is used to read the metadata for records that are being imported. The `load` command is used to load the serialized metadata records into InvenioRDM.

## Installation

This module should already be installed in a standard Knowledge Commons Works instance of InvenioRDM.
To install from source, go to your KC Works instance directory and run

```shell
pipenv install {path/to/invenio-record-importer}
```

## Setup

Prior to running the importer, the required configuration variables listed below must be set either in the `invenio.cfg` file or as environment variables. A jsonlines file containing the serialized metadata records named `records-for-import.json` must also be placed in the folder identified by the MIGRATION_SERVER_DATA_DIR environment variable. All files for the records to be imported should be placed in the folder identified by the MIGRATION_SERVER_FILES_LOCATION environment variable.

## Configuration

The importer relies on several environment variables. These can be set in the `invenio.cfg` file of the InvenioRDM instance, or in a `.env` file in the base directory of the InvenioRDM instance. If they are set in the `.env` file they must be prefixed with `INVENIO_`.

| Variable name                   | Required | Description                                                                                                                                                        |
| ------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| RECORD_IMPORTER_ADMIN_EMAIL                     | Y | The email address of the admin user in the InvenioRDM instance. This defaults to the global ADMIN_EMAIL Flask environment variable.                                                                                                     |
| MIGRATION_SERVER_DATA_DIR       | Y | The folder where the file with serialized metadata records can be found, named `records-for-import.json`                                                           |
| MIGRATION_SERVER_FILES_LOCATION | N | The folder where the files for upload withe the new deposits may be found. It defaults to a subfolder of the MIGRATION_SERVER_DATA_DIR directory.                                                                                         |
| RECORD_IMPORTER_LOGS_LOCATION   | N | The full path to the local directory where the record import log files will be written. It defaults to the `logs` folder of the `invenio-record-importer` modules.                                                                                          |
| RECORD_IMPORTER_OVERRIDES_FOLDER | N | The full path to the local directory where the overrides files can be found. It defaults to the `app_data/importer` subfolder of the InvenioRDM instance folder (`/opt/invenio/src/app_data/importer` in the ui container).                                                                                       |
| RECORD_IMPORTER_TOUCHED_LOG_PATH | N | The full path to the local file where the touched records log will be written. It defaults to the `invenio_record_importer_touched.jsonl` file in the RECORD_IMPORTER_LOGS_LOCATION folder.                                                                                       |
| RECORD_IMPORTER_FAILED_LOG_PATH | N | The full path to the local file where the failed records log will be written. It defaults to the `invenio_record_importer_failed.jsonl` file in the RECORD_IMPORTER_LOGS_LOCATION folder.                                                                                       |
| RECORD_IMPORTER_SERIALIZED_FAILED_PATH | N | The full path to the local file where the serialized failed records will be written. It defaults to the `invenio_record_importer_failed_serialized.jsonl` file in the RECORD_IMPORTER_LOGS_LOCATION folder.                                                                                       |

## Serializer usage

The `serialize` command is run within the knowledge_commons_repository ui container like this:

```shell
`invenio importer serialize`
```

This will create a jsonl file named `records-for-import.json` in the directory specified by the MIGRATION_SERVER_DATA_DIR environment variable. This file will contain the serialized metadata records for import into InvenioRDM. There is at present no way to specify which records to serialize, so all records will be serialized.

### Metadata repair

The serializer will attempt to repair any metadata fields that are missing or have incorrect values. If a record has a missing or incorrect metadata field, the serializer will attempt to fill in the missing field with a value from a related field.

### Logging

Details about the program's progress are sent to Invenio's logging system as it runs. After each serializer run, a list of records with problematic metadata is written to the file at RECORD_IMPORTER_SERIALIZED_FAILED_PATH. Each line of this file is a json object listing metadata fields that the program has flagged as problematic for each record. The file is overwritten each time the serializer is run.

Note that in most cases a record with problematic metadata will still be serialized and included in the output file. The problems flagged by the serializer are usually limited to a single metadata field. Many are not true errors but rather pieces of metadata that require human review.

## Reader usage

The record reader is a convenience utility for retrieving the metadata for records that are being imported into InvenioRDM. It is not necessary to run the reader before running the loader, but it can be useful for debugging purposes. By default records are identified by their index in the jsonl file of records for import, but they can also be identified by their source id. The reader will output the metadata for the specified records to the console. By default it will print the serialized matadata as it will be fed into the loader, but it can also print the raw metadata as it exists prior to serialization.

The `read` command is run within the knowledge_commons_repository instance directory like this:

```shell
pipenv run invenio importer read {RECORDS} {FLAGS}
```

### Command-line arguments

A list of the provided positional arguments specifying which records to read. Defaults to [].

### Command-line flags

| Flag                           | Short flag | Description                                                                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| --raw_input (bool, optional)           | -r | If True, print the raw metadata for the specified records as it exists prior to serialization. Defaults to False.                                                   |
| --use-sourceids (bool, optional) | -s | If True, the positional arguments are interpreted as ids in the source system instead of positional indices. By default these ids are interpreted as DOI identifiers. If a different id scheme is desired, this may be set using the `--scheme` flag. Defaults to False. |
| --scheme (str, optional) | -m | The identifier scheme to use for the records when the --use-sourceids flag is True. Defaults to "doi". |
| --field-path (str, optional) | -f | The dot-separated path to a specific metadata field to be printed. If not specified, the entire record will be printed. |

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
| --no-updates (bool, optional)    | If True, do not update existing records where a record with the same DOI already exists. Defaults to False.                     |
| --retry-failed (bool, optional)  | If True, try to load in all previously failed records that have not already been repaired successfully. Defaults to False.      |
| --use-sourceids (bool, optional) | If True, the positional arguments are interpreted as ids in the source system instead of positional indices. Defaults to False. |
| --scheme (str, optional)         | The identifier scheme to use for the records when the --use-sourceids flag is True. Defaults to "hclegacy-pid" for the record ids used in the old CORE repository.                           |
| --aggregate (bool, optional)     | If True, run Invenio's usage statistics aggregation after importing the records. NOTE: This is a very expensive operation because it re-aggregates *all* usage events for *all* records. If this flag is True, it is strongly recommended that the `start_date` and `end_date` values also be used to define a window of time for the usage events to aggregate. Defaults to False.                                               |
| --start_date (str, optional)     | The start date for the usage statistics aggregation. Must be in the format "YYYY-MM-DD". Defaults to None.                      |
| --end_date (str, optional)       | The end date for the usage statistics aggregation. Must be in the format "YYYY-MM-DD". Defaults to None.                        |
| --clean_filenames (bool, optional) | If True, clean the filenames of the files to be uploaded. Defaults to False. |
| --verbose (bool, optional)       | If True, print verbose output during the loading of each record. Defaults to False.                                                                               |
| --stop_on_error (bool, optional) | If True, stop the loading process if an error is encountered. Defaults to False.                                                                                   |

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

The `load` command must be run from the base knowledge_commons_repository directory. It will look for the exported records in the directory specified by the MIGRATION_SERVER_DATA_DIR environment variable. It will look for the files to be uploaded in the directory specified by the MIGRATION_SERVER_FILES_LOCATION environment variable.

### Overriding metadata during loading

The loader can be configured to override metadata fields during loading. This is done by creating a jsonl file containing the metadata fields to be overridden, and placing it in the directory specified by the RECORD_IMPORTER_OVERRIDES_FOLDER environment variable. The file should be named `record-importer-overrides_mySourceService.json`, where `mySourceService` is the name of the source from which the import data is coming. If this source is a service configured with KCWorks as a SAML authentication provider, the name of the source should be the same as the name of the service in the SAML configuration.

The file should contain one json object per line, with no newline characters within each object. Each object must have the key "source_id" which contains the identifier of the record in the source system. The object may also contain the keys "overrides", "skip", and "notes". The "overrides" value must be a json object containing the metadata fields to be overridden. The "skip" value must be a boolean indicating whether the record should be skipped during loading. The "notes" value is a string that is ignored by invenio-record-importer but allows explanatory notes to be kept together with the override data.

The format of the overrides file should be as follows:

```json
{"source_id": "hc:12345", "overrides": {"metadata|title": "My overridden title", "custom_fields|hclegacy:file_location": "my/overridden/filename.pdf"}, "notes": "optional notes"}
{"source_id": "hc:678910", "skip": true, "notes": "optional notes"}

Note that the metadata field names should be path strings with steps separated by a pipe character. So if you want to update the "title" subfield of the top-level "metadata", the path string should be "metadata|title". If one level in the metadata hierarchy is a list/array, a number may be included in the string indicating which index in the list should be updated. So to update the "date" subfield of the list of "dates" in the "metadata" field, we would use the path string "metadata|dates|1|date".

Whatever value is provided for each path string will *entirely replace* the value at that point in the metadata hierarchy. So if I update "metadata|dates" I will need to provide the *entire list* of dates, even if there are items that should remain the same. If I only want to update one item I must specify it with a more specific path string like "metadata|dates|0".

Note that in some cases field names are namespaced by the schema they belong to. For example, the "file_location" field is in the "hclegacy" schema, so the path string for this field would be "custom_fields|hclegacy:file_location".

To uncover the metadata structure of the Invenio record being overridden, use the `read` command to print the metadata of the record to the terminal.

### Skipping records during loading

To skip a record from the serialized metadata during loading, add a line to the overrides file with the "skip" key set to true. The record will be skipped during loading, but will still be recorded in the touched records log. If a record is skipped, it will not be included in the failed records log and will be removed from that log if has previously failed.

### Authentication

Since the program interacts directly with InvenioRDM (and not via the REST API) it does not require separate authentication.

### Collections and record owners

Where necessary this program will create top-level domain communities, assign the records to the correct domain communities, create new Invenio users corresponding to the users who uploaded the original deposits, and transfer ownership of the Invenio record to the correct users. If the source of the records is associated with a SAML authentication IDP, these new users will be set to authenticate using their account with that IDP.

If the record was part of any group collections in the source system, the program will assign the record to the equivalent KCWorks group collections, creating new collections if necessary.

### Recovering existing records

If a record with the same DOI already exists in Invenio, the program will try to update the existing record with any new metadata and/or files, creating a new draft of published records if necessary. Unpublished existing drafts will be submitted to the appropriate community and published. Alternately, if the --no-updates flag is set, the program will skip any records that match DOIs for records that already exist in Invenio.

### Logging

Details about the program's progress are sent to Invenio's logging system as it runs. In addition, a running list of all records that have been touched (a load attempt has been made) is recorded in the file `invenio_record_importer_touched.json` in the RECORD_IMPORTER_LOGS_LOCATION directory. A record of all records that have failed to load is kept in the file `invenio_record_importer_failed.json` in the same directory. If failed records are later successfully
repaired, they will be removed from the failed records file.

## Copyright

Copyright 2023-24 MESH Research. Released under the MIT license.
