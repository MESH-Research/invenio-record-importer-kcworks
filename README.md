# INVENIO RECORD IMPORTER for Knowledge Commons Works

Version 0.3.0-alpha1

*ALPHA QUALITY SOFTWARE - USE AT OWN RISK*

**This documentation is out of date.**

This is a command line utility to perform a bulk import of records into Knowledge Commons Works, an InvenioRDM instance. The utility could be adapted to work with other InvenioRDM installations, but in its current form it assumes the customized metadata schema, database structures, and other configuration employed in Knowledge Commons Works.

This utility is designed to play two functions.
(1) The first is to serve as a bulk importer for any batch of records coming from an external service or source.
(2) The second is a specific legacy function--to convert legacy Humanities Commons CORE deposits to the metadata schema used in KC Works and prepare a json file for use by the importer.

This module adds a cli command to InvenioRDM's internal via the `console_commands` entry point. The command must be run within a Knowledge Commons Works instance of InvenioRDM. From the command line,
within the instance directory, run

```shell
pipenv run invenio importer
```

## Commands

The importer has three primary commands: `serialize`, `read`, and `load`. These are described in detail below. The `serialize` command is used to serialize metadata records for import into InvenioRDM. The `read` command is used to read the metadata for records that are being imported. The `load` command is used to load the serialized metadata records into InvenioRDM.

Two additional commands are provided for creating synthetic usage statistics to preserve stats coming from a legacy repository. These commands are `stats` and `aggregations`. They are described in the [Usage Statistics Preservation](#usage-statistics-preservation) section below.

## Installation

This module should already be installed in a standard Knowledge Commons Works instance of InvenioRDM.
To install from PyPI, run

```shell
pip install invenio-record-importer-kcworks
```

To install for development of this module, clone the repository and run

```shell
pipenv install -e .
```
from within the repository.

## Setup

Prior to running the importer, the required configuration variables listed below must be set either in the `invenio.cfg` file or as environment variables. A jsonlines file containing the serialized metadata records named `records-for-import.json` must also be placed in the folder identified by the RECORD_IMPORTER_DATA_DIR environment variable. All files for the records to be imported should be placed in the folder identified by the RECORD_IMPORTER_FILES_LOCATION environment variable.

## Dependencies

In addition to the normal InvenioRDM packages and KCWorks, this module relies in particular
on the following packages, which are also installed in the standard KCWorks environment:

- invenio-remote-user-data-kcworks
- invenio-group-collections-kcworks

Fetching of user data and creation of group collections relies on these packages being properly configured.

## Configuration

The importer relies on several environment variables. These can be set in the `invenio.cfg` file of the InvenioRDM instance, or in a `.env` file in the base directory of the InvenioRDM instance. If they are set in the `.env` file they must be prefixed with `INVENIO_`.

| Variable name                   | Required | Description                                                                                                                                                        |
| ------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| RECORD_IMPORTER_ADMIN_EMAIL                     | Y | The email address of the admin user in the InvenioRDM instance. This defaults to the global ADMIN_EMAIL Flask environment variable.                                                                                                     |
| RECORD_IMPORTER_DATA_DIR       | Y | The folder where the file with serialized metadata records can be found, named `records-for-import.json`                                                           |
| RECORD_IMPORTER_FILES_LOCATION | N | The folder where the files for upload withe the new deposits may be found. It defaults to a subfolder of the RECORD_IMPORTER_DATA_DIR directory.                                                                                         |
| RECORD_IMPORTER_LOGS_LOCATION   | N | The full path to the local directory where the record import log files will be written. It defaults to the `logs` folder of the `invenio-record-importer-kcworks` modules.                                                                                          |
| RECORD_IMPORTER_OVERRIDES_FOLDER | N | The full path to the local directory where the overrides files can be found. It defaults to the `overrides` subfolder of the folder at RECORD_IMPORTER_DATA_DIR.                                                                                       |
| RECORD_IMPORTER_CREATED_LOG_PATH | N | The full path to the local file where the created records log will be written. It defaults to the `record_importer_created_records.jsonl` file in the RECORD_IMPORTER_LOGS_LOCATION folder.                                                                                       |
| RECORD_IMPORTER_FAILED_LOG_PATH | N | The full path to the local file where the failed records log will be written. It defaults to the `record_importer_failed_records.jsonl` file in the RECORD_IMPORTER_LOGS_LOCATION folder.                                                                                       |
| RECORD_IMPORTER_SERIALIZED_PATH | N | The full path to the local file where the serialized records will be written. It defaults to the `record_importer_serialized_records.jsonl` file in the RECORD_IMPORTER_DATA_DIR folder.                                                                                       |
| RECORD_IMPORTER_SERIALIZED_FAILED_PATH | N | The full path to the local file where the serialized failed records will be written. It defaults to the `record_importer_failed_serialized.jsonl` file in the RECORD_IMPORTER_LOGS_LOCATION folder.                                                                                       |

The required folders must of course be created before the importer is run. The importer will not create these folders if they do not exist. The various log files and serialized records files will be created by the importer if they do not already exist.

Remember that in containerized environments, the directories holding data and logs must be persistent. In a Docker environment, these directories should be mounted as volumes in the container.

## Serializer usage

The `serialize` command is run within the knowledge_commons_repository ui container like this:

```shell
invenio importer serialize
```

This command serializes metadata records for import into InvenioRDM. It reads records from a source file, processes them, and writes the serialized records to a JSON Lines file. The command also performs metadata repair and logs any problematic records.

### Command-line flags

| Flag                           | Short flag | Description                                                                                                                     |
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------- |
| --start-index INTEGER          | -s         | The index of the first record to serialize (1-based). Defaults to 1.                                                             |
| --end-index INTEGER            | -e         | The index of the last record to serialize (inclusive). If not provided, will serialize to the end of the input file.            |
| --verbose / --no-verbose       | -v / -q    | Enable or disable verbose output. Defaults to False.                                                                             |

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
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------- |
| --raw_input (bool, optional)           | -r | If True, print the raw metadata for the specified records as it exists prior to serialization. Defaults to False.                                                   |
| --use-sourceids (bool, optional) | -s | If True, the positional arguments are interpreted as ids in the source system instead of positional indices. By default these ids are interpreted as DOI identifiers. If a different id scheme is desired, this may be set using the `--scheme` flag. Defaults to False. |
| --scheme (str, optional) | -m | The identifier scheme to use for the records when the --use-sourceids flag is True. Defaults to "doi". |
| --field-path (str, optional) | -f | The dot-separated path to a specific metadata field to be printed. If not specified, the entire record will be printed. |

## Loader usage

The record loader is run within the knowledge_commons_repository instance directory like this:

```shell
pipenv run invenio importer load {RECORDS} {FLAGS}
```

This command loads serialized metadata records into InvenioRDM. It reads records from a JSON Lines file, processes them, and creates or updates records in InvenioRDM accordingly.

### Command-line arguments

A list of the provided positional arguments specifying which records to load. Defaults to [].

If no positional arguments are provided, all records will be loaded.

If positional arguments are provided, they should be either integers specifying the line numbers of the records to load, or source ids specifying the ids of the records to load in the source system. These will be interpreted as line numbers in the jsonl file of records for import (beginning at 1) unless the --use-sourceids flag is set.

If a range is specified in the RECORDS by linking two integers with a hyphen, the program will load all records between the two indices, inclusive. If the range ends in a hyphen with no second integer, the program will load all records from the start index to the end of the input file.

### Command-line flags

| Flag                           | Short flag | Description                                                                                                                     |
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------- |
| --no-updates                   | -n         | If set, do not update existing records where a record with the same DOI already exists. Defaults to False.                       |
| --retry-failed                 | -r         | If set, try to load in all previously failed records that have not already been repaired successfully. Defaults to False.        |
| --use-sourceids                | -s         | If set, the positional arguments are interpreted as ids in the source system instead of positional indices. Defaults to False.   |
| --scheme TEXT                  | -m         | The identifier scheme to use for the records when the --use-sourceids flag is True. Defaults to "hclegacy-pid".                  |
| --aggregate                    | -a         | If set, run Invenio's usage statistics aggregation after importing the records. Defaults to False.                               |
| --start_date TEXT              |            | The start date for the usage statistics aggregation. Must be in the format "YYYY-MM-DD". Defaults to None.                       |
| --end_date TEXT                |            | The end date for the usage statistics aggregation. Must be in the format "YYYY-MM-DD". Defaults to None.                         |
| --clean_filenames              | -c         | If set, clean the filenames of the files to be uploaded. Defaults to False.                                                      |
| --verbose / --no-verbose       | -v / -q    | Enable or disable verbose output. Defaults to False.                                                                             |
| --stop_on_error / --no-stop_on_error | -e / -E | If set, stop the loading process if an error is encountered. Defaults to False.                                                |

### Examples:

To load records 1, 2, 3, and 5, run:

```shell
pipenv run invenio importer load 1 2 3 5
```

A range can be specified in the RECORDS by linking two integers with a hyphen. For example, to load only the first 100 records, run:

```shell
pipenv run invenio importer load 1-100
```

If the range ends in a hyphen with no second integer, the program will load all records from the start index to the end of the input file. For example, to load all records from 100 to the end of the file, run:

```shell
pipenv run invenio importer load 100-
```

Records may be loaded by id in the source system instead of by index. For example, to load records with ids hc:4723, hc:8271, and hc:2246, run:

```shell
pipenv run invenio importer load --use-sourceids hc:4723 hc:8271 hc:2246
```

### Source file locations

The `load` command must be run from the base knowledge_commons_repository directory. It will look for the exported records in the directory specified by the RECORD_IMPORTER_DATA_DIR environment variable. It will look for the files to be uploaded in the directory specified by the RECORD_IMPORTER_FILES_LOCATION environment variable.

### Overriding metadata during loading

The loader can be configured to override metadata fields during loading. This is done by creating a jsonl file containing the metadata fields to be overridden, and placing it in the directory specified by the RECORD_IMPORTER_OVERRIDES_FOLDER environment variable. The file should be named `record-importer-overrides_mySourceService.json`, where `mySourceService` is the name of the source from which the import data is coming. If this source is a service configured with KCWorks as a SAML authentication provider, the name of the source should be the same as the name of the service in the SAML configuration.

The file should contain one json object per line, with no newline characters within each object. Each object must have the key "source_id" which contains the identifier of the record in the source system. The object may also contain the keys "overrides", "skip", and "notes". The "overrides" value must be a json object containing the metadata fields to be overridden. The "skip" value must be a boolean indicating whether the record should be skipped during loading. The "notes" value is a string that is ignored by invenio-record-importer-kcworks but allows explanatory notes to be kept together with the override data.

The format of the overrides file should be as follows:

```json
{"source_id": "hc:12345", "overrides": {"metadata|title": "My overridden title", "custom_fields|hclegacy:file_location": "my/overridden/filename.pdf"}, "notes": "optional notes"}
{"source_id": "hc:678910", "skip": true, "notes": "optional notes"}
```

Note that the metadata field names should be path strings with steps separated by a pipe character. So if you want to update the "title" subfield of the top-level "metadata", the path string should be "metadata|title". If one level in the metadata hierarchy is a list/array, a number may be included in the string indicating which index in the list should be updated. So to update the "date" subfield of the list of "dates" in the "metadata" field, we would use the path string "metadata|dates|1|date".

Whatever value is provided for each path string will *entirely replace* the value at that point in the metadata hierarchy. So if I update "metadata|dates" I will need to provide the *entire list* of dates, even if there are items that should remain the same. If I only want to update one item I must specify it with a more specific path string like "metadata|dates|0".

Note that in some cases field names are namespaced by the schema they belong to. For example, the "file_location" field is in the "hclegacy" schema, so the path string for this field would be "custom_fields|hclegacy:file_location".

To uncover the metadata structure of the Invenio record being overridden, use the `read` command to print the metadata of the record to the terminal.

### Skipping records during loading

To skip a record from the serialized metadata during loading, add a line to the overrides file with the "skip" key set to true. The record will be skipped during loading, but will still be recorded in the created records log. If a record is skipped, it will not be included in the failed records log and will be removed from that log if has previously failed.

### Authentication

Since the program interacts directly with InvenioRDM (and not via the REST API) it does not require separate authentication.

### Collections and record owners

Where necessary this program will create top-level domain communities, assign the records to the correct domain communities, create new Invenio users corresponding to the users who uploaded the original deposits, and transfer ownership of the Invenio record to the correct users. If the source of the records is associated with a SAML authentication IDP, these new users will be set to authenticate using their account with that IDP.

If the record was part of any group collections in the source system, the program will assign the record to the equivalent KCWorks group collections, creating new collections if necessary.

### Recovering existing records

If a record with the same DOI already exists in Invenio, the program will try to update the existing record with any new metadata and/or files, creating a new draft of published records if necessary. Unpublished existing drafts will be submitted to the appropriate community and published. Alternately, if the --no-updates flag is set, the program will skip any records that match DOIs for records that already exist in Invenio.

### Logging

Details about the program's progress are sent to Invenio's logging system as it runs. In addition, a running list of all records that have been created (a load attempt has been made) is recorded in the file `record_importer_created_records.json` in the RECORD_IMPORTER_LOGS_LOCATION directory. A record of all records that have failed to load is kept in the file `record_importer_failed_records.json` in the same directory. If failed records are later successfully repaired, they will be removed from the failed records file.

## Usage Statistics Preservation

Two additional commands are provided for creating synthetic usage statistics to preserve stats coming from a legacy repository. These commands are `stats` and `aggregations`.

### Stats Command

The `stats` command is used to generate synthetic usage statistics for imported records. It's run within the knowledge_commons_repository instance directory like this:

```shell
invenio importer stats {FLAGS}
```

This command creates synthetic usage events for records imported from a legacy system. It reads the created records log and generates view and download events based on the legacy statistics.

This operation is idempotent, so it can be run multiple times without causing errors or creating duplicate events. Each time it will simply collect the events currently in the system for the given date range.

#### Command-line flags

| Flag                           | Short flag | Description                                                                                                                     |
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------- |
| --start-date TEXT              | -s         | The start date for generating events. Must be in the format "YYYY-MM-DD". Defaults to "2013-01-01".                              |
| --end-date TEXT                | -e         | The end date for generating events. Must be in the format "YYYY-MM-DD". Defaults to the current date.                            |
| --verbose        | -v         | Enable or disable verbose output. Defaults to False.                                                                             |
| --record-ids     | -r         | A comma-separated list of record ids to create usage statistics for. If not specified, all records from the specified source will be used. |
| --record-source  | -S         | The source of the records to create usage statistics for. Defaults to 'knowledgeCommons'. |
| --from-db        | -d         | If True, the usage statistics will be created from the database. Defaults to False. |
| --downloads-field | -D         | The field in each record to use for the number of downloads. Defaults to 'hclegacy:total_downloads'. |
| --views-field    | -V         | The field in each record to use for the number of record views. Defaults to 'hclegacy:total_views'. |
| --date-field     | -t         | The field in each record to use for the record creation date. Defaults to 'metadata.publication_date'. |

##### Explanation of flags:

- `--start-date`: This flag allows you to specify the earliest date for which synthetic events should be generated.

- `--end-date`: This flag sets the latest date for which synthetic events should be generated.

- `--verbose`: When verbose mode is enabled, the command will provide detailed output about its progress, including information about each record being processed.

- `--record-ids`: This flag allows you to specify a list of record ids to create usage statistics for. If not specified, all records from the specified source will be used.

- `--record-source`: This flag sets the source of the records to create usage statistics for. If not specified, this will default to 'knowledgeCommons'. If records are being read from a file, the source file should be a JSONL file with this string as its name (without the .jsonl extension).

- `--from-db`: This flag determines whether the usage statistics will be created from the database or from a file. If True, the usage statistics will be created from the database. If False, the usage statistics will be created from the events in the file specified by the RECORD_IMPORTER_USAGE_STATS_PATH config variable.

- `--downloads-field`: This flag sets the field in each record to use for the number of downloads. If the --from-db flag is True, the field should be found in the database record metadata. If the --from-db flag is False, the field should be found in the record data object read from the import file. Defaults to 'hclegacy:total_downloads'.

- `--views-field`: This flag sets the field in each record to use for the number of record views. If the --from-db flag is True, the field should be found in the database record metadata. If the --from-db flag is False, the field should be found in the record data object read from the import file. Defaults to 'hclegacy:total_views'.

- `--date-field`: This flag sets the field in each record to use for the record creation date. If the --from-db flag is True, the field should be found in the database record metadata. If the --from-db flag is False, the field should be found in the record data object read from the import file. Defaults to 'metadata.publication_date'.


### Aggregations Command

The `aggregations` command is used to aggregate the synthetic usage statistics generated by the `stats` command. It's run within the knowledge_commons_repository instance directory like this:

```shell
invenio importer aggregations {FLAGS}
```

This command aggregates usage statistics for records in the InvenioRDM instance. It processes the usage events and creates aggregated statistics.

This operation is idempotent, so it can be run multiple times without causing errors or creating duplicate aggregations. Each time it will simply collect the events currently in the system for the given date range.

This is a very time-consuming operation, but the operations are run as
background tasks so as not to interfere with other operations. It can,
however, overwhelm the search index with too many requests. So the
operation is divided into 1 year chunks.

The `start-date` and `end-date` parameters allow for specification of a
range of dates for which to create aggregations. A start date is required.
If it is not provided in the cli call, the start date will be taken from
the RECORD_IMPORTER_START_DATE config variable. If that is not set, an
will be raised.  An end date is optional. If not end date is
specified, the current date is used.

!NOTE: Currently this operation is intensive on memory and search index
resources and can fail as a result. If you are aggregating a large
number of stats events this can be mitigated by running the operation
for smaller time ranges sequentially.


#### Command-line flags

| Flag                           | Short flag | Description                                                                                                                     |
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------- |
| --start-date TEXT              | -s         | The start date for aggregating events. Must be in the format "YYYY-MM-DD". If not provided, will aggregate all events.           |
| --end-date TEXT                | -e         | The end date for aggregating events. Must be in the format "YYYY-MM-DD". If not provided, will aggregate up to the current date. |
| --verbose / --no-verbose       | -v / -q    | Enable or disable verbose output. Defaults to False.                                                                             |

##### Explanation of flags:

- `--start-date`: This flag determines the earliest date from which to start aggregating events. Any events before this date will not be included in the aggregation. If not specified, the command will include all available events from the beginning of the dataset.

- `--end-date`: This flag sets the latest date up to which events should be aggregated. Events after this date will not be included. If not provided, the command will aggregate events up to the current date, including the most recent data available.

- `--verbose / --no-verbose`: When enabled, this flag causes the command to provide detailed information about the aggregation process. This can include progress updates, information about each record being processed, and any issues encountered. It's particularly useful for monitoring long-running aggregations or troubleshooting.

### Usage Statistics Workflow

To preserve usage statistics from a legacy system:

1. First, run the `stats` command to generate synthetic usage events based on the legacy statistics:

   ```shell
   invenio importer stats --start-date "2013-01-01" --end-date "2023-12-31"
   ```

2. Then, run the `aggregations` command to aggregate these synthetic events:

   ```shell
   invenio importer aggregations --start-date "2013-01-01" --end-date "2023-12-31"
   ```

This process will create and aggregate synthetic usage statistics that reflect the usage patterns from the legacy system. By adjusting the date ranges with the `--start-date` and `--end-date` flags, you can control exactly which period of legacy data is represented in your new system.

## Utility Commands

### Count Objects Command

The `count` command is used to count the number of objects in the JSON file prepared for import. It's run within the knowledge_commons_repository instance directory like this:

```shell
invenio importer count
```

This command reads the serialized records file and counts the number of objects (records) it contains. The file location is specified by the `RECORD_IMPORTER_SERIALIZED_PATH` config variable.

#### Usage

This command doesn't take any arguments or flags. Simply run it to get a count of the objects in the import file.

#### Output

The command will print the total number of objects found in the specified file. For example:

```
Total objects in /path/to/your/file.json: 1000
```

If the file is not found at the specified location, an error message will be displayed.

### Delete Records Command

The `delete` command is used to delete one or more records from InvenioRDM by their record ID. It's run within the knowledge_commons_repository instance directory like this:

```shell
invenio importer delete {RECORD_IDS}
```

This command allows you to remove specific records from the InvenioRDM database.

#### Command-line arguments

A list of record IDs to be deleted. You can specify multiple IDs separated by spaces.

#### Usage Examples

To delete a single record:

```shell
invenio importer delete record_id_1
```

To delete multiple records:

```shell
invenio importer delete record_id_1 record_id_2 record_id_3
```

#### Output

The command will print the results of the deletion operation, showing which records were successfully deleted and any errors that occurred during the process.

### Caution

The delete operation is irreversible. Make sure you have backups of your data before deleting records, especially in a production environment.

## Testing

This project includes a test suite to ensure the functionality of the importer. To run the tests, use the `run-tests.sh` script provided in the root directory of the project.

### Running Tests

To run the tests, execute the following command from the project root:

```bash
./run-tests.sh
```

This script performs the following actions:

1. Checks if the required Python version (3.12) is installed.
2. Sets up a virtual environment using pipenv if it doesn't exist.
3. Installs the project dependencies.
4. Runs the pytest suite with coverage reporting.

### Test Coverage

The `run-tests.sh` script includes coverage reporting. After running the tests, you'll see a coverage report in the terminal output. This report shows which parts of the code were executed during the tests and helps identify areas that may need additional testing.

### Continuous Integration

The test suite is also integrated into our KCWorks CI/CD pipeline. Any push to the repository or pull request will trigger the test suite to run, ensuring that new changes don't break existing functionality.

### Writing New Tests

When adding new features or fixing bugs, it's important to add corresponding tests. Place new test files in the `tests/` directory, following the existing naming conventions (e.g., `test_<module_name>.py`).

Remember to run the test suite locally before pushing changes to ensure all tests pass.

## Developing this Module

### Testing

This project includes a test suite to ensure the functionality of the importer. To run the tests, use the `run-tests.sh` script provided in the root directory of the project.

#### Running Tests

To run the tests, execute the following command from the project root:

```bash
./run-tests.sh
```

This script performs the following actions:

1. Checks if the required Python version (3.12) is installed.
2. Sets up a virtual environment using pipenv if it doesn't exist.
3. Installs the project dependencies.
4. Runs the pytest suite with coverage reporting.

#### Test Coverage

The `run-tests.sh` script includes coverage reporting. After running the tests, you'll see a coverage report in the terminal output. This report shows which parts of the code were executed during the tests and helps identify areas that may need additional testing.

#### Continuous Integration

The test suite is also integrated into our KCWorks CI/CD pipeline. Any push to the repository or pull request will trigger the test suite to run, ensuring that new changes don't break existing functionality.

### Writing New Tests

When adding new features or fixing bugs, it's important to add corresponding tests. Place new test files in the `tests/` directory, following the existing naming conventions (e.g., `test_<module_name>.py`).

Remember to run the test suite locally before pushing changes to ensure all tests pass.

### Versioning

This project uses [bumpversion](https://github.com/peritus/bumpversion) to manage versioning. The version is stored in the `pyproject.toml` file and is updated automatically when a new release is made.

For example, to update the version numbers for a new 'patch' release, from 0.2.20 to 0.2.21, run the following command:

```bash
pipenv run bumpver update --patch
```

To update the tag number, from 'alpha6' to 'alpha7', run the following command:

```bash
pipenv run bumpver update --tag
```

This will update the version throughout this project's files, create a new commit, and tag the commit with the new version.

## Copyright

Copyright 2023-24 MESH Research. Released under the MIT license.
