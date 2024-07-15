#! /usr/bin/env python

"""Importer configuration.

Allows setting configuration variables either through the Flask application
configuration or through environment variables. Also provides sane defaults
where applicable. Flask configuration variables take precedence over
environment variables.
"""

from pathlib import Path


class ImporterConfig:

    def __init__(self, app):
        self.RECORD_IMPORTER_ADMIN_EMAIL = app.config.get("ADMIN_EMAIL", "")

        self.RECORD_IMPORTER_DATA_DIR = Path(
            app.config.get(
                "RECORD_IMPORTER_DATA_DIR",
                Path(Path(__file__).parent, "data"),
            )
        )

        self.RECORD_IMPORTER_FILES_LOCATION = Path(
            app.config.get(
                "RECORD_IMPORTER_FILES_LOCATION",
                Path(self.RECORD_IMPORTER_DATA_DIR / "import_files"),
            )
        )

        self.RECORD_IMPORTER_OVERRIDES_FOLDER = Path(
            app.config.get(
                "RECORD_IMPORTER_OVERRIDES_FOLDER",
                self.RECORD_IMPORTER_DATA_DIR,
            )
        )

        self.RECORD_IMPORTER_LOGS_LOCATION = Path(
            app.config.get(
                "RECORD_IMPORTER_LOGS_LOCATION",
                Path(Path(__file__).parent, "logs"),
            )
        )

        self.RECORD_IMPORTER_FAILED_LOG_PATH = Path(
            app.config.get(
                "RECORD_IMPORTER_FAILED_LOG_PATH",
                Path(
                    self.RECORD_IMPORTER_LOGS_LOCATION,
                    "invenio_record_importer_failed.jsonl",
                ),
            )
        )

        self.RECORD_IMPORTER_TOUCHED_LOG_PATH = Path(
            app.config.get(
                "RECORD_IMPORTER_TOUCHED_LOG_PATH",
                Path(
                    self.RECORD_IMPORTER_LOGS_LOCATION,
                    "invenio_record_importer_touched.jsonl",
                ),
            )
        )

        self.RECORD_IMPORTER_SERIALIZED_FAILED_PATH = Path(
            app.config.get(
                "RECORD_IMPORTER_SERIALIZED_FAILED_PATH",
                Path(
                    self.RECORD_IMPORTER_LOGS_LOCATION,
                    "invenio_record_importer_serialized_failed.jsonl",
                ),
            )
        )
