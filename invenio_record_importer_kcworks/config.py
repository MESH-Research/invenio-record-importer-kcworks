#! /usr/bin/env python

"""Importer configuration.

Allows setting configuration variables either through the Flask application
configuration or through environment variables. Also provides sane defaults
where applicable. Flask configuration variables take precedence over
environment variables.
"""

from pathlib import Path

import arrow
from invenio_records_resources.services.base.config import (
    ConfiguratorMixin,
    ServiceConfig,
)

from .permissions import RecordImporterPermissionPolicy
from .results import ImportResult, ImportResultsList


class RecordImporterServiceConfig(ServiceConfig, ConfiguratorMixin):
    """Configuration for the record importer service."""

    service_id = "record-importer-kcworks"
    permission_policy_cls = RecordImporterPermissionPolicy
    result_item_cls = ImportResult
    result_list_cls = ImportResultsList


class ConfigVariables:
    """Configuration variables for the record importer service."""

    def __init__(self, app):
        self.RECORD_IMPORTER_ADMIN_EMAIL = app.config.get("ADMIN_EMAIL", "")

        self.RECORD_IMPORTER_DATA_DIR = app.config.get(
            "RECORD_IMPORTER_DATA_DIR",
            Path(Path(__file__).parent, "data"),
        )

        self.RECORD_IMPORTER_FILES_LOCATION = app.config.get(
            "RECORD_IMPORTER_FILES_LOCATION",
            Path(self.RECORD_IMPORTER_DATA_DIR, "import_files"),
        )

        self.RECORD_IMPORTER_OVERRIDES_FOLDER = app.config.get(
            "RECORD_IMPORTER_OVERRIDES_FOLDER",
            Path(self.RECORD_IMPORTER_DATA_DIR, "overrides"),
        )

        self.RECORD_IMPORTER_LOGS_LOCATION = app.config.get(
            "RECORD_IMPORTER_LOGS_LOCATION",
            Path(Path(__file__).parent, "logs"),
        )

        self.RECORD_IMPORTER_FAILED_LOG_PATH = app.config.get(
            "RECORD_IMPORTER_FAILED_LOG_PATH",
            f"{self.RECORD_IMPORTER_LOGS_LOCATION}/"
            "record_importer_failed_records_{}.jsonl",
        )

        self.RECORD_IMPORTER_CREATED_LOG_PATH = app.config.get(
            "RECORD_IMPORTER_CREATED_LOG_PATH",
            f"{self.RECORD_IMPORTER_LOGS_LOCATION}/"
            "record_importer_created_records_{}.jsonl",
        )

        self.RECORD_IMPORTER_SERIALIZED_PATH = app.config.get(
            "RECORD_IMPORTER_SERIALIZED_PATH",
            f"{self.RECORD_IMPORTER_DATA_DIR}/"
            "record_importer_source_metadata_{}.jsonl",
        )

        self.RECORD_IMPORTER_SERIALIZED_FAILED_PATH = app.config.get(
            "RECORD_IMPORTER_SERIALIZED_FAILED_PATH",
            f"{self.RECORD_IMPORTER_LOGS_LOCATION}/"
            "record_importer_serialized_failed_{}.jsonl",
        )

        self.RECORD_IMPORTER_USAGE_STATS_PATH = app.config.get(
            "RECORD_IMPORTER_USAGE_STATS_PATH",
            f"{self.RECORD_IMPORTER_DATA_DIR}/usage_stats_{{}}.jsonl",
        )

        self.RECORD_IMPORTER_START_DATE = app.config.get(
            "RECORD_IMPORTER_START_DATE",
            arrow.get("2015-01-01").isoformat(),
        )

        self.RECORD_IMPORTER_COMMUNITIES_DATA = {
            "knowledgeCommons": {
                "kcommons": {
                    "slug": "kcommons",
                    "metadata": {
                        "title": "Knowledge Commons",
                        "description": ("A collection representing Knowledge Commons"),
                        "website": "https://hcommons.org",
                        "organizations": [{"name": "Knowledge Commons"}],
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
                        "description": ("A collection representing HASTAC Commons"),
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
        }
