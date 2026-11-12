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
        """Initialize the instance."""
        self.RECORD_IMPORTER_ADMIN_EMAIL = app.config.get("ADMIN_EMAIL", "")

        # Accounts role that receives import_records_action (see roles.py).
        self.RECORD_IMPORTER_BULK_IMPORT_ROLE = app.config.get(
            "RECORD_IMPORTER_BULK_IMPORT_ROLE",
            "bulk-importer",
        )

        # Max multipart body for /api/import (bytes). Align with nginx.
        self.RECORD_IMPORTER_MAX_CONTENT_LENGTH = app.config.get(
            "RECORD_IMPORTER_MAX_CONTENT_LENGTH",
            (10**10) * 50,  # 500 GB
        )

        # Remote URL fetch allowlists (empty = deny all).
        self.RECORD_IMPORTER_FETCH_ALLOWED_DOMAINS = app.config.get(
            "RECORD_IMPORTER_FETCH_ALLOWED_DOMAINS",
            [],
        )
        self.RECORD_IMPORTER_FETCH_ALLOWED_BUCKETS = app.config.get(
            "RECORD_IMPORTER_FETCH_ALLOWED_BUCKETS",
            [],
        )

        # When True, URL file fetches are enqueued on Celery (202 + job id).
        # When False, fetches run inline during the import request.
        self.RECORD_IMPORTER_FETCH_ASYNC = app.config.get(
            "RECORD_IMPORTER_FETCH_ASYNC",
            True,
        )

        # Async v2 staging: prefix on the existing Invenio S3 bucket.
        prefix = app.config.get("RECORD_IMPORTER_STAGING_PREFIX", "imports/")
        prefix = str(prefix)
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"
        self.RECORD_IMPORTER_STAGING_PREFIX = prefix
        # Defaults to S3_BUCKET_NAME when unset.
        self.RECORD_IMPORTER_STAGING_BUCKET = app.config.get(
            "RECORD_IMPORTER_STAGING_BUCKET",
            app.config.get("S3_BUCKET_NAME", ""),
        )
        # Files at or below this size use a single presigned PUT (not MPU).
        self.RECORD_IMPORTER_STAGING_SINGLE_PUT_MAX = app.config.get(
            "RECORD_IMPORTER_STAGING_SINGLE_PUT_MAX",
            app.config.get("S3_DEFAULT_BLOCK_SIZE", 5 * 2**20),
        )
        self.RECORD_IMPORTER_STAGING_URL_EXPIRATION = app.config.get(
            "RECORD_IMPORTER_STAGING_URL_EXPIRATION",
            app.config.get("S3_UPLOAD_URL_EXPIRATION", 3600 * 24 * 7),
        )
        self.RECORD_IMPORTER_STAGING_MAX_PARTS = app.config.get(
            "RECORD_IMPORTER_STAGING_MAX_PARTS",
            app.config.get("S3_MAXIMUM_NUMBER_OF_PARTS", 10000),
        )
        self.RECORD_IMPORTER_STAGING_PART_SIZE = app.config.get(
            "RECORD_IMPORTER_STAGING_PART_SIZE",
            app.config.get("S3_DEFAULT_BLOCK_SIZE", 5 * 2**20),
        )

        self.RECORD_IMPORTER_JOB_TTL_SECONDS = app.config.get(
            "RECORD_IMPORTER_JOB_TTL_SECONDS",
            60 * 60 * 24,
        )

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
                        "record_submission_policy": "closed",
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
