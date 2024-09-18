# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 MESH Research
#
# invenio-record-importer is free software; you can redistribute it
# and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Pytest configuration for invenio-remote-api-provisioner.

See https://pytest-invenio.readthedocs.io/ for documentation on which test
fixtures are available.
"""


# from traceback import format_exc
import pytest
from invenio_app.factory import create_api
from invenio_communities.proxies import current_communities
from invenio_queues.proxies import current_queues
from invenio_rdm_records.proxies import current_rdm_records
from invenio_record_importer.serializer import serialize_json
from invenio_records_resources.proxies import current_service_registry

import os
from pathlib import Path
from .fixtures.identifiers import test_config_identifiers
from .fixtures.custom_fields import test_config_fields
from .fixtures.stats import test_config_stats
from .fixtures.saml import test_config_saml

pytest_plugins = [
    "celery.contrib.pytest",
    "tests.fixtures.communities",
    "tests.fixtures.custom_fields",
    "tests.fixtures.records",
    "tests.fixtures.stats",
    "tests.fixtures.users",
    "tests.fixtures.vocabularies.affiliations",
    "tests.fixtures.vocabularies.community_types",
    "tests.fixtures.vocabularies.date_types",
    "tests.fixtures.vocabularies.descriptions",
    "tests.fixtures.vocabularies.languages",
    "tests.fixtures.vocabularies.licenses",
    "tests.fixtures.vocabularies.resource_types",
    "tests.fixtures.vocabularies.roles",
    "tests.fixtures.vocabularies.subjects",
]


def _(x):
    """Identity function for string extraction."""
    return x


# @pytest.fixture(scope="module")
# def extra_entry_points():
#     return {}


@pytest.fixture(scope="module")
def subjects_service(app):
    return current_service_registry.get("subjects")


@pytest.fixture(scope="module")
def communities_service(app):
    return current_communities.service


@pytest.fixture(scope="module")
def records_service(app):
    return current_rdm_records.records_service


test_config = {
    **test_config_identifiers,
    **test_config_fields,
    **test_config_stats,
    **test_config_saml,
    "SQLALCHEMY_DATABASE_URI": (
        "postgresql+psycopg2://invenio:invenio@localhost:5432/invenio"
    ),
    "SQLALCHEMY_TRACK_MODIFICATIONS": True,
    "APP_ALLOWED_HOSTS": [
        "0.0.0.0",
        "localhost",
        "127.0.0.1",
        "192.168.0.15",  # Ian's dev machine internal
        "192.168.0.16",  # Ian's dev machine internal
    ],
    "INVENIO_WTF_CSRF_ENABLED": False,
    "INVENIO_WTF_CSRF_METHODS": [],
    "APP_DEFAULT_SECURE_HEADERS": {
        "content_security_policy": {"default-src": []},
        "force_https": False,
    },
    "SECRET_KEY": "test-secret-key",
    "SECURITY_PASSWORD_SALT": "test-secret-key",
    "TESTING": True,
    "SEARCH_INDEX_PREFIX": "",
}

parent_path = Path(__file__).parent.parent

raw_data_path = parent_path / "tests/helpers/sample_records"
test_config["RECORD_IMPORTER_DATA_DIR"] = str(raw_data_path)

test_config["RECORD_IMPORTER_SERIALIZED_PATH"] = str(
    raw_data_path / "record_importer_serialized_records.jsonl"
)

log_file_path = (
    parent_path / "invenio_record_importer" / "logs" / "invenio.log"
)
if not log_file_path.exists():
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    log_file_path.touch()

test_config["FLASK_DEBUG"] = True
test_config["LOGGING_FS_LEVEL"] = "DEBUG"
test_config["INVENIO_LOGGING_FS_LEVEL"] = "DEBUG"
test_config["LOGGING_FS_LOGFILE"] = str(log_file_path)

# enable DataCite DOI provider
test_config["DATACITE_ENABLED"] = True
test_config["DATACITE_USERNAME"] = "INVALID"
test_config["DATACITE_PASSWORD"] = "INVALID"
test_config["DATACITE_DATACENTER_SYMBOL"] = "TEST"
test_config["DATACITE_PREFIX"] = "10.17613"
test_config["DATACITE_TEST_MODE"] = True
# ...but fake it


test_config["SITE_UI_URL"] = os.environ.get(
    "INVENIO_SITE_UI_URL", "https://127.0.0.1:5000"
)

test_config["RECORD_IMPORTER_COMMUNITIES_DATA"] = {
    "knowledgeCommons": {
        "kcommons": {
            "slug": "kcommons",
            "metadata": {
                "title": "Knowledge Commons",
                "description": ("A collection representing Knowledge Commons"),
                "website": "https://kcommons.org",
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


@pytest.fixture(scope="module")
def serialized_records(app):
    actual_serialized_json, actual_bad_data = serialize_json()
    return {
        "actual_serialized_json": actual_serialized_json,
        "actual_bad_data": actual_bad_data,
    }


@pytest.fixture(scope="module")
def app(
    app,
    app_config,
    database,
    search,
    affiliations_v,
    contributors_role_v,
    date_type_v,
    creators_role_v,
    licenses_v,
    subject_v,
    community_type_v,
    resource_type_v,
    description_type_v,
    language_v,
    create_communities_custom_fields,
    create_records_custom_fields,
):
    """Application with database and search."""
    current_queues.declare()
    yield app


@pytest.fixture(scope="module")
def app_config(app_config) -> dict:
    for k, v in test_config.items():
        app_config[k] = v
    return app_config


@pytest.fixture(scope="module")
def create_app():
    return create_api
