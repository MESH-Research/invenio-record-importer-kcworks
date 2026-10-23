# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2026, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Top-level pytest configuration for invenio-record-importer-kcworks tests.

Shared `app_config` from `kcworks-test-fixtures` supplies the common baseline;
this conftest only wraps it for package-specific keys and template paths.
"""

import importlib.util
import sys
from pathlib import Path

import pytest
from invenio_app.factory import create_app as _create_app

from tests.fixtures.app import RunningApp  # noqa: F401 — re-export for tests

pytest_plugins = (
    "celery.contrib.pytest",
    "tests.fixtures.app",
    "tests.fixtures.app_config",
    "tests.fixtures.bootstrap",
    "tests.fixtures.celery",
    "tests.fixtures.cli",
    "tests.fixtures.communities",
    "tests.fixtures.community_events",
    "tests.fixtures.custom_fields",
    "tests.fixtures.files",
    "tests.fixtures.fixtures",
    "tests.fixtures.frontend",
    "tests.fixtures.identifiers",
    "tests.fixtures.indexer_queues",
    "tests.fixtures.logging",
    "tests.fixtures.mail",
    "tests.fixtures.records",
    "tests.fixtures.roles",
    "tests.fixtures.search",
    "tests.fixtures.search_provisioning",
    "tests.fixtures.stats",
    "tests.fixtures.uow",
    "tests.fixtures.users",
    "tests.fixtures.vocabularies.affiliations",
    "tests.fixtures.vocabularies.community_types",
    "tests.fixtures.vocabularies.date_types",
    "tests.fixtures.vocabularies.descriptions",
    "tests.fixtures.vocabularies.funding_and_awards",
    "tests.fixtures.vocabularies.languages",
    "tests.fixtures.vocabularies.licenses",
    "tests.fixtures.vocabularies.resource_types",
    "tests.fixtures.vocabularies.roles",
    "tests.fixtures.vocabularies.subjects",
    "tests.fixtures.vocabularies.title_types",
    "tests.pytest_plugins.pytest_live_status",
)

parent_path = Path(__file__).parent.parent
raw_data_path = parent_path / "tests/helpers/sample_records"

# Test-local helpers include a templates-only shim package named `kcworks`
# for standalone clones (no site package). When nested under KCWorks the
# real `site/kcworks` (with `kcworks.ext`) must win — putting the shim
# first on sys.path shadows it and breaks Invenio entry-point loading.
helpers_path = Path(__file__).parent / "helpers"
_helpers = str(helpers_path)
if _helpers not in sys.path:
    try:
        _has_site_kcworks_ext = importlib.util.find_spec("kcworks.ext") is not None
    except (ModuleNotFoundError, ValueError):
        _has_site_kcworks_ext = False
    if not _has_site_kcworks_ext:
        sys.path.insert(0, _helpers)


RECORD_IMPORTER_COMMUNITIES_DATA = {
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
                "description": ("AJS is no longer a member of Knowledge Commons"),
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
                "description": ("CAA is no longer a member of Humanities Commons"),
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
                "description": ("A community representing the SAH Commons domain"),
                "website": "https://sah.hcommons.org",
                "organizations": [{"name": "SAH Commons"}],
            },
        },
        "up": {
            "access": {
                "visibility": "restricted",
                "member_policy": "closed",
                "record_policy": "closed",
            },
            "slug": "up",
            "metadata": {
                "title": "UP Commons",
                "description": ("A collection representing the UP Commons domain"),
                "website": "https://up.hcommons.org",
                "organizations": [{"name": "UP Commons"}],
            },
        },
    }
}

# Email configuration for communities (used by UsersHelper.send_welcome_email)
RECORD_IMPORTER_COMMUNITIES = {
    "neh": {
        "email_subject_register": "Your NEH Open Access Deposit is Ready",
        "email_template_register": "welcome_neh",
    }
}


@pytest.fixture(scope="session")
def template_loader_paths() -> list[Path]:
    """Importer template directories for the shared `template_loader` fixture.

    Returns:
        list[Path]: Ordered template directories for Jinja.
    """
    return [
        Path(__file__).parent / "helpers" / "templates",
        Path(__file__).parent / "helpers" / "templates" / "semantic-ui",
    ]


@pytest.fixture(scope="module")
def app_config(app_config) -> dict:
    """Add record-importer package keys onto the shared test defaults.

    Returns:
        dict: The layered Flask/Invenio application config.
    """
    app_config["RECORD_IMPORTER_DATA_DIR"] = str(raw_data_path)
    app_config["RECORD_IMPORTER_SERIALIZED_PATH"] = str(
        raw_data_path / "record_importer_serialized_records.jsonl"
    )
    app_config["RECORD_IMPORTER_COMMUNITIES_DATA"] = RECORD_IMPORTER_COMMUNITIES_DATA
    app_config["RECORD_IMPORTER_COMMUNITIES"] = RECORD_IMPORTER_COMMUNITIES
    return app_config


@pytest.fixture(scope="module")
def create_app(instance_path, entry_points):
    """Provide the application factory used to build the Flask app.

    Returns:
        Callable: The application factory function.
    """
    return _create_app
