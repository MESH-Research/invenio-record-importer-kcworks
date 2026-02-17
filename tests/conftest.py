# Part of invenio-record-importer-kcworks.
# Copyright (C) 2024-2025, MESH Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see
# LICENSE file for more details.

"""Top-level pytest configuration for invenio-record-importer-kcworks tests."""

import os
import shutil
import sys
import tempfile
from collections import namedtuple
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import jinja2
import pytest
from flask import Flask
from invenio_app.factory import create_app as _create_app
from invenio_cache import current_cache
from invenio_communities.proxies import current_communities
from invenio_files_rest.models import Location
from invenio_queues import current_queues
from invenio_rdm_records.proxies import current_rdm_records
from invenio_records_resources.proxies import current_service_registry
from invenio_search.proxies import current_search_client
from marshmallow import Schema, fields

from .fixtures.custom_fields import test_config_fields
from .fixtures.identifiers import test_config_identifiers
from .fixtures.stats import test_config_stats

pytest_plugins = (
    "celery.contrib.pytest",
    "tests.pytest_plugins.pytest_live_status",
    "tests.fixtures.communities",
    "tests.fixtures.custom_fields",
    "tests.fixtures.files",
    "tests.fixtures.fixtures",
    "tests.fixtures.mail",
    "tests.fixtures.records",
    "tests.fixtures.search_provisioning",
    "tests.fixtures.stats",
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
)


def _(x: Any) -> Any:
    """Identity function for string extraction.

    Returns:
        Any: The input value unchanged.
    """
    return x


test_config = {
    **test_config_identifiers,
    **test_config_fields,
    **test_config_stats,
    "SQLALCHEMY_DATABASE_URI": (
        "postgresql+psycopg2://invenio:invenio@localhost:5432/invenio"
    ),
    "SQLALCHEMY_TRACK_MODIFICATIONS": False,
    "SEARCH_INDEX_PREFIX": "",
    "POSTGRES_USER": "invenio",
    "POSTGRES_PASSWORD": "invenio",
    "POSTGRES_DB": "invenio",
    "WTF_CSRF_ENABLED": False,
    "WTF_CSRF_METHODS": [],
    "RATELIMIT_ENABLED": False,
    "APP_DEFAULT_SECURE_HEADERS": {
        "content_security_policy": {"default-src": []},
        "force_https": False,
    },
    "BROKER_URL": "amqp://guest:guest@localhost:5672//",
    "CELERY_TASK_ALWAYS_EAGER": False,
    "CELERY_TASK_EAGER_PROPAGATES_EXCEPTIONS": True,
    "CELERY_LOGLEVEL": "DEBUG",
    "INVENIO_INSTANCE_PATH": "/opt/invenio/var/instance",
    # Set MAIL_SUPPRESS_SEND = True to prevent Flask-Mail from attempting
    # SMTP connections. The mailbox fixture still works because the
    # email_dispatched signal fires even when suppress=True (Connection.send()
    # sends the signal regardless of whether self.host exists).
    "MAIL_SUPPRESS_SEND": True,
    "MAIL_DEFAULT_SENDER": "test@example.com",
    "SECRET_KEY": "test-secret-key",
    "SECURITY_PASSWORD_SALT": "test-secret-key",
    "TESTING": True,
    "DEBUG": True,
}

parent_path = Path(__file__).parent.parent
raw_data_path = parent_path / "tests/helpers/sample_records"
test_config["RECORD_IMPORTER_DATA_DIR"] = str(raw_data_path)

test_config["RECORD_IMPORTER_SERIALIZED_PATH"] = str(
    raw_data_path / "record_importer_serialized_records.jsonl"
)

log_folder_path = parent_path / "invenio_record_importer_kcworks" / "logs"
log_file_path = log_folder_path / "invenio.log"
if not log_file_path.exists():
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    log_file_path.touch()

test_config["LOGGING_FS_LEVEL"] = "DEBUG"
test_config["LOGGING_FS_LOGFILE"] = str(log_file_path)
test_config["LOGGING_CONSOLE_LEVEL"] = "DEBUG"
test_config["CELERY_LOGFILE"] = str(log_folder_path / "celery.log")

# Ensure test-local helpers (including a shim kcworks package) take precedence
helpers_path = Path(__file__).parent / "helpers"
if str(helpers_path) not in sys.path:
    sys.path.insert(0, str(helpers_path))

# enable DataCite DOI provider
test_config["DATACITE_ENABLED"] = True
test_config["DATACITE_USERNAME"] = "INVALID"
test_config["DATACITE_PASSWORD"] = "INVALID"
test_config["DATACITE_DATACENTER_SYMBOL"] = "TEST"
test_config["DATACITE_PREFIX"] = "10.17613"
test_config["DATACITE_TEST_MODE"] = True
# ...but fake it

test_config["SITE_API_URL"] = os.environ.get(
    "INVENIO_SITE_API_URL", "https://127.0.0.1:5000/api"
)
test_config["SITE_UI_URL"] = os.environ.get(
    "INVENIO_SITE_UI_URL", "https://127.0.0.1:5000"
)

# OAI Server configuration
test_config["OAISERVER_ID_PREFIX"] = test_config["SITE_UI_URL"]


class CustomUserProfileSchema(Schema):
    """The default user profile schema."""

    full_name = fields.String()
    affiliations = fields.String()
    name_parts = fields.String()
    name_parts_local = fields.String()
    identifier_email = fields.String()
    identifier_orcid = fields.String()
    identifier_kc_username = fields.String()
    identifier_other = fields.String()
    unread_notifications = fields.String()


# custom user profile
test_config["ACCOUNTS_USER_PROFILE_SCHEMA"] = CustomUserProfileSchema

# Submodule-specific configuration
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
test_config["RECORD_IMPORTER_COMMUNITIES"] = {
    "neh": {
        "email_subject_register": "Your NEH Open Access Deposit is Ready",
        "email_template_register": "welcome_neh",
    }
}


@pytest.fixture(scope="session")
def celery_config(celery_config) -> dict:
    """Celery config fixture for invenio-record-importer-kcworks.

    Returns:
        dict: Celery configuration dictionary.
    """
    celery_config["logfile"] = str(log_folder_path / "celery.log")
    celery_config["loglevel"] = "DEBUG"
    celery_config["task_always_eager"] = True
    celery_config["cache_backend"] = "memory"
    celery_config["result_backend"] = "cache"
    celery_config["task_eager_propagates_exceptions"] = True

    return dict(celery_config)


@pytest.fixture(scope="session")
def celery_enable_logging() -> bool:
    """Celery enable logging fixture.

    Returns:
        bool: True to enable Celery logging.
    """
    return True


# FIXME: https://github.com/inveniosoftware/pytest-invenio/issues/30
# Without this, success of test depends on the tests order
@pytest.fixture()
def cache():
    """Empty cache fixture.

    This fixture ensures the cache is cleared before and after each test
    to prevent test interdependencies.

    Yields:
        Cache: The current cache instance.
    """
    try:
        current_cache.clear()
        yield current_cache
    finally:
        current_cache.clear()


@pytest.yield_fixture(scope="module")
def location(database: Callable) -> Generator[Location, None, None]:
    """Creates a simple default location for a test.

    Use this fixture if your test requires a `files location <https://invenio-
    files-rest.readthedocs.io/en/latest/api.html#invenio_files_rest.models.
    Location>`_. The location will be a default location with the name
    ``pytest-location``.

    Yields:
        Location: The created test location.
    """
    uri = tempfile.mkdtemp()
    location_obj = Location(name="pytest-location", uri=uri, default=True)

    database.session.add(location_obj)
    database.session.commit()

    yield location_obj

    shutil.rmtree(uri)


# This is a namedtuple that holds all the fixtures we're likely to need
# in a single test.
RunningApp = namedtuple(
    "RunningApp",
    [
        "app",
        "location",
        "cache",
        "affiliations_v",
        "awards_v",
        "community_type_v",
        "contributors_role_v",
        "creators_role_v",
        "date_type_v",
        "description_type_v",
        "funders_v",
        "language_v",
        "licenses_v",
        "resource_type_v",
        "subject_v",
        "title_type_v",
        "create_communities_custom_fields",
        "create_records_custom_fields",
    ],
)


@pytest.fixture(scope="function")
def running_app(
    app,
    location,
    cache,
    affiliations_v,
    awards_v,
    community_type_v,
    contributors_role_v,
    creators_role_v,
    date_type_v,
    description_type_v,
    funders_v,
    language_v,
    licenses_v,
    resource_type_v,
    subject_v,
    title_type_v,
    create_communities_custom_fields,
    create_records_custom_fields,
) -> RunningApp:
    """This fixture provides an app with the typically needed db data loaded.

    All of these fixtures are often needed together, so collecting them
    under a semantic umbrella makes sense.

    Returns:
        RunningApp: The running application instance fixture.
    """
    return RunningApp(
        app,
        location,
        cache,
        affiliations_v,
        awards_v,
        community_type_v,
        contributors_role_v,
        creators_role_v,
        date_type_v,
        description_type_v,
        funders_v,
        language_v,
        licenses_v,
        resource_type_v,
        subject_v,
        title_type_v,
        create_communities_custom_fields,
        create_records_custom_fields,
    )


@pytest.fixture(scope="function")
def search_clear(search_clear) -> Generator[Any, None, None]:
    """Clear search indices after test finishes (function scope).

    This fixture extends the pytest_invenio ``search_clear`` fixture to also
    clear stats indices and templates, which are not handled by the base
    fixture. It also clears the identity cache before each test to prevent
    stale community role data.

    The base ``search_clear`` fixture should each time start by running:
    ```python
    current_search.create()
    current_search.put_templates()
    ```
    and then clear the indices during the fixture teardown. But this doesn't
    catch the stats indices, so we need to add an additional step to delete
    the stats indices and template manually. Otherwise, the stats indices
    aren't cleared between tests.

    Yields:
        The search client (same as the base ``search_clear`` fixture).
    """
    # Clear identity cache before each test to prevent stale community role data
    from invenio_communities.proxies import current_identities_cache

    current_identities_cache.flush()

    yield search_clear

    # Delete stats indices and templates if they exist
    # Without this we get data pollution between tests
    current_search_client.indices.delete("*stats*", ignore=[404])
    current_search_client.indices.delete_template("*stats*", ignore=[404])


@pytest.fixture(scope="module")
def subjects_service(app):
    """Subjects service fixture.

    Returns:
        Service: The subjects service.
    """
    return current_service_registry.get("subjects")


@pytest.fixture(scope="module")
def communities_service(app):
    """Communities service fixture.

    Returns:
        Service: The communities service.
    """
    return current_communities.service


@pytest.fixture(scope="module")
def records_service(app):
    """Records service fixture.

    Returns:
        Service: The records service.
    """
    return current_rdm_records.records_service


@pytest.fixture(scope="module")
def template_loader() -> Callable:
    """Fixture providing overloaded and custom templates to test app.

    Returns:
        Callable: A function that loads templates for the test app.
    """

    def load_tempates(app):
        """Load templates for the test app."""
        test_file_path = Path(__file__).parent

        # Package template paths
        submodule_templates_path = test_file_path / "helpers" / "templates"
        submodule_semantic_ui_path = (
            test_file_path / "helpers" / "templates" / "semantic-ui"
        )

        # Find installed package template paths
        theme_template_paths = []
        # Package template path structures:
        # - invenio_theme: templates/semantic-ui
        # - invenio_app_rdm: theme/templates/semantic-ui
        # - invenio_banners: templates/semantic-ui
        package_template_paths = {
            "invenio_theme": ["templates", "semantic-ui"],
            "invenio_app_rdm": ["theme", "templates", "semantic-ui"],
            "invenio_banners": ["templates", "semantic-ui"],
        }
        for package_name, path_parts in package_template_paths.items():
            try:
                package = __import__(package_name)
                if hasattr(package, "__file__") and package.__file__:
                    package_path = Path(package.__file__).parent
                    template_path = package_path
                    for part in path_parts:
                        template_path = template_path / part
                    if template_path.exists():
                        theme_template_paths.append(str(template_path))
            except (ImportError, AttributeError):
                # Package not installed or doesn't have __file__ attribute
                pass

        # Build list of paths that exist
        template_paths = []
        all_paths: list[str | Path] = [
            submodule_templates_path,  # Test templates (highest priority)
            submodule_semantic_ui_path,  # Test semantic-ui templates
            *[Path(p) for p in theme_template_paths],  # Installed packages
        ]
        for path in all_paths:
            path_obj = Path(path) if isinstance(path, str) else path
            if path_obj.exists():
                template_paths.append(str(path_obj))

        custom_loader = jinja2.ChoiceLoader([
            app.jinja_loader,
            jinja2.FileSystemLoader(template_paths),
        ])
        app.jinja_loader = custom_loader
        app.jinja_env.loader = custom_loader

    return load_tempates


@pytest.fixture(scope="module")
def app(
    app,
    app_config,
    database,
    search,
    template_loader,
) -> Generator[Flask, None, None]:
    """This fixture provides an app with the typically needed basic fixtures.

    This fixture should be used in conjunction with the `running_app`
    fixture to provide a complete app with all the typically needed
    fixtures. This fixture sets up the basic functions like db, search
    once per module. The `running_app` fixture is function scoped and
    initializes all the fixtures that should be reset between tests.

    Yields:
        Flask: The Flask application instance.
    """
    template_loader(app)
    current_queues.declare()
    yield app


@pytest.fixture(scope="module")
def app_config(app_config) -> dict:
    """App config fixture.

    Returns:
        dict: The application configuration dictionary.
    """
    for k, v in test_config.items():
        app_config[k] = v

    return dict(app_config)


@pytest.fixture(scope="module")
def create_app(instance_path, entry_points):
    """Create the app fixture.

    This initializes the basic Flask app which will then be used
    to set up the `app` fixture with initialized services.

    Returns:
        Callable: The application factory function.
    """
    return _create_app
