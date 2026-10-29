"""Bulk-import capability role for invenio-accounts.

Operators assign the configured role (default ``bulk-importer``) to users
who may call the import API. The role is created idempotently at app
finalize so it appears in the accounts UI without a manual DB seed.
"""

from __future__ import annotations

from flask import Flask
from invenio_accounts.proxies import current_accounts

DEFAULT_BULK_IMPORT_ROLE = "bulk-importer"


def bulk_import_role_name(app: Flask | None = None) -> str:
    """Return the configured bulk-import role name.

    Args:
        app: Flask app; when omitted, uses ``current_app`` via accounts config
            already applied on ``RECORD_IMPORTER_BULK_IMPORT_ROLE``.

    Returns:
        Role name string.
    """
    if app is not None:
        return app.config.get(
            "RECORD_IMPORTER_BULK_IMPORT_ROLE", DEFAULT_BULK_IMPORT_ROLE
        )
    from flask import current_app

    return current_app.config.get(
        "RECORD_IMPORTER_BULK_IMPORT_ROLE", DEFAULT_BULK_IMPORT_ROLE
    )


def ensure_bulk_import_role(app: Flask | None = None) -> str:
    """Create the bulk-import role if it does not exist.

    Args:
        app: Optional Flask app for config lookup.

    Returns:
        The ensured role name.
    """
    name = bulk_import_role_name(app)
    datastore = current_accounts.datastore
    role = datastore.find_role(name)
    if role is None:
        datastore.create_role(name=name)
        datastore.commit()
    return name
