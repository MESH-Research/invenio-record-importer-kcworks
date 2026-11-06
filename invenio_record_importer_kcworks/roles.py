"""Bulk-import accounts role bound to `import_records_action`.

Operators assign the configured role (default `bulk-importer`) to users who
may call the import API. `ensure_bulk_import_role` creates the role and an
`ActionRoles` row for `import_records_action`. Idempotent. Called from app
finalize and again from test `admin_roles` after `create_all`.
"""

from flask import current_app
from invenio_access.models import ActionRoles
from invenio_accounts.proxies import current_accounts
from invenio_db import db
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError

from .permissions import import_records_action

DEFAULT_BULK_IMPORT_ROLE = "bulk-importer"
_ENSURE_RETRIES = 5


def _accounts_tables_ready() -> bool:
    """Return True when the accounts schema is present.

    Finalize hooks can run during test bootstrap before session `create_all`.
    Production always has migrated tables by the time the app finalizes.

    Returns:
        Whether `accounts_role` exists.
    """
    try:
        return inspect(db.engine).has_table("accounts_role")
    except Exception:
        return False


def _role_bound_to_import_action(role) -> bool:
    """Return whether `role` already has `import_records_action`.

    Args:
        role: Accounts role row.

    Returns:
        True if an `ActionRoles` row links the role to the import action.
    """
    return any(
        row.role_id == role.id
        for row in ActionRoles.query_by_action(import_records_action).all()
    )


def _try_ensure_role(role_name: str):
    """Find or create the bulk-import role once.

    Args:
        role_name: Accounts role name.

    Returns:
        The role row, or `None` if a concurrent create raced and the role
        is not visible yet.
    """
    datastore = current_accounts.datastore
    role = datastore.find_role(role_name)
    if role is not None:
        return role
    role = datastore.create_role(name=role_name)
    try:
        datastore.commit()
        return role
    except (IntegrityError, StaleDataError):
        db.session.rollback()
        return datastore.find_role(role_name)


def _try_ensure_action_binding(role) -> bool:
    """Ensure `import_records_action` is bound to `role` once.

    Args:
        role: Accounts role row.

    Returns:
        True if the binding exists (already or after a successful write).
        False if a concurrent write raced and the binding is still missing.
    """
    if _role_bound_to_import_action(role):
        return True
    db.session.add(
        ActionRoles.create(action=import_records_action, role_id=role.id)
    )
    try:
        db.session.commit()
        return True
    except (IntegrityError, StaleDataError):
        db.session.rollback()
        return _role_bound_to_import_action(role)


def ensure_bulk_import_role() -> str | None:
    """Create the bulk-import role and bind `import_records_action` to it.

    Role name comes from `RECORD_IMPORTER_BULK_IMPORT_ROLE` (default
    `bulk-importer` via package config). No-ops when accounts tables are not
    present yet. Retries briefly on concurrent create/bind races.

    Returns:
        The ensured role name, or `None` if tables were not ready.

    Raises:
        RuntimeError: If the role or action binding cannot be ensured after
            retries.
    """
    if not _accounts_tables_ready():
        return None

    role_name = current_app.config.get(
        "RECORD_IMPORTER_BULK_IMPORT_ROLE",
        DEFAULT_BULK_IMPORT_ROLE,
    )

    role = None
    for _ in range(_ENSURE_RETRIES):
        role = _try_ensure_role(role_name)
        if role is not None:
            break
    if role is None:
        raise RuntimeError(
            f"Could not ensure bulk-import role {role_name!r} after retries"
        )

    bound = False
    for _ in range(_ENSURE_RETRIES):
        bound = _try_ensure_action_binding(role)
        if bound:
            break
    if not bound:
        raise RuntimeError(
            "Could not ensure import_records_action binding for role "
            f"{role_name!r} after retries"
        )

    return role_name
