"""Permission policies for the record importer."""

from flask import current_app
from flask_principal import RoleNeed
from invenio_administration.generators import Administration
from invenio_records_permissions.generators import Generator, SystemProcess
from invenio_records_permissions.policies import BasePermissionPolicy

from .roles import DEFAULT_BULK_IMPORT_ROLE


class BulkImporterRole(Generator):
    """Allow holders of the configured invenio-accounts bulk-import role."""

    def needs(self, **kwargs):
        """Enabling need for the bulk-import role.

        Returns:
            List containing a ``RoleNeed`` for the configured role name.
        """
        role_name = current_app.config.get(
            "RECORD_IMPORTER_BULK_IMPORT_ROLE", DEFAULT_BULK_IMPORT_ROLE
        )
        return [RoleNeed(role_name)]


class RecordImporterPermissionPolicy(BasePermissionPolicy):
    """Permission policy for record importer.

    Bulk import is limited to administration users, holders of the dedicated
    bulk-import accounts role, and the system process. Community membership
    alone is not sufficient.
    """

    can_import_records = [
        Administration(),
        BulkImporterRole(),
        SystemProcess(),
    ]

    # Same gate for async import job status (URL fetch progress).
    can_read_import_job = [
        Administration(),
        BulkImporterRole(),
        SystemProcess(),
    ]
