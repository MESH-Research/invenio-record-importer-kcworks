from invenio_records_permissions.policies import BasePermissionPolicy
from invenio_records_permissions.generators import (
    AuthenticatedUser,
    SystemProcess,
    AnyUser,
)


class RecordImporterPermissionPolicy(BasePermissionPolicy):
    """Permission policy for record importer."""

    can_import_records = [AnyUser(), AuthenticatedUser(), SystemProcess()]
