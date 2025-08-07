from invenio_communities.generators import (
    CommunityCurators,
    CommunityOwners,
    IfPolicyClosed,
)
from invenio_records_permissions.generators import (
    SystemProcess,
)
from invenio_records_permissions.policies import BasePermissionPolicy


class RecordImporterPermissionPolicy(BasePermissionPolicy):
    """Permission policy for record importer."""

    can_import_records = [
        IfPolicyClosed(
            "review_policy",
            then_=[CommunityOwners(), SystemProcess()],
            else_=[CommunityCurators(), SystemProcess()],
        ),
    ]
