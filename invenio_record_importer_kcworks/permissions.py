from invenio_communities.generators import (
    CommunityCurators,
    CommunityMembers,
    CommunityOwners,
    ReviewPolicy,
)
from invenio_records_permissions.generators import (
    SystemProcess,
)
from invenio_records_permissions.policies import BasePermissionPolicy


class RecordImporterPermissionPolicy(BasePermissionPolicy):
    """Permission policy for record importer."""

    can_import_records = [
        ReviewPolicy(
            closed_=[CommunityOwners()],
            open_=[CommunityCurators()],
            members_=[CommunityMembers()],
        ),
        SystemProcess(),
    ]
