"""Permission policies for the record importer."""

from itertools import chain

from invenio_access import Permission, action_factory
from invenio_administration.generators import Administration
from invenio_communities.generators import (
    CommunityCurators,
    CommunityOwners,
    ReviewPolicy,
)
from invenio_records_permissions.generators import (
    AdminAction,
    Generator,
    SystemProcess,
)
from invenio_records_permissions.policies import BasePermissionPolicy

# Bound to the configured bulk-import accounts role via ActionRoles (see roles.py).
import_records_action = action_factory("records-import")


class And(Generator):
    """Require that the identity satisfies every wrapped generator.

    Policy generator lists are OR. `And` contributes needs only when each
    child would already allow the identity (junction of satisfaction). Pass
    `identity` through into generator kwargs (see service `check_permission`).
    """

    def __init__(self, *generators: Generator):
        """Store the generators that must all allow.

        Args:
            *generators: Child permission generators.
        """
        self.generators = generators

    def needs(self, identity=None, **kwargs):
        """Enabling needs if every child allows `identity`; else none.

        Args:
            identity: Caller identity used to test each child.
            **kwargs: Forwarded to children (e.g. `record` for ReviewPolicy).

        Returns:
            Union of child needs when all children allow; otherwise `[]`.
        """
        if identity is None:
            return []

        collected: list = []
        for generator in self.generators:
            child_needs = list(generator.needs(identity=identity, **kwargs))
            if not Permission(*child_needs).allows(identity):
                return []
            collected.extend(child_needs)
        return collected

    def excludes(self, identity=None, **kwargs):
        """Union of child excludes (any matching exclude still denies).

        Args:
            identity: Caller identity (forwarded).
            **kwargs: Forwarded to children.

        Returns:
            Combined exclude needs from all children.
        """
        return list(
            chain.from_iterable(
                generator.excludes(identity=identity, **kwargs)
                for generator in self.generators
            )
        )


_community_and_import_action = And(
    ReviewPolicy(
        closed_=[CommunityOwners()],
        open_=[CommunityCurators()],
        members_=[CommunityCurators()],
    ),
    AdminAction(import_records_action),
)


class RecordImporterPermissionPolicy(BasePermissionPolicy):
    """Permission policy for record importer.

    `can_import_records` and `can_read_import_job` require community role
    (per review policy) **and** `import_records_action`, via `And`.
    `Administration` and `SystemProcess` remain top-level OR bypasses.
    """

    can_import_records = [
        _community_and_import_action,
        Administration(),
        SystemProcess(),
    ]

    can_read_import_job = [
        _community_and_import_action,
        Administration(),
        SystemProcess(),
    ]
