#
# This file is part of invenio-record-importer-kcworks.
# Copyright (C) 2026 Mesh Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

"""Create invenio-record-importer-kcworks alembic branch."""

# revision identifiers, used by Alembic.
revision = "dae7279390c4"
down_revision = None
branch_labels = ("invenio_record_importer_kcworks",)
depends_on = "dbdbc1b19cf2"


def upgrade():
    """Upgrade database."""
    pass


def downgrade():
    """Downgrade database."""
    pass
