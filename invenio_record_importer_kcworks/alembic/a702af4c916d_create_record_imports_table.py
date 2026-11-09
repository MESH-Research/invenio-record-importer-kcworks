#
# This file is part of invenio-record-importer-kcworks.
# Copyright (C) 2026 Mesh Research.
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

"""Create record_imports table."""

import sqlalchemy as sa
import sqlalchemy_utils
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a702af4c916d"
down_revision = "dae7279390c4"
branch_labels = ()
depends_on = None


def upgrade():
    """Upgrade database."""
    json_type = (
        sa.JSON()
        .with_variant(sqlalchemy_utils.types.json.JSONType(), "mysql")
        .with_variant(
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            "postgresql",
        )
        .with_variant(sqlalchemy_utils.types.json.JSONType(), "sqlite")
    )

    op.create_table(
        "record_imports",
        sa.Column("created", sa.DateTime(), nullable=False),
        sa.Column("updated", sa.DateTime(), nullable=False),
        sa.Column(
            "id",
            sqlalchemy_utils.types.uuid.UUIDType(),
            nullable=False,
        ),
        sa.Column(
            "community_id",
            sqlalchemy_utils.types.uuid.UUIDType(),
            sa.ForeignKey("communities_metadata.id"),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.Integer(),
            sa.ForeignKey("accounts_user.id"),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("records_metadata", json_type, nullable=False),
        sa.Column("options", json_type, nullable=False),
        sa.Column("staging_manifest", json_type, nullable=False),
        sa.Column("progress", json_type, nullable=False),
        sa.Column("result", json_type, nullable=True),
        sa.Column(
            "task_id",
            sqlalchemy_utils.types.uuid.UUIDType(),
            nullable=True,
        ),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_record_imports")),
    )
    op.create_index(
        "ix_record_imports_community_status",
        "record_imports",
        ["community_id", "status"],
        unique=False,
    )


def downgrade():
    """Downgrade database."""
    op.drop_index(
        "ix_record_imports_community_status",
        table_name="record_imports",
    )
    op.drop_table("record_imports")
