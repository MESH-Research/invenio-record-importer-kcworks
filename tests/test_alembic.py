"""Tests for alembic recipes for invenio-record-importer-kcworks."""

import pytest
from invenio_db.utils import alembic_test_context
from sqlalchemy import inspect

# Ensure the model is imported so metadata registration is visible even if
# entry-point loading order differs across fixture scopes.
import invenio_record_importer_kcworks.models  # noqa: F401

_BRANCH_ROOT = "dae7279390c4"
_CREATE_TABLE = "a702af4c916d"


def test_alembic_upgrade_downgrade_record_imports(running_app, db):
    """Run the create-table recipe upgrade and downgrade (this branch only).

    First asserts the model is registered and `create_all` already created
    the table (the shared suite's boot path). Then stamps our head, downgrades
    to the empty branch (drops via the recipe), upgrades again, checks
    columns/index/FKs, round-trips once more, and leaves the table present
    for sibling tests — without a full-database `drop_all()`.
    """
    app = running_app.app
    ext = app.extensions["invenio-db"]

    if db.engine.name == "sqlite":
        pytest.skip("Upgrades are not supported on SQLite.")

    assert "record_imports" in db.metadata.tables
    assert "record_imports" in inspect(db.engine).get_table_names()

    app.config["ALEMBIC_CONTEXT"] = alembic_test_context()

    # create_all() may have built the table without stamping this branch.
    ext.alembic.stamp(target=_CREATE_TABLE)

    ext.alembic.downgrade(target=_BRANCH_ROOT)
    assert "record_imports" not in inspect(db.engine).get_table_names()

    ext.alembic.upgrade(target=_CREATE_TABLE)
    table_names = inspect(db.engine).get_table_names()
    assert "record_imports" in table_names

    columns = {col["name"] for col in inspect(db.engine).get_columns("record_imports")}
    assert {
        "id",
        "community_id",
        "created_by",
        "status",
        "records_metadata",
        "options",
        "staging_manifest",
        "progress",
        "result",
        "task_id",
        "cancelled_at",
        "completed_at",
        "created",
        "updated",
    } <= columns

    index_names = {
        idx["name"] for idx in inspect(db.engine).get_indexes("record_imports")
    }
    assert "ix_record_imports_community_status" in index_names
    assert "ix_record_imports_community_id" not in index_names

    fk_referred = {
        fk["referred_table"]
        for fk in inspect(db.engine).get_foreign_keys("record_imports")
    }
    assert "communities_metadata" in fk_referred
    assert "accounts_user" in fk_referred

    # Second round-trip, then leave the table in place for siblings.
    ext.alembic.downgrade(target=_BRANCH_ROOT)
    assert "record_imports" not in inspect(db.engine).get_table_names()
    ext.alembic.upgrade(target=_CREATE_TABLE)
    assert "record_imports" in inspect(db.engine).get_table_names()
