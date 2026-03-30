"""Baseline storage schema for carryme."""

from __future__ import annotations

import os

from carryme_storage.db import normalize_database_url
from carryme_storage.schema import SCHEMA_TABLES, initialize_database_schema

from alembic import op

revision = "20260330_0001"
down_revision = None
branch_labels = None
depends_on = None


def _database_url() -> str:
    return normalize_database_url(
        os.environ.get("CARRYME_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or str(op.get_bind().engine.url)
    )


def upgrade() -> None:
    initialize_database_schema(_database_url())


def downgrade() -> None:
    bind = op.get_bind()
    for table_name in reversed(SCHEMA_TABLES):
        bind.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")
