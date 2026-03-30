"""Alembic environment configuration for carryme."""

from __future__ import annotations

import os
from logging.config import fileConfig

from carryme_storage.db import normalize_database_url
from sqlalchemy import create_engine, pool

from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def get_database_url() -> str:
    """Resolve the database URL for migration commands."""

    return normalize_database_url(
        os.environ.get("CARRYME_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or "data/carryme.sqlite3"
    )


def run_migrations_offline() -> None:
    """Run migrations in offline mode."""

    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in online mode."""

    connectable = create_engine(
        get_database_url(),
        future=True,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
