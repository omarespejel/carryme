"""Database-backed execution observation storage."""

from __future__ import annotations

import json
from datetime import UTC
from pathlib import Path

from carryme_models import (
    ExecutionAlertEvent,
    ExecutionObservationEntry,
    ExecutionPairStatus,
)

from carryme_storage.db import Database, DatabaseConnection
from carryme_storage.execution_alerts import ExecutionAlertStore


def _normalize_observation(entry: ExecutionObservationEntry) -> ExecutionObservationEntry:
    """Normalize an observation entry before persistence."""

    if entry.observed_at.tzinfo is None or entry.observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return entry.model_copy(update={"observed_at": entry.observed_at.astimezone(UTC)})


class ExecutionObservationStore:
    """Persist and query append-only execution observation snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the observation table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_observation_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at TEXT NOT NULL,
                    context TEXT NOT NULL,
                    execution_entry_id INTEGER,
                    paper_trade_id INTEGER,
                    preview_hash TEXT,
                    entry_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_execution_observation_entries_observed_at
                ON execution_observation_entries(observed_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_execution_observation_entries_paper_trade_id
                ON execution_observation_entries(paper_trade_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_execution_observation_entries_preview_hash
                ON execution_observation_entries(preview_hash)
                """
            )

    def append(self, entry: ExecutionObservationEntry) -> ExecutionObservationEntry:
        """Append an observation entry and return it with its assigned id."""

        self.initialize()
        with self.database.begin() as connection:
            return self._append_on_connection(connection, entry)

    def append_with_alert(
        self,
        entry: ExecutionObservationEntry,
        *,
        alert_store: ExecutionAlertStore,
        alert_event: ExecutionAlertEvent | None = None,
        previous_pair_status: ExecutionPairStatus | None = None,
    ) -> tuple[ExecutionObservationEntry, bool]:
        """Append an observation and optional alert atomically within one transaction."""

        self.initialize()
        alert_store.initialize()
        if alert_store.database.url != self.database.url:
            raise ValueError("observation and alert stores must share the same database")

        with self.database.begin() as connection:
            alert_saved = False
            if alert_event is not None:
                alert_saved = alert_store._append_if_changed_on_connection(
                    connection,
                    alert_event,
                    previous_pair_status=previous_pair_status,
                )
            saved_entry = self._append_on_connection(connection, entry)
        return saved_entry, alert_saved

    def latest_for_paper_trade(self, paper_trade_id: int) -> ExecutionObservationEntry | None:
        """Return the newest observation entry for one paper trade."""

        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_observation_entries
                WHERE paper_trade_id = ?
                ORDER BY observed_at DESC, id DESC
                LIMIT 1
                """,
                (paper_trade_id,),
            ).fetchone()

        if row is None:
            return None
        stored_id, entry_json = row
        return ExecutionObservationEntry.model_validate(
            {
                **json.loads(entry_json),
                "entry_id": stored_id,
            }
        )

    def list_recent(
        self,
        *,
        limit: int | None = 50,
        offset: int = 0,
        paper_trade_id: int | None = None,
    ) -> list[ExecutionObservationEntry]:
        """Return recent execution observation rows."""

        self.initialize()
        if limit is not None and limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset must be at least 0")
        if limit is None and offset > 0:
            raise ValueError("offset requires a finite limit")
        query = """
            SELECT id, entry_json
            FROM execution_observation_entries
        """
        params: tuple[object, ...]
        if paper_trade_id is not None:
            query += " WHERE paper_trade_id = ?"
            params = (paper_trade_id,)
        else:
            params = ()
        query += " ORDER BY observed_at DESC, id DESC"
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params = (*params, limit, offset)

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            ExecutionObservationEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]

    def _append_on_connection(
        self,
        connection: DatabaseConnection,
        entry: ExecutionObservationEntry,
    ) -> ExecutionObservationEntry:
        """Append an observation entry using an existing transaction."""

        normalized_entry = _normalize_observation(entry)
        row_id = connection.insert_returning_id(
            """
            INSERT INTO execution_observation_entries (
                observed_at,
                context,
                execution_entry_id,
                paper_trade_id,
                preview_hash,
                entry_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_entry.observed_at.isoformat(),
                normalized_entry.context,
                normalized_entry.execution_entry_id,
                normalized_entry.paper_trade_id,
                normalized_entry.preview_hash,
                normalized_entry.model_dump_json(),
            ),
        )
        return ExecutionObservationEntry.model_validate(
            {
                **normalized_entry.model_dump(mode="json"),
                "entry_id": row_id,
            }
        )
