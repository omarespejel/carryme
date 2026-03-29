"""SQLite-backed execution observation storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import ExecutionObservationEntry


class ExecutionObservationStore:
    """Persist and query append-only execution observation snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the observation table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
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
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
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
                    entry.observed_at.isoformat(),
                    entry.context,
                    entry.execution_entry_id,
                    entry.paper_trade_id,
                    entry.preview_hash,
                    entry.model_dump_json(),
                ),
            )
        return ExecutionObservationEntry.model_validate(
            {
                **entry.model_dump(mode="json"),
                "entry_id": cursor.lastrowid,
            }
        )

    def latest_for_paper_trade(self, paper_trade_id: int) -> ExecutionObservationEntry | None:
        """Return the newest observation entry for one paper trade."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
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
        limit: int = 50,
        paper_trade_id: int | None = None,
    ) -> list[ExecutionObservationEntry]:
        """Return recent execution observation rows."""

        self.initialize()
        query = """
            SELECT id, entry_json
            FROM execution_observation_entries
        """
        params: tuple[object, ...]
        if paper_trade_id is not None:
            query += " WHERE paper_trade_id = ?"
            params = (paper_trade_id, limit)
        else:
            params = (limit,)
        query += " ORDER BY observed_at DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
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
