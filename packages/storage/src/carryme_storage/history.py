"""SQLite-backed opportunity history storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import OpportunityRecord


class OpportunityHistoryStore:
    """Persist and query scored opportunity history."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the history table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS opportunity_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at TEXT NOT NULL,
                    label TEXT,
                    canonical_symbol TEXT NOT NULL,
                    pair_json TEXT NOT NULL,
                    opportunity_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_opportunity_history_recorded_at
                ON opportunity_history(recorded_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_opportunity_history_label
                ON opportunity_history(label)
                """
            )

    def append(self, record: OpportunityRecord) -> None:
        """Append a scored opportunity to history."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO opportunity_history (
                    recorded_at,
                    label,
                    canonical_symbol,
                    pair_json,
                    opportunity_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    record.recorded_at.isoformat(),
                    record.pair.label,
                    record.opportunity.canonical_symbol,
                    record.pair.model_dump_json(),
                    record.opportunity.model_dump_json(),
                ),
            )

    def list_recent(self, *, limit: int = 50, label: str | None = None) -> list[OpportunityRecord]:
        """Return recent opportunity history rows."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        normalized_label = label.strip() if label is not None else None
        query = """
            SELECT recorded_at, pair_json, opportunity_json
            FROM opportunity_history
        """
        params: tuple[object, ...]
        if normalized_label:
            query += " WHERE label = ?"
            params = (normalized_label, limit)
        else:
            params = (limit,)
        query += " ORDER BY recorded_at DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            OpportunityRecord.model_validate(
                {
                    "recorded_at": recorded_at,
                    "pair": json.loads(pair_json),
                    "opportunity": json.loads(opportunity_json),
                }
            )
            for recorded_at, pair_json, opportunity_json in rows
        ]
