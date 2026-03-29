"""SQLite-backed paper trade journal storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import PaperTradeEntry


class PaperTradeStore:
    """Persist and query append-only paper trade journal entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the paper trade table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_trade_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    label TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    entry_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_paper_trade_entries_created_at
                ON paper_trade_entries(created_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_paper_trade_entries_label
                ON paper_trade_entries(label)
                """
            )

    def append(self, entry: PaperTradeEntry) -> None:
        """Append a paper trade entry."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO paper_trade_entries (
                    created_at,
                    label,
                    canonical_symbol,
                    entry_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    entry.created_at.isoformat(),
                    entry.intent.label,
                    entry.intent.canonical_symbol,
                    entry.model_dump_json(),
                ),
            )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[PaperTradeEntry]:
        """Return recent paper trade journal entries."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        query = """
            SELECT entry_json
            FROM paper_trade_entries
        """
        params: tuple[object, ...]
        if label:
            query += " WHERE label = ?"
            params = (label, limit)
        else:
            params = (limit,)
        query += " ORDER BY created_at DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [PaperTradeEntry.model_validate(json.loads(entry_json)) for (entry_json,) in rows]
