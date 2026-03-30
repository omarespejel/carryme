"""Database-backed paper trade journal storage."""

from __future__ import annotations

import json
from datetime import UTC
from pathlib import Path

from carryme_models import PaperTradeEntry

from carryme_storage.db import Database


class PaperTradeStore:
    """Persist and query append-only paper trade journal entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the paper trade table if it does not exist."""

        with self.database.begin() as connection:
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

    def append(self, entry: PaperTradeEntry) -> PaperTradeEntry:
        """Append a paper trade entry and return it with its assigned id."""

        self.initialize()
        if entry.created_at.tzinfo is None or entry.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        normalized_entry = entry.model_copy(
            update={"created_at": entry.created_at.astimezone(UTC)}
        )
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
                """
                INSERT INTO paper_trade_entries (
                    created_at,
                    label,
                    canonical_symbol,
                    entry_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    normalized_entry.created_at.isoformat(),
                    normalized_entry.intent.label,
                    normalized_entry.intent.canonical_symbol,
                    normalized_entry.model_dump_json(),
                ),
            )
        return PaperTradeEntry.model_validate(
            {
                **normalized_entry.model_dump(mode="json"),
                "entry_id": row_id,
            }
        )

    def get(self, entry_id: int) -> PaperTradeEntry | None:
        """Return one paper trade entry by id."""

        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM paper_trade_entries
                WHERE id = ?
                """,
                (entry_id,),
            ).fetchone()

        if row is None:
            return None

        stored_id, entry_json = row
        return PaperTradeEntry.model_validate(
            {
                **json.loads(entry_json),
                "entry_id": stored_id,
            }
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
            SELECT id, entry_json
            FROM paper_trade_entries
        """
        params: tuple[object, ...]
        if label:
            query += " WHERE label = ?"
            params = (label, limit)
        else:
            params = (limit,)
        query += " ORDER BY created_at DESC, id DESC LIMIT ?"

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            PaperTradeEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]
