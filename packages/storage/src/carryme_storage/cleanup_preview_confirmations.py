"""SQLite-backed cleanup preview confirmation storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import CleanupPreviewConfirmationEntry


class CleanupPreviewConfirmationStore:
    """Persist and query append-only cleanup preview confirmation entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.initialize()

    def initialize(self) -> None:
        """Create the cleanup preview confirmation table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cleanup_preview_confirmation_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    confirmed_at TEXT NOT NULL,
                    paper_trade_id INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    preview_hash TEXT NOT NULL,
                    entry_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cleanup_preview_confirmation_entries_confirmed_at
                ON cleanup_preview_confirmation_entries(confirmed_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cleanup_preview_confirmation_entries_label
                ON cleanup_preview_confirmation_entries(label)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cleanup_preview_confirmation_entries_paper_trade_id
                ON cleanup_preview_confirmation_entries(paper_trade_id)
                """
            )

    def append(self, entry: CleanupPreviewConfirmationEntry) -> CleanupPreviewConfirmationEntry:
        """Append a cleanup preview confirmation entry and return it with its assigned id."""

        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO cleanup_preview_confirmation_entries (
                    confirmed_at,
                    paper_trade_id,
                    label,
                    preview_hash,
                    entry_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    entry.confirmed_at.isoformat(),
                    entry.paper_trade_id,
                    entry.label,
                    entry.preview_hash,
                    entry.model_dump_json(),
                ),
            )
        return CleanupPreviewConfirmationEntry.model_validate(
            {
                **entry.model_dump(mode="json"),
                "entry_id": cursor.lastrowid,
            }
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
        paper_trade_id: int | None = None,
    ) -> list[CleanupPreviewConfirmationEntry]:
        """Return recent cleanup preview confirmation entries."""

        clauses: list[str] = []
        values: list[object] = []
        if label:
            clauses.append("label = ?")
            values.append(label)
        if paper_trade_id is not None:
            clauses.append("paper_trade_id = ?")
            values.append(paper_trade_id)
        query = """
            SELECT id, entry_json
            FROM cleanup_preview_confirmation_entries
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY confirmed_at DESC LIMIT ?"
        values.append(limit)

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, tuple(values)).fetchall()

        return [
            CleanupPreviewConfirmationEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]
