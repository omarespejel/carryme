"""SQLite-backed execution journal storage."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC
from pathlib import Path

from carryme_models import ExecutionJournalEntry


class ExecutionJournalStore:
    """Persist and query append-only execution journal entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the execution journal table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_journal_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    executed_at TEXT NOT NULL,
                    adapter TEXT NOT NULL,
                    status TEXT NOT NULL,
                    paper_trade_id INTEGER,
                    label TEXT NOT NULL,
                    entry_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_executed_at
                ON execution_journal_entries(executed_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_label
                ON execution_journal_entries(label)
                """
            )

    def append(self, entry: ExecutionJournalEntry) -> ExecutionJournalEntry:
        """Append an execution journal entry and return it with its assigned id."""

        self.initialize()
        if entry.executed_at.tzinfo is None or entry.executed_at.utcoffset() is None:
            raise ValueError("executed_at must be timezone-aware")
        normalized_label = entry.paper_trade.intent.label.strip()
        normalized_paper_trade = entry.paper_trade.model_copy(
            update={
                "intent": entry.paper_trade.intent.model_copy(
                    update={"label": normalized_label}
                )
            }
        )
        normalized_entry = entry.model_copy(
            update={
                "executed_at": entry.executed_at.astimezone(UTC),
                "paper_trade": normalized_paper_trade,
            }
        )
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO execution_journal_entries (
                    executed_at,
                    adapter,
                    status,
                    paper_trade_id,
                    label,
                    entry_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_entry.executed_at.isoformat(),
                    normalized_entry.adapter,
                    normalized_entry.status,
                    normalized_entry.paper_trade_id,
                    normalized_label,
                    normalized_entry.model_dump_json(),
                ),
            )
        return ExecutionJournalEntry.model_validate(
            {
                **normalized_entry.model_dump(mode="json"),
                "entry_id": cursor.lastrowid,
            }
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[ExecutionJournalEntry]:
        """Return recent execution journal entries."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        normalized_label = label.strip() if label is not None else None
        query = """
            SELECT id, entry_json
            FROM execution_journal_entries
        """
        params: tuple[object, ...]
        if normalized_label:
            query += " WHERE label = ?"
            params = (normalized_label, limit)
        else:
            params = (limit,)
        query += " ORDER BY executed_at DESC, id DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            ExecutionJournalEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]
