"""SQLite-backed execution journal storage."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC
from pathlib import Path
from threading import Lock

from carryme_models import ExecutionJournalEntry


class ExecutionJournalStore:
    """Persist and query append-only execution journal entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        """Create the execution journal table if it does not exist."""

        if self._initialized:
            return

        with self._initialize_lock:
            if self._initialized:
                return
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
                        confirmation_entry_id INTEGER,
                        label TEXT NOT NULL,
                        entry_json TEXT NOT NULL
                    )
                    """
                )
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(execution_journal_entries)"
                    ).fetchall()
                }
                if "confirmation_entry_id" not in columns:
                    connection.execute(
                        """
                        ALTER TABLE execution_journal_entries
                        ADD COLUMN confirmation_entry_id INTEGER
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
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_confirmation_entry_id
                    ON execution_journal_entries(confirmation_entry_id)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS live_submission_reservations (
                        confirmation_entry_id INTEGER PRIMARY KEY,
                        preview_hash TEXT NOT NULL,
                        execution_entry_id INTEGER
                    )
                    """
                )
            self._initialized = True

    def append(self, entry: ExecutionJournalEntry) -> ExecutionJournalEntry:
        """Append an execution journal entry and return it with its assigned id."""

        self.initialize()
        if entry.executed_at.tzinfo is None or entry.executed_at.utcoffset() is None:
            raise ValueError("executed_at must be timezone-aware")
        normalized_label = entry.paper_trade.intent.label.strip()
        if not normalized_label:
            raise ValueError("label must be non-empty")
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
                    confirmation_entry_id,
                    label,
                    entry_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_entry.executed_at.isoformat(),
                    normalized_entry.adapter,
                    normalized_entry.status,
                    normalized_entry.paper_trade_id,
                    normalized_entry.confirmation_entry_id,
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

    def reserve_live_submission(self, *, confirmation_entry_id: int, preview_hash: str) -> bool:
        """Reserve one live submission slot for a confirmed preview."""

        self.initialize()
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with sqlite3.connect(self.database_path) as connection:
            before_changes = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO live_submission_reservations (
                    confirmation_entry_id,
                    preview_hash
                ) VALUES (?, ?)
                """,
                (confirmation_entry_id, normalized_preview_hash),
            )
            return connection.total_changes > before_changes

    def mark_live_submission_completed(
        self,
        *,
        confirmation_entry_id: int,
        execution_entry_id: int,
    ) -> None:
        """Attach the persisted execution journal id to an existing reservation."""

        self.initialize()
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        if execution_entry_id < 1:
            raise ValueError("execution_entry_id must be positive")
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                UPDATE live_submission_reservations
                SET execution_entry_id = ?
                WHERE confirmation_entry_id = ?
                """,
                (execution_entry_id, confirmation_entry_id),
            )

    def find_by_confirmation_entry_id(
        self,
        confirmation_entry_id: int,
    ) -> ExecutionJournalEntry | None:
        """Return the most recent execution journal entry for one confirmation id."""

        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE confirmation_entry_id = ?
                ORDER BY executed_at DESC, id DESC
                LIMIT 1
                """,
                (confirmation_entry_id,),
            ).fetchone()
        if row is None:
            return None
        stored_id, entry_json = row
        return ExecutionJournalEntry.model_validate(
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
