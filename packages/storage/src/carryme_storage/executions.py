"""Database-backed execution journal storage."""

from __future__ import annotations

import json
from datetime import UTC
from pathlib import Path
from threading import Lock

from carryme_models import ExecutionJournalEntry

from carryme_storage.db import Database


class ExecutionJournalStore:
    """Persist and query append-only execution journal entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        """Create the execution journal table if it does not exist."""

        if self._initialized:
            return

        with self._initialize_lock:
            if self._initialized:
                return
            with self.database.begin() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS execution_journal_entries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        executed_at TEXT NOT NULL,
                        adapter TEXT NOT NULL,
                        status TEXT NOT NULL,
                        paper_trade_id INTEGER,
                        preview_hash TEXT,
                        confirmation_entry_id INTEGER,
                        label TEXT NOT NULL,
                        entry_json TEXT NOT NULL
                    )
                    """
                )
                columns = connection.table_columns("execution_journal_entries")
                if "confirmation_entry_id" not in columns:
                    connection.execute(
                        """
                        ALTER TABLE execution_journal_entries
                        ADD COLUMN confirmation_entry_id INTEGER
                        """
                    )
                if "preview_hash" not in columns:
                    connection.execute(
                        """
                        ALTER TABLE execution_journal_entries
                        ADD COLUMN preview_hash TEXT
                        """
                    )
                    legacy_rows = connection.execute(
                        """
                        SELECT id, entry_json
                        FROM execution_journal_entries
                        WHERE preview_hash IS NULL
                        """
                    ).fetchall()
                    for entry_id, entry_json in legacy_rows:
                        preview_hash = json.loads(entry_json).get("preview_hash")
                        if isinstance(preview_hash, str):
                            connection.execute(
                                """
                                UPDATE execution_journal_entries
                                SET preview_hash = ?
                                WHERE id = ?
                                """,
                                (preview_hash.strip() or None, entry_id),
                            )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_executed_at
                    ON execution_journal_entries(executed_at DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_active_live
                    ON execution_journal_entries(status, executed_at DESC, id DESC)
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
                    CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_confirmation_preview
                    ON execution_journal_entries(confirmation_entry_id, preview_hash)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_journal_entries_paper_trade_preview
                    ON execution_journal_entries(
                        paper_trade_id,
                        preview_hash,
                        executed_at DESC,
                        id DESC
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS live_submission_reservations (
                        confirmation_entry_id INTEGER NOT NULL,
                        preview_hash TEXT NOT NULL,
                        execution_entry_id INTEGER,
                        PRIMARY KEY (confirmation_entry_id, preview_hash)
                    )
                    """
                )
                reservation_pk = connection.primary_key_columns("live_submission_reservations")
                if reservation_pk != ["confirmation_entry_id", "preview_hash"]:
                    connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS live_submission_reservations_v2 (
                            confirmation_entry_id INTEGER NOT NULL,
                            preview_hash TEXT NOT NULL,
                            execution_entry_id INTEGER,
                            PRIMARY KEY (confirmation_entry_id, preview_hash)
                        )
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO live_submission_reservations_v2 (
                            confirmation_entry_id,
                            preview_hash,
                            execution_entry_id
                        )
                        SELECT confirmation_entry_id, preview_hash, execution_entry_id
                        FROM live_submission_reservations
                        ON CONFLICT(confirmation_entry_id, preview_hash) DO NOTHING
                        """
                    )
                    connection.execute("DROP TABLE live_submission_reservations")
                    connection.execute(
                        """
                        ALTER TABLE live_submission_reservations_v2
                        RENAME TO live_submission_reservations
                        """
                    )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_live_submission_reservations_confirmation_entry_id
                    ON live_submission_reservations(confirmation_entry_id)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pair_open_live_submission_reservations (
                        paper_trade_id INTEGER NOT NULL,
                        preview_hash TEXT NOT NULL,
                        confirmation_entry_id INTEGER NOT NULL,
                        execution_entry_id INTEGER,
                        PRIMARY KEY (paper_trade_id, preview_hash)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_pair_open_live_submission_reservations_confirmation_entry_id
                    ON pair_open_live_submission_reservations(confirmation_entry_id)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pair_close_live_submission_reservations (
                        paper_trade_id INTEGER NOT NULL,
                        preview_hash TEXT NOT NULL,
                        confirmation_entry_id INTEGER NOT NULL,
                        execution_entry_id INTEGER,
                        PRIMARY KEY (paper_trade_id, preview_hash)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_pair_close_live_submission_reservations_confirmation_entry_id
                    ON pair_close_live_submission_reservations(confirmation_entry_id)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cleanup_live_submission_reservations (
                        paper_trade_id INTEGER NOT NULL,
                        preview_hash TEXT NOT NULL,
                        confirmation_entry_id INTEGER NOT NULL,
                        execution_entry_id INTEGER,
                        PRIMARY KEY (paper_trade_id, preview_hash)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_cleanup_live_submission_reservations_confirmation_entry_id
                    ON cleanup_live_submission_reservations(confirmation_entry_id)
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
        normalized_preview_hash = (
            entry.preview_hash.strip() if isinstance(entry.preview_hash, str) else None
        ) or None
        normalized_paper_trade = entry.paper_trade.model_copy(
            update={
                "intent": entry.paper_trade.intent.model_copy(update={"label": normalized_label})
            }
        )
        normalized_entry = entry.model_copy(
            update={
                "executed_at": entry.executed_at.astimezone(UTC),
                "preview_hash": normalized_preview_hash,
                "paper_trade": normalized_paper_trade,
            }
        )
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
                """
                INSERT INTO execution_journal_entries (
                    executed_at,
                    adapter,
                    status,
                    paper_trade_id,
                    preview_hash,
                    confirmation_entry_id,
                    label,
                    entry_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_entry.executed_at.isoformat(),
                    normalized_entry.adapter,
                    normalized_entry.status,
                    normalized_entry.paper_trade_id,
                    normalized_entry.preview_hash,
                    normalized_entry.confirmation_entry_id,
                    normalized_label,
                    normalized_entry.model_dump_json(),
                ),
            )
        return ExecutionJournalEntry.model_validate(
            {
                **normalized_entry.model_dump(mode="json"),
                "entry_id": row_id,
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
        with self.database.begin() as connection:
            result = connection.execute(
                """
                INSERT INTO live_submission_reservations (
                    confirmation_entry_id,
                    preview_hash
                ) VALUES (?, ?)
                ON CONFLICT(confirmation_entry_id, preview_hash) DO NOTHING
                """,
                (confirmation_entry_id, normalized_preview_hash),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def mark_live_submission_completed(
        self,
        *,
        confirmation_entry_id: int,
        preview_hash: str,
        execution_entry_id: int,
    ) -> None:
        """Attach the persisted execution journal id to an existing reservation."""

        self.initialize()
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        if execution_entry_id < 1:
            raise ValueError("execution_entry_id must be positive")
        with self.database.begin() as connection:
            connection.execute(
                """
                UPDATE live_submission_reservations
                SET execution_entry_id = ?
                WHERE confirmation_entry_id = ? AND preview_hash = ?
                """,
                (execution_entry_id, confirmation_entry_id, normalized_preview_hash),
            )

    def release_live_submission(
        self,
        *,
        confirmation_entry_id: int,
        preview_hash: str,
    ) -> bool:
        """Release one in-flight live submission reservation that never journaled."""

        self.initialize()
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            result = connection.execute(
                """
                DELETE FROM live_submission_reservations
                WHERE confirmation_entry_id = ?
                  AND preview_hash = ?
                  AND execution_entry_id IS NULL
                """,
                (confirmation_entry_id, normalized_preview_hash),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def reserve_pair_open_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        confirmation_entry_id: int,
    ) -> bool:
        """Reserve a paired-open live submission for one trade and preview hash."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            result = connection.execute(
                """
                INSERT INTO pair_open_live_submission_reservations (
                    paper_trade_id,
                    preview_hash,
                    confirmation_entry_id
                ) VALUES (?, ?, ?)
                ON CONFLICT(paper_trade_id, preview_hash) DO NOTHING
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def reserve_pair_open_and_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        confirmation_entry_id: int,
    ) -> str:
        """Reserve paired-open and confirmation submission slots atomically."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            pair_open_result = connection.execute(
                """
                INSERT INTO pair_open_live_submission_reservations (
                    paper_trade_id,
                    preview_hash,
                    confirmation_entry_id
                ) VALUES (?, ?, ?)
                ON CONFLICT(paper_trade_id, preview_hash) DO NOTHING
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
            pair_open_rowcount = getattr(pair_open_result, "rowcount", None)
            if not (isinstance(pair_open_rowcount, int) and pair_open_rowcount > 0):
                return "pair_open_conflict"

            live_result = connection.execute(
                """
                INSERT INTO live_submission_reservations (
                    confirmation_entry_id,
                    preview_hash
                ) VALUES (?, ?)
                ON CONFLICT(confirmation_entry_id, preview_hash) DO NOTHING
                """,
                (confirmation_entry_id, normalized_preview_hash),
            )
            live_rowcount = getattr(live_result, "rowcount", None)
            if isinstance(live_rowcount, int) and live_rowcount > 0:
                return "reserved"

            connection.execute(
                """
                DELETE FROM pair_open_live_submission_reservations
                WHERE paper_trade_id = ? AND preview_hash = ? AND confirmation_entry_id = ?
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
            return "live_conflict"

    def mark_pair_open_live_submission_completed(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        execution_entry_id: int,
    ) -> None:
        """Attach the execution journal id to a paired-open live submission reservation."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        if execution_entry_id < 1:
            raise ValueError("execution_entry_id must be positive")
        with self.database.begin() as connection:
            connection.execute(
                """
                UPDATE pair_open_live_submission_reservations
                SET execution_entry_id = ?
                WHERE paper_trade_id = ? AND preview_hash = ?
                """,
                (execution_entry_id, paper_trade_id, normalized_preview_hash),
            )

    def reserve_pair_close_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        confirmation_entry_id: int,
    ) -> bool:
        """Reserve a pair-close live submission for one trade and preview hash."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            result = connection.execute(
                """
                INSERT INTO pair_close_live_submission_reservations (
                    paper_trade_id,
                    preview_hash,
                    confirmation_entry_id
                ) VALUES (?, ?, ?)
                ON CONFLICT(paper_trade_id, preview_hash) DO NOTHING
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def mark_pair_close_live_submission_completed(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        execution_entry_id: int,
    ) -> None:
        """Attach the execution journal id to a pair-close live submission reservation."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        if execution_entry_id < 1:
            raise ValueError("execution_entry_id must be positive")
        with self.database.begin() as connection:
            connection.execute(
                """
                UPDATE pair_close_live_submission_reservations
                SET execution_entry_id = ?
                WHERE paper_trade_id = ? AND preview_hash = ?
                """,
                (execution_entry_id, paper_trade_id, normalized_preview_hash),
            )

    def release_pair_close_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        confirmation_entry_id: int,
    ) -> bool:
        """Release one pair-close reservation that never reached the journal."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            result = connection.execute(
                """
                DELETE FROM pair_close_live_submission_reservations
                WHERE paper_trade_id = ?
                  AND preview_hash = ?
                  AND confirmation_entry_id = ?
                  AND execution_entry_id IS NULL
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def reserve_cleanup_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        confirmation_entry_id: int,
    ) -> bool:
        """Reserve a cleanup live submission for one trade and preview hash."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            result = connection.execute(
                """
                INSERT INTO cleanup_live_submission_reservations (
                    paper_trade_id,
                    preview_hash,
                    confirmation_entry_id
                ) VALUES (?, ?, ?)
                ON CONFLICT(paper_trade_id, preview_hash) DO NOTHING
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def mark_cleanup_live_submission_completed(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        execution_entry_id: int,
    ) -> None:
        """Attach the execution journal id to a cleanup live submission reservation."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        if execution_entry_id < 1:
            raise ValueError("execution_entry_id must be positive")
        with self.database.begin() as connection:
            connection.execute(
                """
                UPDATE cleanup_live_submission_reservations
                SET execution_entry_id = ?
                WHERE paper_trade_id = ? AND preview_hash = ?
                """,
                (execution_entry_id, paper_trade_id, normalized_preview_hash),
            )

    def release_cleanup_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
        confirmation_entry_id: int,
    ) -> bool:
        """Release one cleanup reservation that never reached the journal."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            result = connection.execute(
                """
                DELETE FROM cleanup_live_submission_reservations
                WHERE paper_trade_id = ?
                  AND preview_hash = ?
                  AND confirmation_entry_id = ?
                  AND execution_entry_id IS NULL
                """,
                (paper_trade_id, normalized_preview_hash, confirmation_entry_id),
            )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def has_pending_cleanup_live_submission(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
    ) -> bool:
        """Return whether one cleanup submission is reserved but not journaled."""

        self.initialize()
        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM cleanup_live_submission_reservations
                WHERE paper_trade_id = ?
                  AND preview_hash = ?
                  AND execution_entry_id IS NULL
                LIMIT 1
                """,
                (paper_trade_id, normalized_preview_hash),
            ).fetchone()
        return row is not None

    def find_by_paper_trade_preview_hash(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
    ) -> ExecutionJournalEntry | None:
        """Return the most recent execution for one paper trade and preview hash."""

        if paper_trade_id < 1:
            raise ValueError("paper_trade_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE paper_trade_id = ? AND preview_hash = ?
                ORDER BY executed_at DESC, id DESC
                LIMIT 1
                """,
                (paper_trade_id, normalized_preview_hash),
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

    def find_by_confirmation(
        self,
        *,
        confirmation_entry_id: int,
        preview_hash: str,
    ) -> ExecutionJournalEntry | None:
        """Return the most recent execution journal entry for one confirmation/hash pair."""

        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE confirmation_entry_id = ? AND preview_hash = ?
                ORDER BY executed_at DESC, id DESC
                LIMIT 1
                """,
                (confirmation_entry_id, normalized_preview_hash),
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

    def find_by_confirmation_entry_id(
        self,
        confirmation_entry_id: int,
    ) -> ExecutionJournalEntry | None:
        """Return the most recent execution journal entry for one confirmation id."""

        if confirmation_entry_id < 1:
            raise ValueError("confirmation_entry_id must be positive")
        self.initialize()
        with self.database.begin() as connection:
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
        offset: int = 0,
    ) -> list[ExecutionJournalEntry]:
        """Return recent execution journal entries."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset must be at least 0")
        self.initialize()
        normalized_label = label.strip() if label is not None else None
        query = """
            SELECT id, entry_json
            FROM execution_journal_entries
        """
        params: tuple[object, ...]
        if normalized_label:
            query += " WHERE label = ?"
            params = (normalized_label, limit, offset)
        else:
            params = (limit, offset)
        query += " ORDER BY executed_at DESC, id DESC LIMIT ? OFFSET ?"

        with self.database.begin() as connection:
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

    def list_recent_active_live(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> list[ExecutionJournalEntry]:
        """Return recent live executions that still have an in-flight status."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset must be at least 0")
        self.initialize()
        with self.database.begin() as connection:
            rows = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE status IN (?, ?) AND entry_json LIKE ?
                ORDER BY executed_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                ("submitted", "partial", '%"mode":"live"%', limit, offset),
            ).fetchall()

        return [
            ExecutionJournalEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]

    def get(self, entry_id: int) -> ExecutionJournalEntry | None:
        """Return one execution journal entry by id."""

        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE id = ?
                """,
                (entry_id,),
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

    def latest_for_paper_trade(self, paper_trade_id: int) -> ExecutionJournalEntry | None:
        """Return the newest execution journal entry for one paper trade."""

        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE paper_trade_id = ?
                ORDER BY executed_at DESC, id DESC
                LIMIT 1
                """,
                (paper_trade_id,),
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

    def list_latest_for_paper_trades(
        self,
        paper_trade_ids: list[int],
    ) -> dict[int, ExecutionJournalEntry]:
        """Return the newest execution journal entry for each requested paper trade."""

        unique_ids = list(dict.fromkeys(paper_trade_ids))
        if not unique_ids:
            return {}
        if any(paper_trade_id < 1 for paper_trade_id in unique_ids):
            raise ValueError("paper_trade_ids must be positive")
        self.initialize()
        placeholders = ", ".join("?" for _ in unique_ids)
        with self.database.begin() as connection:
            rows = connection.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        paper_trade_id,
                        id,
                        entry_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY paper_trade_id
                            ORDER BY executed_at DESC, id DESC
                        ) AS row_number
                    FROM execution_journal_entries
                    WHERE paper_trade_id IN ({placeholders})
                )
                SELECT paper_trade_id, id, entry_json
                FROM ranked
                WHERE row_number = 1
                """,
                tuple(unique_ids),
            ).fetchall()

        return {
            int(paper_trade_id): ExecutionJournalEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for paper_trade_id, stored_id, entry_json in rows
        }

    def list_for_paper_trade(
        self,
        paper_trade_id: int,
        *,
        limit: int = 100,
    ) -> list[ExecutionJournalEntry]:
        """Return recent execution journal entries for one paper trade."""

        self.initialize()
        with self.database.begin() as connection:
            rows = connection.execute(
                """
                SELECT id, entry_json
                FROM execution_journal_entries
                WHERE paper_trade_id = ?
                ORDER BY executed_at DESC, id DESC
                LIMIT ?
                """,
                (paper_trade_id, limit),
            ).fetchall()

        return [
            ExecutionJournalEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]
