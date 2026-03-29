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

        normalized_label = entry.label.strip()
        if not normalized_label:
            raise ValueError("label must be non-empty")
        normalized_preview_hash = entry.preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        normalized_entry = CleanupPreviewConfirmationEntry.model_validate(
            {
                **entry.model_dump(mode="json"),
                "label": normalized_label,
                "preview_hash": normalized_preview_hash,
            }
        )
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
                    normalized_entry.confirmed_at.isoformat(),
                    normalized_entry.paper_trade_id,
                    normalized_entry.label,
                    normalized_entry.preview_hash,
                    normalized_entry.model_dump_json(),
                ),
            )
        return CleanupPreviewConfirmationEntry.model_validate(
            {
                **normalized_entry.model_dump(mode="json"),
                "entry_id": cursor.lastrowid,
            }
        )

    def _normalize_entry(
        self,
        *,
        stored_id: int,
        stored_entry_json: str,
        stored_label: str | None = None,
        stored_preview_hash: str | None = None,
    ) -> CleanupPreviewConfirmationEntry:
        """Normalize one stored entry and backfill legacy whitespace if needed."""

        raw_entry = CleanupPreviewConfirmationEntry.model_validate(
            {
                **json.loads(stored_entry_json),
                "entry_id": stored_id,
            }
        )
        normalized_label = raw_entry.label.strip()
        normalized_preview_hash = raw_entry.preview_hash.strip()
        if not normalized_label:
            raise ValueError("label must be non-empty")
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        normalized_entry = raw_entry.model_copy(
            update={
                "label": normalized_label,
                "preview_hash": normalized_preview_hash,
                "preview": raw_entry.preview.model_copy(
                    update={
                        "preview_hash": normalized_preview_hash,
                    }
                ),
            }
        )
        normalized_entry_json = normalized_entry.model_dump_json()
        if (
            stored_label != normalized_label
            or stored_preview_hash != normalized_preview_hash
            or stored_entry_json != normalized_entry_json
        ):
            with sqlite3.connect(self.database_path) as connection:
                connection.execute(
                    """
                    UPDATE cleanup_preview_confirmation_entries
                    SET label = ?, preview_hash = ?, entry_json = ?
                    WHERE id = ?
                    """,
                    (
                        normalized_label,
                        normalized_preview_hash,
                        normalized_entry_json,
                        stored_id,
                    ),
                )
        return normalized_entry

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
        paper_trade_id: int | None = None,
    ) -> list[CleanupPreviewConfirmationEntry]:
        """Return recent cleanup preview confirmation entries."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        normalized_label = label.strip() if label is not None else None
        clauses: list[str] = []
        values: list[object] = []
        if normalized_label:
            clauses.append("label = ?")
            values.append(normalized_label)
        if paper_trade_id is not None:
            clauses.append("paper_trade_id = ?")
            values.append(paper_trade_id)
        query = """
            SELECT id, label, preview_hash, entry_json
            FROM cleanup_preview_confirmation_entries
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY confirmed_at DESC, id DESC LIMIT ?"
        values.append(limit)

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, tuple(values)).fetchall()

        return [
            self._normalize_entry(
                stored_id=stored_id,
                stored_label=stored_label,
                stored_preview_hash=stored_preview_hash,
                stored_entry_json=entry_json,
            )
            for stored_id, stored_label, stored_preview_hash, entry_json in rows
        ]

    def find_latest_by_preview_hash(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
    ) -> CleanupPreviewConfirmationEntry | None:
        """Return the latest stored cleanup confirmation for one trade/hash pair."""

        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        self.initialize()
        query = """
            SELECT id, label, preview_hash, entry_json
            FROM cleanup_preview_confirmation_entries
            WHERE paper_trade_id = ? AND TRIM(preview_hash) = ?
            ORDER BY confirmed_at DESC, id DESC
            LIMIT 1
        """
        with sqlite3.connect(self.database_path) as connection:
            row = connection.execute(
                query,
                (paper_trade_id, normalized_preview_hash),
            ).fetchone()
        if row is None:
            return None
        stored_id, stored_label, stored_preview_hash, entry_json = row
        return self._normalize_entry(
            stored_id=stored_id,
            stored_label=stored_label,
            stored_preview_hash=stored_preview_hash,
            stored_entry_json=entry_json,
        )
