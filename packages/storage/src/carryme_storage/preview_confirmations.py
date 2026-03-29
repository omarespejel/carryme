"""SQLite-backed preview confirmation storage."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC
from pathlib import Path

from carryme_models import PreviewConfirmationEntry


class PreviewConfirmationStore:
    """Persist and query append-only preview confirmation entries."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the preview confirmation table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS preview_confirmation_entries (
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
                CREATE INDEX IF NOT EXISTS idx_preview_confirmation_entries_confirmed_at
                ON preview_confirmation_entries(confirmed_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_preview_confirmation_entries_label
                ON preview_confirmation_entries(label)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_preview_confirmation_entries_paper_trade_id
                ON preview_confirmation_entries(paper_trade_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_preview_confirmation_trade_hash_confirmed_desc
                ON preview_confirmation_entries(
                    paper_trade_id,
                    preview_hash,
                    confirmed_at DESC,
                    id DESC
                )
                """
            )

    def append(self, entry: PreviewConfirmationEntry) -> PreviewConfirmationEntry:
        """Append a preview confirmation entry and return it with its assigned id."""

        self.initialize()
        if entry.confirmed_at.tzinfo is None or entry.confirmed_at.utcoffset() is None:
            raise ValueError("confirmed_at must be timezone-aware")
        normalized_label = entry.label.strip()
        if not normalized_label:
            raise ValueError("label must be non-empty")
        normalized_preview_hash = entry.preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        if entry.preview.paper_trade_id != entry.paper_trade_id:
            raise ValueError("preview.paper_trade_id must match paper_trade_id")
        normalized_entry = entry.model_copy(
            update={
                "entry_id": None,
                "confirmed_at": entry.confirmed_at.astimezone(UTC),
                "label": normalized_label,
                "preview_hash": normalized_preview_hash,
                "preview": entry.preview.model_copy(
                    update={
                        "paper_trade_id": entry.paper_trade_id,
                        "label": normalized_label,
                        "preview_hash": normalized_preview_hash,
                    }
                ),
            }
        )
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO preview_confirmation_entries (
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
                    normalized_label,
                    normalized_preview_hash,
                    normalized_entry.model_dump_json(),
                ),
            )
        return PreviewConfirmationEntry.model_validate(
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
        paper_trade_id: int | None = None,
    ) -> list[PreviewConfirmationEntry]:
        """Return recent preview confirmation entries."""

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
            SELECT id, entry_json
            FROM preview_confirmation_entries
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY confirmed_at DESC, id DESC LIMIT ?"
        values.append(limit)

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, tuple(values)).fetchall()

        return [
            PreviewConfirmationEntry.model_validate(
                {
                    **json.loads(entry_json),
                    "entry_id": stored_id,
                }
            )
            for stored_id, entry_json in rows
        ]

    def find_latest_by_preview_hash(
        self,
        *,
        paper_trade_id: int,
        preview_hash: str,
    ) -> PreviewConfirmationEntry | None:
        """Return the latest stored confirmation for one trade/hash pair."""

        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise ValueError("preview_hash must be non-empty")
        self.initialize()
        query = """
            SELECT id, entry_json
            FROM preview_confirmation_entries
            WHERE paper_trade_id = ? AND preview_hash = ?
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
        stored_id, entry_json = row
        return PreviewConfirmationEntry.model_validate(
            {
                **json.loads(entry_json),
                "entry_id": stored_id,
            }
        )
