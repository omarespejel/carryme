"""SQLite-backed opportunity history storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from threading import Lock

from carryme_models import OpportunityRecord


def _normalize_label(label: str | None) -> str | None:
    """Normalize optional labels consistently for write and read paths."""

    if label is None:
        return None
    normalized = label.strip()
    return normalized or None


def _normalize_pair_payload(pair_payload: dict[str, object]) -> dict[str, object]:
    """Normalize persisted pair payloads before model validation."""

    normalized = dict(pair_payload)
    raw_label = normalized.get("label")
    normalized["label"] = _normalize_label(raw_label if isinstance(raw_label, str) else None)
    return normalized


class OpportunityHistoryStore:
    """Persist and query scored opportunity history."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        """Create the history table if it does not exist."""

        if self._initialized:
            return

        with self._initialize_lock:
            if self._initialized:
                return
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
            self._initialized = True

    def append(self, record: OpportunityRecord) -> None:
        """Append a scored opportunity to history."""

        self.initialize()
        normalized_label = _normalize_label(record.pair.label)
        pair_payload = record.pair.model_dump()
        pair_payload["label"] = normalized_label
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
                    normalized_label,
                    record.opportunity.canonical_symbol,
                    json.dumps(pair_payload),
                    record.opportunity.model_dump_json(),
                ),
            )

    def list_recent(self, *, limit: int = 50, label: str | None = None) -> list[OpportunityRecord]:
        """Return recent opportunity history rows."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        normalized_label = _normalize_label(label)
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
        query += " ORDER BY recorded_at DESC, id DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            OpportunityRecord.model_validate(
                {
                    "recorded_at": recorded_at,
                    "pair": _normalize_pair_payload(json.loads(pair_json)),
                    "opportunity": json.loads(opportunity_json),
                }
            )
            for recorded_at, pair_json, opportunity_json in rows
        ]
