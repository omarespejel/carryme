"""SQLite-backed candidate alert storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import CandidateAlertEvent


class CandidateAlertStore:
    """Persist and query emitted candidate alert events."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the alert table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS candidate_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    emitted_at TEXT NOT NULL,
                    label TEXT,
                    canonical_symbol TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    event_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_candidate_alert_events_emitted_at
                ON candidate_alert_events(emitted_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_candidate_alert_events_label
                ON candidate_alert_events(label)
                """
            )

    def append(self, event: CandidateAlertEvent) -> None:
        """Append a candidate alert event."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO candidate_alert_events (
                    emitted_at,
                    label,
                    canonical_symbol,
                    alert_type,
                    event_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.emitted_at.isoformat(),
                    event.record.pair.label,
                    event.record.opportunity.canonical_symbol,
                    event.alert_type,
                    event.model_dump_json(),
                ),
            )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[CandidateAlertEvent]:
        """Return recent candidate alert events."""

        self.initialize()
        query = """
            SELECT event_json
            FROM candidate_alert_events
        """
        params: tuple[object, ...]
        if label:
            query += " WHERE label = ?"
            params = (label, limit)
        else:
            params = (limit,)
        query += " ORDER BY emitted_at DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            CandidateAlertEvent.model_validate(json.loads(event_json))
            for (event_json,) in rows
        ]
