"""Database-backed approved canary alert storage."""

from __future__ import annotations

import json
from pathlib import Path

from carryme_models import ApprovedCanaryAlertEvent

from carryme_storage.db import Database


class ApprovedCanaryAlertStore:
    """Persist and query approved-canary alert events."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the approved-canary alert table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS approved_canary_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    emitted_at TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    label TEXT,
                    event_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_approved_canary_alert_events_emitted_at
                ON approved_canary_alert_events(emitted_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_approved_canary_alert_events_label
                ON approved_canary_alert_events(label)
                """
            )

    def append(self, event: ApprovedCanaryAlertEvent) -> None:
        """Append one approved-canary alert event."""

        self.initialize()
        with self.database.begin() as connection:
            connection.execute(
                """
                INSERT INTO approved_canary_alert_events (
                    emitted_at,
                    alert_type,
                    label,
                    event_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    event.emitted_at.isoformat(),
                    event.alert_type,
                    (
                        event.current_snapshot.label
                        if event.current_snapshot is not None
                        else (
                            event.previous_snapshot.label
                            if event.previous_snapshot is not None
                            else None
                        )
                    ),
                    event.model_dump_json(),
                ),
            )

    def latest(self, *, label: str | None = None) -> ApprovedCanaryAlertEvent | None:
        """Return the latest approved-canary alert event, if any."""

        events = self.list_recent(limit=1, label=label)
        return events[0] if events else None

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[ApprovedCanaryAlertEvent]:
        """Return recent approved-canary alert events."""

        self.initialize()
        query = """
            SELECT event_json
            FROM approved_canary_alert_events
        """
        params: tuple[object, ...]
        if label:
            query += " WHERE label = ?"
            params = (label, limit)
        else:
            params = (limit,)
        query += " ORDER BY emitted_at DESC, id DESC LIMIT ?"

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            ApprovedCanaryAlertEvent.model_validate(json.loads(event_json))
            for (event_json,) in rows
        ]
