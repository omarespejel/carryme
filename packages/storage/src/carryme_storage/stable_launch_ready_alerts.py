"""Database-backed stable launch-ready alert storage."""

from __future__ import annotations

import json
from pathlib import Path

from carryme_models import StableLaunchReadyAlertEvent

from carryme_storage.db import Database


class StableLaunchReadyAlertStore:
    """Persist and query stable launch-ready alert events."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the stable launch-ready alert table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS stable_launch_ready_alert_events (
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
                CREATE INDEX IF NOT EXISTS idx_stable_launch_ready_alert_events_emitted_at
                ON stable_launch_ready_alert_events(emitted_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stable_launch_ready_alert_events_label
                ON stable_launch_ready_alert_events(label)
                """
            )

    def append(self, event: StableLaunchReadyAlertEvent) -> None:
        """Append one stable launch-ready alert event."""

        self.initialize()
        with self.database.begin() as connection:
            connection.execute(
                """
                INSERT INTO stable_launch_ready_alert_events (
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
                        event.current_stability.snapshot.label
                        if event.current_stability is not None
                        else (
                            event.previous_stability.snapshot.label
                            if event.previous_stability is not None
                            else None
                        )
                    ),
                    event.model_dump_json(),
                ),
            )

    def latest(self, *, label: str | None = None) -> StableLaunchReadyAlertEvent | None:
        """Return the latest stable launch-ready alert event, if any."""

        events = self.list_recent(limit=1, label=label)
        return events[0] if events else None

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[StableLaunchReadyAlertEvent]:
        """Return recent stable launch-ready alert events."""

        self.initialize()
        query = """
            SELECT event_json
            FROM stable_launch_ready_alert_events
        """
        params: tuple[object, ...]
        if label:
            query += " WHERE label = ?"
            params = (label, limit)
        else:
            params = (limit,)
        query += " ORDER BY emitted_at DESC, rowid DESC LIMIT ?"

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            StableLaunchReadyAlertEvent.model_validate(json.loads(event_json))
            for (event_json,) in rows
        ]
