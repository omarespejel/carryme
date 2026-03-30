"""SQLite-backed system-state alert storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import SystemStateAlertEvent


class SystemStateAlertStore:
    """Persist and query emitted system-state alert events."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the system-state alert table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS system_state_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    emitted_at TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    event_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_system_state_alert_events_emitted_at
                ON system_state_alert_events(emitted_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_system_state_alert_events_venue
                ON system_state_alert_events(venue)
                """
            )

    def append(self, event: SystemStateAlertEvent) -> None:
        """Append one system-state alert event."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO system_state_alert_events (
                    emitted_at,
                    venue,
                    alert_type,
                    event_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    event.emitted_at.isoformat(),
                    event.venue,
                    event.alert_type,
                    event.model_dump_json(),
                ),
            )

    def latest(self, *, venue: str | None = None) -> SystemStateAlertEvent | None:
        """Return the latest system-state alert event, if any."""

        events = self.list_recent(limit=1, venue=venue)
        return events[0] if events else None

    def list_recent(
        self,
        *,
        limit: int = 50,
        venue: str | None = None,
    ) -> list[SystemStateAlertEvent]:
        """Return recent system-state alert events."""

        self.initialize()
        query = """
            SELECT event_json
            FROM system_state_alert_events
        """
        params: tuple[object, ...]
        if venue:
            query += " WHERE venue = ?"
            params = (venue, limit)
        else:
            params = (limit,)
        query += " ORDER BY emitted_at DESC, rowid DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            SystemStateAlertEvent.model_validate(json.loads(event_json)) for (event_json,) in rows
        ]
