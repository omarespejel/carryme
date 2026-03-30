"""Database-backed execution alert storage."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import cast

from carryme_models import ExecutionAlertEvent, ExecutionPairStatus

from carryme_storage.db import Database, DatabaseConnection

ALERTING_EXECUTION_STATES = {"cleanup_needed", "review_required"}


def _normalize_emitted_at(emitted_at: datetime) -> datetime:
    """Normalize alert timestamps to timezone-aware UTC values."""

    if emitted_at.tzinfo is None or emitted_at.utcoffset() is None:
        raise ValueError("emitted_at must be timezone-aware")
    return emitted_at.astimezone(UTC)


def _normalize_event(event: ExecutionAlertEvent) -> ExecutionAlertEvent:
    """Normalize an execution alert event before persistence."""

    return event.model_copy(update={"emitted_at": _normalize_emitted_at(event.emitted_at)})


def _normalize_event_payload(event: ExecutionAlertEvent) -> dict[str, object]:
    """Normalize an execution alert payload before persistence."""

    normalized_event = _normalize_event(event)
    return cast(dict[str, object], json.loads(normalized_event.model_dump_json()))


def _alert_identity_key(event: ExecutionAlertEvent) -> str:
    """Build a deterministic insert key for one alert emission attempt."""

    normalized_emitted_at = _normalize_emitted_at(event.emitted_at)
    return json.dumps(
        {
            "emitted_at": normalized_emitted_at.isoformat(),
            "paper_trade_id": event.paper_trade_id,
            "preview_hash": event.preview_hash,
            "alert_type": event.alert_type,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


class ExecutionAlertStore:
    """Persist and query emitted execution alert events."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        """Create the execution alert table if it does not exist."""

        if self._initialized:
            return

        with self._initialize_lock:
            if self._initialized:
                return
            with self.database.begin() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS execution_alert_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        emitted_at TEXT NOT NULL,
                        paper_trade_id INTEGER NOT NULL,
                        preview_hash TEXT,
                        alert_type TEXT NOT NULL,
                        alert_key TEXT,
                        event_json TEXT NOT NULL
                    )
                    """
                )
                columns = connection.table_columns("execution_alert_events")
                if "alert_key" not in columns:
                    connection.execute(
                        "ALTER TABLE execution_alert_events ADD COLUMN alert_key TEXT"
                    )
                connection.execute(
                    """
                    UPDATE execution_alert_events
                    SET alert_key = 'legacy:' || CAST(id AS TEXT)
                    WHERE alert_key IS NULL
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_alert_events_emitted_at
                    ON execution_alert_events(emitted_at DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_alert_events_paper_trade_id
                    ON execution_alert_events(paper_trade_id)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_execution_alert_events_alert_type
                    ON execution_alert_events(alert_type)
                    """
                )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_alert_events_alert_key
                    ON execution_alert_events(alert_key)
                    """
                )
            self._initialized = True

    def append(self, event: ExecutionAlertEvent) -> bool:
        """Append one execution alert event unless it is an exact duplicate."""

        return self.append_if_changed(event)

    def append_if_changed(
        self,
        event: ExecutionAlertEvent,
        *,
        previous_pair_status: ExecutionPairStatus | None = None,
    ) -> bool:
        """Append one execution alert event when it represents a new alert transition."""

        self.initialize()
        with self.database.begin() as connection:
            return self._append_if_changed_on_connection(
                connection,
                event,
                previous_pair_status=previous_pair_status,
            )

    def _append_if_changed_on_connection(
        self,
        connection: DatabaseConnection,
        event: ExecutionAlertEvent,
        *,
        previous_pair_status: ExecutionPairStatus | None = None,
    ) -> bool:
        """Append one execution alert event using an existing transaction."""

        normalized_event = _normalize_event(event)
        alert_key = _alert_identity_key(normalized_event)
        normalized_payload = _normalize_event_payload(normalized_event)
        latest = self._latest_for_paper_trade(connection, normalized_event.paper_trade_id)
        if latest is not None and latest.alert_type == normalized_event.alert_type:
            same_preview = latest.preview_hash == normalized_event.preview_hash
            continued_same_state = (
                previous_pair_status is not None
                and previous_pair_status.derived_state in ALERTING_EXECUTION_STATES
                and previous_pair_status.derived_state == normalized_event.alert_type
                and previous_pair_status.preview_hash == normalized_event.preview_hash
            )
            exact_retry = same_preview and latest.emitted_at == normalized_event.emitted_at
            if continued_same_state or exact_retry:
                return False
        result = connection.execute(
            """
            INSERT INTO execution_alert_events (
                emitted_at,
                paper_trade_id,
                preview_hash,
                alert_type,
                alert_key,
                event_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(alert_key) DO NOTHING
            """,
            (
                normalized_event.emitted_at.isoformat(),
                normalized_event.paper_trade_id,
                normalized_event.preview_hash,
                normalized_event.alert_type,
                alert_key,
                json.dumps(normalized_payload, sort_keys=True),
            ),
        )
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def latest_for_paper_trade(self, paper_trade_id: int) -> ExecutionAlertEvent | None:
        """Return the newest execution alert for one paper trade."""

        self.initialize()
        with self.database.begin() as connection:
            return self._latest_for_paper_trade(connection, paper_trade_id)

    def list_recent(
        self,
        *,
        limit: int = 50,
        paper_trade_id: int | None = None,
    ) -> list[ExecutionAlertEvent]:
        """Return recent execution alert events."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        query = """
            SELECT event_json
            FROM execution_alert_events
        """
        params: tuple[object, ...]
        if paper_trade_id is not None:
            query += " WHERE paper_trade_id = ?"
            params = (paper_trade_id, limit)
        else:
            params = (limit,)
        query += " ORDER BY emitted_at DESC, id DESC LIMIT ?"

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            ExecutionAlertEvent.model_validate(json.loads(event_json))
            for (event_json,) in rows
        ]

    def _latest_for_paper_trade(
        self,
        connection: DatabaseConnection,
        paper_trade_id: int,
    ) -> ExecutionAlertEvent | None:
        row = self._latest_row_for_paper_trade(connection, paper_trade_id)
        if row is None:
            return None
        return ExecutionAlertEvent.model_validate(json.loads(row[0]))

    def _latest_row_for_paper_trade(
        self,
        connection: DatabaseConnection,
        paper_trade_id: int,
    ) -> tuple[str] | None:
        row = connection.fetchone(
            """
            SELECT event_json
            FROM execution_alert_events
            WHERE paper_trade_id = ?
            ORDER BY emitted_at DESC, id DESC
            LIMIT 1
            """,
            (paper_trade_id,),
        )
        if row is None:
            return None
        return (str(row[0]),)
