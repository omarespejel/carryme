"""SQLite-backed execution alert storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from threading import Lock
from typing import cast

from carryme_models import ExecutionAlertEvent, ExecutionPairStatus

ALERTING_EXECUTION_STATES = {"cleanup_needed", "review_required"}


def _normalize_event_payload(event: ExecutionAlertEvent) -> dict[str, object]:
    """Normalize an execution alert payload before persistence."""

    return cast(dict[str, object], json.loads(event.model_dump_json()))


def _alert_identity_key(event: ExecutionAlertEvent) -> str:
    """Build a deterministic insert key for one alert emission attempt."""

    return json.dumps(
        {
            "emitted_at": event.emitted_at.isoformat(),
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
        self.database_path = Path(database_path)
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        """Create the execution alert table if it does not exist."""

        if self._initialized:
            return

        with self._initialize_lock:
            if self._initialized:
                return
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.database_path) as connection:
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
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(execution_alert_events)"
                    ).fetchall()
                }
                if "alert_key" not in columns:
                    connection.execute(
                        "ALTER TABLE execution_alert_events ADD COLUMN alert_key TEXT"
                    )
                connection.execute(
                    """
                    UPDATE execution_alert_events
                    SET alert_key = printf('legacy:%s', id)
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
        alert_key = _alert_identity_key(event)
        normalized_payload = _normalize_event_payload(event)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            latest = self._latest_for_paper_trade(connection, event.paper_trade_id)
            if latest is not None and latest.alert_type == event.alert_type:
                same_preview = latest.preview_hash == event.preview_hash
                continued_same_state = (
                    previous_pair_status is not None
                    and previous_pair_status.derived_state in ALERTING_EXECUTION_STATES
                    and previous_pair_status.derived_state == event.alert_type
                    and previous_pair_status.preview_hash == event.preview_hash
                )
                exact_retry = same_preview and latest.emitted_at == event.emitted_at
                if continued_same_state or exact_retry:
                    return False
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO execution_alert_events (
                    emitted_at,
                    paper_trade_id,
                    preview_hash,
                    alert_type,
                    alert_key,
                    event_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.emitted_at.isoformat(),
                    event.paper_trade_id,
                    event.preview_hash,
                    event.alert_type,
                    alert_key,
                    json.dumps(normalized_payload, sort_keys=True),
                ),
            )
        return cursor.rowcount > 0

    def latest_for_paper_trade(self, paper_trade_id: int) -> ExecutionAlertEvent | None:
        """Return the newest execution alert for one paper trade."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            row = self._latest_row_for_paper_trade(connection, paper_trade_id)

        if row is None:
            return None
        return ExecutionAlertEvent.model_validate(json.loads(row[0]))

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
        query += " ORDER BY emitted_at DESC, rowid DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            ExecutionAlertEvent.model_validate(json.loads(event_json))
            for (event_json,) in rows
        ]

    def _latest_for_paper_trade(
        self,
        connection: sqlite3.Connection,
        paper_trade_id: int,
    ) -> ExecutionAlertEvent | None:
        row = self._latest_row_for_paper_trade(connection, paper_trade_id)
        if row is None:
            return None
        return ExecutionAlertEvent.model_validate(json.loads(row[0]))

    def _latest_row_for_paper_trade(
        self,
        connection: sqlite3.Connection,
        paper_trade_id: int,
    ) -> tuple[str] | None:
        row = connection.execute(
            """
            SELECT event_json
            FROM execution_alert_events
            WHERE paper_trade_id = ?
            ORDER BY emitted_at DESC, id DESC
            LIMIT 1
            """,
            (paper_trade_id,),
        ).fetchone()
        return cast(tuple[str] | None, row)
