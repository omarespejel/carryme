"""SQLite-backed candidate alert storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from threading import Lock
from typing import cast

from carryme_models import CandidateAlertEvent


def _normalize_label(label: str | None) -> str | None:
    """Normalize optional labels consistently for write and read paths."""

    if label is None:
        return None
    normalized = label.strip()
    return normalized or None


def _normalize_event_payload(event: CandidateAlertEvent) -> dict[str, object]:
    """Normalize persisted event payloads before storage."""

    raw_payload = cast(dict[str, object], event.model_dump(mode="json"))
    payload = cast(dict[str, object], json.loads(json.dumps(raw_payload)))
    record = cast(dict[str, object], payload["record"])
    pair = cast(dict[str, object], record["pair"])
    pair["label"] = _normalize_label(cast(str | None, pair.get("label")))
    record["pair"] = pair
    payload["record"] = record
    payload["raw_payload"] = raw_payload
    return payload


def _alert_identity_key(event: CandidateAlertEvent) -> str:
    """Build a deterministic dedupe key for candidate alerts."""

    payload = _normalize_event_payload(event)
    payload.pop("raw_payload", None)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )


class CandidateAlertStore:
    """Persist and query emitted candidate alert events."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self._initialized = False
        self._initialize_lock = Lock()

    def initialize(self) -> None:
        """Create the alert table if it does not exist and migrate older schemas."""

        if self._initialized:
            return

        with self._initialize_lock:
            if self._initialized:
                return
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
                        alert_key TEXT,
                        event_json TEXT NOT NULL,
                        raw_event_json TEXT
                    )
                    """
                )
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(candidate_alert_events)"
                    ).fetchall()
                }
                if "alert_key" not in columns:
                    connection.execute(
                        "ALTER TABLE candidate_alert_events ADD COLUMN alert_key TEXT"
                    )
                if "raw_event_json" not in columns:
                    connection.execute(
                        "ALTER TABLE candidate_alert_events ADD COLUMN raw_event_json TEXT"
                    )
                connection.execute(
                    """
                    UPDATE candidate_alert_events
                    SET alert_key = printf('legacy:%s', id)
                    WHERE alert_key IS NULL
                    """
                )
                connection.execute(
                    """
                    UPDATE candidate_alert_events
                    SET raw_event_json = event_json
                    WHERE raw_event_json IS NULL
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
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_alert_events_alert_key
                    ON candidate_alert_events(alert_key)
                    """
                )
            self._initialized = True

    def append(self, event: CandidateAlertEvent) -> bool:
        """Append a candidate alert event when it has not been persisted already."""

        self.initialize()
        normalized_payload = _normalize_event_payload(event)
        raw_payload = cast(dict[str, object], normalized_payload.pop("raw_payload"))
        record_payload = cast(dict[str, object], normalized_payload["record"])
        pair_payload = cast(dict[str, object], record_payload["pair"])
        normalized_label = cast(str | None, pair_payload["label"])
        alert_key = _alert_identity_key(event)
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO candidate_alert_events (
                    emitted_at,
                    label,
                    canonical_symbol,
                    alert_type,
                    alert_key,
                    event_json,
                    raw_event_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.emitted_at.isoformat(),
                    normalized_label,
                    event.record.opportunity.canonical_symbol,
                    event.alert_type,
                    alert_key,
                    json.dumps(normalized_payload, sort_keys=True),
                    json.dumps(raw_payload, sort_keys=True),
                ),
            )
        return cursor.rowcount > 0

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[CandidateAlertEvent]:
        """Return recent candidate alert events."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        normalized_label = _normalize_label(label)
        query = """
            SELECT event_json
            FROM candidate_alert_events
        """
        params: tuple[object, ...]
        if normalized_label:
            query += " WHERE label = ?"
            params = (normalized_label, limit)
        else:
            params = (limit,)
        query += " ORDER BY emitted_at DESC, id DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            CandidateAlertEvent.model_validate(json.loads(event_json)) for (event_json,) in rows
        ]
