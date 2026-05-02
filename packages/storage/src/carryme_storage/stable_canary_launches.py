"""Database-backed stable canary launch record storage."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from carryme_models import StableCanaryLaunchRecord

from carryme_storage.db import Database


class StableCanaryLaunchStore:
    """Persist and query stable canary launch records."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the stable canary launch table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS stable_canary_launch_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    launched_at TEXT NOT NULL,
                    label TEXT NOT NULL,
                    launch_ready_snapshot_id INTEGER NOT NULL,
                    approved_snapshot_id INTEGER NOT NULL,
                    paper_trade_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    record_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stable_canary_launch_records_snapshot
                ON stable_canary_launch_records(launch_ready_snapshot_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stable_canary_launch_records_label
                ON stable_canary_launch_records(label)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS stable_canary_launch_reservations (
                    launch_ready_snapshot_id INTEGER PRIMARY KEY,
                    reserved_at TEXT NOT NULL,
                    label TEXT NOT NULL,
                    owner_id TEXT
                )
                """
            )
            columns = connection.table_columns("stable_canary_launch_reservations")
            if "owner_id" not in columns:
                connection.execute(
                    """
                    ALTER TABLE stable_canary_launch_reservations
                    ADD COLUMN owner_id TEXT
                    """
                )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stable_canary_launch_reservations_label
                ON stable_canary_launch_reservations(label)
                """
            )

    def append(self, record: StableCanaryLaunchRecord) -> StableCanaryLaunchRecord:
        """Append one stable canary launch record."""

        self.initialize()
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
                """
                INSERT INTO stable_canary_launch_records (
                    launched_at,
                    label,
                    launch_ready_snapshot_id,
                    approved_snapshot_id,
                    paper_trade_id,
                    status,
                    record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.launched_at.isoformat(),
                    record.label,
                    record.launch_ready_snapshot_id,
                    record.approved_snapshot_id,
                    record.paper_trade_id,
                    record.status,
                    record.model_dump_json(),
                ),
            )

        return StableCanaryLaunchRecord.model_validate(
            {
                **record.model_dump(mode="python"),
                "launch_id": row_id,
            }
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
        status: str | None = None,
        launch_ready_snapshot_id: int | None = None,
    ) -> list[StableCanaryLaunchRecord]:
        """Return recent stable canary launch records."""

        self.initialize()
        query = """
            SELECT id, record_json
            FROM stable_canary_launch_records
        """
        clauses: list[str] = []
        params: list[object] = []
        if label is not None:
            clauses.append("label = ?")
            params.append(label)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if launch_ready_snapshot_id is not None:
            clauses.append("launch_ready_snapshot_id = ?")
            params.append(launch_ready_snapshot_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY launched_at DESC, id DESC LIMIT ?"
        params.append(limit)

        with self.database.begin() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()

        return [
            StableCanaryLaunchRecord.model_validate(
                {
                    **json.loads(record_json),
                    "launch_id": row_id,
                }
            )
            for row_id, record_json in rows
        ]

    def latest_for_snapshot(
        self,
        launch_ready_snapshot_id: int,
    ) -> StableCanaryLaunchRecord | None:
        """Return the latest launch record for one snapshot, if any."""

        records = self.list_recent(limit=1, launch_ready_snapshot_id=launch_ready_snapshot_id)
        return records[0] if records else None

    def reserve_snapshot_launch(
        self,
        *,
        launch_ready_snapshot_id: int,
        label: str,
        reserved_at: datetime,
        max_age_seconds: int | None = None,
        retention_seconds: int | None = None,
        owner_id: str | None = None,
    ) -> bool:
        """Reserve one launch-ready snapshot for a single live launch attempt."""

        self.initialize()
        if launch_ready_snapshot_id < 1:
            raise ValueError("launch_ready_snapshot_id must be positive")
        normalized_label = label.strip()
        if not normalized_label:
            raise ValueError("label must be non-empty")
        if reserved_at.tzinfo is None or reserved_at.utcoffset() is None:
            raise ValueError("reserved_at must be timezone-aware")
        if max_age_seconds is not None and max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        if retention_seconds is not None and retention_seconds <= 0:
            raise ValueError("retention_seconds must be positive")
        if (
            max_age_seconds is not None
            and retention_seconds is not None
            and retention_seconds < max_age_seconds
        ):
            raise ValueError("retention_seconds must be greater than or equal to max_age_seconds")
        normalized_owner_id = (owner_id or uuid.uuid4().hex).strip()
        if not normalized_owner_id:
            raise ValueError("owner_id must be non-empty")

        normalized_reserved_at = reserved_at.astimezone(UTC)
        with self.database.begin() as connection:
            effective_retention_seconds = retention_seconds
            if effective_retention_seconds is None and max_age_seconds is not None:
                effective_retention_seconds = max(max_age_seconds * 12, 86_400)
            if effective_retention_seconds is not None:
                retention_before = normalized_reserved_at - timedelta(
                    seconds=effective_retention_seconds
                )
                connection.execute(
                    """
                    DELETE FROM stable_canary_launch_reservations
                    WHERE reserved_at < ?
                    """,
                    (retention_before.isoformat(),),
                )
            if max_age_seconds is not None:
                stale_before = normalized_reserved_at - timedelta(seconds=max_age_seconds)
                connection.execute(
                    """
                    DELETE FROM stable_canary_launch_reservations
                    WHERE launch_ready_snapshot_id = ? AND reserved_at < ?
                    """,
                    (launch_ready_snapshot_id, stale_before.isoformat()),
                )
            result = connection.execute(
                """
                INSERT INTO stable_canary_launch_reservations (
                    launch_ready_snapshot_id,
                    reserved_at,
                    label,
                    owner_id
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(launch_ready_snapshot_id) DO NOTHING
                """,
                (
                    launch_ready_snapshot_id,
                    normalized_reserved_at.isoformat(),
                    normalized_label,
                    normalized_owner_id,
                ),
            )

        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def renew_snapshot_launch_reservation(
        self,
        *,
        launch_ready_snapshot_id: int,
        owner_id: str,
        reserved_at: datetime,
    ) -> bool:
        """Renew one snapshot reservation only if the caller still owns its lease."""

        self.initialize()
        if launch_ready_snapshot_id < 1:
            raise ValueError("launch_ready_snapshot_id must be positive")
        normalized_owner_id = owner_id.strip()
        if not normalized_owner_id:
            raise ValueError("owner_id must be non-empty")
        if reserved_at.tzinfo is None or reserved_at.utcoffset() is None:
            raise ValueError("reserved_at must be timezone-aware")

        normalized_reserved_at = reserved_at.astimezone(UTC)
        with self.database.begin() as connection:
            result = connection.execute(
                """
                UPDATE stable_canary_launch_reservations
                SET reserved_at = ?
                WHERE launch_ready_snapshot_id = ? AND owner_id = ?
                """,
                (
                    normalized_reserved_at.isoformat(),
                    launch_ready_snapshot_id,
                    normalized_owner_id,
                ),
            )

        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0
