"""Database-backed stable canary launch record storage."""

from __future__ import annotations

import json
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
