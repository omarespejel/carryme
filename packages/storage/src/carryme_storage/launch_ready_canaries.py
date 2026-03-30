"""SQLite-backed launch-ready canary snapshot storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from carryme_models import LaunchReadyCanarySnapshot


class LaunchReadyCanaryStore:
    """Persist and query launch-ready canary snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        """Create the launch-ready canary table if it does not exist."""

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS launch_ready_canary_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    label TEXT NOT NULL,
                    approved_snapshot_id INTEGER,
                    snapshot_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_launch_ready_canary_snapshots_captured_at
                ON launch_ready_canary_snapshots(captured_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_launch_ready_canary_snapshots_label
                ON launch_ready_canary_snapshots(label)
                """
            )

    def append(self, snapshot: LaunchReadyCanarySnapshot) -> LaunchReadyCanarySnapshot:
        """Append one launch-ready canary snapshot."""

        self.initialize()
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO launch_ready_canary_snapshots (
                    captured_at,
                    label,
                    approved_snapshot_id,
                    snapshot_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    snapshot.captured_at.isoformat(),
                    snapshot.label,
                    snapshot.approved_snapshot.snapshot_id,
                    snapshot.model_dump_json(),
                ),
            )
            row_id = cursor.lastrowid

        return LaunchReadyCanarySnapshot.model_validate(
            {
                **snapshot.model_dump(mode="python"),
                "launch_ready_snapshot_id": row_id,
            }
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[LaunchReadyCanarySnapshot]:
        """Return recent launch-ready canary snapshots."""

        self.initialize()
        query = """
            SELECT id, snapshot_json
            FROM launch_ready_canary_snapshots
        """
        params: tuple[object, ...]
        if label:
            query += " WHERE label = ?"
            params = (label, limit)
        else:
            params = (limit,)
        query += " ORDER BY captured_at DESC, id DESC LIMIT ?"

        with sqlite3.connect(self.database_path) as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            LaunchReadyCanarySnapshot.model_validate(
                {
                    **json.loads(snapshot_json),
                    "launch_ready_snapshot_id": row_id,
                }
            )
            for row_id, snapshot_json in rows
        ]

    def latest(self, *, label: str | None = None) -> LaunchReadyCanarySnapshot | None:
        """Return the latest launch-ready canary snapshot, if any."""

        snapshots = self.list_recent(limit=1, label=label)
        return snapshots[0] if snapshots else None
