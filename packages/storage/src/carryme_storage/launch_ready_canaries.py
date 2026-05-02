"""Database-backed launch-ready canary snapshot storage."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from carryme_models import LaunchReadyCanarySnapshot

from carryme_storage.db import Database

MAX_RECENT_LABEL_LIMIT = 250


class LaunchReadyCanaryStore:
    """Persist and query launch-ready canary snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the launch-ready canary table if it does not exist."""

        with self.database.begin() as connection:
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
                CREATE INDEX IF NOT EXISTS idx_launch_ready_canary_snapshots_captured_at_id
                ON launch_ready_canary_snapshots(captured_at DESC, id DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_launch_ready_canary_snapshots_label
                ON launch_ready_canary_snapshots(label)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_launch_ready_canary_snapshots_label_captured_at_id
                ON launch_ready_canary_snapshots(label, captured_at DESC, id DESC)
                """
            )

    def append(self, snapshot: LaunchReadyCanarySnapshot) -> LaunchReadyCanarySnapshot:
        """Append one launch-ready canary snapshot."""

        self.initialize()
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
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

        return [
            LaunchReadyCanarySnapshot.model_validate(
                {
                    **snapshot_payload,
                    "launch_ready_snapshot_id": row_id,
                }
            )
            for row_id, snapshot_payload in self.list_recent_payloads(limit=limit, label=label)
        ]

    def list_recent_payloads(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[tuple[int, dict[str, Any]]]:
        """Return recent launch-ready snapshot payloads without model validation."""

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

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [(row_id, json.loads(snapshot_json)) for row_id, snapshot_json in rows]

    def latest(self, *, label: str | None = None) -> LaunchReadyCanarySnapshot | None:
        """Return the latest launch-ready canary snapshot, if any."""

        snapshots = self.list_recent(limit=1, label=label)
        return snapshots[0] if snapshots else None

    def list_recent_labels(self, *, limit: int = 50) -> list[str]:
        """Return recent distinct labels ordered by latest snapshot timestamp."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        if limit > MAX_RECENT_LABEL_LIMIT:
            raise ValueError(f"limit must be at most {MAX_RECENT_LABEL_LIMIT}")

        self.initialize()
        with self.database.begin() as connection:
            rows = connection.execute(
                """
                SELECT label
                FROM (
                    SELECT
                        label,
                        captured_at,
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY label
                            ORDER BY captured_at DESC, id DESC
                        ) AS label_rank
                    FROM launch_ready_canary_snapshots
                ) latest_label_rows
                WHERE label_rank = 1
                ORDER BY captured_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [str(row[0]) for row in rows]

    def delete_label(self, label: str) -> int:
        """Delete all launch-ready canary snapshots for one label."""

        self.initialize()
        with self.database.begin() as connection:
            result = connection.execute(
                """
                DELETE FROM launch_ready_canary_snapshots
                WHERE label = ?
                """,
                (label,),
            )
        return int(getattr(result, "rowcount", 0) or 0)
