"""Database-backed approved canary snapshot storage."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from carryme_models import ApprovedCanarySnapshot

from carryme_storage.db import Database


class ApprovedCanaryStore:
    """Persist and query approved canary snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the approved canary table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS approved_canary_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    label TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_approved_canary_snapshots_captured_at
                ON approved_canary_snapshots(captured_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_approved_canary_snapshots_label
                ON approved_canary_snapshots(label)
                """
            )

    def append(self, snapshot: ApprovedCanarySnapshot) -> ApprovedCanarySnapshot:
        """Append one approved canary snapshot."""

        self.initialize()
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
                """
                INSERT INTO approved_canary_snapshots (
                    captured_at,
                    label,
                    snapshot_json
                ) VALUES (?, ?, ?)
                """,
                (
                    snapshot.captured_at.isoformat(),
                    snapshot.label,
                    snapshot.model_dump_json(),
                ),
            )
        return ApprovedCanarySnapshot.model_validate(
            {
                **snapshot.model_dump(mode="json"),
                "snapshot_id": row_id,
            }
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[ApprovedCanarySnapshot]:
        """Return recent approved canary snapshots."""

        return [
            ApprovedCanarySnapshot.model_validate(
                {
                    **snapshot_payload,
                    "snapshot_id": stored_id,
                }
            )
            for stored_id, snapshot_payload in self.list_recent_payloads(limit=limit, label=label)
        ]

    def list_recent_payloads(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
    ) -> list[tuple[int, dict[str, Any]]]:
        """Return recent approved canary snapshot payloads without model validation."""

        self.initialize()
        query = """
            SELECT id, snapshot_json
            FROM approved_canary_snapshots
        """
        values: list[object] = []
        if label:
            query += " WHERE label = ?"
            values.append(label)
        query += " ORDER BY captured_at DESC, id DESC LIMIT ?"
        values.append(limit)

        with self.database.begin() as connection:
            rows = connection.execute(query, tuple(values)).fetchall()

        return [(stored_id, json.loads(snapshot_json)) for stored_id, snapshot_json in rows]

    def latest(self, *, label: str | None = None) -> ApprovedCanarySnapshot | None:
        """Return the latest approved canary snapshot, if any."""

        snapshots = self.list_recent(limit=1, label=label)
        return snapshots[0] if snapshots else None
