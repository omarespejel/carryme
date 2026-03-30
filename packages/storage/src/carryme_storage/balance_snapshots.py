"""Database-backed balance snapshot storage."""

from __future__ import annotations

import json
from pathlib import Path

from carryme_models import VenueBalanceSnapshot

from carryme_storage.db import Database


class BalanceSnapshotStore:
    """Persist and query append-only venue balance snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the balance snapshot table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS balance_snapshot_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    paper_trade_id INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_balance_snapshot_entries_paper_trade
                ON balance_snapshot_entries(paper_trade_id, captured_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_balance_snapshot_entries_stage
                ON balance_snapshot_entries(stage, captured_at DESC)
                """
            )

    def append(self, snapshot: VenueBalanceSnapshot) -> VenueBalanceSnapshot:
        """Append one balance snapshot and return it with its assigned id."""

        self.initialize()
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
                """
                INSERT INTO balance_snapshot_entries (
                    captured_at,
                    paper_trade_id,
                    label,
                    stage,
                    venue,
                    snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.captured_at.isoformat(),
                    snapshot.paper_trade_id,
                    snapshot.label,
                    snapshot.stage,
                    snapshot.venue,
                    snapshot.model_dump_json(),
                ),
            )
        return snapshot.model_copy(update={"snapshot_id": row_id})

    def list_recent(
        self,
        *,
        limit: int = 100,
        paper_trade_id: int | None = None,
        label: str | None = None,
        stage: str | None = None,
        venue: str | None = None,
    ) -> list[VenueBalanceSnapshot]:
        """Return recent balance snapshots with optional filters."""

        self.initialize()
        query = """
            SELECT id, snapshot_json
            FROM balance_snapshot_entries
        """
        filters: list[str] = []
        params: list[object] = []
        if paper_trade_id is not None:
            filters.append("paper_trade_id = ?")
            params.append(paper_trade_id)
        if label is not None:
            filters.append("label = ?")
            params.append(label)
        if stage is not None:
            filters.append("stage = ?")
            params.append(stage)
        if venue is not None:
            filters.append("venue = ?")
            params.append(venue)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY captured_at DESC, id DESC LIMIT ?"
        params.append(limit)

        with self.database.begin() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()

        return [
            VenueBalanceSnapshot.model_validate(
                {
                    **json.loads(snapshot_json),
                    "snapshot_id": snapshot_id,
                }
            )
            for snapshot_id, snapshot_json in rows
        ]
