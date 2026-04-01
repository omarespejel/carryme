"""Database-backed canary basket launch record storage."""

from __future__ import annotations

import json
from pathlib import Path

from carryme_models import CanaryBasketLaunchResult

from carryme_storage.db import Database


class CanaryBasketLaunchStore:
    """Persist and query approved-canary basket launch records."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the canary basket launch table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS canary_basket_launch_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    launched_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    record_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_canary_basket_launch_records_launched_at
                ON canary_basket_launch_records(launched_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_canary_basket_launch_records_status
                ON canary_basket_launch_records(status)
                """
            )

    def append(self, record: CanaryBasketLaunchResult) -> CanaryBasketLaunchResult:
        """Append one canary basket launch record."""

        self.initialize()
        with self.database.begin() as connection:
            row_id = connection.insert_returning_id(
                """
                INSERT INTO canary_basket_launch_records (
                    launched_at,
                    status,
                    record_json
                ) VALUES (?, ?, ?)
                """,
                (
                    record.launched_at.isoformat(),
                    record.status,
                    record.model_dump_json(),
                ),
            )

        return CanaryBasketLaunchResult.model_validate(
            {
                **record.model_dump(mode="python"),
                "basket_id": row_id,
            }
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        status: str | None = None,
    ) -> list[CanaryBasketLaunchResult]:
        """Return recent canary basket launch records."""

        self.initialize()
        query = """
            SELECT id, record_json
            FROM canary_basket_launch_records
        """
        params: tuple[object, ...]
        if status is not None:
            query += " WHERE status = ?"
            params = (status, limit)
        else:
            params = (limit,)
        query += " ORDER BY launched_at DESC, id DESC LIMIT ?"

        with self.database.begin() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            CanaryBasketLaunchResult.model_validate(
                {
                    **json.loads(record_json),
                    "basket_id": row_id,
                }
            )
            for row_id, record_json in rows
        ]

    def latest(self, *, status: str | None = None) -> CanaryBasketLaunchResult | None:
        """Return the latest canary basket launch record, if any."""

        records = self.list_recent(limit=1, status=status)
        return records[0] if records else None
