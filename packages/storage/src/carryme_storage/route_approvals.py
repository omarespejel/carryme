"""Database-backed route approval storage."""

from __future__ import annotations

import json
from pathlib import Path

from carryme_models import RouteApprovalEntry

from carryme_storage.db import Database


class RouteApprovalStore:
    """Persist and query operator route approvals."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = (
            Path(database_path) if "://" not in str(database_path) else str(database_path)
        )
        self.database = Database(database_path)

    def initialize(self) -> None:
        """Create the route approval table if it does not exist."""

        with self.database.begin() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS route_approval_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    updated_at TEXT NOT NULL,
                    label TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    short_venue TEXT NOT NULL,
                    long_venue TEXT NOT NULL,
                    short_fee_profile TEXT NOT NULL,
                    long_fee_profile TEXT NOT NULL,
                    approved INTEGER NOT NULL,
                    max_live_notional REAL NOT NULL,
                    entry_json TEXT NOT NULL,
                    UNIQUE (
                        label,
                        canonical_symbol,
                        short_venue,
                        long_venue,
                        short_fee_profile,
                        long_fee_profile
                    )
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_route_approval_entries_updated_at
                ON route_approval_entries(updated_at DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_route_approval_entries_label
                ON route_approval_entries(label)
                """
            )

    def upsert(self, entry: RouteApprovalEntry) -> RouteApprovalEntry:
        """Insert or replace one route approval entry."""

        self.initialize()
        with self.database.begin() as connection:
            connection.execute(
                """
                INSERT INTO route_approval_entries (
                    updated_at,
                    label,
                    canonical_symbol,
                    short_venue,
                    long_venue,
                    short_fee_profile,
                    long_fee_profile,
                    approved,
                    max_live_notional,
                    entry_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                    label,
                    canonical_symbol,
                    short_venue,
                    long_venue,
                    short_fee_profile,
                    long_fee_profile
                ) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    approved = excluded.approved,
                    max_live_notional = excluded.max_live_notional,
                    entry_json = excluded.entry_json
                """,
                (
                    entry.updated_at.isoformat(),
                    entry.label,
                    entry.canonical_symbol,
                    entry.short_venue,
                    entry.long_venue,
                    entry.short_fee_profile,
                    entry.long_fee_profile,
                    int(entry.approved),
                    entry.max_live_notional,
                    entry.model_dump_json(),
                ),
            )
        return entry

    def list_recent(
        self,
        *,
        limit: int | None = 50,
        label: str | None = None,
        canonical_symbol: str | None = None,
        approved: bool | None = None,
    ) -> list[RouteApprovalEntry]:
        """Return recent route approval entries."""

        self.initialize()
        query = """
            SELECT entry_json
            FROM route_approval_entries
        """
        filters: list[str] = []
        params: list[object] = []
        if label:
            filters.append("label = ?")
            params.append(label)
        if canonical_symbol:
            filters.append("canonical_symbol = ?")
            params.append(canonical_symbol)
        if approved is not None:
            filters.append("approved = ?")
            params.append(int(approved))
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY updated_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self.database.begin() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()

        return [RouteApprovalEntry.model_validate(json.loads(entry_json)) for (entry_json,) in rows]

    def get_route(
        self,
        *,
        label: str,
        canonical_symbol: str,
        short_venue: str,
        long_venue: str,
        short_fee_profile: str,
        long_fee_profile: str,
    ) -> RouteApprovalEntry | None:
        """Return the stored approval for one exact route, if present."""

        self.initialize()
        with self.database.begin() as connection:
            row = connection.execute(
                """
                SELECT entry_json
                FROM route_approval_entries
                WHERE label = ?
                  AND canonical_symbol = ?
                  AND short_venue = ?
                  AND long_venue = ?
                  AND short_fee_profile = ?
                  AND long_fee_profile = ?
                LIMIT 1
                """,
                (
                    label,
                    canonical_symbol,
                    short_venue,
                    long_venue,
                    short_fee_profile,
                    long_fee_profile,
                ),
            ).fetchone()

        if row is None:
            return None
        return RouteApprovalEntry.model_validate(json.loads(row[0]))
