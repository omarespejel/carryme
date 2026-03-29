"""Balance snapshot capture and delta summaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from carryme_models import (
    PaperTradeAccountPreflight,
    PaperTradeBalanceDelta,
    PaperTradeEntry,
    VenueBalanceDelta,
    VenueBalanceSnapshot,
)
from carryme_storage import BalanceSnapshotStore


@dataclass
class BalanceAccountingService:
    """Persist authenticated balance snapshots and compute deltas."""

    store: BalanceSnapshotStore

    def capture_paper_trade(
        self,
        *,
        paper_trade: PaperTradeEntry,
        preflight: PaperTradeAccountPreflight,
        stage: str,
        note: str | None = None,
    ) -> list[VenueBalanceSnapshot]:
        """Persist one balance snapshot row per venue in the paper trade."""

        snapshots: list[VenueBalanceSnapshot] = []
        for venue_status in preflight.venues:
            snapshot = VenueBalanceSnapshot(
                captured_at=datetime.now(UTC),
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                stage=stage,
                venue=venue_status.venue,
                total_collateral=venue_status.total_collateral,
                available_to_trade=venue_status.available_to_trade,
                free_collateral=venue_status.free_collateral,
                balance_assets=venue_status.balance_assets,
                position_symbols=venue_status.position_symbols,
                note=note,
            )
            snapshots.append(self.store.append(snapshot))
        return snapshots

    def list_snapshots(
        self,
        *,
        limit: int = 100,
        paper_trade_id: int | None = None,
        label: str | None = None,
        stage: str | None = None,
        venue: str | None = None,
    ) -> list[VenueBalanceSnapshot]:
        """Return recent balance snapshots."""

        return self.store.list_recent(
            limit=limit,
            paper_trade_id=paper_trade_id,
            label=label,
            stage=stage,
            venue=venue,
        )

    def summarize_paper_trade(
        self,
        paper_trade_id: int,
    ) -> PaperTradeBalanceDelta | None:
        """Return first-vs-latest balance deltas for one paper trade."""

        snapshots = self.store.list_recent(limit=500, paper_trade_id=paper_trade_id)
        if not snapshots:
            return None
        ordered = sorted(snapshots, key=lambda item: item.captured_at)
        first = ordered[0]
        grouped: dict[str, list[VenueBalanceSnapshot]] = {}
        for snapshot in ordered:
            grouped.setdefault(snapshot.venue, []).append(snapshot)

        venue_deltas: list[VenueBalanceDelta] = []
        total_collateral_delta = 0.0
        total_available_delta = 0.0
        total_free_delta = 0.0
        have_total_collateral = False
        have_available = False
        have_free = False

        for venue, venue_snapshots in grouped.items():
            start = venue_snapshots[0]
            end = venue_snapshots[-1]
            collateral_delta = _delta(start.total_collateral, end.total_collateral)
            available_delta = _delta(start.available_to_trade, end.available_to_trade)
            free_delta = _delta(start.free_collateral, end.free_collateral)
            if collateral_delta is not None:
                total_collateral_delta += collateral_delta
                have_total_collateral = True
            if available_delta is not None:
                total_available_delta += available_delta
                have_available = True
            if free_delta is not None:
                total_free_delta += free_delta
                have_free = True
            venue_deltas.append(
                VenueBalanceDelta(
                    venue=venue,
                    snapshot_count=len(venue_snapshots),
                    first_captured_at=start.captured_at,
                    latest_captured_at=end.captured_at,
                    first_stage=start.stage,
                    latest_stage=end.stage,
                    first_total_collateral=start.total_collateral,
                    latest_total_collateral=end.total_collateral,
                    total_collateral_delta=collateral_delta,
                    first_available_to_trade=start.available_to_trade,
                    latest_available_to_trade=end.available_to_trade,
                    available_to_trade_delta=available_delta,
                    first_free_collateral=start.free_collateral,
                    latest_free_collateral=end.free_collateral,
                    free_collateral_delta=free_delta,
                )
            )

        latest = ordered[-1]
        return PaperTradeBalanceDelta(
            paper_trade_id=paper_trade_id,
            label=first.label,
            snapshot_count=len(ordered),
            venue_count=len(venue_deltas),
            first_captured_at=first.captured_at,
            latest_captured_at=latest.captured_at,
            total_collateral_delta=total_collateral_delta if have_total_collateral else None,
            total_available_to_trade_delta=total_available_delta if have_available else None,
            total_free_collateral_delta=total_free_delta if have_free else None,
            venues=venue_deltas,
        )


def _delta(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    return end - start
