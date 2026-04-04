"""Balance snapshot capture and delta summaries."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from carryme_models import (
    BalanceAttributionPhase,
    PaperTradeAccountPreflight,
    PaperTradeBalanceAttribution,
    PaperTradeBalanceDelta,
    PaperTradeEntry,
    VenueBalanceDelta,
    VenueBalanceSnapshot,
)
from carryme_storage import BalanceSnapshotStore

PRE_OPEN_STAGE = "pre_open"
POST_OPEN_STAGE = "post_open"
POST_CLOSE_STAGE = "post_close"
FUNDING_WINDOW_CHECKPOINT_STAGE = "funding_window_checkpoint"


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
        captured_at = datetime.now(UTC)
        for venue_status in preflight.venues:
            snapshot = VenueBalanceSnapshot(
                captured_at=captured_at,
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

        ordered = self._list_ordered_snapshots(paper_trade_id)
        if not ordered:
            return None
        return _build_balance_delta_summary(paper_trade_id, ordered)

    def summarize_paper_trade_attribution(
        self,
        paper_trade_id: int,
    ) -> PaperTradeBalanceAttribution | None:
        """Return attributed entry, hold, and exit deltas for one paper trade."""

        ordered = self._list_ordered_snapshots(paper_trade_id)
        if not ordered:
            return None

        total = _build_balance_delta_summary(paper_trade_id, ordered)
        grouped = _group_snapshots_by_venue(ordered)
        post_open = _latest_stage_snapshots(grouped, POST_OPEN_STAGE)
        latest_hold = _latest_stage_snapshots(grouped, FUNDING_WINDOW_CHECKPOINT_STAGE)
        post_close = _latest_stage_snapshots(grouped, POST_CLOSE_STAGE)
        latest_by_venue = {venue: snapshots[-1] for venue, snapshots in grouped.items()}

        entry = _build_snapshot_segment(
            phase="entry",
            start_snapshots=_earliest_stage_snapshots(grouped, PRE_OPEN_STAGE),
            end_snapshots=post_open,
        )
        hold: BalanceAttributionPhase | None = None
        exit_phase: BalanceAttributionPhase | None = None
        if post_open is not None:
            if latest_hold is not None:
                hold = _build_snapshot_segment(
                    phase="hold",
                    start_snapshots=post_open,
                    end_snapshots=latest_hold,
                )
                if post_close is not None:
                    exit_phase = _build_snapshot_segment(
                        phase="exit",
                        start_snapshots=latest_hold,
                        end_snapshots=post_close,
                    )
            elif post_close is not None:
                hold = _build_snapshot_segment(
                    phase="hold",
                    start_snapshots=post_open,
                    end_snapshots=post_close,
                )
            elif _snapshots_advanced(post_open, latest_by_venue):
                hold = _build_snapshot_segment(
                    phase="hold",
                    start_snapshots=post_open,
                    end_snapshots=latest_by_venue,
                )

        funding_checkpoint_count = len(
            {
                snapshot.captured_at
                for snapshot in ordered
                if snapshot.stage == FUNDING_WINDOW_CHECKPOINT_STAGE
            }
        )

        return PaperTradeBalanceAttribution(
            paper_trade_id=paper_trade_id,
            label=total.label,
            snapshot_count=total.snapshot_count,
            venue_count=total.venue_count,
            funding_checkpoint_count=funding_checkpoint_count,
            first_captured_at=total.first_captured_at,
            latest_captured_at=total.latest_captured_at,
            total_collateral_delta=total.total_collateral_delta,
            total_available_to_trade_delta=total.total_available_to_trade_delta,
            total_free_collateral_delta=total.total_free_collateral_delta,
            entry=entry,
            hold=hold,
            exit=exit_phase,
        )

    def _list_ordered_snapshots(self, paper_trade_id: int) -> list[VenueBalanceSnapshot]:
        snapshots = self.store.list_recent(limit=500, paper_trade_id=paper_trade_id)
        return sorted(snapshots, key=lambda item: item.captured_at)


@dataclass(frozen=True)
class _AggregateDeltas:
    total_collateral_delta: float | None
    total_available_delta: float | None
    total_free_delta: float | None


def _build_balance_delta_summary(
    paper_trade_id: int,
    ordered: list[VenueBalanceSnapshot],
) -> PaperTradeBalanceDelta:
    first = ordered[0]
    grouped = _group_snapshots_by_venue(ordered)
    pairs = [
        (venue, snapshots[0], snapshots[-1], len(snapshots))
        for venue, snapshots in grouped.items()
    ]
    venue_deltas, totals = _summarize_venue_snapshot_pairs(pairs)
    latest = ordered[-1]
    return PaperTradeBalanceDelta(
        paper_trade_id=paper_trade_id,
        label=first.label,
        snapshot_count=len(ordered),
        venue_count=len(venue_deltas),
        first_captured_at=first.captured_at,
        latest_captured_at=latest.captured_at,
        total_collateral_delta=totals.total_collateral_delta,
        total_available_to_trade_delta=totals.total_available_delta,
        total_free_collateral_delta=totals.total_free_delta,
        venues=venue_deltas,
    )


def _build_snapshot_segment(
    *,
    phase: str,
    start_snapshots: dict[str, VenueBalanceSnapshot] | None,
    end_snapshots: dict[str, VenueBalanceSnapshot] | None,
) -> BalanceAttributionPhase | None:
    if start_snapshots is None or end_snapshots is None:
        return None

    common_venues = sorted(set(start_snapshots).intersection(end_snapshots))
    if not common_venues:
        return None

    pairs = [
        (venue, start_snapshots[venue], end_snapshots[venue], 2)
        for venue in common_venues
        if end_snapshots[venue].captured_at >= start_snapshots[venue].captured_at
    ]
    if not pairs:
        return None

    venue_deltas, totals = _summarize_venue_snapshot_pairs(pairs)
    start_points = [start.captured_at for _, start, _, _ in pairs]
    end_points = [end.captured_at for _, _, end, _ in pairs]
    start_stages = [start.stage for _, start, _, _ in pairs]
    end_stages = [end.stage for _, _, end, _ in pairs]
    return BalanceAttributionPhase(
        phase=phase,  # type: ignore[arg-type]
        snapshot_count=len(pairs) * 2,
        venue_count=len(venue_deltas),
        start_captured_at=min(start_points),
        end_captured_at=max(end_points),
        start_stage=_shared_stage_name(start_stages),
        end_stage=_shared_stage_name(end_stages),
        duration_seconds=max(0.0, (max(end_points) - min(start_points)).total_seconds()),
        total_collateral_delta=totals.total_collateral_delta,
        total_available_to_trade_delta=totals.total_available_delta,
        total_free_collateral_delta=totals.total_free_delta,
        venues=venue_deltas,
    )


def _group_snapshots_by_venue(
    ordered: list[VenueBalanceSnapshot],
) -> dict[str, list[VenueBalanceSnapshot]]:
    grouped: dict[str, list[VenueBalanceSnapshot]] = {}
    for snapshot in ordered:
        grouped.setdefault(snapshot.venue, []).append(snapshot)
    return grouped


def _earliest_stage_snapshots(
    grouped: dict[str, list[VenueBalanceSnapshot]],
    stage: str,
) -> dict[str, VenueBalanceSnapshot] | None:
    result = {
        venue: snapshot
        for venue, snapshots in grouped.items()
        if (snapshot := next((item for item in snapshots if item.stage == stage), None)) is not None
    }
    return result or None


def _latest_stage_snapshots(
    grouped: dict[str, list[VenueBalanceSnapshot]],
    stage: str,
) -> dict[str, VenueBalanceSnapshot] | None:
    result = {
        venue: snapshot
        for venue, snapshots in grouped.items()
        if (snapshot := next((item for item in reversed(snapshots) if item.stage == stage), None))
        is not None
    }
    return result or None


def _snapshots_advanced(
    start_snapshots: dict[str, VenueBalanceSnapshot],
    end_snapshots: dict[str, VenueBalanceSnapshot],
) -> bool:
    for venue, start_snapshot in start_snapshots.items():
        end_snapshot = end_snapshots.get(venue)
        if end_snapshot is None:
            continue
        if end_snapshot.captured_at > start_snapshot.captured_at:
            return True
    return False


def _summarize_venue_snapshot_pairs(
    pairs: Iterable[tuple[str, VenueBalanceSnapshot, VenueBalanceSnapshot, int]],
) -> tuple[list[VenueBalanceDelta], _AggregateDeltas]:
    venue_deltas: list[VenueBalanceDelta] = []
    total_collateral_delta = 0.0
    total_available_delta = 0.0
    total_free_delta = 0.0
    have_total_collateral = False
    have_available = False
    have_free = False

    for venue, start, end, snapshot_count in pairs:
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
                snapshot_count=snapshot_count,
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

    return venue_deltas, _AggregateDeltas(
        total_collateral_delta=total_collateral_delta if have_total_collateral else None,
        total_available_delta=total_available_delta if have_available else None,
        total_free_delta=total_free_delta if have_free else None,
    )


def _shared_stage_name(stages: Iterable[str]) -> str:
    values = tuple(stages)
    unique = sorted(set(values))
    if len(unique) == 1:
        return unique[0]
    return "mixed"


def _delta(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    return end - start
