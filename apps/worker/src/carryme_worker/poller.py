"""Polling helpers for the carryme worker."""

from __future__ import annotations

import asyncio
import logging
import math
import signal
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, cast

import httpx
from carryme_api.app import (
    _append_pair_close_confirmation_for_preview,
    _build_cleanup_live_execution_router_for_candidate,
    _build_cleanup_preview_router_for_candidate,
    _build_pair_close_context_for_paper_trade,
    _build_pair_close_live_execution_coordinator_for_candidate,
    _build_pair_close_preview_service_for_candidate,
    _build_paired_live_execution_coordinator_for_candidate,
    _execute_guarded_pair_close_from_confirmation,
    _run_guarded_canary_lifecycle,
    _select_latest_launch_ready_canary_snapshot,
)
from carryme_api.app import (
    _build_launch_ready_canary_stability as _api_build_launch_ready_canary_stability,
)
from carryme_api.config import ApiSettings
from carryme_models import (
    ApprovedCanaryAlertEvent,
    ApprovedCanarySnapshot,
    CanaryLifecycleResult,
    CandidateAlertEvent,
    ExecutionAlertEvent,
    ExecutionJournalEntry,
    ExecutionObservationEntry,
    ExecutionPairStatus,
    FundingArbOpportunity,
    FundingUniverseCanaryCandidate,
    FundingUniverseScan,
    LaunchReadyCanarySnapshot,
    LaunchReadyCanaryStability,
    OpportunityRecord,
    PaperTradeAccountPreflight,
    PaperTradeBalanceAttribution,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    PaperTradeSystemState,
    RouteApprovalEntry,
    StableCanaryLaunchRecord,
    StableLaunchReadyAlertEvent,
    SystemStateAlertEvent,
    VenueExecutionPreflight,
    VenueSystemState,
)
from carryme_runtime import (
    AccountPreflightConfigMap,
    AccountPreflightService,
    BalanceAccountingService,
    ConnectorError,
    ExecutionOrderStateService,
    ExecutionQualityService,
    ExtendedOrderStateObserver,
    HyperliquidOrderStateObserver,
    OpportunityService,
    OpportunityUniverseService,
    OrderPreviewService,
    ParadexOrderStateObserver,
    RouteApprovalService,
    RouteStabilityService,
    SystemStateConfigMap,
    SystemStateService,
    UpstreamDataError,
    build_account_preflight_configs,
    build_execution_pair_status,
    build_live_execution_configs,
    build_opportunity_record_from_universe_opportunity,
    build_pair_spec_from_universe_opportunity,
    build_venue_execution_preflights,
    filter_candidate_records,
    reconcile_execution,
    review_required_pair_requires_continued_monitoring,
)
from carryme_runtime.balance_accounting import FUNDING_WINDOW_CHECKPOINT_STAGE
from carryme_runtime.execution_order_state import ExecutionLegOrderObserver
from carryme_runtime.route_approvals import (
    scan_exact_canary_candidate_for_approval,
    scan_live_route_candidate_for_approval,
)
from carryme_storage import (
    ApprovedCanaryAlertStore,
    ApprovedCanaryStore,
    BalanceSnapshotStore,
    CandidateAlertStore,
    CleanupPreviewConfirmationStore,
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    LaunchReadyCanaryStore,
    OpportunityHistoryStore,
    PairClosePreviewConfirmationStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    RouteApprovalStore,
    StableCanaryLaunchStore,
    StableLaunchReadyAlertStore,
    SystemStateAlertStore,
    load_watchlist,
)
from fastapi import HTTPException

from carryme_worker.config import WorkerSettings
from carryme_worker.notifications import (
    ApprovedCanaryAlertNotifier,
    CompositeExecutionAlertNotifier,
    ExecutionAlertNotifier,
    StableLaunchReadyAlertNotifier,
    SystemStateAlertNotifier,
)

logger = logging.getLogger(__name__)
OBSERVATION_CALL_TIMEOUT_SECONDS = 10.0
FUNDING_WINDOW_HOURS_BY_VENUE: dict[str, float] = {
    "extended": 1.0,
    "paradex": 8.0,
    "hyperliquid": 8.0,
}


def _build_launch_ready_canary_stability(**kwargs: Any) -> LaunchReadyCanaryStability:
    """Delegate to the API-layer stability helper for legacy worker call sites and tests."""

    return _api_build_launch_ready_canary_stability(**kwargs)


def _rank_approved_canary_candidate(
    candidate: FundingUniverseCanaryCandidate,
) -> tuple[float, float, float]:
    """Return the deterministic ranking tuple for exact approved-canary matches."""

    opportunity = candidate.opportunity
    return (
        (
            opportunity.route_adjusted_quality_score
            if opportunity.route_adjusted_quality_score is not None
            else float("-inf")
        ),
        (
            opportunity.execution_adjusted_quality_score
            if opportunity.execution_adjusted_quality_score is not None
            else float("-inf")
        ),
        (
            opportunity.estimated_one_day_pnl_after_round_trip
            if opportunity.estimated_one_day_pnl_after_round_trip is not None
            else float("-inf")
        ),
    )


class PairScorer(Protocol):
    """Interface for scoring configured funding pairs."""

    async def score_pair(
        self,
        *,
        left_venue: str,
        left_symbol: str,
        left_fee_profile: str,
        right_venue: str,
        right_symbol: str,
        right_fee_profile: str,
    ) -> FundingArbOpportunity: ...


class UniverseScanner(Protocol):
    """Interface for scanning the live funding universe."""

    async def scan(
        self,
        *,
        venues: list[str],
        ranking: str,
        fee_profile_overrides: dict[str, str] | None,
        target_notional: float,
        min_capacity_notional: float,
        min_daily_volume: float,
        min_open_interest: float,
        min_roundtrip_edge: float,
        min_execution_quality_score: float,
        min_execution_samples: int,
        min_route_stability_weight: float,
        min_route_presence_ratio: float,
        min_route_samples: int,
        include_symbols: list[str] | None,
        exclude_symbols: list[str] | None,
        exclude_tags: list[str] | None,
        limit: int,
    ) -> FundingUniverseScan: ...


class CandidateAlertSink(Protocol):
    """Append-only sink for emitted candidate alert events."""

    def append(self, event: CandidateAlertEvent) -> bool: ...


class ApprovedCanaryAlertSink(Protocol):
    """Append-only sink for emitted approved-canary alert events."""

    def append(self, event: ApprovedCanaryAlertEvent) -> None: ...


class SystemStateAlertSink(Protocol):
    """Append-only sink for emitted system-state alert events."""

    def append(self, event: SystemStateAlertEvent) -> None: ...


class StableLaunchReadyAlertSink(Protocol):
    """Append-only sink for stable launch-ready alert events."""

    def append(self, event: StableLaunchReadyAlertEvent) -> None: ...


class ExecutionAlertSink(Protocol):
    """Append-only sink for emitted execution alert events."""

    def append_if_changed(
        self,
        event: ExecutionAlertEvent,
        *,
        previous_pair_status: ExecutionPairStatus | None = None,
    ) -> bool: ...


@dataclass
class PollCycleSummary:
    """Summary emitted after a watchlist poll cycle."""

    watched_pairs: int
    saved_records: int
    failed_records: int
    database_path: str
    records: list[OpportunityRecord] = field(default_factory=list, repr=False)


@dataclass
class PollLoopSummary:
    """Summary emitted after a multi-iteration worker loop."""

    attempts: int
    successful_cycles: int
    failures: int
    saved_records: int
    database_path: str
    alert_events: int = 0


@dataclass
class UniverseScanSummary:
    """Summary emitted after one funding-universe scan."""

    overlap_count: int
    scanned_opportunities: int
    saved_records: int
    alert_events: int
    database_path: str
    records: list[OpportunityRecord] = field(default_factory=list, repr=False)


@dataclass
class UniverseScanLoopSummary:
    """Summary emitted after a supervised funding-universe scan loop."""

    attempts: int
    successful_cycles: int
    failures: int
    overlap_count: int
    scanned_opportunities: int
    saved_records: int
    alert_events: int
    database_path: str


@dataclass
class ApprovedCanaryScanSummary:
    """Summary emitted after one approved-canary scan."""

    scanned_candidates: int
    approved_candidates: int
    saved_snapshots: int
    alert_events: int
    sent_notifications: int
    database_path: str
    snapshots: list[ApprovedCanarySnapshot] = field(default_factory=list, repr=False)
    alerts: list[ApprovedCanaryAlertEvent] = field(default_factory=list, repr=False)


@dataclass
class ApprovedCanaryScanLoopSummary:
    """Summary emitted after a supervised approved-canary scan loop."""

    attempts: int
    successful_cycles: int
    failures: int
    scanned_candidates: int
    approved_candidates: int
    saved_snapshots: int
    alert_events: int
    sent_notifications: int
    database_path: str


@dataclass
class LaunchReadyCanaryCacheSummary:
    """Summary emitted after one launch-ready canary cache cycle."""

    scanned_snapshots: int
    launch_ready_candidates: int
    saved_snapshots: int
    alert_events: int
    sent_notifications: int
    database_path: str
    snapshots: list[LaunchReadyCanarySnapshot] = field(default_factory=list, repr=False)
    alerts: list[StableLaunchReadyAlertEvent] = field(default_factory=list, repr=False)


@dataclass
class LaunchReadyCanaryCacheLoopSummary:
    """Summary emitted after a supervised launch-ready canary cache loop."""

    attempts: int
    successful_cycles: int
    failures: int
    scanned_snapshots: int
    launch_ready_candidates: int
    saved_snapshots: int
    alert_events: int
    sent_notifications: int
    database_path: str


@dataclass
class StableCanaryLaunchSummary:
    """Summary emitted after attempting to launch the latest stable canary."""

    status: Literal["launched", "skipped"]
    database_path: str
    label: str | None = None
    launch_ready_snapshot_id: int | None = None
    approved_snapshot_id: int | None = None
    paper_trade_id: int | None = None
    final_pair_state: str | None = None
    detail: object | None = None
    lifecycle: CanaryLifecycleResult | None = field(default=None, repr=False)


@dataclass(frozen=True)
class _StableCanaryCandidateSkip:
    """Candidate-level launch blocker captured while trying ranked fallbacks."""

    detail: object
    label: str | None = None
    launch_ready_snapshot_id: int | None = None
    approved_snapshot_id: int | None = None
    paper_trade_id: int | None = None
    final_pair_state: str | None = None


@dataclass
class StableCanaryLaunchLoopSummary:
    """Summary emitted after a supervised stable-canary launch loop."""

    attempts: int
    successful_cycles: int
    failures: int
    launched: int
    skipped: int
    database_path: str


def _utc_now() -> datetime:
    """Return the current UTC timestamp.

    Kept as a tiny seam so tests can model slow production cycles without
    sleeping or monkeypatching the datetime type.
    """

    return datetime.now(UTC)


def _list_blocking_live_executions_for_stable_launch(
    *,
    settings: WorkerSettings,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    now: datetime,
) -> list[tuple[int, str, str]]:
    """Return live executions that should block unattended launch."""

    blocking: list[tuple[int, str, str]] = []
    scan_limit = max(
        settings.stable_canary_launch_active_execution_limit,
        settings.stable_canary_launch_max_active_live_executions + 1,
    )
    recent_live_executions = _list_recent_live_executions(
        execution_store,
        observation_store,
        limit=scan_limit,
        now=now,
        max_age_seconds=settings.execution_observation_max_age_seconds,
        unobserved_requires_monitoring=True,
    )
    for execution in recent_live_executions:
        paper_trade_id = execution.paper_trade_id
        paper_trade = execution.paper_trade
        if paper_trade_id is None or paper_trade is None:
            continue
        latest = observation_store.latest_for_paper_trade(paper_trade_id)
        if latest is None:
            blocking.append(
                (
                    paper_trade_id,
                    paper_trade.intent.label,
                    "pending_initial_monitoring",
                )
            )
            continue
        pair_status = latest.pair_status
        if pair_status is None:
            if _execution_requires_continued_monitoring(
                observation_store,
                execution=execution,
                latest_observation=latest,
            ):
                blocking.append(
                    (
                        paper_trade_id,
                        paper_trade.intent.label,
                        "pending_pair_status",
                    )
                )
            continue
        if (
            pair_status.derived_state in {"hedged", "cleanup_needed", "review_required"}
            or pair_status.recommended_action != "no_action"
        ):
            blocking.append(
                (
                    paper_trade_id,
                    paper_trade.intent.label,
                    f"{pair_status.derived_state}/{pair_status.recommended_action}",
                )
            )
    return blocking


def _build_blocking_live_execution_detail(
    *,
    blocking: list[tuple[int, str, str]],
    max_active_live_executions: int,
) -> str:
    summary = ", ".join(
        f"paper_trade_id={paper_trade_id} label={label} state={state}"
        for paper_trade_id, label, state in blocking[:3]
    )
    if len(blocking) > 3:
        summary += f", +{len(blocking) - 3} more"
    return (
        "Active live executions still require monitoring before unattended launch "
        f"(max_allowed={max_active_live_executions}, current={len(blocking)}): {summary}"
    )


def _summarize_active_live_notional_for_stable_launch(
    executions: list[ExecutionJournalEntry],
) -> tuple[float, dict[str, float]]:
    """Return total and per-venue active live target notional."""

    total_active_notional = 0.0
    active_notional_by_venue: dict[str, float] = {}
    for execution in executions:
        paper_trade = execution.paper_trade
        if paper_trade is None:
            continue
        intent = paper_trade.intent
        target_notional = intent.target_notional
        total_active_notional += target_notional
        for venue in {intent.long_leg.venue, intent.short_leg.venue}:
            active_notional_by_venue[venue] = (
                active_notional_by_venue.get(venue, 0.0) + target_notional
            )
    return total_active_notional, active_notional_by_venue


def _build_stable_launch_risk_budget_reason(
    *,
    settings: WorkerSettings,
    active_executions: list[ExecutionJournalEntry],
    candidate: FundingUniverseCanaryCandidate,
) -> str | None:
    """Return the deterministic reason a proposed unattended launch exceeds budget."""

    max_total_live_notional = settings.stable_canary_launch_max_total_live_notional
    max_live_notional_per_venue = settings.stable_canary_launch_max_live_notional_per_venue
    if max_total_live_notional is None and max_live_notional_per_venue is None:
        return None

    proposed_notional = candidate.suggested_canary_notional
    total_active_notional, active_notional_by_venue = (
        _summarize_active_live_notional_for_stable_launch(active_executions)
    )
    if max_total_live_notional is not None:
        proposed_total_notional = total_active_notional + proposed_notional
        if proposed_total_notional - max_total_live_notional > 1e-9:
            return (
                "Stable launch total live notional budget exceeded: "
                f"active={total_active_notional:.2f} + proposed={proposed_notional:.2f} "
                f"> max={max_total_live_notional:.2f}"
            )

    if max_live_notional_per_venue is None:
        return None

    opportunity = candidate.opportunity.opportunity
    for venue in (opportunity.short_venue, opportunity.long_venue):
        venue_active_notional = active_notional_by_venue.get(venue, 0.0)
        proposed_venue_notional = venue_active_notional + proposed_notional
        if proposed_venue_notional - max_live_notional_per_venue > 1e-9:
            return (
                "Stable launch venue live notional budget exceeded: "
                f"venue={venue} active={venue_active_notional:.2f} + "
                f"proposed={proposed_notional:.2f} > max={max_live_notional_per_venue:.2f}"
            )

    return None


def _build_stable_launch_hold_mode_blocker(settings: WorkerSettings) -> str | None:
    """Return why stable launch cannot leave a live hedge open unattended."""

    if settings.stable_canary_launch_close_position:
        return None
    if not settings.execution_auto_pair_close_enabled:
        return (
            "Stable launch hold mode is blocked because "
            "execution_auto_pair_close_enabled is false"
        )
    if settings.execution_auto_pair_close_shadow_mode:
        return (
            "Stable launch hold mode is blocked because "
            "execution_auto_pair_close_shadow_mode is true"
        )
    return None


def _list_recent_closed_live_trade_outcomes(
    *,
    settings: WorkerSettings,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    balance_service: BalanceAccountingService,
    now: datetime,
) -> list[tuple[int, float]]:
    """Return recent closed live paper trades with realized total collateral deltas."""

    limit = settings.stable_canary_launch_recent_closed_trade_limit
    page_size = max(limit * 4, 20)
    offset = 0
    seen_paper_trade_ids: set[int] = set()
    outcomes: list[tuple[int, float]] = []

    while len(outcomes) < limit:
        batch = execution_store.list_recent(limit=page_size, offset=offset)
        if not batch:
            break
        offset += len(batch)

        for execution in batch:
            if execution.mode != "live" or execution.paper_trade_id is None:
                continue
            paper_trade_id = execution.paper_trade_id
            if paper_trade_id in seen_paper_trade_ids:
                continue
            seen_paper_trade_ids.add(paper_trade_id)
            latest_observation = observation_store.latest_for_paper_trade(paper_trade_id)
            if latest_observation is None or latest_observation.pair_status is None:
                continue
            pair_status = latest_observation.pair_status
            if pair_status.derived_state not in {"closed", "unfilled"}:
                continue
            attribution = balance_service.summarize_paper_trade_attribution(paper_trade_id)
            if attribution is None or attribution.total_collateral_delta is None:
                continue
            outcomes.append((paper_trade_id, attribution.total_collateral_delta))
            if len(outcomes) >= limit:
                break

        if len(batch) < page_size:
            break

    return outcomes


def _build_stable_launch_loss_circuit_breaker_reason(
    *,
    settings: WorkerSettings,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    balance_service: BalanceAccountingService,
    now: datetime,
) -> str | None:
    """Return the deterministic reason unattended launch is blocked by recent losses."""

    max_negative_total = settings.stable_canary_launch_max_recent_negative_total_collateral
    max_consecutive_losses = settings.stable_canary_launch_max_consecutive_losing_trades
    if max_negative_total is None and max_consecutive_losses is None:
        return None

    outcomes = _list_recent_closed_live_trade_outcomes(
        settings=settings,
        execution_store=execution_store,
        observation_store=observation_store,
        balance_service=balance_service,
        now=now,
    )
    if not outcomes:
        return None

    if max_negative_total is not None:
        total_negative_collateral = -sum(min(delta, 0.0) for _, delta in outcomes)
        if total_negative_collateral - max_negative_total > 1e-9:
            return (
                "Stable launch recent negative collateral budget exceeded: "
                f"loss={total_negative_collateral:.6f} > max={max_negative_total:.6f}"
            )

    if max_consecutive_losses is None:
        return None

    consecutive_losses = 0
    for _, delta in outcomes:
        if delta < 0:
            consecutive_losses += 1
        else:
            break
    if consecutive_losses >= max_consecutive_losses:
        return (
            "Stable launch consecutive losing trades threshold reached: "
            f"losses={consecutive_losses} >= max={max_consecutive_losses}"
        )

    return None


def _list_recent_effective_stable_launch_records(
    *,
    launch_store: StableCanaryLaunchStore,
    limit: int = 20,
    label: str | None = None,
    include_shadowed: bool,
) -> list[StableCanaryLaunchRecord]:
    """Return recent launch records relevant to the current worker mode."""

    return launch_store.list_recent(
        limit=limit,
        label=label,
        status=None if include_shadowed else "launched",
    )


def _build_stable_launch_cooldown_reason(
    *,
    settings: WorkerSettings,
    launch_store: StableCanaryLaunchStore,
    now: datetime,
    label: str | None = None,
) -> str | None:
    """Return the deterministic reason unattended launch is blocked by cooldowns."""

    global_cooldown_seconds = settings.stable_canary_launch_global_cooldown_seconds
    label_cooldown_seconds = settings.stable_canary_launch_label_cooldown_seconds
    if global_cooldown_seconds is None and label_cooldown_seconds is None:
        return None

    include_shadowed = settings.stable_canary_launch_shadow_mode
    if global_cooldown_seconds is not None:
        recent_records = _list_recent_effective_stable_launch_records(
            launch_store=launch_store,
            include_shadowed=include_shadowed,
        )
        if recent_records:
            latest_record = recent_records[0]
            elapsed_seconds = (now - latest_record.launched_at).total_seconds()
            if elapsed_seconds + 1e-9 < global_cooldown_seconds:
                remaining_seconds = max(
                    0,
                    math.ceil(global_cooldown_seconds - elapsed_seconds),
                )
                return (
                    "Stable launch global cooldown active: "
                    f"last_label={latest_record.label} "
                    f"remaining_seconds={remaining_seconds}"
                )

    if label_cooldown_seconds is None or label is None:
        return None

    recent_label_records = _list_recent_effective_stable_launch_records(
        launch_store=launch_store,
        label=label,
        include_shadowed=include_shadowed,
    )
    if not recent_label_records:
        return None

    latest_label_record = recent_label_records[0]
    elapsed_seconds = (now - latest_label_record.launched_at).total_seconds()
    if elapsed_seconds + 1e-9 >= label_cooldown_seconds:
        return None

    remaining_seconds = max(
        0,
        math.ceil(label_cooldown_seconds - elapsed_seconds),
    )
    return (
        "Stable launch label cooldown active: "
        f"label={label} remaining_seconds={remaining_seconds}"
    )


def _build_stable_launch_rate_cap_reason(
    *,
    settings: WorkerSettings,
    launch_store: StableCanaryLaunchStore,
    now: datetime,
    label: str | None = None,
) -> str | None:
    """Return the deterministic reason unattended launch is blocked by launch-rate caps."""

    window_seconds = settings.stable_canary_launch_recent_launch_window_seconds
    max_launches = settings.stable_canary_launch_max_launches_per_window
    max_label_launches = settings.stable_canary_launch_max_label_launches_per_window
    if window_seconds is None or (max_launches is None and max_label_launches is None):
        return None

    include_shadowed = settings.stable_canary_launch_shadow_mode
    cutoff = now - timedelta(seconds=window_seconds)
    lookup_limit = max(
        20,
        (max_launches or 0) + (max_label_launches or 0) + 5,
    )

    if max_launches is not None:
        recent_records = _list_recent_effective_stable_launch_records(
            launch_store=launch_store,
            limit=lookup_limit,
            include_shadowed=include_shadowed,
        )
        launches_in_window = sum(1 for record in recent_records if record.launched_at >= cutoff)
        if launches_in_window >= max_launches:
            return (
                "Stable launch rate cap reached: "
                f"launches_in_window={launches_in_window} >= max={max_launches} "
                f"window_seconds={window_seconds}"
            )

    if max_label_launches is None or label is None:
        return None

    recent_label_records = _list_recent_effective_stable_launch_records(
        launch_store=launch_store,
        limit=lookup_limit,
        label=label,
        include_shadowed=include_shadowed,
    )
    label_launches_in_window = sum(
        1 for record in recent_label_records if record.launched_at >= cutoff
    )
    if label_launches_in_window < max_label_launches:
        return None

    return (
        "Stable launch label rate cap reached: "
        f"label={label} launches_in_window={label_launches_in_window} "
        f">= max={max_label_launches} window_seconds={window_seconds}"
    )


def _build_stable_launch_execution_maturity_reason(
    *,
    settings: WorkerSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> str | None:
    """Return the deterministic reason unattended launch is blocked by thin route history."""

    min_quality_score = settings.stable_canary_launch_min_execution_quality_score
    min_samples = settings.stable_canary_launch_min_execution_samples
    if min_quality_score is None and min_samples is None:
        return None

    execution_quality = candidate.opportunity.execution_quality
    if execution_quality is None:
        return "Stable launch execution maturity is missing for the selected route"

    if min_samples is not None and execution_quality.sample_size < min_samples:
        return (
            "Stable launch execution sample requirement not met: "
            f"samples={execution_quality.sample_size} < min={min_samples}"
        )

    if (
        min_quality_score is not None
        and execution_quality.weighted_score + 1e-9 < min_quality_score
    ):
        return (
            "Stable launch execution quality requirement not met: "
            f"score={execution_quality.weighted_score:.4f} < min={min_quality_score:.4f}"
        )

    return None


def _build_stable_launch_latest_outcome_reason(
    *,
    settings: WorkerSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> str | None:
    """Return the deterministic reason unattended launch is blocked by a bad latest outcome."""

    if not settings.stable_canary_launch_block_adverse_latest_outcome:
        return None

    execution_quality = candidate.opportunity.execution_quality
    if execution_quality is None:
        return None

    latest_outcome = execution_quality.latest_outcome
    if latest_outcome not in {"cleanup_needed", "review_required"}:
        return None

    return (
        "Stable launch blocked by adverse latest execution outcome: "
        f"latest_outcome={latest_outcome}"
    )


def _scaled_stable_launch_round_trip_pnl(candidate: FundingUniverseCanaryCandidate) -> float:
    """Return one-day expected round-trip PnL scaled to the selected launch notional."""

    opportunity = candidate.opportunity
    deployable_notional = opportunity.deployable_notional or 0.0
    selected_notional = candidate.suggested_canary_notional
    if deployable_notional <= 0 or selected_notional <= 0:
        return 0.0

    raw_pnl = (
        opportunity.execution_adjusted_one_day_pnl_after_round_trip
        if opportunity.execution_adjusted_one_day_pnl_after_round_trip is not None
        else opportunity.estimated_one_day_pnl_after_round_trip
    )
    if raw_pnl is None:
        return 0.0

    return raw_pnl * (selected_notional / deployable_notional)


def _build_stable_launch_liquidity_and_value_reason(
    *,
    settings: WorkerSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> str | None:
    """Return the deterministic reason unattended launch is blocked by weak economics."""

    min_daily_volume = settings.stable_canary_launch_min_daily_volume
    min_deployable_notional = settings.stable_canary_launch_min_deployable_notional
    min_expected_pnl = settings.stable_canary_launch_min_expected_one_day_round_trip_pnl
    if (
        min_daily_volume is None
        and min_deployable_notional is None
        and min_expected_pnl is None
    ):
        return None

    opportunity = candidate.opportunity
    if min_daily_volume is not None:
        actual_min_daily_volume = opportunity.min_daily_volume or 0.0
        if actual_min_daily_volume + 1e-9 < min_daily_volume:
            return (
                "Stable launch minimum daily volume requirement not met: "
                f"min_daily_volume={actual_min_daily_volume:.4f} < min={min_daily_volume:.4f}"
            )

    if min_deployable_notional is not None:
        deployable_notional = opportunity.deployable_notional or 0.0
        if deployable_notional + 1e-9 < min_deployable_notional:
            return (
                "Stable launch deployable notional requirement not met: "
                f"deployable_notional={deployable_notional:.4f} "
                f"< min={min_deployable_notional:.4f}"
            )

    if min_expected_pnl is None:
        return None

    scaled_round_trip_pnl = _scaled_stable_launch_round_trip_pnl(candidate)
    if scaled_round_trip_pnl + 1e-9 >= min_expected_pnl:
        return None

    return (
        "Stable launch expected one-day round-trip pnl requirement not met: "
        f"expected_pnl={scaled_round_trip_pnl:.6f} < min={min_expected_pnl:.6f}"
    )


@dataclass
class ProductionSupervisorCycleSummary:
    """Summary emitted after one end-to-end production supervisor cycle."""

    checked_venues: int
    degraded_venues: int
    scanned_candidates: int
    approved_candidates: int
    saved_approved_snapshots: int
    scanned_launch_ready_snapshots: int
    launch_ready_candidates: int
    saved_launch_ready_snapshots: int
    launch_status: Literal["launched", "skipped"]
    paper_trade_id: int | None
    final_pair_state: str | None
    observed_executions: int
    saved_execution_observations: int
    execution_alerts: int
    sent_notifications: int
    database_path: str


@dataclass
class ProductionSupervisorLoopSummary:
    """Summary emitted after a supervised production supervisor loop."""

    attempts: int
    successful_cycles: int
    failures: int
    launched: int
    skipped: int
    observed_executions: int
    execution_alerts: int
    sent_notifications: int
    database_path: str


@dataclass
class CandidateRecordSummary:
    """Summary emitted after applying candidate thresholds to a cycle."""

    total_records: int
    candidate_records: int


@dataclass
class ExecutionObservationSummary:
    """Summary emitted after observing recent live executions."""

    scanned_executions: int
    observed_executions: int
    saved_observations: int
    saved_alerts: int
    sent_notifications: int
    database_path: str


@dataclass
class ExecutionObservationLoopSummary:
    """Summary emitted after a supervised execution-observation loop."""

    attempts: int
    successful_cycles: int
    failures: int
    scanned_executions: int
    observed_executions: int
    saved_observations: int
    saved_alerts: int
    sent_notifications: int
    database_path: str


@dataclass
class SystemStateObservationSummary:
    """Summary emitted after one system-state observation cycle."""

    checked_venues: int
    degraded_venues: int
    saved_alerts: int
    sent_notifications: int
    database_path: str
    states: list[VenueSystemState] = field(default_factory=list, repr=False)
    alerts: list[SystemStateAlertEvent] = field(default_factory=list, repr=False)


@dataclass
class SystemStateObservationLoopSummary:
    """Summary emitted after a supervised system-state observation loop."""

    attempts: int
    successful_cycles: int
    failures: int
    checked_venues: int
    degraded_venues: int
    saved_alerts: int
    sent_notifications: int
    database_path: str


RECOVERABLE_UNIVERSE_SCAN_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectorError,
    UpstreamDataError,
    httpx.HTTPError,
    TimeoutError,
    sqlite3.Error,
)


async def poll_watchlist_once(
    settings: WorkerSettings,
    *,
    scorer: PairScorer | None = None,
    store: OpportunityHistoryStore | None = None,
    now: datetime | None = None,
) -> PollCycleSummary:
    """Score the configured watchlist once and persist results."""

    pairs = load_watchlist(settings.watchlist_path)
    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    timestamp = now or datetime.now(UTC)

    saved_records = 0
    failed_records = 0
    records: list[OpportunityRecord] = []
    for pair in pairs:
        try:
            async with asyncio.timeout(settings.score_timeout_seconds):
                opportunity = await runtime.score_pair(
                    left_venue=pair.left_venue,
                    left_symbol=pair.left_symbol,
                    left_fee_profile=pair.left_fee_profile,
                    right_venue=pair.right_venue,
                    right_symbol=pair.right_symbol,
                    right_fee_profile=pair.right_fee_profile,
                )
            record = OpportunityRecord(
                recorded_at=timestamp,
                pair=pair,
                opportunity=opportunity,
            )
            history_store.append(record)
        except (ConnectorError, httpx.HTTPError, ValueError, TimeoutError, sqlite3.Error) as exc:
            logger.warning(
                "Failed to score or persist pair %s/%s ↔ %s/%s: %s",
                pair.left_venue,
                pair.left_symbol,
                pair.right_venue,
                pair.right_symbol,
                exc,
                exc_info=True,
            )
            failed_records += 1
            continue
        records.append(record)
        saved_records += 1

    return PollCycleSummary(
        watched_pairs=len(pairs),
        saved_records=saved_records,
        failed_records=failed_records,
        database_path=settings.database_target,
        records=records,
    )


async def scan_funding_universe_once(
    settings: WorkerSettings,
    *,
    scanner: UniverseScanner | None = None,
    store: OpportunityHistoryStore | None = None,
    alert_sink: CandidateAlertSink | None = None,
    now: datetime | None = None,
) -> UniverseScanSummary:
    """Scan the live funding universe once and persist ranked opportunities."""

    history_store = store or OpportunityHistoryStore(settings.database_path)
    candidate_alert_sink = alert_sink or CandidateAlertStore(settings.database_path)
    timestamp = now or datetime.now(UTC)
    runtime = scanner or _build_worker_universe_scanner(settings, history_store=history_store)

    async with asyncio.timeout(settings.universe_scan_timeout_seconds):
        scan = await runtime.scan(
            venues=list(settings.universe_scan_venues),
            ranking=settings.universe_scan_ranking,
            fee_profile_overrides=_build_universe_fee_profile_overrides(settings),
            target_notional=settings.universe_scan_target_notional,
            min_capacity_notional=settings.universe_scan_min_capacity_notional,
            min_daily_volume=settings.universe_scan_min_daily_volume,
            min_open_interest=settings.universe_scan_min_open_interest,
            min_roundtrip_edge=settings.universe_scan_min_roundtrip_edge,
            min_execution_quality_score=settings.universe_scan_min_execution_quality_score,
            min_execution_samples=settings.universe_scan_min_execution_samples,
            min_route_stability_weight=settings.universe_scan_min_route_stability_weight,
            min_route_presence_ratio=settings.universe_scan_min_route_presence_ratio,
            min_route_samples=settings.universe_scan_min_route_samples,
            include_symbols=list(settings.universe_scan_include_symbols) or None,
            exclude_symbols=list(settings.universe_scan_exclude_symbols) or None,
            exclude_tags=list(settings.universe_scan_exclude_tags) or None,
            limit=settings.universe_scan_limit,
        )

    records = [
        build_opportunity_record_from_universe_opportunity(
            recorded_at=timestamp,
            opportunity=opportunity,
        )
        for opportunity in scan.opportunities
    ]
    persisted_records: list[OpportunityRecord] = []
    for record in records:
        try:
            history_store.append(record)
        except sqlite3.Error:
            logger.warning(
                "Failed to persist universe record for %s",
                record.pair.label,
                exc_info=True,
            )
            continue
        persisted_records.append(record)

    candidate_records = filter_candidate_records(
        persisted_records,
        min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
        min_capacity_notional=settings.min_candidate_capacity_notional,
    )
    alert_events = emit_candidate_alerts(
        candidate_records,
        sink=candidate_alert_sink,
        emitted_at=timestamp,
        min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
        min_capacity_notional=settings.min_candidate_capacity_notional,
    )

    return UniverseScanSummary(
        overlap_count=scan.overlap_count,
        scanned_opportunities=len(scan.opportunities),
        saved_records=len(persisted_records),
        alert_events=alert_events,
        database_path=settings.database_target,
        records=records,
    )


def _build_universe_fee_profile_overrides(
    settings: WorkerSettings,
) -> dict[str, str] | None:
    overrides = {
        venue: profile
        for venue in settings.universe_scan_venues
        if (profile := getattr(settings, f"universe_scan_{venue}_fee_profile", None))
    }
    return overrides or None


def _build_worker_universe_scanner(
    settings: WorkerSettings,
    *,
    history_store: OpportunityHistoryStore,
) -> OpportunityUniverseService:
    return OpportunityUniverseService(
        execution_quality_service=ExecutionQualityService(
            journal_store=ExecutionJournalStore(settings.database_path),
            observation_store=ExecutionObservationStore(settings.database_path),
        ),
        route_stability_service=RouteStabilityService(history_store=history_store),
        snapshot_batch_size=settings.universe_scan_snapshot_batch_size,
        snapshot_concurrency_by_venue={
            "extended": settings.universe_scan_extended_snapshot_concurrency,
            "paradex": settings.universe_scan_paradex_snapshot_concurrency,
            "hyperliquid": settings.universe_scan_hyperliquid_snapshot_concurrency,
        },
    )


def _build_approved_canary_fee_profile_overrides(
    settings: WorkerSettings,
) -> dict[str, str] | None:
    overrides = {
        venue: profile
        for venue, profile in {
            "extended": settings.approved_canary_scan_extended_fee_profile,
            "paradex": settings.approved_canary_scan_paradex_fee_profile,
            "hyperliquid": settings.approved_canary_scan_hyperliquid_fee_profile,
        }.items()
        if profile
    }
    return overrides or None


def _build_system_state_configs(settings: WorkerSettings) -> SystemStateConfigMap:
    """Build the public system-state config map from worker settings."""

    return {
        "extended": {
            "enabled": settings.extended_live_enabled,
        },
        "paradex": {
            "enabled": settings.paradex_live_enabled,
        },
        "hyperliquid": {
            "enabled": settings.hyperliquid_live_enabled,
        },
    }


def _candidate_execution_venue_names(candidate: FundingUniverseCanaryCandidate) -> list[str]:
    """Return de-duplicated venue names touched by a canary candidate."""

    venue_names = [
        candidate.opportunity.opportunity.long_venue,
        candidate.opportunity.opportunity.short_venue,
    ]
    selected_names: list[str] = []
    for venue in venue_names:
        if venue not in selected_names:
            selected_names.append(venue)
    return selected_names


def _build_candidate_live_execution_preflight(
    *,
    settings: WorkerSettings | ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
    label: str,
) -> PaperTradeExecutionPreflight:
    """Build live-execution readiness for the exact venues touched by one canary."""

    all_statuses = {
        item.venue: item
        for item in build_venue_execution_preflights(build_live_execution_configs(settings))
    }
    selected: list[VenueExecutionPreflight] = []
    blocking_reasons: list[str] = []
    for venue in _candidate_execution_venue_names(candidate):
        status = all_statuses.get(venue)
        if status is None:
            blocking_reasons.append(f"Venue {venue} live execution is not configured")
            continue
        selected.append(status)
        if not status.enabled:
            blocking_reasons.append(f"Venue {venue} live execution is not enabled")
        if status.missing_env_vars:
            blocking_reasons.append(
                f"Venue {venue} is missing required credentials: "
                + ", ".join(status.missing_env_vars)
            )

    return PaperTradeExecutionPreflight(
        paper_trade_id=0,
        label=label,
        ready=not blocking_reasons,
        venues=selected,
        blocking_reasons=blocking_reasons,
    )


async def _probe_candidate_system_state(
    *,
    settings: WorkerSettings,
    service: SystemStateService,
    candidate: FundingUniverseCanaryCandidate,
    label: str,
) -> PaperTradeSystemState:
    """Probe only the venues touched by one canary candidate."""

    all_statuses = {
        item.venue: item
        for item in await service.probe_venues(_build_system_state_configs(settings))
    }
    selected: list[VenueSystemState] = []
    blocking_reasons: list[str] = []
    for venue in _candidate_execution_venue_names(candidate):
        status = all_statuses.get(venue)
        if status is None:
            blocking_reasons.append(f"Venue {venue} system state is unknown")
            continue
        selected.append(status)
        blocking_reasons.extend(status.blocking_reasons)

    return PaperTradeSystemState(
        paper_trade_id=0,
        label=label,
        ready=not blocking_reasons,
        venues=selected,
        blocking_reasons=blocking_reasons,
    )


def _launch_ready_snapshot_payload_changed(
    previous_snapshot: LaunchReadyCanarySnapshot,
    current_snapshot: LaunchReadyCanarySnapshot,
) -> bool:
    """Return whether the meaningful launch-ready payload changed."""

    previous_payload = _normalized_launch_ready_snapshot_payload(previous_snapshot)
    current_payload = _normalized_launch_ready_snapshot_payload(current_snapshot)
    return previous_payload != current_payload


def _normalized_launch_ready_snapshot_payload(
    snapshot: LaunchReadyCanarySnapshot,
) -> dict[str, object]:
    """Return the stable launch envelope for one launch-ready snapshot.

    The approved snapshot is repriced every scan. Stability should reset when the
    route, live-readiness, or notional envelope changes, not when normal market
    edge/PnL fields move inside a snapshot that has already passed launch-ready
    gates.
    """

    approved_snapshot = snapshot.approved_snapshot
    candidate = approved_snapshot.candidate
    opportunity = candidate.opportunity.opportunity
    approval = approved_snapshot.approval
    venue_markets = candidate.opportunity.venue_markets
    return {
        "label": snapshot.label,
        "max_snapshot_age_seconds": snapshot.max_snapshot_age_seconds,
        "suggested_canary_notional": candidate.suggested_canary_notional,
        "route": {
            "canonical_symbol": opportunity.canonical_symbol,
            "long_venue": opportunity.long_venue,
            "short_venue": opportunity.short_venue,
            "long_fee_profile": opportunity.long_fee_profile,
            "short_fee_profile": opportunity.short_fee_profile,
            "venue_markets": tuple(
                sorted(
                    (venue, market.symbol)
                    for venue, market in venue_markets.items()
                )
            ),
        },
        "approval": {
            "label": approval.label,
            "canonical_symbol": approval.canonical_symbol,
            "short_venue": approval.short_venue,
            "long_venue": approval.long_venue,
            "short_fee_profile": approval.short_fee_profile,
            "long_fee_profile": approval.long_fee_profile,
            "approved": approval.approved,
            "max_live_notional": approval.max_live_notional,
        },
        "system_state": _normalized_launch_ready_system_state_payload(snapshot.system_state),
    }


def _normalized_launch_ready_system_state_payload(
    system_state: PaperTradeSystemState,
) -> dict[str, object]:
    """Return an order-insensitive system-state payload for stability checks."""

    payload = system_state.model_dump(mode="python")
    venues = payload.get("venues")
    if isinstance(venues, list):
        payload["venues"] = sorted(
            venues,
            key=lambda venue: str(cast(dict[str, object], venue)["venue"]),
        )
    return payload


def _approved_snapshot_route_key(
    snapshot: ApprovedCanarySnapshot,
) -> tuple[str, str, str, str, str, str]:
    """Return the normalized identity key for one approved snapshot route."""

    opportunity = snapshot.candidate.opportunity.opportunity
    return (
        snapshot.label,
        opportunity.canonical_symbol,
        opportunity.short_venue,
        opportunity.long_venue,
        opportunity.short_fee_profile,
        opportunity.long_fee_profile,
    )


def _normalized_approved_snapshot_launch_payload(
    snapshot: ApprovedCanarySnapshot,
) -> dict[str, object]:
    """Return the approved-snapshot fields that must stay fixed for launch safety."""

    candidate = snapshot.candidate
    opportunity = candidate.opportunity.opportunity
    approval = snapshot.approval
    venue_markets = candidate.opportunity.venue_markets
    return {
        "label": snapshot.label,
        "suggested_canary_notional": candidate.suggested_canary_notional,
        "route": {
            "canonical_symbol": opportunity.canonical_symbol,
            "long_venue": opportunity.long_venue,
            "short_venue": opportunity.short_venue,
            "long_fee_profile": opportunity.long_fee_profile,
            "short_fee_profile": opportunity.short_fee_profile,
            "venue_markets": tuple(
                sorted(
                    (venue, market.symbol)
                    for venue, market in venue_markets.items()
                )
            ),
        },
        "approval": {
            "label": approval.label,
            "canonical_symbol": approval.canonical_symbol,
            "short_venue": approval.short_venue,
            "long_venue": approval.long_venue,
            "short_fee_profile": approval.short_fee_profile,
            "long_fee_profile": approval.long_fee_profile,
            "approved": approval.approved,
            "max_live_notional": approval.max_live_notional,
        },
    }


def _approved_snapshot_launch_payload_changed(
    previous_snapshot: ApprovedCanarySnapshot,
    current_snapshot: ApprovedCanarySnapshot,
) -> bool:
    """Return whether a newer approved snapshot invalidates stable launch evidence."""

    return (
        _normalized_approved_snapshot_launch_payload(previous_snapshot)
        != _normalized_approved_snapshot_launch_payload(current_snapshot)
    )


def _effective_funding_window_hours(snapshot: ApprovedCanarySnapshot) -> float:
    """Return the fastest relevant funding interval across the route venues."""

    opportunity = snapshot.candidate.opportunity.opportunity
    hours = [
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.short_venue, 8.0),
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.long_venue, 8.0),
    ]
    return min(hours)


def _hold_window_hours(snapshot: ApprovedCanarySnapshot) -> float:
    """Return the slowest relevant funding interval across the route venues."""

    opportunity = snapshot.candidate.opportunity.opportunity
    hours = [
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.short_venue, 8.0),
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.long_venue, 8.0),
    ]
    return max(hours)


def _list_recent_approved_snapshot_chain(
    *,
    store: ApprovedCanaryStore,
    snapshot: ApprovedCanarySnapshot,
    max_snapshot_age_seconds: int,
    limit: int = 20,
) -> list[ApprovedCanarySnapshot]:
    """Return the recent same-route approved snapshot chain for one label."""

    route_key = _approved_snapshot_route_key(snapshot)
    recent_snapshots = store.list_recent(limit=limit, label=snapshot.label)
    chain: list[ApprovedCanarySnapshot] = []
    for recent_snapshot in recent_snapshots:
        if _approved_snapshot_route_key(recent_snapshot) != route_key:
            break
        age_seconds = max(
            0.0,
            (snapshot.captured_at - recent_snapshot.captured_at).total_seconds(),
        )
        if age_seconds > max_snapshot_age_seconds:
            break
        chain.append(recent_snapshot)
    return chain


def _build_approved_snapshot_automation_gate_reason(
    *,
    snapshot: ApprovedCanarySnapshot,
    recent_chain: list[ApprovedCanarySnapshot],
    settings: WorkerSettings,
) -> str | None:
    """Return the blocking reason for unattended launch-readiness, if any."""

    opportunity = snapshot.candidate.opportunity.opportunity
    current_entry_edge = opportunity.one_day_net_edge_after_entry
    current_round_trip_edge = opportunity.one_day_net_edge_after_round_trip

    max_entry_edge = max(
        item.candidate.opportunity.opportunity.one_day_net_edge_after_entry
        for item in recent_chain
    )
    max_round_trip_edge = max(
        item.candidate.opportunity.opportunity.one_day_net_edge_after_round_trip
        for item in recent_chain
    )
    if max_entry_edge <= 0 or max_round_trip_edge <= 0:
        return "recent approved snapshot chain has non-positive net edge"

    min_retention_ratio = settings.stable_launch_ready_min_edge_retention_ratio
    entry_retention_ratio = current_entry_edge / max_entry_edge
    if entry_retention_ratio < min_retention_ratio:
        return (
            "entry edge retention "
            f"{entry_retention_ratio:.2f} below minimum {min_retention_ratio:.2f}"
        )

    round_trip_retention_ratio = current_round_trip_edge / max_round_trip_edge
    if round_trip_retention_ratio < min_retention_ratio:
        return (
            "round-trip edge retention "
            f"{round_trip_retention_ratio:.2f} below minimum {min_retention_ratio:.2f}"
        )

    break_even_days_entry = opportunity.break_even_days_entry
    break_even_days_round_trip = opportunity.break_even_days_round_trip
    if break_even_days_entry is None or break_even_days_round_trip is None:
        return "approved canary snapshot is missing break-even timing"

    funding_window_hours = _effective_funding_window_hours(snapshot)
    entry_break_even_windows = break_even_days_entry * 24.0 / funding_window_hours
    if (
        entry_break_even_windows
        > settings.stable_launch_ready_max_entry_break_even_funding_windows
    ):
        return (
            "entry break-even funding windows "
            f"{entry_break_even_windows:.2f} exceeds maximum "
            f"{settings.stable_launch_ready_max_entry_break_even_funding_windows:.2f}"
        )

    round_trip_break_even_windows = (
        break_even_days_round_trip * 24.0 / funding_window_hours
    )
    if (
        round_trip_break_even_windows
        > settings.stable_launch_ready_max_round_trip_break_even_funding_windows
    ):
        return (
            "round-trip break-even funding windows "
            f"{round_trip_break_even_windows:.2f} exceeds maximum "
            f"{settings.stable_launch_ready_max_round_trip_break_even_funding_windows:.2f}"
        )

    return None


def _approved_snapshot_matches_paper_trade(
    *,
    snapshot: ApprovedCanarySnapshot,
    paper_trade: PaperTradeEntry,
) -> bool:
    """Return whether an approved snapshot still matches the live paper trade route."""

    opportunity = snapshot.candidate.opportunity.opportunity
    intent = paper_trade.intent
    return (
        snapshot.label == intent.label
        and opportunity.canonical_symbol == intent.canonical_symbol
        and opportunity.long_venue == intent.long_leg.venue
        and opportunity.short_venue == intent.short_leg.venue
        and opportunity.long_fee_profile == intent.long_leg.fee_profile
        and opportunity.short_fee_profile == intent.short_leg.fee_profile
    )


def _summarize_open_hedge_profit_totals(
    balance_service: BalanceAccountingService,
    *,
    paper_trade_id: int,
) -> tuple[float | None, float | None]:
    """Return the latest and peak total collateral deltas for one open hedge."""

    ordered = sorted(
        balance_service.list_snapshots(limit=500, paper_trade_id=paper_trade_id),
        key=lambda snapshot: snapshot.captured_at,
    )
    if not ordered:
        return None, None

    baseline_by_venue: dict[str, Any] = {}
    grouped_by_capture: dict[datetime, dict[str, Any]] = {}
    for snapshot in ordered:
        baseline_by_venue.setdefault(snapshot.venue, snapshot)
        grouped_by_capture.setdefault(snapshot.captured_at, {})[snapshot.venue] = snapshot

    required_venues = set(baseline_by_venue)
    if any(snapshot.total_collateral is None for snapshot in baseline_by_venue.values()):
        return None, None
    latest_total: float | None = None
    peak_total: float | None = None
    for captured_at in sorted(grouped_by_capture):
        snapshots = grouped_by_capture[captured_at]
        if not required_venues.issubset(snapshots):
            continue
        total = 0.0
        for venue, baseline in baseline_by_venue.items():
            current = snapshots[venue]
            if current.total_collateral is None:
                break
            total += current.total_collateral - cast(float, baseline.total_collateral)
        else:
            latest_total = total
            peak_total = total if peak_total is None else max(peak_total, total)
            continue

    return latest_total, peak_total


def _build_open_hedge_profit_protection_reason(
    *,
    paper_trade_id: int,
    balance_service: BalanceAccountingService,
    settings: WorkerSettings,
    attribution: PaperTradeBalanceAttribution | None = None,
) -> str | None:
    """Return the deterministic reason to close one profitable hedge after giveback."""

    min_profit = settings.execution_auto_pair_close_min_profit_total_collateral
    giveback_ratio = settings.execution_auto_pair_close_max_profit_giveback_ratio
    if min_profit is None or giveback_ratio is None:
        return None

    latest_total, peak_total = _summarize_open_hedge_profit_totals(
        balance_service,
        paper_trade_id=paper_trade_id,
    )
    current_total = latest_total
    if current_total is None or peak_total is None or peak_total < min_profit:
        return None

    minimum_allowed_total = peak_total * (1.0 - giveback_ratio)
    if current_total > minimum_allowed_total:
        return None

    giveback_total = peak_total - current_total
    return (
        "profit giveback "
        f"{giveback_total:.6f} from peak {peak_total:.6f} exceeded allowed ratio "
        f"{giveback_ratio:.2f} (current {current_total:.6f})"
    )


def _build_open_hedge_auto_close_reason(
    *,
    paper_trade: PaperTradeEntry,
    opened_at: datetime,
    snapshot: ApprovedCanarySnapshot,
    settings: WorkerSettings,
    now: datetime,
) -> str | None:
    """Return the deterministic reason to close one open hedged pair, if any."""

    opportunity = snapshot.candidate.opportunity.opportunity
    current_round_trip_edge = opportunity.one_day_net_edge_after_round_trip
    if current_round_trip_edge <= 0:
        return (
            "latest approved round-trip edge is non-positive "
            f"({current_round_trip_edge:.6f})"
        )

    entry_edge = paper_trade.intent.one_day_net_edge_after_entry
    if entry_edge > 0:
        entry_retention_ratio = opportunity.one_day_net_edge_after_entry / entry_edge
        if (
            entry_retention_ratio
            < settings.execution_auto_pair_close_min_entry_edge_retention_ratio
        ):
            return (
                "entry edge retention "
                f"{entry_retention_ratio:.2f} fell below "
                f"{settings.execution_auto_pair_close_min_entry_edge_retention_ratio:.2f}"
            )

    hold_window_hours = _hold_window_hours(snapshot)
    break_even_days_round_trip = opportunity.break_even_days_round_trip
    if break_even_days_round_trip is not None:
        round_trip_break_even_hold_windows = (
            break_even_days_round_trip * 24.0 / hold_window_hours
        )
        if (
            round_trip_break_even_hold_windows
            > settings.execution_auto_pair_close_max_round_trip_break_even_hold_windows
        ):
            return (
                "round-trip break-even hold windows "
                f"{round_trip_break_even_hold_windows:.2f} exceeds maximum "
                f"{settings.execution_auto_pair_close_max_round_trip_break_even_hold_windows:.2f}"
            )

    hold_age_seconds = max(0.0, (now - opened_at).total_seconds())
    hold_age_windows = hold_age_seconds / (hold_window_hours * 3600.0)
    if hold_age_windows >= settings.execution_auto_pair_close_max_hold_windows:
        return (
            "hold age windows "
            f"{hold_age_windows:.2f} reached maximum "
            f"{settings.execution_auto_pair_close_max_hold_windows:.2f}"
        )

    return None


def _build_open_hedge_stale_snapshot_auto_close_reason(
    *,
    opened_at: datetime,
    snapshot: ApprovedCanarySnapshot,
    settings: WorkerSettings,
    now: datetime,
) -> str | None:
    """Return the protective stale-snapshot close reason, if any."""

    hold_window_hours = _hold_window_hours(snapshot)
    hold_age_seconds = max(0.0, (now - opened_at).total_seconds())
    hold_age_windows = hold_age_seconds / (hold_window_hours * 3600.0)
    if hold_age_windows >= settings.execution_auto_pair_close_max_hold_windows:
        return (
            "hold age windows "
            f"{hold_age_windows:.2f} reached maximum "
            f"{settings.execution_auto_pair_close_max_hold_windows:.2f}"
        )

    return None


def _approved_snapshot_is_fresh(
    *,
    snapshot: ApprovedCanarySnapshot,
    settings: WorkerSettings,
    now: datetime,
) -> bool:
    age_seconds = max(0.0, (now - snapshot.captured_at).total_seconds())
    return age_seconds <= settings.execution_auto_pair_close_max_snapshot_age_seconds


async def _resolve_auto_close_snapshot(
    *,
    settings: WorkerSettings,
    execution: ExecutionJournalEntry,
    approved_store: ApprovedCanaryStore,
    approval_service: RouteApprovalService | None,
    scanner: OpportunityUniverseService | None,
    logger: logging.Logger,
    now: datetime,
) -> tuple[
    ApprovedCanarySnapshot | None,
    Literal["approved_snapshot", "live_revalidation", "stale_approved_snapshot"] | None,
]:
    paper_trade = execution.paper_trade
    if paper_trade is None:
        return None, None

    latest_snapshot = approved_store.latest(label=paper_trade.intent.label)
    stale_matching_snapshot: ApprovedCanarySnapshot | None = None
    if latest_snapshot is not None and _approved_snapshot_matches_paper_trade(
        snapshot=latest_snapshot,
        paper_trade=paper_trade,
    ):
        if _approved_snapshot_is_fresh(
            snapshot=latest_snapshot,
            settings=settings,
            now=now,
        ):
            return latest_snapshot, "approved_snapshot"
        stale_matching_snapshot = latest_snapshot

    fallback_reason = "no approved snapshot exists"
    if latest_snapshot is not None:
        if not _approved_snapshot_matches_paper_trade(
            snapshot=latest_snapshot,
            paper_trade=paper_trade,
        ):
            fallback_reason = "latest approved snapshot no longer matches the live route"
        else:
            fallback_reason = (
                "latest approved snapshot is stale "
                f"(captured_at={latest_snapshot.captured_at.isoformat()})"
            )

    route_approval_service = approval_service or RouteApprovalService(
        store=RouteApprovalStore(settings.database_path)
    )
    approval = route_approval_service.get_for_intent(paper_trade.intent)
    if approval is None:
        intent = paper_trade.intent
        approval = RouteApprovalEntry(
            updated_at=now,
            label=intent.label,
            canonical_symbol=intent.canonical_symbol,
            short_venue=intent.short_leg.venue,
            long_venue=intent.long_leg.venue,
            short_fee_profile=intent.short_leg.fee_profile,
            long_fee_profile=intent.long_leg.fee_profile,
            approved=False,
            max_live_notional=intent.max_target_notional,
            note="synthetic exit revalidation route",
        )

    runtime = scanner or _build_worker_universe_scanner(
        settings,
        history_store=OpportunityHistoryStore(settings.database_path),
    )
    try:
        async with asyncio.timeout(
            settings.execution_auto_pair_close_live_revalidation_timeout_seconds
        ):
            live_candidate, _ = await scan_live_route_candidate_for_approval(
                scanner=runtime,
                approval=approval,
                venues=[approval.short_venue, approval.long_venue],
                fee_profile_overrides=_build_approved_canary_fee_profile_overrides(settings),
                target_notional=max(
                    settings.approved_canary_scan_target_notional,
                    paper_trade.intent.target_notional,
                    approval.max_live_notional,
                ),
                canary_max_notional=max(
                    settings.approved_canary_scan_max_notional,
                    paper_trade.intent.target_notional,
                    approval.max_live_notional,
                ),
                min_capacity_notional=0.0,
                min_daily_volume=0.0,
                min_open_interest=0.0,
                min_roundtrip_edge=-1.0,
                min_execution_quality_score=0.0,
                min_execution_samples=0,
                min_route_stability_weight=0.0,
                min_route_presence_ratio=0.0,
                min_route_samples=0,
                include_symbols=[approval.canonical_symbol],
                exclude_symbols=None,
                exclude_tags=None,
                limit=max(2, settings.approved_canary_exact_scan_limit),
            )
    except TimeoutError:
        logger.warning(
            "auto-close live revalidation timed out for paper_trade_id=%s label=%s",
            paper_trade.entry_id,
            approval.label,
        )
        if stale_matching_snapshot is not None:
            logger.warning(
                "auto-close falling back to stale approved snapshot for paper_trade_id=%s label=%s",
                paper_trade.entry_id,
                approval.label,
            )
            return stale_matching_snapshot, "stale_approved_snapshot"
        return None, None
    except (ConnectorError, UpstreamDataError, httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "auto-close live revalidation failed for paper_trade_id=%s label=%s: %s",
            paper_trade.entry_id,
            approval.label,
            exc,
        )
        if stale_matching_snapshot is not None:
            logger.warning(
                "auto-close falling back to stale approved snapshot for paper_trade_id=%s label=%s",
                paper_trade.entry_id,
                approval.label,
            )
            return stale_matching_snapshot, "stale_approved_snapshot"
        return None, None

    if live_candidate is None:
        if stale_matching_snapshot is not None:
            logger.debug(
                (
                    "live revalidation found no exact match for paper_trade_id=%s; "
                    "falling back to stale approved snapshot because %s"
                ),
                paper_trade.entry_id,
                fallback_reason,
            )
            return stale_matching_snapshot, "stale_approved_snapshot"
        logger.debug(
            (
                "skipping auto-close for paper_trade_id=%s because %s and live "
                "route revalidation found no exact match"
            ),
            paper_trade.entry_id,
            fallback_reason,
        )
        return None, None

    live_snapshot = ApprovedCanarySnapshot(
        captured_at=now,
        label=approval.label,
        candidate=live_candidate,
        approval=approval,
    )
    if not _approved_snapshot_matches_paper_trade(
        snapshot=live_snapshot,
        paper_trade=paper_trade,
    ):
        if stale_matching_snapshot is not None:
            logger.debug(
                (
                    "live revalidation returned a mismatched route for paper_trade_id=%s; "
                    "falling back to stale approved snapshot"
                ),
                paper_trade.entry_id,
            )
            return stale_matching_snapshot, "stale_approved_snapshot"
        logger.debug(
            (
                "skipping auto-close for paper_trade_id=%s because live route "
                "revalidation returned a mismatched route"
            ),
            paper_trade.entry_id,
        )
        return None, None

    logger.debug(
        "auto-close using live route revalidation for paper_trade_id=%s because %s",
        paper_trade.entry_id,
        fallback_reason,
    )
    return live_snapshot, "live_revalidation"


def _build_execution_hold_window_hours(paper_trade: PaperTradeEntry) -> float:
    """Return the effective hold window for one open hedged route."""

    venues = {
        paper_trade.intent.long_leg.venue.lower(),
        paper_trade.intent.short_leg.venue.lower(),
    }
    hours = [
        FUNDING_WINDOW_HOURS_BY_VENUE[venue]
        for venue in venues
        if venue in FUNDING_WINDOW_HOURS_BY_VENUE
    ]
    if not hours:
        return 1.0
    return min(hours)


def _build_funding_checkpoint_note(*, window_index: int, hold_window_hours: float) -> str:
    """Return the persisted metadata note for one funding-window checkpoint."""

    return (
        "worker funding checkpoint "
        f"window_index={window_index} hold_window_hours={hold_window_hours:.6f}"
    )


def _parse_funding_checkpoint_index(note: str | None) -> int | None:
    """Extract the checkpoint window index from a persisted note."""

    if note is None:
        return None
    marker = "window_index="
    start = note.find(marker)
    if start < 0:
        return None
    value = note[start + len(marker) :].split(" ", 1)[0].strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _latest_funding_checkpoint_index(
    balance_service: BalanceAccountingService,
    *,
    paper_trade_id: int,
) -> int | None:
    """Return the latest persisted funding checkpoint window index for one paper trade."""

    snapshots = balance_service.list_snapshots(
        limit=1,
        paper_trade_id=paper_trade_id,
        stage=FUNDING_WINDOW_CHECKPOINT_STAGE,
    )
    if not snapshots:
        return None
    snapshot = snapshots[0]
    parsed = _parse_funding_checkpoint_index(snapshot.note)
    if parsed is not None:
        return parsed
    return 1


def _build_funding_checkpoint_window_index(
    *,
    opened_at: datetime,
    hold_window_hours: float,
    now: datetime,
) -> int:
    """Return the effective crossed funding-window count for one paper trade."""

    window_seconds = hold_window_hours * 3600.0
    if window_seconds <= 0:
        return 0
    entry_bucket = int(opened_at.timestamp() // window_seconds)
    current_bucket = int(now.timestamp() // window_seconds)
    return max(0, current_bucket - entry_bucket)


def _maybe_capture_open_hedge_funding_checkpoint(
    *,
    settings: WorkerSettings,
    execution: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
    preflight: PaperTradeAccountPreflight,
    balance_service: BalanceAccountingService,
    logger: logging.Logger,
    now: datetime,
) -> list[Any]:
    """Persist one periodic balance checkpoint while a hedge remains open."""

    if not settings.execution_balance_checkpoint_enabled:
        return []
    if pair_status.derived_state != "hedged":
        return []
    if pair_status.recommended_action != "monitor_open_hedge":
        return []

    paper_trade = execution.paper_trade
    paper_trade_id = execution.paper_trade_id
    if paper_trade is None or paper_trade_id is None:
        return []
    if any(not venue.authenticated for venue in preflight.venues):
        logger.debug(
            (
                "skipping funding checkpoint for paper_trade_id=%s because "
                "account reads are unauthenticated"
            ),
            paper_trade_id,
        )
        return []

    hold_window_hours = _build_execution_hold_window_hours(paper_trade)
    window_index = _build_funding_checkpoint_window_index(
        opened_at=execution.executed_at,
        hold_window_hours=hold_window_hours,
        now=now,
    )
    if window_index < 1:
        return []

    latest_index = _latest_funding_checkpoint_index(
        balance_service,
        paper_trade_id=paper_trade_id,
    )
    if latest_index is not None and latest_index >= window_index:
        return []

    logger.info(
        "capturing funding checkpoint for paper_trade_id=%s at window_index=%s",
        paper_trade_id,
        window_index,
    )
    return balance_service.capture_paper_trade(
        paper_trade=paper_trade,
        preflight=preflight,
        stage=FUNDING_WINDOW_CHECKPOINT_STAGE,
        note=_build_funding_checkpoint_note(
            window_index=window_index,
            hold_window_hours=hold_window_hours,
        ),
    )


async def _maybe_auto_close_open_hedged_execution(
    *,
    settings: WorkerSettings,
    execution: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
    approved_store: ApprovedCanaryStore,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    balance_service: BalanceAccountingService | None = None,
    account_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    approval_service: RouteApprovalService | None = None,
    scanner: OpportunityUniverseService | None = None,
    logger: logging.Logger,
    now: datetime,
) -> ExecutionJournalEntry | None:
    """Close one monitored hedged pair when the automated exit rules trigger."""

    if (
        not settings.execution_auto_pair_close_enabled
        and not settings.execution_auto_pair_close_shadow_mode
    ):
        return None
    if pair_status.derived_state != "hedged":
        return None
    if pair_status.recommended_action != "monitor_open_hedge":
        return None

    paper_trade = execution.paper_trade
    if paper_trade is None or paper_trade.entry_id is None:
        return None

    latest_snapshot, snapshot_source = await _resolve_auto_close_snapshot(
        settings=settings,
        execution=execution,
        approved_store=approved_store,
        approval_service=approval_service,
        scanner=scanner,
        logger=logger,
        now=now,
    )
    if latest_snapshot is None:
        return None

    balance_snapshot_service = balance_service or BalanceAccountingService(
        store=BalanceSnapshotStore(settings.database_path)
    )
    latest_attribution = balance_snapshot_service.summarize_paper_trade_attribution(
        paper_trade.entry_id
    )
    close_reason = _build_open_hedge_profit_protection_reason(
        paper_trade_id=paper_trade.entry_id,
        balance_service=balance_snapshot_service,
        settings=settings,
        attribution=latest_attribution,
    )
    if close_reason is None:
        if snapshot_source == "stale_approved_snapshot":
            close_reason = _build_open_hedge_stale_snapshot_auto_close_reason(
                opened_at=execution.executed_at,
                snapshot=latest_snapshot,
                settings=settings,
                now=now,
            )
        else:
            close_reason = _build_open_hedge_auto_close_reason(
                paper_trade=paper_trade,
                opened_at=execution.executed_at,
                snapshot=latest_snapshot,
                settings=settings,
                now=now,
            )
    if close_reason is None:
        return None

    if settings.execution_auto_pair_close_shadow_mode:
        logger.info(
            "shadow auto-close for paper_trade_id=%s because %s (source=%s)",
            paper_trade.entry_id,
            close_reason,
            snapshot_source or "unknown",
        )
        return None

    api_settings = _build_api_settings_from_worker_settings(settings)
    pair_close_preview_service = _build_pair_close_preview_service_for_candidate(
        api_settings,
        latest_snapshot.candidate,
    )
    cleanup_preview_service = _build_cleanup_preview_router_for_candidate(
        api_settings,
        latest_snapshot.candidate,
    )
    pair_close_live_service = _build_pair_close_live_execution_coordinator_for_candidate(
        api_settings,
        latest_snapshot.candidate,
    )
    cleanup_live_router = _build_cleanup_live_execution_router_for_candidate(
        api_settings,
        latest_snapshot.candidate,
    )

    try:
        async with asyncio.timeout(settings.execution_auto_pair_close_timeout_seconds):
            paper_trade_entry, latest_execution, _, pair_close_preview = (
                await _build_pair_close_context_for_paper_trade(
                    paper_trade_id=paper_trade.entry_id,
                    settings=api_settings,
                    paper_store=PaperTradeStore(api_settings.database_path),
                    execution_store=execution_store,
                    account_service=account_service,
                    order_state_service=order_state_service,
                    pair_close_service=pair_close_preview_service,
                )
            )
            confirmation = _append_pair_close_confirmation_for_preview(
                paper_trade=paper_trade_entry,
                confirmation_store=PairClosePreviewConfirmationStore(api_settings.database_path),
                preview=pair_close_preview,
                note=f"worker auto-close [{snapshot_source or 'unknown'}]: {close_reason}",
            )
            result = await _execute_guarded_pair_close_from_confirmation(
                paper_trade=paper_trade_entry,
                confirmation=confirmation,
                settings=api_settings,
                execution_store=execution_store,
                observation_store=observation_store,
                cleanup_confirmation_store=CleanupPreviewConfirmationStore(
                    api_settings.database_path
                ),
                account_preflight_service=account_service,
                order_state_service=order_state_service,
                cleanup_preview_service=cleanup_preview_service,
                cleanup_live_router=cleanup_live_router,
                service=pair_close_live_service,
                first_venue="auto",
                poll_attempts=5,
                poll_interval_seconds=2.0,
                auto_cleanup=True,
            )
    except TimeoutError:
        logger.warning(
            (
                "timed out auto-closing paper_trade_id=%s from execution_entry_id=%s "
                "after %.1f seconds"
            ),
            paper_trade.entry_id,
            execution.entry_id,
            settings.execution_auto_pair_close_timeout_seconds,
        )
        return None
    logger.info(
        "auto-closed paper_trade_id=%s from execution_entry_id=%s because %s",
        paper_trade.entry_id,
        latest_execution.entry_id,
        close_reason,
    )
    return result.primary_execution


def _build_latest_launch_ready_stability(
    *,
    store: LaunchReadyCanaryStore,
    label: str,
    max_snapshot_age_seconds: int,
    min_snapshot_count: int,
    min_stable_seconds: float,
    now: datetime,
) -> LaunchReadyCanaryStability | None:
    """Return the latest stable launch-ready snapshot for one label, if any."""

    stability, _ = _evaluate_latest_launch_ready_stability(
        store=store,
        label=label,
        max_snapshot_age_seconds=max_snapshot_age_seconds,
        min_snapshot_count=min_snapshot_count,
        min_stable_seconds=min_stable_seconds,
        now=now,
    )
    return stability


def _evaluate_latest_launch_ready_stability(
    *,
    store: LaunchReadyCanaryStore,
    label: str,
    max_snapshot_age_seconds: int,
    min_snapshot_count: int,
    min_stable_seconds: float,
    now: datetime,
) -> tuple[LaunchReadyCanaryStability | None, str | None]:
    """Return the latest stable launch-ready snapshot and failure detail for one label."""

    snapshots = store.list_recent(limit=max(min_snapshot_count + 5, 20), label=label)
    if not snapshots:
        return None, f"label={label}: no launch-ready canary snapshot found"

    latest_snapshot = snapshots[0]
    snapshot_age_seconds = (now - latest_snapshot.captured_at).total_seconds()
    if snapshot_age_seconds < 0:
        return None, f"label={label}: snapshot timestamp is in the future"
    effective_max_age_seconds = min(
        max_snapshot_age_seconds,
        latest_snapshot.max_snapshot_age_seconds,
    )
    if snapshot_age_seconds > effective_max_age_seconds:
        return (
            None,
            "label="
            f"{label}: snapshot stale ({snapshot_age_seconds:.1f}s > {effective_max_age_seconds}s)",
        )

    chain: list[LaunchReadyCanarySnapshot] = []
    for snapshot in snapshots:
        if _launch_ready_snapshot_payload_changed(snapshot, latest_snapshot):
            break
        chain.append(snapshot)

    if not chain:
        return None, f"label={label}: stability chain missing current snapshot"

    consecutive_snapshots = len(chain)
    oldest_snapshot = chain[-1]
    stable_seconds = max(
        0.0,
        (latest_snapshot.captured_at - oldest_snapshot.captured_at).total_seconds(),
    )
    if consecutive_snapshots < min_snapshot_count:
        return (
            None,
            "label="
            f"{label}: not yet stable ({consecutive_snapshots} < {min_snapshot_count} "
            "consecutive snapshots)",
        )
    if stable_seconds < min_stable_seconds:
        return (
            None,
            "label="
            f"{label}: stable time too short ({stable_seconds:.1f}s < {min_stable_seconds:.1f}s)",
        )
    return (
        LaunchReadyCanaryStability(
            snapshot=latest_snapshot,
            consecutive_snapshots=consecutive_snapshots,
            stable_seconds=stable_seconds,
            min_snapshot_count=min_snapshot_count,
            min_stable_seconds=min_stable_seconds,
        ),
        None,
    )


def _rank_launch_ready_stability(
    stability: LaunchReadyCanaryStability,
) -> tuple[float, float, float, datetime, int]:
    """Return the deterministic launch ranking for one stable launch-ready label."""

    snapshot_id = stability.snapshot.launch_ready_snapshot_id or 0
    return (
        *_rank_approved_canary_candidate(stability.snapshot.approved_snapshot.candidate),
        stability.snapshot.captured_at,
        snapshot_id,
    )


def _list_ranked_stable_launch_ready_stabilities(
    *,
    store: LaunchReadyCanaryStore,
    max_snapshot_age_seconds: int,
    min_snapshot_count: int,
    min_stable_seconds: float,
    now: datetime,
    scan_limit: int,
) -> tuple[list[LaunchReadyCanaryStability], str | None]:
    """Return stable launch-ready labels ranked by route quality, then freshness."""

    if scan_limit < 1:
        raise ValueError("scan_limit must be positive")

    labels = store.list_recent_labels(limit=scan_limit)
    if not labels:
        try:
            return [
                _build_launch_ready_canary_stability(
                    store=store,
                    label=None,
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                    min_snapshot_count=min_snapshot_count,
                    min_stable_seconds=min_stable_seconds,
                    now=now,
                )
            ], None
        except HTTPException as exc:
            if exc.status_code in {404, 409}:
                return [], str(exc.detail)
            raise

    stabilities: list[LaunchReadyCanaryStability] = []
    failure_details: list[str] = []
    for label in labels:
        stability, failure_detail = _evaluate_latest_launch_ready_stability(
            store=store,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            min_snapshot_count=min_snapshot_count,
            min_stable_seconds=min_stable_seconds,
            now=now,
        )
        if stability is not None:
            stabilities.append(stability)
            continue
        if failure_detail is not None:
            failure_details.append(failure_detail)

    ranked = sorted(stabilities, key=_rank_launch_ready_stability, reverse=True)
    if ranked:
        return ranked, None

    detail = "No stable launch-ready canary snapshot found"
    if failure_details:
        summarized_failures = "; ".join(failure_details[:3])
        if len(failure_details) > 3:
            summarized_failures += f"; +{len(failure_details) - 3} more labels"
        detail = f"{detail}: {summarized_failures}"
    return [], detail


def _build_stable_launch_candidate_skip_summary(
    *,
    database_path: str,
    skipped_candidates: list[_StableCanaryCandidateSkip],
) -> StableCanaryLaunchSummary:
    """Return a deterministic skip summary after all ranked candidates are blocked."""

    if not skipped_candidates:
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=database_path,
            detail="No stable launch-ready canary snapshot found",
        )

    if len(skipped_candidates) == 1:
        candidate = skipped_candidates[0]
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=database_path,
            label=candidate.label,
            launch_ready_snapshot_id=candidate.launch_ready_snapshot_id,
            approved_snapshot_id=candidate.approved_snapshot_id,
            paper_trade_id=candidate.paper_trade_id,
            final_pair_state=candidate.final_pair_state,
            detail=candidate.detail,
        )

    rendered_details = []
    for candidate in skipped_candidates[:5]:
        label = candidate.label or "unknown"
        rendered_details.append(f"{label}: {candidate.detail}")
    if len(skipped_candidates) > 5:
        rendered_details.append(f"+{len(skipped_candidates) - 5} more")

    return StableCanaryLaunchSummary(
        status="skipped",
        database_path=database_path,
        detail=(
            "No stable launch-ready candidate passed launch guards: "
            + "; ".join(rendered_details)
        ),
    )


async def scan_approved_canary_once(
    settings: WorkerSettings,
    *,
    scanner: OpportunityUniverseService | None = None,
    approval_service: RouteApprovalService | None = None,
    store: ApprovedCanaryStore | None = None,
    alert_sink: ApprovedCanaryAlertSink | None = None,
    alert_notifier: ApprovedCanaryAlertNotifier | None = None,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> ApprovedCanaryScanSummary:
    """Scan the live universe for canaries that are currently operator-approved."""

    timestamp = now or datetime.now(UTC)
    snapshot_store = store or ApprovedCanaryStore(settings.database_path)
    approved_canary_alert_sink = alert_sink or ApprovedCanaryAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    route_approval_service = approval_service or RouteApprovalService(
        store=RouteApprovalStore(settings.database_path)
    )
    runtime = scanner or _build_worker_universe_scanner(
        settings,
        history_store=OpportunityHistoryStore(settings.database_path),
    )

    approvals = route_approval_service.list_recent(limit=None, approved=True)
    unique_approvals: list[RouteApprovalEntry] = []
    seen_route_keys: set[tuple[str, str, str, str, str, str]] = set()
    for approval in approvals:
        route_key = (
            approval.label,
            approval.canonical_symbol,
            approval.short_venue,
            approval.long_venue,
            approval.short_fee_profile,
            approval.long_fee_profile,
        )
        if route_key in seen_route_keys:
            continue
        seen_route_keys.add(route_key)
        unique_approvals.append(approval)
    approvals_by_label: dict[str, list[RouteApprovalEntry]] = {}
    for approval in unique_approvals:
        approvals_by_label.setdefault(approval.label, []).append(approval)
    approved_matches_by_label: dict[
        str, tuple[FundingUniverseCanaryCandidate, RouteApprovalEntry]
    ] = {}
    scanned_labels: set[str] = set()
    scanned_candidates = 0

    all_labels = list(approvals_by_label)
    selected_labels = (
        all_labels
        if settings.approved_canary_scan_limit == 0
        else all_labels[: settings.approved_canary_scan_limit]
    )
    if len(selected_labels) < len(approvals_by_label):
        loop_logger.info(
            "approved canary label scan limit selected %s of %s labels",
            settings.approved_canary_scan_limit,
            len(approvals_by_label),
        )
    async def _scan_label_unbounded(
        label: str,
    ) -> tuple[
        str,
        int,
        bool,
        FundingUniverseCanaryCandidate | None,
        RouteApprovalEntry | None,
    ]:
        best_match: tuple[FundingUniverseCanaryCandidate, RouteApprovalEntry] | None = None
        label_scanned = False
        scanned_candidate_count = 0
        for approved_route in approvals_by_label[label]:
            try:
                async with asyncio.timeout(settings.universe_scan_timeout_seconds):
                    candidate, candidate_count = await scan_exact_canary_candidate_for_approval(
                        scanner=runtime,
                        approval_service=route_approval_service,
                        approval=approved_route,
                        venues=list(settings.approved_canary_scan_venues),
                        fee_profile_overrides=_build_approved_canary_fee_profile_overrides(
                            settings
                        ),
                        target_notional=settings.approved_canary_scan_target_notional,
                        canary_max_notional=settings.approved_canary_scan_max_notional,
                        min_capacity_notional=settings.approved_canary_scan_min_capacity_notional,
                        min_daily_volume=settings.approved_canary_scan_min_daily_volume,
                        min_open_interest=settings.approved_canary_scan_min_open_interest,
                        min_roundtrip_edge=settings.approved_canary_scan_min_roundtrip_edge,
                        min_execution_quality_score=settings.approved_canary_scan_min_execution_quality_score,
                        min_execution_samples=settings.approved_canary_scan_min_execution_samples,
                        min_route_stability_weight=settings.approved_canary_scan_min_route_stability_weight,
                        min_route_presence_ratio=settings.approved_canary_scan_min_route_presence_ratio,
                        min_route_samples=settings.approved_canary_scan_min_route_samples,
                        include_symbols=list(settings.approved_canary_scan_include_symbols)
                        or None,
                        exclude_symbols=list(settings.approved_canary_scan_exclude_symbols)
                        or None,
                        exclude_tags=list(settings.approved_canary_scan_exclude_tags) or None,
                        limit=settings.approved_canary_exact_scan_limit,
                    )
            except TimeoutError:
                loop_logger.warning(
                    "approved canary exact scan timed out for label=%s",
                    approved_route.label,
                )
                continue
            except (ConnectorError, UpstreamDataError, httpx.HTTPError, ValueError) as exc:
                loop_logger.warning(
                    "approved canary exact scan failed for label=%s: %s",
                    approved_route.label,
                    exc,
                )
                continue
            except Exception:
                loop_logger.exception(
                    "approved canary exact scan crashed for label=%s",
                    approved_route.label,
                )
                continue
            label_scanned = True
            scanned_candidate_count += candidate_count
            if candidate is None:
                continue
            existing = best_match
            if existing is None or _rank_approved_canary_candidate(
                candidate
            ) > _rank_approved_canary_candidate(existing[0]):
                best_match = (candidate, approved_route)
        if best_match is None:
            return label, scanned_candidate_count, label_scanned, None, None
        candidate, matched_approval = best_match
        return label, scanned_candidate_count, label_scanned, candidate, matched_approval

    async def _scan_label(
        label: str,
    ) -> tuple[
        str,
        int,
        bool,
        FundingUniverseCanaryCandidate | None,
        RouteApprovalEntry | None,
    ]:
        try:
            return await _scan_label_unbounded(label)
        except Exception:
            loop_logger.exception("approved canary label scan crashed for label=%s", label)
            return label, 0, False, None, None

    results: list[
        tuple[
            str,
            int,
            bool,
            FundingUniverseCanaryCandidate | None,
            RouteApprovalEntry | None,
        ]
    ] = []
    batch_size = settings.approved_canary_scan_concurrency
    for start in range(0, len(selected_labels), batch_size):
        batch_labels = selected_labels[start : start + batch_size]
        results.extend(await asyncio.gather(*(_scan_label(label) for label in batch_labels)))
    for label, candidate_count, label_scanned, candidate, matched_approval in results:
        scanned_candidates += candidate_count
        if label_scanned:
            scanned_labels.add(label)
        if candidate is not None and matched_approval is not None:
            approved_matches_by_label[label] = (candidate, matched_approval)
    previous_snapshots = {label: snapshot_store.latest(label=label) for label in scanned_labels}
    snapshots: list[ApprovedCanarySnapshot] = []
    approved_matches = list(approved_matches_by_label.values())
    for candidate, matched_approval in approved_matches:
        snapshot_label = matched_approval.label
        try:
            derived_label = build_pair_spec_from_universe_opportunity(candidate.opportunity).label
        except Exception:
            loop_logger.warning(
                "failed to derive approved canary snapshot label; using approval label=%s",
                matched_approval.label,
                exc_info=True,
            )
        else:
            if derived_label and derived_label != matched_approval.label:
                loop_logger.warning(
                    "approved canary label mismatch approval=%s derived=%s; keeping approval label",
                    matched_approval.label,
                    derived_label,
                )
            elif derived_label:
                snapshot_label = derived_label
        snapshots.append(
            snapshot_store.append(
                ApprovedCanarySnapshot(
                    captured_at=timestamp,
                    label=snapshot_label,
                    candidate=candidate,
                    approval=matched_approval,
                )
            )
        )

    alerts = _emit_approved_canary_alerts(
        approved_labels=scanned_labels,
        previous_snapshots=previous_snapshots,
        current_snapshots=snapshots,
        max_snapshot_age_seconds=settings.approved_canary_alert_max_snapshot_age_seconds,
        emitted_at=timestamp,
        sink=approved_canary_alert_sink,
    )
    sent_notifications = 0
    if alert_notifier is not None:
        for event in alerts:
            try:
                await _wait_for_notification(
                    alert_notifier.notify(event),
                    timeout=settings.approved_canary_alert_webhook_timeout_seconds,
                )
                sent_notifications += 1
            except Exception:
                loop_logger.exception(
                    "approved canary alert notification failed for label=%s type=%s",
                    event.current_snapshot.label
                    if event.current_snapshot is not None
                    else (
                        event.previous_snapshot.label
                        if event.previous_snapshot is not None
                        else None
                    ),
                    event.alert_type,
                )

    return ApprovedCanaryScanSummary(
        scanned_candidates=scanned_candidates,
        approved_candidates=len(approved_matches),
        saved_snapshots=len(snapshots),
        alert_events=len(alerts),
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
        snapshots=snapshots,
        alerts=alerts,
    )


async def cache_launch_ready_canaries_once(
    settings: WorkerSettings,
    *,
    approved_store: ApprovedCanaryStore | None = None,
    launch_ready_store: LaunchReadyCanaryStore | None = None,
    alert_sink: StableLaunchReadyAlertSink | None = None,
    alert_notifier: StableLaunchReadyAlertNotifier | None = None,
    approval_service: RouteApprovalService | None = None,
    system_state_service: SystemStateService | None = None,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> LaunchReadyCanaryCacheSummary:
    """Persist fresh approved canaries whose touched venues are currently healthy."""

    timestamp = now or datetime.now(UTC)
    source_store = approved_store or ApprovedCanaryStore(settings.database_path)
    ready_store = launch_ready_store or LaunchReadyCanaryStore(settings.database_path)
    stable_alert_sink = alert_sink or StableLaunchReadyAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    route_approval_service = approval_service or RouteApprovalService(
        store=RouteApprovalStore(settings.database_path)
    )
    runtime = system_state_service or SystemStateService()

    latest_approvals_by_label: set[str] = set()
    for approval in route_approval_service.list_recent(limit=1_000, approved=True):
        latest_approvals_by_label.add(approval.label)

    previous_stabilities = {
        label: (
            latest_alert.current_stability
            if isinstance(stable_alert_sink, StableLaunchReadyAlertStore)
            and (latest_alert := stable_alert_sink.latest(label=label)) is not None
            else None
        )
        for label in sorted(latest_approvals_by_label)
    }

    scanned_snapshots = 0
    snapshots: list[LaunchReadyCanarySnapshot] = []
    for label in sorted(latest_approvals_by_label):
        snapshot = source_store.latest(label=label)
        if snapshot is None:
            continue
        scanned_snapshots += 1
        snapshot_age_seconds = max(
            0.0,
            (timestamp - snapshot.captured_at).total_seconds(),
        )
        if snapshot_age_seconds > settings.launch_ready_canary_max_snapshot_age_seconds:
            loop_logger.debug(
                ("skipping stale approved canary snapshot label=%s age=%.1fs (max=%ss)"),
                label,
                snapshot_age_seconds,
                settings.launch_ready_canary_max_snapshot_age_seconds,
            )
            continue

        refreshed_approval = route_approval_service.get_for_candidate(snapshot.candidate)
        if refreshed_approval is None or not refreshed_approval.approved:
            continue

        capped_notional = min(
            snapshot.candidate.suggested_canary_notional,
            refreshed_approval.max_live_notional,
        )
        if capped_notional <= 0:
            continue

        refreshed_candidate = snapshot.candidate.model_copy(
            update={"suggested_canary_notional": capped_notional}
        )
        refreshed_snapshot = snapshot.model_copy(
            update={
                "candidate": refreshed_candidate,
                "approval": refreshed_approval,
            }
        )
        recent_approved_chain = _list_recent_approved_snapshot_chain(
            store=source_store,
            snapshot=refreshed_snapshot,
            max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
        )
        automation_gate_reason = _build_approved_snapshot_automation_gate_reason(
            snapshot=refreshed_snapshot,
            recent_chain=recent_approved_chain or [refreshed_snapshot],
            settings=settings,
        )
        if automation_gate_reason is not None:
            if settings.stable_canary_launch_shadow_mode:
                loop_logger.info(
                    "shadow launch gate blocked label=%s because %s",
                    label,
                    automation_gate_reason,
                )
            else:
                loop_logger.debug(
                    "skipping launch-ready snapshot label=%s because %s",
                    label,
                    automation_gate_reason,
                )
            continue
        execution_preflight = _build_candidate_live_execution_preflight(
            settings=settings,
            candidate=refreshed_candidate,
            label=label,
        )
        if not execution_preflight.ready:
            try:
                ready_store.delete_label(label)
            except Exception:
                loop_logger.exception(
                    "failed to invalidate launch-ready snapshots for label=%s "
                    "after live execution preflight failure",
                    label,
                )
            loop_logger.warning(
                "skipping launch-ready snapshot label=%s because live execution is not ready: %s",
                label,
                "; ".join(execution_preflight.blocking_reasons),
            )
            continue
        system_state = await _probe_candidate_system_state(
            settings=settings,
            service=runtime,
            candidate=refreshed_candidate,
            label=label,
        )
        if not system_state.ready:
            loop_logger.debug(
                "skipping launch-ready snapshot label=%s because system state is not ready",
                label,
            )
            continue

        snapshots.append(
            ready_store.append(
                LaunchReadyCanarySnapshot(
                    captured_at=timestamp,
                    label=label,
                    max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
                    approved_snapshot=refreshed_snapshot,
                    system_state=system_state,
                )
            )
        )

    alerts = _emit_stable_launch_ready_alerts(
        labels=set(latest_approvals_by_label),
        previous_stabilities=previous_stabilities,
        store=ready_store,
        sink=stable_alert_sink,
        emitted_at=timestamp,
        max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
        min_snapshot_count=settings.stable_launch_ready_min_snapshot_count,
        min_stable_seconds=settings.stable_launch_ready_min_stable_seconds,
    )
    sent_notifications = 0
    if alert_notifier is not None:
        for event in alerts:
            try:
                await _wait_for_notification(
                    alert_notifier.notify(event),
                    timeout=settings.stable_launch_ready_alert_webhook_timeout_seconds,
                )
                sent_notifications += 1
            except Exception:
                loop_logger.exception(
                    "stable launch-ready alert notification failed for label=%s type=%s",
                    (
                        event.current_stability.snapshot.label
                        if event.current_stability is not None
                        else (
                            event.previous_stability.snapshot.label
                            if event.previous_stability is not None
                            else None
                        )
                    ),
                    event.alert_type,
                )

    return LaunchReadyCanaryCacheSummary(
        scanned_snapshots=scanned_snapshots,
        launch_ready_candidates=len(snapshots),
        saved_snapshots=len(snapshots),
        alert_events=len(alerts),
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
        snapshots=snapshots,
        alerts=alerts,
    )


async def run_supervised_launch_ready_canary_cache_loop(
    settings: WorkerSettings,
    *,
    approved_store: ApprovedCanaryStore | None = None,
    launch_ready_store: LaunchReadyCanaryStore | None = None,
    alert_sink: StableLaunchReadyAlertSink | None = None,
    alert_notifier: StableLaunchReadyAlertNotifier | None = None,
    approval_service: RouteApprovalService | None = None,
    system_state_service: SystemStateService | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> LaunchReadyCanaryCacheLoopSummary:
    """Run the launch-ready canary cache loop until stopped or capped."""

    source_store = approved_store or ApprovedCanaryStore(settings.database_path)
    ready_store = launch_ready_store or LaunchReadyCanaryStore(settings.database_path)
    stable_alert_sink = alert_sink or StableLaunchReadyAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    stable_launch_ready_notifier = alert_notifier
    supervised_stop_event = stop_event or asyncio.Event()
    if stable_launch_ready_notifier is None:
        from carryme_worker.notifications import build_stable_launch_ready_alert_notifier

        stable_launch_ready_notifier = build_stable_launch_ready_alert_notifier(
            settings,
            logger=loop_logger,
        )

    attempts = 0
    successful_cycles = 0
    failures = 0
    scanned_snapshots = 0
    launch_ready_candidates = 0
    saved_snapshots = 0
    alert_events = 0
    sent_notifications = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised launch-ready canary cache cycle %s", attempts)
        try:
            summary = await cache_launch_ready_canaries_once(
                settings,
                approved_store=source_store,
                launch_ready_store=ready_store,
                alert_sink=stable_alert_sink,
                alert_notifier=stable_launch_ready_notifier,
                approval_service=approval_service,
                system_state_service=system_state_service,
                logger=loop_logger,
            )
            successful_cycles += 1
            consecutive_failures = 0
            scanned_snapshots += summary.scanned_snapshots
            launch_ready_candidates += summary.launch_ready_candidates
            saved_snapshots += summary.saved_snapshots
            alert_events += summary.alert_events
            sent_notifications += summary.sent_notifications
            loop_logger.info(
                (
                    "completed supervised launch-ready canary cache cycle %s with "
                    "%s scanned snapshots, %s saved launch-ready snapshots, %s alerts, "
                    "and %s notifications"
                ),
                attempts,
                summary.scanned_snapshots,
                summary.saved_snapshots,
                summary.alert_events,
                summary.sent_notifications,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await sleep(settings.launch_ready_canary_interval_seconds)
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.launch_ready_canary_max_backoff_seconds,
                settings.launch_ready_canary_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "launch-ready canary cache cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await sleep(backoff_seconds)

    return LaunchReadyCanaryCacheLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        scanned_snapshots=scanned_snapshots,
        launch_ready_candidates=launch_ready_candidates,
        saved_snapshots=saved_snapshots,
        alert_events=alert_events,
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
    )


def _build_api_settings_from_worker_settings(settings: WorkerSettings) -> ApiSettings:
    """Build API settings aligned to the worker's runtime paths and environment."""

    return ApiSettings(
        environment=settings.environment,
        # Keep the real URL for runtime DB access. `database_target` is redacted and
        # only safe for summaries/logging.
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        extended_live_enabled=settings.extended_live_enabled,
        extended_api_key=settings.extended_api_key,
        extended_stark_private_key=settings.extended_stark_private_key,
        paradex_live_enabled=settings.paradex_live_enabled,
        paradex_account_address=settings.paradex_account_address,
        paradex_private_key=settings.paradex_private_key,
        paradex_bearer_token=settings.paradex_bearer_token,
        paradex_recv_window_ms=settings.paradex_recv_window_ms,
        hyperliquid_live_enabled=settings.hyperliquid_live_enabled,
        hyperliquid_account_address=settings.hyperliquid_account_address,
        hyperliquid_vault_address=settings.hyperliquid_vault_address,
        hyperliquid_api_wallet_private_key=settings.hyperliquid_api_wallet_private_key,
    )


async def launch_latest_stable_canary_once(
    settings: WorkerSettings,
    *,
    api_settings: ApiSettings | None = None,
    launch_store: StableCanaryLaunchStore | None = None,
    approved_store: ApprovedCanaryStore | None = None,
    now: datetime | None = None,
) -> StableCanaryLaunchSummary:
    """Launch the latest stable cached canary once, or skip deterministically."""

    runtime_settings = api_settings or _build_api_settings_from_worker_settings(settings)
    launch_ready_store = LaunchReadyCanaryStore(runtime_settings.database_path)
    stable_launch_store = launch_store or StableCanaryLaunchStore(runtime_settings.database_path)
    source_approved_store = approved_store or ApprovedCanaryStore(runtime_settings.database_path)
    execution_store = ExecutionJournalStore(runtime_settings.database_path)
    observation_store = ExecutionObservationStore(runtime_settings.database_path)
    balance_service = BalanceAccountingService(
        store=BalanceSnapshotStore(runtime_settings.database_path)
    )
    route_approval_service = RouteApprovalService(
        store=RouteApprovalStore(runtime_settings.database_path)
    )
    timestamp = now or _utc_now()

    blocking_live_executions = _list_blocking_live_executions_for_stable_launch(
        settings=settings,
        execution_store=execution_store,
        observation_store=observation_store,
        now=timestamp,
    )
    if (
        len(blocking_live_executions)
        > settings.stable_canary_launch_max_active_live_executions
    ):
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=settings.database_target,
            detail=_build_blocking_live_execution_detail(
                blocking=blocking_live_executions,
                max_active_live_executions=settings.stable_canary_launch_max_active_live_executions,
            ),
        )

    loss_circuit_breaker_reason = _build_stable_launch_loss_circuit_breaker_reason(
        settings=settings,
        execution_store=execution_store,
        observation_store=observation_store,
        balance_service=balance_service,
        now=timestamp,
    )
    if loss_circuit_breaker_reason is not None:
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=settings.database_target,
            detail=loss_circuit_breaker_reason,
        )

    global_cooldown_reason = _build_stable_launch_cooldown_reason(
        settings=settings,
        launch_store=stable_launch_store,
        now=timestamp,
    )
    if global_cooldown_reason is not None:
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=settings.database_target,
            detail=global_cooldown_reason,
        )

    global_rate_cap_reason = _build_stable_launch_rate_cap_reason(
        settings=settings,
        launch_store=stable_launch_store,
        now=timestamp,
    )
    if global_rate_cap_reason is not None:
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=settings.database_target,
            detail=global_rate_cap_reason,
        )

    risk_budget_scan_limit = settings.stable_canary_launch_active_execution_limit
    active_executions_for_budget = _list_recent_live_executions(
        execution_store,
        observation_store,
        limit=risk_budget_scan_limit + 1,
        now=timestamp,
        max_age_seconds=settings.execution_observation_max_age_seconds,
        unobserved_requires_monitoring=True,
    )
    if (
        (
            settings.stable_canary_launch_max_total_live_notional is not None
            or settings.stable_canary_launch_max_live_notional_per_venue is not None
        )
        and len(active_executions_for_budget) > risk_budget_scan_limit
    ):
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=settings.database_target,
            detail=(
                "Stable launch risk-budget check truncated by "
                "stable_canary_launch_active_execution_limit; increase limit"
            ),
        )

    # Some production pre-checks intentionally touch live execution and balance
    # history. Use a fresh timestamp for market-snapshot freshness so a slow
    # pre-check cannot make a newly captured stable snapshot look future-dated.
    snapshot_selection_timestamp = now or _utc_now()

    stable_candidates, no_candidate_detail = _list_ranked_stable_launch_ready_stabilities(
        store=launch_ready_store,
        max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
        min_snapshot_count=settings.stable_launch_ready_min_snapshot_count,
        min_stable_seconds=settings.stable_launch_ready_min_stable_seconds,
        now=snapshot_selection_timestamp,
        scan_limit=settings.stable_canary_launch_candidate_scan_limit,
    )
    if not stable_candidates:
        return StableCanaryLaunchSummary(
            status="skipped",
            database_path=settings.database_target,
            detail=no_candidate_detail or "No stable launch-ready canary snapshot found",
        )

    skipped_candidates: list[_StableCanaryCandidateSkip] = []

    for candidate_stability in stable_candidates:
        candidate_snapshot = candidate_stability.snapshot
        try:
            latest_launch_ready_snapshot, candidate_selected, candidate_approval = (
                _select_latest_launch_ready_canary_snapshot(
                    store=launch_ready_store,
                    approval_service=route_approval_service,
                    label=candidate_stability.snapshot.label,
                    max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
                    now=snapshot_selection_timestamp,
                )
            )
        except HTTPException as exc:
            if exc.status_code in {404, 409}:
                skipped_candidates.append(
                    _StableCanaryCandidateSkip(
                        label=candidate_stability.snapshot.label,
                        launch_ready_snapshot_id=(
                            candidate_stability.snapshot.launch_ready_snapshot_id
                        ),
                        approved_snapshot_id=(
                            candidate_stability.snapshot.approved_snapshot.snapshot_id
                        ),
                        detail=exc.detail,
                    )
                )
                continue
            raise

        if (
            latest_launch_ready_snapshot.launch_ready_snapshot_id
            != candidate_snapshot.launch_ready_snapshot_id
            and _launch_ready_snapshot_payload_changed(
                candidate_snapshot,
                latest_launch_ready_snapshot,
            )
        ):
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=(
                        "Latest launch-ready canary snapshot changed route or readiness "
                        "after stable evidence was collected"
                    ),
                )
            )
            continue

        launch_approved_snapshot_id = (
            latest_launch_ready_snapshot.approved_snapshot.snapshot_id
            or candidate_snapshot.approved_snapshot.snapshot_id
        )

        label_cooldown_reason = _build_stable_launch_cooldown_reason(
            settings=settings,
            launch_store=stable_launch_store,
            now=snapshot_selection_timestamp,
            label=candidate_snapshot.label,
        )
        if label_cooldown_reason is not None:
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=label_cooldown_reason,
                )
            )
            continue

        label_rate_cap_reason = _build_stable_launch_rate_cap_reason(
            settings=settings,
            launch_store=stable_launch_store,
            now=snapshot_selection_timestamp,
            label=candidate_snapshot.label,
        )
        if label_rate_cap_reason is not None:
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=label_rate_cap_reason,
                )
            )
            continue

        latest_approved_snapshot = source_approved_store.latest(label=candidate_snapshot.label)
        if latest_approved_snapshot is None:
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail="Latest approved canary snapshot is missing for the selected label",
                )
            )
            continue
        if latest_approved_snapshot.snapshot_id != candidate_snapshot.approved_snapshot.snapshot_id:
            if _approved_snapshot_launch_payload_changed(
                candidate_snapshot.approved_snapshot,
                latest_approved_snapshot,
            ):
                skipped_candidates.append(
                    _StableCanaryCandidateSkip(
                        label=candidate_snapshot.label,
                        launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                        approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                        detail=(
                            "Launch-ready canary snapshot is stale relative to the latest "
                            "approved snapshot"
                        ),
                    )
                )
                continue
            latest_approval = route_approval_service.get_for_candidate(
                latest_approved_snapshot.candidate
            )
            if latest_approval is None or not latest_approval.approved:
                skipped_candidates.append(
                    _StableCanaryCandidateSkip(
                        label=candidate_snapshot.label,
                        launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                        approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                        detail=(
                            "Latest approved snapshot is no longer approved for live execution"
                        ),
                    )
                )
                continue
            capped_notional = min(
                latest_approved_snapshot.candidate.suggested_canary_notional,
                latest_approval.max_live_notional,
            )
            if capped_notional <= 0:
                skipped_candidates.append(
                    _StableCanaryCandidateSkip(
                        label=candidate_snapshot.label,
                        launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                        approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                        detail=(
                            "Latest approved snapshot no longer permits a positive live notional"
                        ),
                    )
                )
                continue
            candidate_selected = latest_approved_snapshot.candidate.model_copy(
                update={"suggested_canary_notional": capped_notional}
            )
            candidate_approval = latest_approval
            launch_approved_snapshot_id = latest_approved_snapshot.snapshot_id
        elif (
            latest_launch_ready_snapshot.approved_snapshot.snapshot_id
            != candidate_snapshot.approved_snapshot.snapshot_id
        ):
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=(
                        "Launch-ready canary snapshot is stale relative to the latest "
                        "approved snapshot"
                    ),
                )
            )
            continue

        execution_maturity_reason = _build_stable_launch_execution_maturity_reason(
            settings=settings,
            candidate=latest_approved_snapshot.candidate,
        )
        if execution_maturity_reason is not None:
            if settings.stable_canary_launch_shadow_mode:
                logging.getLogger("carryme.worker").info(
                    "shadow launch maturity blocked label=%s because %s",
                    candidate_snapshot.label,
                    execution_maturity_reason,
                )
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=execution_maturity_reason,
                )
            )
            continue

        latest_outcome_reason = _build_stable_launch_latest_outcome_reason(
            settings=settings,
            candidate=latest_approved_snapshot.candidate,
        )
        if latest_outcome_reason is not None:
            if settings.stable_canary_launch_shadow_mode:
                logging.getLogger("carryme.worker").info(
                    "shadow launch latest-outcome blocked label=%s because %s",
                    candidate_snapshot.label,
                    latest_outcome_reason,
                )
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=latest_outcome_reason,
                )
            )
            continue

        liquidity_and_value_reason = _build_stable_launch_liquidity_and_value_reason(
            settings=settings,
            candidate=latest_approved_snapshot.candidate,
        )
        if liquidity_and_value_reason is not None:
            if settings.stable_canary_launch_shadow_mode:
                logging.getLogger("carryme.worker").info(
                    "shadow launch liquidity/value blocked label=%s because %s",
                    candidate_snapshot.label,
                    liquidity_and_value_reason,
                )
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=liquidity_and_value_reason,
                )
            )
            continue

        recent_approved_chain = _list_recent_approved_snapshot_chain(
            store=source_approved_store,
            snapshot=latest_approved_snapshot,
            max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
        )
        automation_gate_reason = _build_approved_snapshot_automation_gate_reason(
            snapshot=latest_approved_snapshot,
            recent_chain=recent_approved_chain or [latest_approved_snapshot],
            settings=settings,
        )
        if automation_gate_reason is not None:
            if settings.stable_canary_launch_shadow_mode:
                logging.getLogger("carryme.worker").info(
                    "shadow launch gate blocked label=%s because %s",
                    candidate_snapshot.label,
                    automation_gate_reason,
                )
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=(
                        "Latest approved snapshot no longer satisfies automated launch "
                        f"gates: {automation_gate_reason}"
                    ),
                )
            )
            continue

        if candidate_snapshot.launch_ready_snapshot_id is not None:
            previous_launch = stable_launch_store.latest_for_snapshot(
                candidate_snapshot.launch_ready_snapshot_id
            )
            if previous_launch is not None:
                if previous_launch.status == "shadowed":
                    if settings.stable_canary_launch_shadow_mode:
                        skipped_candidates.append(
                            _StableCanaryCandidateSkip(
                                label=candidate_snapshot.label,
                                launch_ready_snapshot_id=(
                                    candidate_snapshot.launch_ready_snapshot_id
                                ),
                                approved_snapshot_id=(
                                    candidate_snapshot.approved_snapshot.snapshot_id
                                ),
                                final_pair_state=previous_launch.final_pair_state,
                                detail=(
                                    "Launch-ready canary snapshot already evaluated in shadow mode "
                                    "by worker"
                                ),
                            )
                        )
                        continue
                    # Shadow mode is off but was previously on: allow a real launch now.
                else:
                    skipped_candidates.append(
                        _StableCanaryCandidateSkip(
                            label=candidate_snapshot.label,
                            launch_ready_snapshot_id=(
                                candidate_snapshot.launch_ready_snapshot_id
                            ),
                            approved_snapshot_id=launch_approved_snapshot_id,
                            paper_trade_id=previous_launch.paper_trade_id,
                            final_pair_state=previous_launch.final_pair_state,
                            detail=(
                                "Launch-ready canary snapshot already launched by worker as "
                                f"paper trade {previous_launch.paper_trade_id}"
                            ),
                        )
                    )
                    continue

        risk_budget_reason = _build_stable_launch_risk_budget_reason(
            settings=settings,
            active_executions=active_executions_for_budget,
            candidate=candidate_selected,
        )
        if risk_budget_reason is not None:
            if settings.stable_canary_launch_shadow_mode:
                logging.getLogger("carryme.worker").info(
                    "shadow launch budget blocked label=%s because %s",
                    candidate_snapshot.label,
                    risk_budget_reason,
                )
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=candidate_snapshot.approved_snapshot.snapshot_id,
                    detail=risk_budget_reason,
                )
            )
            continue

        if settings.stable_canary_launch_shadow_mode:
            logging.getLogger("carryme.worker").info(
                "shadow launch for label=%s from launch_ready_snapshot_id=%s",
                candidate_snapshot.label,
                candidate_snapshot.launch_ready_snapshot_id,
            )
            stable_launch_store.append(
                StableCanaryLaunchRecord(
                    launched_at=timestamp,
                    status="shadowed",
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id or 0,
                    approved_snapshot_id=launch_approved_snapshot_id or 0,
                    paper_trade_id=0,
                    final_pair_state="shadowed",
                    detail="Shadow mode launch marker",
                )
            )
            return StableCanaryLaunchSummary(
                status="skipped",
                database_path=settings.database_target,
                label=candidate_snapshot.label,
                launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                approved_snapshot_id=launch_approved_snapshot_id,
                detail=(
                    "Shadow mode: would launch stable canary from launch-ready snapshot "
                    f"{candidate_snapshot.launch_ready_snapshot_id}"
                ),
            )

        hold_mode_blocker = _build_stable_launch_hold_mode_blocker(settings)
        if hold_mode_blocker is not None:
            return StableCanaryLaunchSummary(
                status="skipped",
                database_path=settings.database_target,
                label=candidate_snapshot.label,
                launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                approved_snapshot_id=launch_approved_snapshot_id,
                detail=hold_mode_blocker,
            )

        execution_preflight = _build_candidate_live_execution_preflight(
            settings=runtime_settings,
            candidate=candidate_selected,
            label=candidate_snapshot.label,
        )
        if not execution_preflight.ready:
            try:
                launch_ready_store.delete_label(candidate_snapshot.label)
            except Exception:
                logging.getLogger("carryme.worker").exception(
                    "failed to invalidate launch-ready snapshots for label=%s "
                    "during stable launch preflight",
                    candidate_snapshot.label,
                )
            skipped_candidates.append(
                _StableCanaryCandidateSkip(
                    label=candidate_snapshot.label,
                    launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                    approved_snapshot_id=launch_approved_snapshot_id,
                    detail=(
                        "Latest launch-ready snapshot no longer satisfies live execution "
                        f"readiness: {'; '.join(execution_preflight.blocking_reasons)}"
                    ),
                )
            )
            continue

        candidate_reservation_owner_id: str | None = None
        if candidate_snapshot.launch_ready_snapshot_id is not None:
            candidate_reservation_owner_id = uuid.uuid4().hex
            reserved = stable_launch_store.reserve_snapshot_launch(
                launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                label=candidate_snapshot.label,
                reserved_at=timestamp,
                max_age_seconds=settings.stable_canary_launch_reservation_ttl_seconds,
                retention_seconds=(
                    settings.stable_canary_launch_reservation_retention_seconds
                ),
                owner_id=candidate_reservation_owner_id,
            )
            if not reserved:
                skipped_candidates.append(
                    _StableCanaryCandidateSkip(
                        label=candidate_snapshot.label,
                        launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                        approved_snapshot_id=launch_approved_snapshot_id,
                        detail=(
                            "Launch-ready canary snapshot is already reserved for launch "
                            "by worker"
                        ),
                    )
                )
                continue

        async def renew_stable_launch_reservation_before_open(
            launch_ready_snapshot_id: int | None = candidate_snapshot.launch_ready_snapshot_id,
            owner_id: str | None = candidate_reservation_owner_id,
        ) -> None:
            if launch_ready_snapshot_id is None or owner_id is None:
                return
            renewed = stable_launch_store.renew_snapshot_launch_reservation(
                launch_ready_snapshot_id=launch_ready_snapshot_id,
                owner_id=owner_id,
                reserved_at=datetime.now(UTC),
                max_age_seconds=settings.stable_canary_launch_reservation_ttl_seconds,
            )
            if not renewed:
                raise HTTPException(
                    status_code=409,
                    detail="Stable launch reservation ownership was lost before live submission",
                )

        try:
            lifecycle = await _run_guarded_canary_lifecycle(
                candidate=candidate_selected,
                approval=candidate_approval,
                desired_notional=None,
                note="worker stable launch-ready canary",
                lifecycle_note=(
                    "Launched by carryme-worker from stable launch-ready snapshot "
                    f"{candidate_snapshot.launch_ready_snapshot_id} after "
                    f"{candidate_stability.consecutive_snapshots} stable snapshots over "
                    f"{candidate_stability.stable_seconds:.1f}s."
                ),
                paper_store=PaperTradeStore(runtime_settings.database_path),
                confirmation_store=PreviewConfirmationStore(runtime_settings.database_path),
                pair_close_confirmation_store=PairClosePreviewConfirmationStore(
                    runtime_settings.database_path
                ),
                cleanup_confirmation_store=CleanupPreviewConfirmationStore(
                    runtime_settings.database_path
                ),
                execution_store=ExecutionJournalStore(runtime_settings.database_path),
                observation_store=ExecutionObservationStore(runtime_settings.database_path),
                settings=runtime_settings,
                account_preflight_service=AccountPreflightService(),
                system_state_service=SystemStateService(),
                balance_service=BalanceAccountingService(
                    store=BalanceSnapshotStore(runtime_settings.database_path)
                ),
                order_preview_service=OrderPreviewService(),
                order_state_service=ExecutionOrderStateService(
                    observers=_build_order_state_observers(settings)
                ),
                cleanup_preview_service=_build_cleanup_preview_router_for_candidate(
                    runtime_settings,
                    candidate_selected,
                ),
                pair_close_preview_service=_build_pair_close_preview_service_for_candidate(
                    runtime_settings,
                    candidate_selected,
                ),
                cleanup_live_router=_build_cleanup_live_execution_router_for_candidate(
                    runtime_settings,
                    candidate_selected,
                ),
                paired_service=_build_paired_live_execution_coordinator_for_candidate(
                    runtime_settings,
                    candidate_selected,
                ),
                pair_close_live_service=_build_pair_close_live_execution_coordinator_for_candidate(
                    runtime_settings,
                    candidate_selected,
                ),
                approval_service=route_approval_service,
                slippage_tolerance_bps=20,
                open_first_venue="auto",
                close_first_venue="auto",
                poll_attempts=5,
                poll_interval_seconds=2.0,
                auto_cleanup=True,
                close_position=settings.stable_canary_launch_close_position,
                before_open_submission=renew_stable_launch_reservation_before_open,
            )
        except HTTPException as exc:
            if exc.status_code in {404, 409}:
                skipped_candidates.append(
                    _StableCanaryCandidateSkip(
                        label=candidate_snapshot.label,
                        launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
                        approved_snapshot_id=launch_approved_snapshot_id,
                        detail=exc.detail,
                    )
                )
                if exc.status_code == 404:
                    continue
                return _build_stable_launch_candidate_skip_summary(
                    database_path=settings.database_target,
                    skipped_candidates=skipped_candidates,
                )
            raise

        paper_trade_id = lifecycle.paper_trade.entry_id
        if paper_trade_id is None:
            raise RuntimeError(
                "stable canary lifecycle returned launched paper trade without entry_id"
            )

        stable_launch_store.append(
            StableCanaryLaunchRecord(
                launched_at=timestamp,
                status="launched",
                label=candidate_snapshot.label,
                launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id or 0,
                approved_snapshot_id=launch_approved_snapshot_id or 0,
                paper_trade_id=paper_trade_id,
                final_pair_state=lifecycle.final_pair_status.derived_state,
            )
        )
        return StableCanaryLaunchSummary(
            status="launched",
            database_path=settings.database_target,
            label=candidate_snapshot.label,
            launch_ready_snapshot_id=candidate_snapshot.launch_ready_snapshot_id,
            approved_snapshot_id=launch_approved_snapshot_id,
            paper_trade_id=paper_trade_id,
            final_pair_state=lifecycle.final_pair_status.derived_state,
            detail=None,
            lifecycle=lifecycle,
        )

    return _build_stable_launch_candidate_skip_summary(
        database_path=settings.database_target,
        skipped_candidates=skipped_candidates,
    )


async def run_supervised_stable_canary_launch_loop(
    settings: WorkerSettings,
    *,
    launch_store: StableCanaryLaunchStore | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> StableCanaryLaunchLoopSummary:
    """Run the signal-aware stable canary launch loop until stopped or capped."""

    stable_launch_store = launch_store or StableCanaryLaunchStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    supervised_stop_event = stop_event or asyncio.Event()

    attempts = 0
    successful_cycles = 0
    failures = 0
    launched = 0
    skipped = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised stable canary launch cycle %s", attempts)
        try:
            summary = await launch_latest_stable_canary_once(
                settings,
                launch_store=stable_launch_store,
            )
            successful_cycles += 1
            consecutive_failures = 0
            if summary.status == "launched":
                launched += 1
            else:
                skipped += 1
            loop_logger.info(
                "completed supervised stable canary launch cycle %s with status=%s "
                "label=%s launch_ready_snapshot_id=%s approved_snapshot_id=%s "
                "paper_trade_id=%s final_pair_state=%s detail=%s",
                attempts,
                summary.status,
                summary.label,
                summary.launch_ready_snapshot_id,
                summary.approved_snapshot_id,
                summary.paper_trade_id,
                summary.final_pair_state,
                summary.detail,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await sleep(settings.stable_canary_launch_interval_seconds)
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.stable_canary_launch_max_backoff_seconds,
                settings.stable_canary_launch_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "stable canary launch cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await sleep(backoff_seconds)

    return StableCanaryLaunchLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        launched=launched,
        skipped=skipped,
        database_path=settings.database_target,
    )


async def run_production_supervisor_cycle_once(
    settings: WorkerSettings,
    *,
    system_state_alert_notifier: SystemStateAlertNotifier | None = None,
    approved_canary_alert_notifier: ApprovedCanaryAlertNotifier | None = None,
    stable_launch_ready_alert_notifier: StableLaunchReadyAlertNotifier | None = None,
    execution_alert_notifier: ExecutionAlertNotifier | None = None,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> ProductionSupervisorCycleSummary:
    """Run one end-to-end production supervisor cycle using the safe worker primitives."""

    loop_logger = logger or logging.getLogger("carryme.worker")
    timestamp = now or datetime.now(UTC)

    system_summary = await observe_system_state_once(
        settings,
        alert_notifier=system_state_alert_notifier,
        logger=loop_logger,
        now=timestamp,
    )
    approved_summary = await scan_approved_canary_once(
        settings,
        alert_notifier=approved_canary_alert_notifier,
        logger=loop_logger,
        now=timestamp,
    )
    launch_ready_summary = await cache_launch_ready_canaries_once(
        settings,
        alert_notifier=stable_launch_ready_alert_notifier,
        logger=loop_logger,
        now=timestamp,
    )
    launch_summary = await launch_latest_stable_canary_once(
        settings,
        now=timestamp,
    )
    execution_summary = await observe_live_executions_once(
        settings,
        alert_notifier=execution_alert_notifier,
        logger=loop_logger,
        now=timestamp,
    )

    return ProductionSupervisorCycleSummary(
        checked_venues=system_summary.checked_venues,
        degraded_venues=system_summary.degraded_venues,
        scanned_candidates=approved_summary.scanned_candidates,
        approved_candidates=approved_summary.approved_candidates,
        saved_approved_snapshots=approved_summary.saved_snapshots,
        scanned_launch_ready_snapshots=launch_ready_summary.scanned_snapshots,
        launch_ready_candidates=launch_ready_summary.launch_ready_candidates,
        saved_launch_ready_snapshots=launch_ready_summary.saved_snapshots,
        launch_status=launch_summary.status,
        paper_trade_id=launch_summary.paper_trade_id,
        final_pair_state=launch_summary.final_pair_state,
        observed_executions=execution_summary.observed_executions,
        saved_execution_observations=execution_summary.saved_observations,
        execution_alerts=execution_summary.saved_alerts,
        sent_notifications=(
            system_summary.sent_notifications
            + approved_summary.sent_notifications
            + launch_ready_summary.sent_notifications
            + execution_summary.sent_notifications
        ),
        database_path=settings.database_target,
    )


async def run_supervised_production_supervisor_loop(
    settings: WorkerSettings,
    *,
    system_state_alert_notifier: SystemStateAlertNotifier | None = None,
    approved_canary_alert_notifier: ApprovedCanaryAlertNotifier | None = None,
    stable_launch_ready_alert_notifier: StableLaunchReadyAlertNotifier | None = None,
    execution_alert_notifier: ExecutionAlertNotifier | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> ProductionSupervisorLoopSummary:
    """Run the production supervisor loop until stopped or capped."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    loop_logger = logger or logging.getLogger("carryme.worker")
    supervised_stop_event = stop_event or asyncio.Event()
    attempts = 0
    successful_cycles = 0
    failures = 0
    launched = 0
    skipped = 0
    observed_executions = 0
    execution_alerts = 0
    sent_notifications = 0
    consecutive_failures = 0
    system_state_notifier = system_state_alert_notifier
    approved_canary_notifier = approved_canary_alert_notifier
    stable_launch_ready_notifier = stable_launch_ready_alert_notifier
    execution_notifier = execution_alert_notifier

    if system_state_notifier is None:
        from carryme_worker.notifications import build_system_state_alert_notifier

        system_state_notifier = build_system_state_alert_notifier(
            settings,
            logger=loop_logger,
        )
    if approved_canary_notifier is None:
        from carryme_worker.notifications import build_approved_canary_alert_notifier

        approved_canary_notifier = build_approved_canary_alert_notifier(
            settings,
            logger=loop_logger,
        )
    if stable_launch_ready_notifier is None:
        from carryme_worker.notifications import build_stable_launch_ready_alert_notifier

        stable_launch_ready_notifier = build_stable_launch_ready_alert_notifier(
            settings,
            logger=loop_logger,
        )
    if execution_notifier is None:
        from carryme_worker.notifications import build_execution_alert_notifier

        execution_notifier = build_execution_alert_notifier(
            settings,
            logger=loop_logger,
        )

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting production supervisor cycle %s", attempts)
        try:
            summary = await run_production_supervisor_cycle_once(
                settings,
                system_state_alert_notifier=system_state_notifier,
                approved_canary_alert_notifier=approved_canary_notifier,
                stable_launch_ready_alert_notifier=stable_launch_ready_notifier,
                execution_alert_notifier=execution_notifier,
                logger=loop_logger,
            )
            successful_cycles += 1
            consecutive_failures = 0
            if summary.launch_status == "launched":
                launched += 1
            else:
                skipped += 1
            observed_executions += summary.observed_executions
            execution_alerts += summary.execution_alerts
            sent_notifications += summary.sent_notifications
            loop_logger.info(
                (
                    "completed production supervisor cycle %s with launch_status=%s, "
                    "%s approved candidates, %s launch-ready candidates, %s observed "
                    "executions, and %s sent notifications"
                ),
                attempts,
                summary.launch_status,
                summary.approved_candidates,
                summary.launch_ready_candidates,
                summary.observed_executions,
                summary.sent_notifications,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                settings.stable_canary_launch_interval_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.stable_canary_launch_max_backoff_seconds,
                settings.stable_canary_launch_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "production supervisor cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )

    return ProductionSupervisorLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        launched=launched,
        skipped=skipped,
        observed_executions=observed_executions,
        execution_alerts=execution_alerts,
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
    )


async def run_supervised_universe_scan_loop(
    settings: WorkerSettings,
    *,
    scanner: UniverseScanner | None = None,
    store: OpportunityHistoryStore | None = None,
    alert_sink: CandidateAlertSink | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> UniverseScanLoopSummary:
    """Run supervised funding-universe scans until stopped or capped."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    history_store = store or OpportunityHistoryStore(settings.database_path)
    candidate_alert_sink = alert_sink or CandidateAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    supervised_stop_event = stop_event or asyncio.Event()

    attempts = 0
    successful_cycles = 0
    failures = 0
    overlap_count = 0
    scanned_opportunities = 0
    saved_records = 0
    alert_events = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised universe scan cycle %s", attempts)
        try:
            summary = await scan_funding_universe_once(
                settings,
                scanner=scanner,
                store=history_store,
                alert_sink=candidate_alert_sink,
            )
            successful_cycles += 1
            consecutive_failures = 0
            overlap_count += summary.overlap_count
            scanned_opportunities += summary.scanned_opportunities
            saved_records += summary.saved_records
            alert_events += summary.alert_events
            loop_logger.info(
                (
                    "completed supervised universe scan cycle %s with %s overlaps, "
                    "%s ranked opportunities, %s saved records, and %s alerts"
                ),
                attempts,
                summary.overlap_count,
                summary.scanned_opportunities,
                summary.saved_records,
                summary.alert_events,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                settings.universe_scan_interval_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except RECOVERABLE_UNIVERSE_SCAN_EXCEPTIONS as exc:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.universe_scan_max_backoff_seconds,
                settings.universe_scan_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.warning(
                (
                    "supervised universe scan cycle %s hit a recoverable upstream/storage "
                    "failure (%s); backing off for %s seconds"
                ),
                attempts,
                exc,
                backoff_seconds,
            )
            loop_logger.debug(
                "recoverable universe scan failure details",
                exc_info=exc,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.universe_scan_max_backoff_seconds,
                settings.universe_scan_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "supervised universe scan cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )

    return UniverseScanLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        overlap_count=overlap_count,
        scanned_opportunities=scanned_opportunities,
        saved_records=saved_records,
        alert_events=alert_events,
        database_path=settings.database_target,
    )


async def run_supervised_approved_canary_scan_loop(
    settings: WorkerSettings,
    *,
    scanner: OpportunityUniverseService | None = None,
    approval_service: RouteApprovalService | None = None,
    store: ApprovedCanaryStore | None = None,
    alert_sink: ApprovedCanaryAlertSink | None = None,
    alert_notifier: ApprovedCanaryAlertNotifier | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> ApprovedCanaryScanLoopSummary:
    """Run supervised approved-canary scans until stopped or capped."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    snapshot_store = store or ApprovedCanaryStore(settings.database_path)
    approved_canary_alert_sink = alert_sink or ApprovedCanaryAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    approved_canary_notifier = alert_notifier
    shared_scanner = scanner or _build_worker_universe_scanner(
        settings,
        history_store=OpportunityHistoryStore(settings.database_path),
    )
    shared_approval_service = approval_service or RouteApprovalService(
        store=RouteApprovalStore(settings.database_path)
    )
    supervised_stop_event = stop_event or asyncio.Event()
    if approved_canary_notifier is None:
        from carryme_worker.notifications import build_approved_canary_alert_notifier

        approved_canary_notifier = build_approved_canary_alert_notifier(
            settings,
            logger=loop_logger,
        )

    attempts = 0
    successful_cycles = 0
    failures = 0
    scanned_candidates = 0
    approved_candidates = 0
    saved_snapshots = 0
    alert_events = 0
    sent_notifications = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised approved canary scan cycle %s", attempts)
        try:
            summary = await scan_approved_canary_once(
                settings,
                scanner=shared_scanner,
                approval_service=shared_approval_service,
                store=snapshot_store,
                alert_sink=approved_canary_alert_sink,
                alert_notifier=approved_canary_notifier,
                logger=loop_logger,
            )
            successful_cycles += 1
            consecutive_failures = 0
            scanned_candidates += summary.scanned_candidates
            approved_candidates += summary.approved_candidates
            saved_snapshots += summary.saved_snapshots
            alert_events += summary.alert_events
            sent_notifications += summary.sent_notifications
            loop_logger.info(
                (
                    "completed supervised approved canary scan cycle %s with %s candidates, "
                    "%s approved candidates, %s saved snapshots, and %s alerts"
                ),
                attempts,
                summary.scanned_candidates,
                summary.approved_candidates,
                summary.saved_snapshots,
                summary.alert_events,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                settings.approved_canary_scan_interval_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.approved_canary_scan_max_backoff_seconds,
                settings.approved_canary_scan_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "supervised approved canary scan cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )

    return ApprovedCanaryScanLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        scanned_candidates=scanned_candidates,
        approved_candidates=approved_candidates,
        saved_snapshots=saved_snapshots,
        alert_events=alert_events,
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
    )


async def observe_system_state_once(
    settings: WorkerSettings,
    *,
    service: SystemStateService | None = None,
    alert_sink: SystemStateAlertSink | None = None,
    alert_notifier: SystemStateAlertNotifier | None = None,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> SystemStateObservationSummary:
    """Probe current venue system-state and emit transition alerts."""

    runtime = service or SystemStateService()
    system_state_alert_sink = alert_sink or SystemStateAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    timestamp = now or datetime.now(UTC)

    states = await runtime.probe_venues(_build_system_state_configs(settings))
    alerts = _emit_system_state_alerts(
        states=states,
        sink=system_state_alert_sink,
        emitted_at=timestamp,
    )

    sent_notifications = 0
    if alert_notifier is not None:
        for event in alerts:
            try:
                await _wait_for_notification(
                    alert_notifier.notify(event),
                    timeout=settings.system_state_alert_webhook_timeout_seconds,
                )
                sent_notifications += 1
            except Exception:
                loop_logger.exception(
                    "system state alert notification failed for venue=%s type=%s",
                    event.venue,
                    event.alert_type,
                )

    degraded_venues = len([state for state in states if not state.healthy and state.enabled])
    return SystemStateObservationSummary(
        checked_venues=len([state for state in states if state.checked]),
        degraded_venues=degraded_venues,
        saved_alerts=len(alerts),
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
        states=states,
        alerts=alerts,
    )


async def run_supervised_system_state_observation_loop(
    settings: WorkerSettings,
    *,
    service: SystemStateService | None = None,
    alert_sink: SystemStateAlertSink | None = None,
    alert_notifier: SystemStateAlertNotifier | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> SystemStateObservationLoopSummary:
    """Run the system-state monitor until stopped or capped."""

    runtime = service or SystemStateService()
    system_state_alert_sink = alert_sink or SystemStateAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    system_state_notifier = alert_notifier
    supervised_stop_event = stop_event or asyncio.Event()
    if system_state_notifier is None:
        from carryme_worker.notifications import build_system_state_alert_notifier

        system_state_notifier = build_system_state_alert_notifier(
            settings,
            logger=loop_logger,
        )

    attempts = 0
    successful_cycles = 0
    failures = 0
    checked_venues = 0
    degraded_venues = 0
    saved_alerts = 0
    sent_notifications = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised system-state observation cycle %s", attempts)
        try:
            summary = await observe_system_state_once(
                settings,
                service=runtime,
                alert_sink=system_state_alert_sink,
                alert_notifier=system_state_notifier,
                logger=loop_logger,
            )
            successful_cycles += 1
            consecutive_failures = 0
            checked_venues += summary.checked_venues
            degraded_venues += summary.degraded_venues
            saved_alerts += summary.saved_alerts
            sent_notifications += summary.sent_notifications
            loop_logger.info(
                (
                    "completed supervised system-state observation cycle %s with "
                    "%s checked venues, %s degraded venues, and %s alerts"
                ),
                attempts,
                summary.checked_venues,
                summary.degraded_venues,
                summary.saved_alerts,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await sleep(settings.system_state_observation_interval_seconds)
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.system_state_observation_max_backoff_seconds,
                settings.system_state_observation_interval_seconds
                * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "system-state observation cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await sleep(backoff_seconds)

    return SystemStateObservationLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        checked_venues=checked_venues,
        degraded_venues=degraded_venues,
        saved_alerts=saved_alerts,
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
    )


def _emit_system_state_alerts(
    *,
    states: list[VenueSystemState],
    sink: SystemStateAlertSink,
    emitted_at: datetime,
) -> list[SystemStateAlertEvent]:
    """Emit transition alerts for venue system-state changes."""

    events: list[SystemStateAlertEvent] = []
    latest_by_venue = (
        {state.venue: sink.latest(venue=state.venue) for state in states}
        if isinstance(sink, SystemStateAlertStore)
        else {}
    )
    for state in states:
        if not state.enabled:
            continue
        previous_event = latest_by_venue.get(state.venue)
        previous_state = previous_event.current_state if previous_event is not None else None

        if not state.healthy:
            if previous_state is None or previous_state.healthy:
                event = SystemStateAlertEvent(
                    emitted_at=emitted_at,
                    venue=state.venue,
                    alert_type="venue_degraded",
                    current_state=state,
                    previous_state=previous_state,
                )
                sink.append(event)
                events.append(event)
                continue
            if previous_state.status != state.status:
                event = SystemStateAlertEvent(
                    emitted_at=emitted_at,
                    venue=state.venue,
                    alert_type="venue_status_changed",
                    current_state=state,
                    previous_state=previous_state,
                )
                sink.append(event)
                events.append(event)
            continue

        if previous_state is not None and not previous_state.healthy:
            event = SystemStateAlertEvent(
                emitted_at=emitted_at,
                venue=state.venue,
                alert_type="venue_recovered",
                current_state=state,
                previous_state=previous_state,
            )
            sink.append(event)
            events.append(event)

    return events


def _emit_approved_canary_alerts(
    *,
    approved_labels: set[str],
    previous_snapshots: dict[str, ApprovedCanarySnapshot | None],
    current_snapshots: list[ApprovedCanarySnapshot],
    max_snapshot_age_seconds: int,
    emitted_at: datetime,
    sink: ApprovedCanaryAlertSink,
) -> list[ApprovedCanaryAlertEvent]:
    """Emit transition alerts for approved-canary availability changes."""

    current_by_label = {snapshot.label: snapshot for snapshot in current_snapshots}
    alert_events: list[ApprovedCanaryAlertEvent] = []
    for label in sorted(approved_labels):
        previous_snapshot = previous_snapshots.get(label)
        current_snapshot = current_by_label.get(label)

        if current_snapshot is not None:
            if previous_snapshot is None:
                event = ApprovedCanaryAlertEvent(
                    emitted_at=emitted_at,
                    alert_type="approved_canary_available",
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                    current_snapshot=current_snapshot,
                    previous_snapshot=None,
                )
                sink.append(event)
                alert_events.append(event)
                continue

            if _snapshot_payload_changed(previous_snapshot, current_snapshot):
                event = ApprovedCanaryAlertEvent(
                    emitted_at=emitted_at,
                    alert_type="approved_canary_changed",
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                    current_snapshot=current_snapshot,
                    previous_snapshot=previous_snapshot,
                )
                sink.append(event)
                alert_events.append(event)
            continue

        if previous_snapshot is None:
            continue

        snapshot_age_seconds = (emitted_at - previous_snapshot.captured_at).total_seconds()
        if snapshot_age_seconds <= max_snapshot_age_seconds:
            continue

        event = ApprovedCanaryAlertEvent(
            emitted_at=emitted_at,
            alert_type="approved_canary_stale",
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            current_snapshot=None,
            previous_snapshot=previous_snapshot,
        )
        sink.append(event)
        alert_events.append(event)

    return alert_events


def _emit_stable_launch_ready_alerts(
    *,
    labels: set[str],
    previous_stabilities: dict[str, LaunchReadyCanaryStability | None],
    store: LaunchReadyCanaryStore,
    sink: StableLaunchReadyAlertSink,
    emitted_at: datetime,
    max_snapshot_age_seconds: int,
    min_snapshot_count: int,
    min_stable_seconds: float,
) -> list[StableLaunchReadyAlertEvent]:
    """Emit transition alerts for stable launch-ready availability changes."""

    alert_events: list[StableLaunchReadyAlertEvent] = []
    for label in sorted(labels):
        previous_stability = previous_stabilities.get(label)
        current_stability = _build_latest_launch_ready_stability(
            store=store,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            min_snapshot_count=min_snapshot_count,
            min_stable_seconds=min_stable_seconds,
            now=emitted_at,
        )

        if current_stability is not None:
            if previous_stability is None:
                event = StableLaunchReadyAlertEvent(
                    emitted_at=emitted_at,
                    alert_type="stable_launch_ready_available",
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                    min_snapshot_count=min_snapshot_count,
                    min_stable_seconds=min_stable_seconds,
                    current_stability=current_stability,
                    previous_stability=None,
                )
                sink.append(event)
                alert_events.append(event)
                continue

            if _launch_ready_snapshot_payload_changed(
                previous_stability.snapshot,
                current_stability.snapshot,
            ):
                event = StableLaunchReadyAlertEvent(
                    emitted_at=emitted_at,
                    alert_type="stable_launch_ready_changed",
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                    min_snapshot_count=min_snapshot_count,
                    min_stable_seconds=min_stable_seconds,
                    current_stability=current_stability,
                    previous_stability=previous_stability,
                )
                sink.append(event)
                alert_events.append(event)
            continue

        if previous_stability is None:
            continue

        event = StableLaunchReadyAlertEvent(
            emitted_at=emitted_at,
            alert_type="stable_launch_ready_stale",
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            min_snapshot_count=min_snapshot_count,
            min_stable_seconds=min_stable_seconds,
            current_stability=None,
            previous_stability=previous_stability,
        )
        sink.append(event)
        alert_events.append(event)

    return alert_events


def _snapshot_payload_changed(
    previous_snapshot: ApprovedCanarySnapshot,
    current_snapshot: ApprovedCanarySnapshot,
) -> bool:
    """Return whether the meaningful approved-canary payload changed."""

    previous_payload = previous_snapshot.model_dump(
        mode="python",
        exclude={"snapshot_id", "captured_at"},
    )
    current_payload = current_snapshot.model_dump(
        mode="python",
        exclude={"snapshot_id", "captured_at"},
    )
    return previous_payload != current_payload


def summarize_candidates(
    records: list[OpportunityRecord],
    *,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
) -> CandidateRecordSummary:
    """Count candidate records that satisfy the worker thresholds."""

    candidate_records = len(
        filter_candidate_records(
            records,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
        )
    )

    return CandidateRecordSummary(
        total_records=len(records),
        candidate_records=candidate_records,
    )


async def observe_live_executions_once(
    settings: WorkerSettings,
    *,
    execution_store: ExecutionJournalStore | None = None,
    observation_store: ExecutionObservationStore | None = None,
    approved_store: ApprovedCanaryStore | None = None,
    alert_sink: ExecutionAlertSink | None = None,
    alert_notifier: ExecutionAlertNotifier | None = None,
    balance_service: BalanceAccountingService | None = None,
    account_service: AccountPreflightService | None = None,
    order_state_service: ExecutionOrderStateService | None = None,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> ExecutionObservationSummary:
    """Observe recent live executions once and persist append-only snapshots."""

    journal_store = execution_store or ExecutionJournalStore(settings.database_path)
    history_store = observation_store or ExecutionObservationStore(settings.database_path)
    approved_snapshot_store = approved_store or ApprovedCanaryStore(settings.database_path)
    execution_alert_sink = alert_sink or ExecutionAlertStore(settings.database_path)
    balance_snapshot_service = balance_service or BalanceAccountingService(
        store=BalanceSnapshotStore(settings.database_path)
    )
    account_probe_service = account_service or AccountPreflightService()
    state_service = order_state_service or ExecutionOrderStateService(
        observers=_build_order_state_observers(settings)
    )
    auto_close_approval_service = None
    auto_close_scanner = None
    if (
        settings.execution_auto_pair_close_enabled
        or settings.execution_auto_pair_close_shadow_mode
    ):
        auto_close_approval_service = RouteApprovalService(
            store=RouteApprovalStore(settings.database_path)
        )
        auto_close_scanner = _build_worker_universe_scanner(
            settings,
            history_store=OpportunityHistoryStore(settings.database_path),
        )
    loop_logger = logger or logging.getLogger("carryme.worker")
    timestamp = now or datetime.now(UTC)
    recent_live_executions = _list_recent_live_executions(
        journal_store,
        history_store,
        limit=settings.execution_observation_limit,
        now=timestamp,
        max_age_seconds=settings.execution_observation_max_age_seconds,
        unobserved_requires_monitoring=True,
    )

    scanned_executions = 0
    observed_executions = 0
    saved_observations = 0
    saved_alerts = 0
    sent_notifications = 0
    for execution in recent_live_executions:
        scanned_executions += 1

        try:
            paper_trade_id = execution.paper_trade_id
            assert paper_trade_id is not None
            previous_observation = history_store.latest_for_paper_trade(paper_trade_id)
            account_preflight = await asyncio.wait_for(
                account_probe_service.probe_paper_trade(
                    execution.paper_trade,
                    _build_account_preflight_configs(settings),
                ),
                timeout=OBSERVATION_CALL_TIMEOUT_SECONDS,
            )
            order_state = await asyncio.wait_for(
                state_service.observe_execution(execution),
                timeout=OBSERVATION_CALL_TIMEOUT_SECONDS,
            )
            pair_status = build_execution_pair_status(
                execution,
                order_state,
                reconcile_execution(execution, account_preflight),
            )
            previous_pair_status = (
                previous_observation.pair_status if previous_observation is not None else None
            )
            alert_event: ExecutionAlertEvent | None = None
            if _should_emit_execution_alert(
                pair_status,
                previous_pair_status=previous_pair_status,
            ):
                alert_type = cast(
                    Literal["cleanup_needed", "review_required"],
                    pair_status.derived_state,
                )
                alert_event = ExecutionAlertEvent(
                    emitted_at=timestamp,
                    alert_type=alert_type,
                    paper_trade_id=paper_trade_id,
                    preview_hash=execution.preview_hash,
                    pair_status=pair_status,
                )
            observation_entry = ExecutionObservationEntry(
                observed_at=timestamp,
                context="worker_execution_monitor",
                execution_entry_id=execution.entry_id,
                paper_trade_id=paper_trade_id,
                preview_hash=execution.preview_hash,
                order_state=order_state,
                pair_status=pair_status,
            )
            if isinstance(history_store, ExecutionObservationStore) and isinstance(
                execution_alert_sink,
                ExecutionAlertStore,
            ):
                _, alert_saved = history_store.append_with_alert(
                    observation_entry,
                    alert_store=execution_alert_sink,
                    alert_event=alert_event,
                    previous_pair_status=previous_pair_status,
                )
                saved_alerts += int(alert_saved)
            else:
                alert_saved = False
                if alert_event is not None:
                    alert_saved = execution_alert_sink.append_if_changed(
                        alert_event,
                        previous_pair_status=previous_pair_status,
                    )
                    saved_alerts += int(alert_saved)
                history_store.append(observation_entry)
            if alert_event is not None and alert_saved and alert_notifier is not None:
                try:
                    sent_notifications += await _notify_execution_alert(
                        settings,
                        alert_notifier,
                        alert_event,
                    )
                except Exception:
                    loop_logger.exception(
                        (
                            "execution alert notification failed for paper_trade_id=%s "
                            "preview_hash=%s"
                        ),
                        execution.paper_trade_id,
                        execution.preview_hash,
                    )
        except Exception:
            loop_logger.warning(
                "Failed to observe execution entry_id=%s paper_trade_id=%s",
                execution.entry_id,
                execution.paper_trade_id,
                exc_info=True,
            )
            continue
        observed_executions += 1
        saved_observations += 1
        try:
            _maybe_capture_open_hedge_funding_checkpoint(
                settings=settings,
                execution=execution,
                pair_status=pair_status,
                preflight=account_preflight,
                balance_service=balance_snapshot_service,
                logger=loop_logger,
                now=timestamp,
            )
        except Exception:
            loop_logger.warning(
                "Failed to capture funding checkpoint for execution entry_id=%s paper_trade_id=%s",
                execution.entry_id,
                execution.paper_trade_id,
                exc_info=True,
            )
        try:
            await _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=pair_status,
                approved_store=approved_snapshot_store,
                execution_store=journal_store,
                observation_store=history_store,
                balance_service=balance_snapshot_service,
                account_service=account_probe_service,
                order_state_service=state_service,
                approval_service=auto_close_approval_service,
                scanner=auto_close_scanner,
                logger=loop_logger,
                now=timestamp,
            )
        except Exception:
            loop_logger.warning(
                "Failed to auto-close hedged execution entry_id=%s paper_trade_id=%s",
                execution.entry_id,
                execution.paper_trade_id,
                exc_info=True,
            )

    return ExecutionObservationSummary(
        scanned_executions=scanned_executions,
        observed_executions=observed_executions,
        saved_observations=saved_observations,
        saved_alerts=saved_alerts,
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
    )


async def run_supervised_execution_observation_loop(
    settings: WorkerSettings,
    *,
    execution_store: ExecutionJournalStore | None = None,
    observation_store: ExecutionObservationStore | None = None,
    approved_store: ApprovedCanaryStore | None = None,
    alert_sink: ExecutionAlertSink | None = None,
    alert_notifier: ExecutionAlertNotifier | None = None,
    account_service: AccountPreflightService | None = None,
    order_state_service: ExecutionOrderStateService | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> ExecutionObservationLoopSummary:
    """Run the execution monitor until stopped by signal or max-iteration limit."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    journal_store = execution_store or ExecutionJournalStore(settings.database_path)
    history_store = observation_store or ExecutionObservationStore(settings.database_path)
    approved_snapshot_store = approved_store or ApprovedCanaryStore(settings.database_path)
    execution_alert_sink = alert_sink or ExecutionAlertStore(settings.database_path)
    account_probe_service = account_service or AccountPreflightService()
    state_service = order_state_service or ExecutionOrderStateService(
        observers=_build_order_state_observers(settings)
    )
    loop_logger = logger or logging.getLogger("carryme.worker")
    execution_notifier = alert_notifier
    supervised_stop_event = stop_event or asyncio.Event()
    if execution_notifier is None:
        from carryme_worker.notifications import build_execution_alert_notifier

        execution_notifier = build_execution_alert_notifier(settings, logger=loop_logger)

    attempts = 0
    successful_cycles = 0
    failures = 0
    scanned_executions = 0
    observed_executions = 0
    saved_observations = 0
    saved_alerts = 0
    sent_notifications = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised execution observation cycle %s", attempts)
        try:
            summary = await observe_live_executions_once(
                settings,
                execution_store=journal_store,
                observation_store=history_store,
                approved_store=approved_snapshot_store,
                alert_sink=execution_alert_sink,
                alert_notifier=execution_notifier,
                account_service=account_probe_service,
                order_state_service=state_service,
                logger=loop_logger,
            )
            successful_cycles += 1
            consecutive_failures = 0
            scanned_executions += summary.scanned_executions
            observed_executions += summary.observed_executions
            saved_observations += summary.saved_observations
            saved_alerts += summary.saved_alerts
            sent_notifications += summary.sent_notifications
            loop_logger.info(
                (
                    "completed supervised execution observation cycle %s with "
                    "%s observed executions, %s saved observations, %s saved alerts, "
                    "and %s sent notifications"
                ),
                attempts,
                summary.observed_executions,
                summary.saved_observations,
                summary.saved_alerts,
                summary.sent_notifications,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                settings.execution_observation_interval_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.execution_observation_max_backoff_seconds,
                settings.execution_observation_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "supervised execution observation cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            if supervised_stop_event.is_set():
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )

    return ExecutionObservationLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        scanned_executions=scanned_executions,
        observed_executions=observed_executions,
        saved_observations=saved_observations,
        saved_alerts=saved_alerts,
        sent_notifications=sent_notifications,
        database_path=settings.database_target,
    )


def emit_candidate_alerts(
    records: list[OpportunityRecord],
    *,
    sink: CandidateAlertSink,
    emitted_at: datetime,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
) -> int:
    """Emit one append-only candidate event per selected record."""

    inserted = 0
    for record in records:
        inserted += int(
            sink.append(
                CandidateAlertEvent(
                    emitted_at=emitted_at,
                    min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
                    min_capacity_notional=min_capacity_notional,
                    record=record,
                )
            )
        )
    return inserted


async def run_polling_loop(
    settings: WorkerSettings,
    *,
    iterations: int,
    scorer: PairScorer | None = None,
    store: OpportunityHistoryStore | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
) -> PollLoopSummary:
    """Run the worker for a fixed number of scheduled iterations."""

    if iterations < 1:
        raise ValueError("iterations must be at least 1")

    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")

    successful_cycles = 0
    failures = 0
    consecutive_failures = 0
    saved_records = 0

    for attempt in range(1, iterations + 1):
        loop_logger.info("starting poll cycle %s of %s", attempt, iterations)
        try:
            summary = await poll_watchlist_once(
                settings,
                scorer=runtime,
                store=history_store,
            )
            successful_cycles += 1
            consecutive_failures = 0
            saved_records += summary.saved_records
            loop_logger.info(
                "completed poll cycle %s of %s with %s saved records",
                attempt,
                iterations,
                summary.saved_records,
            )
            if attempt < iterations:
                await sleep(settings.poll_interval_seconds)
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.max_backoff_seconds,
                settings.poll_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "poll cycle %s of %s failed; backing off for %s seconds",
                attempt,
                iterations,
                backoff_seconds,
            )
            if attempt < iterations:
                await sleep(backoff_seconds)

    return PollLoopSummary(
        attempts=iterations,
        successful_cycles=successful_cycles,
        failures=failures,
        saved_records=saved_records,
        database_path=settings.database_target,
    )


async def _sleep_or_stop(
    seconds: float,
    *,
    sleep: Callable[[float], Awaitable[None]],
    stop_event: asyncio.Event | None,
) -> None:
    """Sleep until the next cycle unless a stop signal arrives first."""

    if stop_event is None:
        await sleep(seconds)
        return
    if stop_event.is_set():
        return

    async def _await_sleep() -> None:
        await sleep(seconds)

    async def _await_stop() -> None:
        await stop_event.wait()

    sleep_task: asyncio.Task[None] = asyncio.create_task(_await_sleep())
    stop_task: asyncio.Task[None] = asyncio.create_task(_await_stop())
    done, pending = await asyncio.wait(
        {sleep_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await asyncio.gather(*done, return_exceptions=True)


async def run_supervised_polling_loop(
    settings: WorkerSettings,
    *,
    scorer: PairScorer | None = None,
    store: OpportunityHistoryStore | None = None,
    alert_sink: CandidateAlertSink | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> PollLoopSummary:
    """Run the worker until stopped by signal or max-iteration limit."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    candidate_alert_sink = alert_sink or CandidateAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    supervised_stop_event = stop_event or asyncio.Event()
    attempts = 0
    successful_cycles = 0
    failures = 0
    consecutive_failures = 0
    saved_records = 0
    alert_events = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised poll cycle %s", attempts)
        try:
            summary = await poll_watchlist_once(
                settings,
                scorer=runtime,
                store=history_store,
            )
            successful_cycles += 1
            consecutive_failures = 0
            saved_records += summary.saved_records

            candidate_summary = CandidateRecordSummary(
                total_records=0,
                candidate_records=0,
            )
            inserted_alerts = 0
            if summary.records:
                candidate_records = filter_candidate_records(
                    summary.records,
                    min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
                    min_capacity_notional=settings.min_candidate_capacity_notional,
                )
                candidate_summary = CandidateRecordSummary(
                    total_records=len(summary.records),
                    candidate_records=len(candidate_records),
                )
                if candidate_records:
                    try:
                        inserted_alerts = emit_candidate_alerts(
                            candidate_records,
                            sink=candidate_alert_sink,
                            emitted_at=summary.records[0].recorded_at,
                            min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
                            min_capacity_notional=settings.min_candidate_capacity_notional,
                        )
                    except Exception:
                        loop_logger.exception(
                            "failed to persist candidate alerts for supervised poll cycle %s",
                            attempts,
                        )
            alert_events += inserted_alerts
            loop_logger.info(
                (
                    "completed supervised poll cycle %s with %s saved records, "
                    "%s candidates, and %s alerts"
                ),
                attempts,
                summary.saved_records,
                candidate_summary.candidate_records,
                inserted_alerts,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            await _sleep_or_stop(
                settings.poll_interval_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.max_backoff_seconds,
                settings.poll_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "supervised poll cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )

    return PollLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        saved_records=saved_records,
        alert_events=alert_events,
        database_path=settings.database_target,
    )


def _build_account_preflight_configs(settings: WorkerSettings) -> AccountPreflightConfigMap:
    return build_account_preflight_configs(
        extended_live_enabled=settings.extended_live_enabled,
        extended_api_key=settings.extended_api_key,
        paradex_live_enabled=settings.paradex_live_enabled,
        paradex_account_address=settings.paradex_account_address,
        paradex_private_key=settings.paradex_private_key,
        paradex_bearer_token=settings.paradex_bearer_token,
        hyperliquid_live_enabled=settings.hyperliquid_live_enabled,
        hyperliquid_account_address=settings.hyperliquid_account_address,
        hyperliquid_api_wallet_private_key=settings.hyperliquid_api_wallet_private_key,
    )


def _build_order_state_observers(
    settings: WorkerSettings,
) -> dict[str, ExecutionLegOrderObserver]:
    observers: dict[str, ExecutionLegOrderObserver] = {}
    if settings.extended_live_enabled and settings.extended_api_key:
        observers["extended"] = ExtendedOrderStateObserver(api_key=settings.extended_api_key)
    if (
        settings.paradex_live_enabled
        and settings.paradex_account_address
        and (settings.paradex_private_key or settings.paradex_bearer_token)
    ):
        observers["paradex"] = ParadexOrderStateObserver(
            account_address=settings.paradex_account_address,
            private_key=settings.paradex_private_key,
            bearer_token=settings.paradex_bearer_token,
        )
    if (
        settings.hyperliquid_live_enabled
        and settings.hyperliquid_account_address
        and settings.hyperliquid_api_wallet_private_key
    ):
        observers["hyperliquid"] = HyperliquidOrderStateObserver(
            account_address=settings.hyperliquid_account_address,
            vault_address=settings.hyperliquid_vault_address,
        )
    return observers


async def _notify_execution_alert(
    settings: WorkerSettings,
    alert_notifier: ExecutionAlertNotifier,
    alert_event: ExecutionAlertEvent,
) -> int:
    if isinstance(alert_notifier, CompositeExecutionAlertNotifier):
        return await alert_notifier.notify(alert_event)
    return cast(
        int,
        await _wait_for_notification(
            alert_notifier.notify(alert_event),
            timeout=settings.execution_alert_webhook_timeout_seconds,
        ),
    )


async def _wait_for_notification(
    awaitable: Awaitable[Any],
    *,
    timeout: float,
) -> Any:
    return await asyncio.wait_for(awaitable, timeout=timeout)


def _list_recent_live_executions(
    journal_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    *,
    limit: int,
    now: datetime,
    max_age_seconds: int,
    unobserved_requires_monitoring: bool = False,
) -> list[ExecutionJournalEntry]:
    """Return recent unique live executions after filtering irrelevant journal rows."""

    page_size = max(limit, 20)
    offset = 0
    seen_paper_trade_ids: set[int] = set()
    selected: list[ExecutionJournalEntry] = []

    while len(selected) < limit:
        batch = journal_store.list_recent(limit=page_size, offset=offset)
        if not batch:
            break
        offset += len(batch)

        for execution in batch:
            if execution.mode != "live" or execution.status not in {"submitted", "partial"}:
                continue
            age_seconds = max(0.0, (now - execution.executed_at).total_seconds())
            if age_seconds > max_age_seconds and not _execution_requires_continued_monitoring(
                observation_store,
                execution=execution,
                unobserved_requires_monitoring=unobserved_requires_monitoring,
            ):
                continue
            if execution.paper_trade_id is None or execution.paper_trade_id in seen_paper_trade_ids:
                continue
            seen_paper_trade_ids.add(execution.paper_trade_id)
            selected.append(execution)
            if len(selected) >= limit:
                break

        if len(batch) < page_size:
            break

    return selected


def _execution_requires_continued_monitoring(
    observation_store: ExecutionObservationStore,
    *,
    execution: ExecutionJournalEntry,
    latest_observation: ExecutionObservationEntry | None = None,
    unobserved_requires_monitoring: bool = False,
) -> bool:
    """Return whether an older live execution still has an active monitoring state."""

    paper_trade_id = execution.paper_trade_id
    if paper_trade_id is None:
        return False
    latest = latest_observation
    if latest is None:
        latest = observation_store.latest_for_paper_trade(paper_trade_id)
    if latest is None:
        return unobserved_requires_monitoring
    pair_status = latest.pair_status
    if pair_status is None:
        latest_with_pair_status = observation_store.latest_with_pair_status_for_paper_trade(
            paper_trade_id
        )
        if latest_with_pair_status is None or latest_with_pair_status.pair_status is None:
            return True
        pair_status = latest_with_pair_status.pair_status
    if (
        pair_status.derived_state == "review_required"
        and not review_required_pair_requires_continued_monitoring(pair_status)
    ):
        return False
    return pair_status.derived_state in {
        "hedged",
        "cleanup_needed",
        "review_required",
    } or pair_status.recommended_action != "no_action"


def _should_emit_execution_alert(
    pair_status: ExecutionPairStatus,
    *,
    previous_pair_status: ExecutionPairStatus | None,
) -> bool:
    derived_state = pair_status.derived_state
    if derived_state not in {"cleanup_needed", "review_required"}:
        return False
    if previous_pair_status is None:
        return True
    return (
        previous_pair_status.derived_state != derived_state
        or previous_pair_status.preview_hash != pair_status.preview_hash
    )


def install_signal_handlers(
    stop_event: asyncio.Event,
    *,
    signals_to_handle: tuple[str, ...],
    logger: logging.Logger | None = None,
) -> None:
    """Register signal handlers that request a supervised stop."""

    loop_logger = logger or logging.getLogger("carryme.worker")
    event_loop = asyncio.get_running_loop()
    for signal_name in signals_to_handle:
        sig = getattr(signal, signal_name)
        event_loop.add_signal_handler(
            sig,
            _request_stop,
            stop_event,
            loop_logger,
            signal_name,
        )


def _request_stop(stop_event: asyncio.Event, logger: logging.Logger, signal_name: str) -> None:
    logger.info("received %s, requesting supervised worker shutdown", signal_name)
    stop_event.set()
