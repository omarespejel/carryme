"""Polling helpers for the carryme worker."""

from __future__ import annotations

import asyncio
import logging
import signal
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, Protocol, cast

import httpx
from carryme_models import (
    ApprovedCanarySnapshot,
    CandidateAlertEvent,
    ExecutionAlertEvent,
    ExecutionJournalEntry,
    ExecutionObservationEntry,
    ExecutionPairStatus,
    FundingArbOpportunity,
    FundingUniverseScan,
    OpportunityRecord,
)
from carryme_runtime import (
    AccountPreflightConfigMap,
    AccountPreflightService,
    ConnectorError,
    ExecutionOrderStateService,
    ExecutionQualityService,
    ExtendedOrderStateObserver,
    HyperliquidOrderStateObserver,
    OpportunityService,
    OpportunityUniverseService,
    ParadexOrderStateObserver,
    RouteApprovalService,
    RouteStabilityService,
    build_account_preflight_configs,
    build_execution_pair_status,
    build_opportunity_record_from_universe_opportunity,
    build_pair_spec_from_universe_opportunity,
    filter_candidate_records,
    reconcile_execution,
)
from carryme_runtime.execution_order_state import ExecutionLegOrderObserver
from carryme_storage import (
    ApprovedCanaryStore,
    CandidateAlertStore,
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    OpportunityHistoryStore,
    RouteApprovalStore,
    load_watchlist,
)

from carryme_worker.config import WorkerSettings
from carryme_worker.notifications import CompositeExecutionAlertNotifier, ExecutionAlertNotifier

logger = logging.getLogger(__name__)
OBSERVATION_CALL_TIMEOUT_SECONDS = 10.0


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
    database_path: str
    snapshots: list[ApprovedCanarySnapshot] = field(default_factory=list, repr=False)


@dataclass
class ApprovedCanaryScanLoopSummary:
    """Summary emitted after a supervised approved-canary scan loop."""

    attempts: int
    successful_cycles: int
    failures: int
    scanned_candidates: int
    approved_candidates: int
    saved_snapshots: int
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
        database_path=settings.database_path,
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
    runtime = scanner or OpportunityUniverseService(
        execution_quality_service=ExecutionQualityService(
            journal_store=ExecutionJournalStore(settings.database_path),
            observation_store=ExecutionObservationStore(settings.database_path),
        ),
        route_stability_service=RouteStabilityService(history_store=history_store),
    )

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
        database_path=settings.database_path,
        records=persisted_records,
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


async def scan_approved_canary_once(
    settings: WorkerSettings,
    *,
    scanner: OpportunityUniverseService | None = None,
    approval_service: RouteApprovalService | None = None,
    store: ApprovedCanaryStore | None = None,
    now: datetime | None = None,
) -> ApprovedCanaryScanSummary:
    """Scan the live universe for canaries that are currently operator-approved."""

    timestamp = now or datetime.now(UTC)
    snapshot_store = store or ApprovedCanaryStore(settings.database_path)
    route_approval_service = approval_service or RouteApprovalService(
        store=RouteApprovalStore(settings.database_path)
    )
    runtime = scanner or OpportunityUniverseService(
        execution_quality_service=ExecutionQualityService(
            journal_store=ExecutionJournalStore(settings.database_path),
            observation_store=ExecutionObservationStore(settings.database_path),
        ),
        route_stability_service=RouteStabilityService(
            history_store=OpportunityHistoryStore(settings.database_path)
        ),
    )

    candidates = await runtime.scan_canary_candidates(
        venues=list(settings.approved_canary_scan_venues),
        fee_profile_overrides=_build_approved_canary_fee_profile_overrides(settings),
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
        include_symbols=list(settings.approved_canary_scan_include_symbols) or None,
        exclude_symbols=list(settings.approved_canary_scan_exclude_symbols) or None,
        exclude_tags=list(settings.approved_canary_scan_exclude_tags) or None,
        limit=settings.approved_canary_scan_limit,
    )
    approved_candidates = route_approval_service.filter_approved_canary_candidates(candidates)
    snapshots: list[ApprovedCanarySnapshot] = []
    for candidate in approved_candidates:
        approval = route_approval_service.get_for_candidate(candidate)
        if approval is None or not approval.approved:
            continue
        label = build_pair_spec_from_universe_opportunity(candidate.opportunity).label
        snapshots.append(
            snapshot_store.append(
                ApprovedCanarySnapshot(
                    captured_at=timestamp,
                    label=label or approval.label,
                    candidate=candidate,
                    approval=approval,
                )
            )
        )

    return ApprovedCanaryScanSummary(
        scanned_candidates=len(candidates),
        approved_candidates=len(approved_candidates),
        saved_snapshots=len(snapshots),
        database_path=settings.database_path,
        snapshots=snapshots,
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
        database_path=settings.database_path,
    )


async def run_supervised_approved_canary_scan_loop(
    settings: WorkerSettings,
    *,
    scanner: OpportunityUniverseService | None = None,
    approval_service: RouteApprovalService | None = None,
    store: ApprovedCanaryStore | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> ApprovedCanaryScanLoopSummary:
    """Run supervised approved-canary scans until stopped or capped."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    snapshot_store = store or ApprovedCanaryStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    supervised_stop_event = stop_event or asyncio.Event()

    attempts = 0
    successful_cycles = 0
    failures = 0
    scanned_candidates = 0
    approved_candidates = 0
    saved_snapshots = 0
    consecutive_failures = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised approved canary scan cycle %s", attempts)
        try:
            summary = await scan_approved_canary_once(
                settings,
                scanner=scanner,
                approval_service=approval_service,
                store=snapshot_store,
            )
            successful_cycles += 1
            consecutive_failures = 0
            scanned_candidates += summary.scanned_candidates
            approved_candidates += summary.approved_candidates
            saved_snapshots += summary.saved_snapshots
            loop_logger.info(
                (
                    "completed supervised approved canary scan cycle %s with %s candidates, "
                    "%s approved candidates, and %s saved snapshots"
                ),
                attempts,
                summary.scanned_candidates,
                summary.approved_candidates,
                summary.saved_snapshots,
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
        database_path=settings.database_path,
    )


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
    alert_sink: ExecutionAlertSink | None = None,
    alert_notifier: ExecutionAlertNotifier | None = None,
    account_service: AccountPreflightService | None = None,
    order_state_service: ExecutionOrderStateService | None = None,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> ExecutionObservationSummary:
    """Observe recent live executions once and persist append-only snapshots."""

    journal_store = execution_store or ExecutionJournalStore(settings.database_path)
    history_store = observation_store or ExecutionObservationStore(settings.database_path)
    execution_alert_sink = alert_sink or ExecutionAlertStore(settings.database_path)
    account_probe_service = account_service or AccountPreflightService()
    state_service = order_state_service or ExecutionOrderStateService(
        observers=_build_order_state_observers(settings)
    )
    loop_logger = logger or logging.getLogger("carryme.worker")
    timestamp = now or datetime.now(UTC)
    recent_live_executions = _list_recent_live_executions(
        journal_store,
        limit=settings.execution_observation_limit,
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

    return ExecutionObservationSummary(
        scanned_executions=scanned_executions,
        observed_executions=observed_executions,
        saved_observations=saved_observations,
        saved_alerts=saved_alerts,
        sent_notifications=sent_notifications,
        database_path=settings.database_path,
    )


async def run_supervised_execution_observation_loop(
    settings: WorkerSettings,
    *,
    execution_store: ExecutionJournalStore | None = None,
    observation_store: ExecutionObservationStore | None = None,
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
        database_path=settings.database_path,
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
        database_path=settings.database_path,
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
        database_path=settings.database_path,
        alert_events=alert_events,
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
    return await asyncio.wait_for(
        alert_notifier.notify(alert_event),
        timeout=settings.execution_alert_webhook_timeout_seconds,
    )


def _list_recent_live_executions(
    journal_store: ExecutionJournalStore,
    *,
    limit: int,
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
            if execution.paper_trade_id is None or execution.paper_trade_id in seen_paper_trade_ids:
                continue
            seen_paper_trade_ids.add(execution.paper_trade_id)
            selected.append(execution)
            if len(selected) >= limit:
                break

        if len(batch) < page_size:
            break

    return selected


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
