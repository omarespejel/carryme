"""FastAPI application factory for carryme."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, cast

import httpx
from carryme_models import (
    SUPPORTED_UNIVERSE_VENUES,
    AppDescriptor,
    ApprovedCanaryAlertEvent,
    ApprovedCanaryBasketPlan,
    ApprovedCanarySnapshot,
    AutomationSnapshotSummary,
    CanaryBasketBalanceDelta,
    CanaryBasketLaunchResult,
    CanaryBasketRouteOutcome,
    CanaryLifecycleResult,
    CandidateAlertEvent,
    CleanupPreviewConfirmationEntry,
    DatabaseReadiness,
    ExecutionAlertEvent,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionObservationEntry,
    ExecutionOrderState,
    ExecutionPairClosePreview,
    ExecutionPairStatus,
    ExecutionQualitySummary,
    ExecutionReconciliation,
    FundingArbOpportunity,
    FundingPairTradeIntent,
    FundingUniverseCanaryApprovalProposal,
    FundingUniverseCanaryApprovalProposalSummary,
    FundingUniverseCanaryCandidate,
    FundingUniversePortfolioPlan,
    FundingUniverseScan,
    GuardedPairExecutionResult,
    LaunchReadyCanarySnapshot,
    LaunchReadyCanaryStability,
    LiveSubmissionReadiness,
    OpportunityRecord,
    PairClosePreviewConfirmationEntry,
    PaperTradeAccountingSummary,
    PaperTradeAccountPreflight,
    PaperTradeBalanceAttribution,
    PaperTradeBalanceDelta,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    PaperTradeOrderPreview,
    PaperTradeSystemState,
    PreviewConfirmationEntry,
    ProductionAutomationReadiness,
    RouteAccountingSummary,
    RouteApprovalEntry,
    RouteApprovalUpsert,
    RouteStabilitySummary,
    ServiceHealth,
    ServiceReadiness,
    StableLaunchReadyAlertEvent,
    SystemStateAlertEvent,
    TradingFeeProfile,
    VenueAccountPreflight,
    VenueBalanceSnapshot,
    VenueExecutionPreflight,
    VenueOrderPreview,
    VenueSystemState,
    WatchlistDocument,
)
from carryme_normalizers import list_fee_profiles
from carryme_runtime import (
    AccountPreflightConfigMap,
    AccountPreflightService,
    BalanceAccountingService,
    CleanupLiveExecutionRouter,
    CleanupPreviewRouter,
    ConnectorError,
    ExecutionAccountingService,
    ExecutionAdapter,
    ExecutionOrderStateService,
    ExecutionQualityService,
    ExtendedCleanupPreviewService,
    ExtendedLiveExecutionService,
    ExtendedOrderStateObserver,
    HyperliquidCleanupPreviewService,
    HyperliquidLiveExecutionService,
    HyperliquidOrderStateObserver,
    InvalidTradeCandidateError,
    LaunchReadyAutomationGatePolicy,
    MockExecutionAdapter,
    OpportunityService,
    OpportunityUniverseService,
    OrderPreviewService,
    PairCloseLiveExecutionCoordinator,
    PairClosePreviewService,
    PairedLiveExecutionCoordinator,
    ParadexCleanupPreviewService,
    ParadexLiveExecutionService,
    ParadexOrderStateObserver,
    RouteApprovalService,
    RouteStabilityService,
    SystemStateConfigMap,
    SystemStateService,
    UpstreamDataError,
    build_account_preflight_configs,
    build_approved_snapshot_automation_gate_reason,
    build_candidate_live_execution_preflight,
    build_execution_pair_status,
    build_live_execution_configs,
    build_live_submission_readiness,
    build_opportunity_record_from_universe_opportunity,
    build_pair_spec_from_universe_opportunity,
    build_paper_trade_execution_preflight,
    build_portfolio_plan,
    build_trade_intent,
    build_venue_execution_preflights,
    list_recent_approved_snapshot_chain,
    probe_candidate_system_state,
    reconcile_execution,
    require_confirmed_cleanup_preview,
    review_required_pair_requires_continued_monitoring,
)
from carryme_runtime.execution_order_state import ExecutionLegOrderObserver
from carryme_runtime.pair_close_preview import _pair_close_hash, select_pair_close_preview_venues
from carryme_runtime.route_approvals import (
    scan_exact_canary_candidate_for_approval,
    scan_live_route_candidate_for_approval,
)
from carryme_runtime.universe_policy import passes_symbol_policy
from carryme_storage import (
    ApprovedCanaryAlertStore,
    ApprovedCanaryStore,
    BalanceSnapshotStore,
    CanaryBasketLaunchStore,
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
    WatchlistStore,
)
from carryme_storage.db import (
    DEFAULT_DATABASE_PING_TIMEOUT_SECONDS,
    Database,
    normalize_database_url,
)
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from carryme_api.config import ApiSettings, get_api_settings
from carryme_api.history_view import (
    filter_candidate_records,
    latest_records_by_label,
    rank_history_records,
    render_candidate_dashboard,
    render_dashboard,
)

APP_NAME = "carryme-api"
APP_VERSION = "0.1.0"
DEFAULT_APP_ENVIRONMENT = "development"
APP_ENVIRONMENT_VARIABLE = "CARRYME_API_ENVIRONMENT"
MAX_GUARDED_AUTO_CLEANUP_STEPS = 3
MAX_HISTORY_LIMIT = 1000
AUTOMATION_READINESS_MAX_SNAPSHOT_AGE_SECONDS = 300
AUTOMATION_READINESS_MIN_SNAPSHOT_COUNT = 2
AUTOMATION_READINESS_MIN_STABLE_SECONDS = 30.0
AUTOMATION_READINESS_MAX_ACTIVE_LIVE_EXECUTIONS = 0
AUTOMATION_READINESS_ACTIVE_EXECUTION_SCAN_LIMIT = 200
AUTOMATION_READINESS_MAX_OBSERVATION_AGE_SECONDS = 1800
PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS = 10.0
UNIVERSE_SCAN_TIMEOUT_SECONDS = 20.0
UNIVERSE_SCAN_ACQUIRE_TIMEOUT_SECONDS = 0.25
MUTATING_HTTP_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
DEFAULT_CANARY_EXCLUDE_TAGS = ["meme", "political"]
DEFAULT_HISTORY_SHORTLIST_MAX_AGE_SECONDS = 3600
MAX_HISTORY_SHORTLIST_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
DEFAULT_APPROVAL_PROPOSAL_MAX_AGE_SECONDS = 300
MAX_APPROVAL_PROPOSAL_MAX_AGE_SECONDS = 3600
MIN_AWARE_UTC_DATETIME = datetime.min.replace(tzinfo=UTC)
logger = logging.getLogger(__name__)
_UNIVERSE_SCAN_SEMAPHORE = asyncio.Semaphore(1)


async def _run_bounded_universe_scan[UniverseScanResultT](
    name: str,
    call: Callable[[], Awaitable[UniverseScanResultT]],
) -> UniverseScanResultT:
    """Run one expensive live-universe scan without starving normal API traffic."""

    acquired = False
    try:
        await asyncio.wait_for(
            _UNIVERSE_SCAN_SEMAPHORE.acquire(),
            timeout=UNIVERSE_SCAN_ACQUIRE_TIMEOUT_SECONDS,
        )
        acquired = True
        return await asyncio.wait_for(call(), timeout=UNIVERSE_SCAN_TIMEOUT_SECONDS)
    except TimeoutError as exc:
        if not acquired:
            raise HTTPException(
                status_code=429,
                detail=f"{name} is already running; retry shortly",
            ) from exc
        raise HTTPException(
            status_code=504,
            detail=f"{name} exceeded {UNIVERSE_SCAN_TIMEOUT_SECONDS:g}s timeout",
        ) from exc
    finally:
        if acquired:
            _UNIVERSE_SCAN_SEMAPHORE.release()


def _rank_approved_canary_candidate(
    candidate: FundingUniverseCanaryCandidate,
) -> tuple[float, float, float]:
    """Return the canary ranking tuple used for exact approval selection."""

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


@dataclass(frozen=True)
class _CanaryExecutionServices:
    """Candidate-scoped live execution services."""

    cleanup_preview_service: CleanupPreviewRouter
    pair_close_preview_service: PairClosePreviewService
    cleanup_live_router: CleanupLiveExecutionRouter
    paired_service: PairedLiveExecutionCoordinator
    pair_close_live_service: PairCloseLiveExecutionCoordinator


class ConfirmPreviewRequest(BaseModel):
    """Operator confirmation payload for one unsigned order preview."""

    preview_hash: str = Field(min_length=1)
    slippage_tolerance_bps: int = Field(default=10, ge=0)
    note: str | None = None


class ApprovedCanarySnapshotSummary(BaseModel):
    """Lightweight approved-canary snapshot payload for production operators."""

    snapshot_id: int | None = None
    captured_at: datetime = Field(
        description="ISO-8601 UTC timestamp when the approved canary snapshot was captured.",
    )
    label: str
    canonical_symbol: str
    long_venue: str
    short_venue: str
    long_fee_profile: str
    short_fee_profile: str
    suggested_canary_notional: float
    max_live_notional: float
    one_day_net_edge_after_round_trip: float
    estimated_one_day_pnl_after_round_trip: float | None = None
    deployable_notional: float | None = None
    route_adjusted_quality_score: float | None = None
    execution_adjusted_quality_score: float | None = None


class LaunchReadyCanarySnapshotSummary(ApprovedCanarySnapshotSummary):
    """Lightweight launch-ready snapshot payload for production operators."""

    launch_ready_snapshot_id: int | None = None
    approved_snapshot_id: int | None = None
    max_snapshot_age_seconds: int = Field(
        description=(
            "Maximum age in seconds before the launch-ready snapshot should be treated "
            "as stale."
        ),
    )
    system_state_ready: bool
    system_state_blocking_reasons: list[str] = Field(default_factory=list)


def _summarize_approved_canary_snapshot(
    snapshot: ApprovedCanarySnapshot,
) -> ApprovedCanarySnapshotSummary:
    """Return the operator-safe scalar fields from one approved canary snapshot."""

    opportunity = snapshot.candidate.opportunity
    route = opportunity.opportunity
    return ApprovedCanarySnapshotSummary(
        snapshot_id=snapshot.snapshot_id,
        captured_at=snapshot.captured_at,
        label=snapshot.label,
        canonical_symbol=route.canonical_symbol,
        long_venue=route.long_venue,
        short_venue=route.short_venue,
        long_fee_profile=route.long_fee_profile,
        short_fee_profile=route.short_fee_profile,
        suggested_canary_notional=snapshot.candidate.suggested_canary_notional,
        max_live_notional=snapshot.approval.max_live_notional,
        one_day_net_edge_after_round_trip=route.one_day_net_edge_after_round_trip,
        estimated_one_day_pnl_after_round_trip=(
            opportunity.estimated_one_day_pnl_after_round_trip
        ),
        deployable_notional=opportunity.deployable_notional,
        route_adjusted_quality_score=opportunity.route_adjusted_quality_score,
        execution_adjusted_quality_score=opportunity.execution_adjusted_quality_score,
    )


def _summarize_launch_ready_canary_snapshot(
    snapshot: LaunchReadyCanarySnapshot,
) -> LaunchReadyCanarySnapshotSummary:
    """Return the operator-safe scalar fields from one launch-ready canary snapshot."""

    approved_summary = _summarize_approved_canary_snapshot(snapshot.approved_snapshot)
    return LaunchReadyCanarySnapshotSummary(
        **approved_summary.model_dump(mode="python", exclude={"captured_at"}),
        captured_at=snapshot.captured_at,
        launch_ready_snapshot_id=snapshot.launch_ready_snapshot_id,
        approved_snapshot_id=snapshot.approved_snapshot.snapshot_id,
        max_snapshot_age_seconds=snapshot.max_snapshot_age_seconds,
        system_state_ready=snapshot.system_state.ready,
        system_state_blocking_reasons=snapshot.system_state.blocking_reasons,
    )


def _summarize_approved_canary_snapshot_payload(
    *,
    snapshot_id: int | None,
    snapshot_payload: dict[str, Any],
) -> ApprovedCanarySnapshotSummary:
    """Return a lightweight approved snapshot summary from raw stored JSON."""

    opportunity = cast(dict[str, Any], snapshot_payload["candidate"]["opportunity"])
    route = cast(dict[str, Any], opportunity["opportunity"])
    approval = cast(dict[str, Any], snapshot_payload["approval"])
    candidate = cast(dict[str, Any], snapshot_payload["candidate"])
    return ApprovedCanarySnapshotSummary(
        snapshot_id=snapshot_id,
        captured_at=snapshot_payload["captured_at"],
        label=snapshot_payload["label"],
        canonical_symbol=route["canonical_symbol"],
        long_venue=route["long_venue"],
        short_venue=route["short_venue"],
        long_fee_profile=route["long_fee_profile"],
        short_fee_profile=route["short_fee_profile"],
        suggested_canary_notional=candidate["suggested_canary_notional"],
        max_live_notional=approval["max_live_notional"],
        one_day_net_edge_after_round_trip=route["one_day_net_edge_after_round_trip"],
        estimated_one_day_pnl_after_round_trip=opportunity.get(
            "estimated_one_day_pnl_after_round_trip"
        ),
        deployable_notional=opportunity.get("deployable_notional"),
        route_adjusted_quality_score=opportunity.get("route_adjusted_quality_score"),
        execution_adjusted_quality_score=opportunity.get("execution_adjusted_quality_score"),
    )


def _summarize_launch_ready_canary_snapshot_payload(
    *,
    launch_ready_snapshot_id: int | None,
    snapshot_payload: dict[str, Any],
) -> LaunchReadyCanarySnapshotSummary:
    """Return a lightweight launch-ready snapshot summary from raw stored JSON."""

    approved_payload = cast(dict[str, Any], snapshot_payload["approved_snapshot"])
    approved_summary = _summarize_approved_canary_snapshot_payload(
        snapshot_id=approved_payload.get("snapshot_id"),
        snapshot_payload=approved_payload,
    )
    system_state = cast(dict[str, Any], snapshot_payload["system_state"])
    return LaunchReadyCanarySnapshotSummary(
        **approved_summary.model_dump(mode="python", exclude={"captured_at"}),
        captured_at=snapshot_payload["captured_at"],
        launch_ready_snapshot_id=launch_ready_snapshot_id,
        approved_snapshot_id=approved_payload.get("snapshot_id"),
        max_snapshot_age_seconds=snapshot_payload["max_snapshot_age_seconds"],
        system_state_ready=system_state["ready"],
        system_state_blocking_reasons=system_state.get("blocking_reasons", []),
    )


def _summarize_canary_approval_proposal(
    proposal: FundingUniverseCanaryApprovalProposal,
    *,
    generated_at: datetime,
) -> FundingUniverseCanaryApprovalProposalSummary:
    """Return the operator-safe scalar fields for one approval proposal."""

    return FundingUniverseCanaryApprovalProposalSummary(
        generated_at=generated_at,
        candidate_rank=proposal.candidate_rank,
        label=proposal.label,
        canonical_symbol=proposal.canonical_symbol,
        short_venue=proposal.short_venue,
        long_venue=proposal.long_venue,
        short_fee_profile=proposal.short_fee_profile,
        long_fee_profile=proposal.long_fee_profile,
        approval_status=proposal.approval_status,
        suggested_canary_notional=proposal.candidate.suggested_canary_notional,
        suggested_max_live_notional=proposal.suggested_max_live_notional,
        deployable_notional=proposal.candidate.opportunity.deployable_notional,
        estimated_one_day_pnl_after_round_trip=(
            proposal.candidate.opportunity.estimated_one_day_pnl_after_round_trip
        ),
        route_adjusted_quality_score=proposal.candidate.opportunity.route_adjusted_quality_score,
        pair=proposal.pair,
        approval_payload=proposal.approval_payload,
        existing_approval=proposal.existing_approval,
    )


def _build_approved_route_payload_from_canary_proposal(
    *,
    label: str,
    proposal: FundingUniverseCanaryApprovalProposalSummary,
    current_time: datetime,
    max_proposal_age_seconds: int,
) -> RouteApprovalUpsert:
    """Return a live-approved route payload from a current operator-reviewed proposal."""

    if max_proposal_age_seconds < 1:
        raise HTTPException(
            status_code=400,
            detail="max_proposal_age_seconds must be at least 1",
        )
    if max_proposal_age_seconds > MAX_APPROVAL_PROPOSAL_MAX_AGE_SECONDS:
        raise HTTPException(
            status_code=400,
            detail=(
                "max_proposal_age_seconds must be at most "
                f"{MAX_APPROVAL_PROPOSAL_MAX_AGE_SECONDS}"
            ),
        )
    if proposal.label != label:
        raise HTTPException(
            status_code=400,
            detail="approval proposal label does not match requested route label",
        )
    if proposal.pair.label is not None and proposal.pair.label != proposal.label:
        raise HTTPException(
            status_code=400,
            detail="approval proposal pair label does not match requested route label",
        )

    generated_at = _ensure_aware_utc(proposal.generated_at)
    checked_at = _ensure_aware_utc(current_time)
    age_seconds = (checked_at - generated_at).total_seconds()
    if age_seconds < 0:
        raise HTTPException(
            status_code=409,
            detail="approval proposal was generated in the future",
        )
    if age_seconds > max_proposal_age_seconds:
        raise HTTPException(
            status_code=409,
            detail=(
                "approval proposal is stale "
                f"({age_seconds:.1f}s > {max_proposal_age_seconds}s)"
            ),
        )

    proposal_payload = proposal.approval_payload
    if proposal_payload.approved:
        raise HTTPException(
            status_code=400,
            detail="approval proposal payload must be unapproved before promotion",
        )

    identity_fields = (
        "canonical_symbol",
        "short_venue",
        "long_venue",
        "short_fee_profile",
        "long_fee_profile",
    )
    for field in identity_fields:
        if getattr(proposal, field) == getattr(proposal_payload, field):
            continue
        raise HTTPException(
            status_code=400,
            detail=f"approval proposal payload does not match summary field {field}",
        )

    if (
        proposal_payload.max_live_notional
        - proposal.suggested_max_live_notional
        > 1e-9
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "approval proposal payload max_live_notional exceeds the suggested "
                "proposal cap"
            ),
        )

    existing_note = (proposal_payload.note or "").strip()
    promotion_note = (
        "Approved from current canary approval proposal "
        f"generated_at={generated_at.isoformat()} "
        f"rank={proposal.candidate_rank} status={proposal.approval_status}."
    )
    note = f"{existing_note} {promotion_note}".strip()
    return proposal_payload.model_copy(
        update={
            "approved": True,
            "note": note,
        }
    )


async def _scan_current_approved_canary_snapshot_for_promoted_proposal(
    *,
    universe_service: OpportunityUniverseService,
    approval: RouteApprovalEntry,
    now: datetime,
    target_notional: float,
    canary_max_notional: float,
    min_capacity_notional: float,
    min_daily_volume: float,
    min_open_interest: float,
    min_roundtrip_edge: float,
    min_execution_quality_score: float,
    min_execution_samples: int,
    min_route_stability_weight: float,
    min_route_presence_ratio: float,
    min_route_samples: int,
    exclude_tags: list[str] | None,
) -> ApprovedCanarySnapshot:
    """Return a fresh approved canary snapshot for a just-promoted route."""

    _validate_route_stability_filters(
        min_route_stability_weight=min_route_stability_weight,
        min_route_presence_ratio=min_route_presence_ratio,
        min_route_samples=min_route_samples,
    )
    candidate, _ = await _run_bounded_universe_scan(
        "canary approval proposal promotion refresh",
        lambda: scan_live_route_candidate_for_approval(
            scanner=universe_service,
            approval=approval,
            venues=[approval.short_venue, approval.long_venue],
            fee_profile_overrides=None,
            target_notional=target_notional,
            canary_max_notional=canary_max_notional,
            min_capacity_notional=min_capacity_notional,
            min_daily_volume=min_daily_volume,
            min_open_interest=min_open_interest,
            min_roundtrip_edge=min_roundtrip_edge,
            min_execution_quality_score=min_execution_quality_score,
            min_execution_samples=min_execution_samples,
            min_route_stability_weight=min_route_stability_weight,
            min_route_presence_ratio=min_route_presence_ratio,
            min_route_samples=min_route_samples,
            include_symbols=None,
            exclude_symbols=None,
            exclude_tags=exclude_tags,
            limit=1,
        ),
    )
    if candidate is None:
        raise HTTPException(
            status_code=409,
            detail="approved proposal no longer matches a current live canary route",
        )

    capped_notional = min(candidate.suggested_canary_notional, approval.max_live_notional)
    if capped_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail="approved proposal no longer permits a positive live notional",
        )
    return ApprovedCanarySnapshot(
        captured_at=now,
        label=approval.label,
        candidate=candidate.model_copy(
            update={"suggested_canary_notional": capped_notional}
        ),
        approval=approval,
    )


async def _generate_canary_approval_proposal_summaries(
    *,
    universe_service: OpportunityUniverseService,
    approval_service: RouteApprovalService,
    history_store: OpportunityHistoryStore,
    current_time: datetime,
    venues: list[str] | None,
    extended_fee_profile: str | None,
    paradex_fee_profile: str | None,
    hyperliquid_fee_profile: str | None,
    target_notional: float,
    canary_max_notional: float,
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
    candidate_sample: int,
    use_history_shortlist: bool,
    history_shortlist_sample: int,
    history_shortlist_limit: int,
    history_shortlist_max_age_seconds: int,
) -> list[FundingUniverseCanaryApprovalProposalSummary]:
    """Return current canary approval proposal summaries using production scan gates."""

    limit = _validated_history_limit("limit", limit)
    candidate_sample = max(
        limit,
        _validated_history_limit("candidate_sample", candidate_sample),
    )
    history_shortlist_sample = _validated_history_limit(
        "history_shortlist_sample",
        history_shortlist_sample,
    )
    history_shortlist_limit = max(
        limit,
        _validated_history_limit("history_shortlist_limit", history_shortlist_limit),
    )
    history_shortlist_sample = max(
        history_shortlist_sample,
        history_shortlist_limit,
    )
    if history_shortlist_max_age_seconds < 1:
        raise HTTPException(
            status_code=400,
            detail="history_shortlist_max_age_seconds must be at least 1",
        )
    if history_shortlist_max_age_seconds > MAX_HISTORY_SHORTLIST_MAX_AGE_SECONDS:
        raise HTTPException(
            status_code=400,
            detail=(
                "history_shortlist_max_age_seconds must be at most "
                f"{MAX_HISTORY_SHORTLIST_MAX_AGE_SECONDS}"
            ),
        )
    selected_venues = venues or list(SUPPORTED_UNIVERSE_VENUES)
    effective_exclude_tags = DEFAULT_CANARY_EXCLUDE_TAGS if exclude_tags is None else exclude_tags
    effective_include_symbols = include_symbols
    used_history_shortlist = False
    if use_history_shortlist and include_symbols is None:
        try:
            shortlisted_symbols = await asyncio.to_thread(
                _select_canary_reprice_symbols_from_history,
                history_store,
                sample=history_shortlist_sample,
                limit=history_shortlist_limit,
                now=current_time,
                max_age_seconds=history_shortlist_max_age_seconds,
                venues=selected_venues,
                exclude_symbols=exclude_symbols,
                exclude_tags=effective_exclude_tags,
                min_roundtrip_edge=min_roundtrip_edge,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
            )
        except Exception:
            logger.exception("failed to build history shortlist for canary approval proposals")
            shortlisted_symbols = []
        if shortlisted_symbols:
            effective_include_symbols = shortlisted_symbols
            used_history_shortlist = True

    async def _scan_candidates(
        include_symbols_override: list[str] | None,
        scan_limit: int,
    ) -> list[FundingUniverseCanaryCandidate]:
        return await _run_bounded_universe_scan(
            "funding universe canary approval proposal scan",
            lambda: universe_service.scan_canary_candidates(
                venues=selected_venues,
                fee_profile_overrides=_build_fee_profile_overrides(
                    extended_fee_profile=extended_fee_profile,
                    paradex_fee_profile=paradex_fee_profile,
                    hyperliquid_fee_profile=hyperliquid_fee_profile,
                    selected_venues=selected_venues,
                ),
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols_override,
                exclude_symbols=exclude_symbols,
                exclude_tags=effective_exclude_tags,
                limit=scan_limit,
            ),
        )

    candidates = await _scan_candidates(effective_include_symbols, candidate_sample)
    if used_history_shortlist and len(candidates) < limit:
        broad_candidates = await _scan_candidates(None, limit)
        candidates = _merge_canary_candidates(
            candidates,
            broad_candidates,
            limit=limit,
        )
    proposals = approval_service.propose_canary_route_approvals(
        candidates,
        limit=limit,
    )
    return [
        _summarize_canary_approval_proposal(
            proposal,
            generated_at=current_time,
        )
        for proposal in proposals
    ]


def get_app_environment() -> str:
    """Return the runtime environment exposed by the API health endpoints."""

    return (
        os.getenv(APP_ENVIRONMENT_VARIABLE, DEFAULT_APP_ENVIRONMENT).strip()
        or DEFAULT_APP_ENVIRONMENT
    )


def get_automation_readiness_checked_at() -> datetime:
    """Return the authoritative UTC evaluation time for automation readiness."""

    return datetime.now(UTC)


def get_current_utc_time() -> datetime:
    """Return the current UTC time through a dependency for deterministic tests."""

    return datetime.now(UTC)


async def _resolve_api_settings_for_app(app: FastAPI) -> ApiSettings:
    """Resolve API settings while honoring FastAPI dependency overrides."""

    resolver = app.dependency_overrides.get(get_api_settings) or get_api_settings
    settings = resolver()
    if inspect.isawaitable(settings):
        settings = await cast(Awaitable[ApiSettings], settings)
    return cast(ApiSettings, settings)


async def _resolve_api_settings_for_request(request: Request) -> ApiSettings:
    """Resolve API settings while honoring FastAPI dependency overrides in tests."""

    return await _resolve_api_settings_for_app(request.app)


def _should_prewarm_execution_journal(settings: ApiSettings) -> bool:
    """Return whether startup should prewarm the execution journal store."""

    def _normalized_database_target(database: str) -> str:
        normalized = normalize_database_url(database.strip())
        sqlite_prefix = "sqlite:///"
        if normalized == f"{sqlite_prefix}:memory:":
            return normalized
        if normalized.startswith(sqlite_prefix):
            return f"{sqlite_prefix}{Path(normalized.removeprefix(sqlite_prefix)).resolve()}"
        return normalized

    return not (
        settings.environment == "development"
        and _normalized_database_target(settings.database_path)
        == _normalized_database_target("data/carryme.sqlite3")
    )


def _operator_auth_error(status_code: int, detail: str) -> JSONResponse:
    headers = {"WWW-Authenticate": "Bearer"} if status_code == 401 else None
    return JSONResponse(status_code=status_code, content={"detail": detail}, headers=headers)


@lru_cache
def _history_store_for_path(database_path: str) -> OpportunityHistoryStore:
    """Return a shared store wrapper for the configured SQLite path."""

    return OpportunityHistoryStore(database_path)


@lru_cache
def _candidate_alert_store_for_path(database_path: str) -> CandidateAlertStore:
    """Return a shared candidate alert store for the configured SQLite path."""

    return CandidateAlertStore(database_path)


@lru_cache
def _watchlist_store_for_path(watchlist_path: str) -> WatchlistStore:
    """Return a shared watchlist store for the configured watchlist path."""

    return WatchlistStore(watchlist_path)


@lru_cache
def _paper_trade_store_for_path(database_path: str) -> PaperTradeStore:
    """Return a shared paper trade journal wrapper for the configured SQLite path."""

    return PaperTradeStore(database_path)


@lru_cache
def _execution_journal_store_for_path(database_path: str) -> ExecutionJournalStore:
    """Return a shared execution journal store for the configured SQLite path."""

    return ExecutionJournalStore(database_path)


@lru_cache
def _preview_confirmation_store_for_path(database_path: str) -> PreviewConfirmationStore:
    """Return a shared preview confirmation store for the configured SQLite path."""

    return PreviewConfirmationStore(database_path)


@lru_cache
def _cleanup_preview_confirmation_store_for_path(
    database_path: str,
) -> CleanupPreviewConfirmationStore:
    """Return a shared cleanup preview confirmation store for the configured SQLite path."""

    return CleanupPreviewConfirmationStore(database_path)


@lru_cache
def _pair_close_preview_confirmation_store_for_path(
    database_path: str,
) -> PairClosePreviewConfirmationStore:
    """Return a shared pair-close preview confirmation store for the configured SQLite path."""

    return PairClosePreviewConfirmationStore(database_path)


@lru_cache
def _execution_observation_store_for_path(database_path: str) -> ExecutionObservationStore:
    """Return a shared execution observation store for the configured SQLite path."""

    return ExecutionObservationStore(database_path)


def _build_fee_profile_overrides(
    *,
    extended_fee_profile: str | None,
    paradex_fee_profile: str | None,
    hyperliquid_fee_profile: str | None,
    selected_venues: list[str] | None = None,
) -> dict[str, str] | None:
    configured_profiles = {
        "extended": extended_fee_profile,
        "paradex": paradex_fee_profile,
        "hyperliquid": hyperliquid_fee_profile,
    }
    normalized_selected_venues = (
        {venue.strip().lower() for venue in selected_venues if venue.strip()}
        if selected_venues is not None
        else set(SUPPORTED_UNIVERSE_VENUES)
    )
    overrides = {
        venue: profile
        for venue in SUPPORTED_UNIVERSE_VENUES
        if venue in normalized_selected_venues and (profile := configured_profiles.get(venue))
    }
    return overrides or None


def _require_live_route_approval(
    *,
    paper_trade: PaperTradeEntry,
    approval_service: RouteApprovalService,
) -> RouteApprovalEntry:
    try:
        return approval_service.require_live_approval(paper_trade.intent)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def get_opportunity_service() -> OpportunityService:
    """Return the live opportunity scoring service."""

    return OpportunityService()


@lru_cache
def _execution_quality_service_for_path(database_path: str) -> ExecutionQualityService:
    """Return the execution-quality summary service."""

    return ExecutionQualityService(
        journal_store=_execution_journal_store_for_path(database_path),
        observation_store=_execution_observation_store_for_path(database_path),
    )


def get_execution_quality_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExecutionQualityService:
    """Return the shared execution-quality summary service."""

    return _execution_quality_service_for_path(settings.database_path)


@lru_cache
def _opportunity_universe_service_for_path(database_path: str) -> OpportunityUniverseService:
    """Return the live funding-universe discovery and ranking service."""

    return OpportunityUniverseService(
        execution_quality_service=_execution_quality_service_for_path(database_path),
        route_stability_service=RouteStabilityService(
            history_store=_history_store_for_path(database_path)
        ),
    )


def get_opportunity_universe_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> OpportunityUniverseService:
    """Return the shared live funding-universe discovery and ranking service."""

    return _opportunity_universe_service_for_path(settings.database_path)


def get_history_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> OpportunityHistoryStore:
    """Return the shared opportunity history store."""

    return _history_store_for_path(settings.database_path)


@lru_cache
def _route_stability_service_for_path(database_path: str) -> RouteStabilityService:
    """Return the shared route-stability service for the configured SQLite path."""

    return RouteStabilityService(history_store=_history_store_for_path(database_path))


def get_approved_canary_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ApprovedCanaryStore:
    """Return the shared approved-canary snapshot store."""

    return ApprovedCanaryStore(settings.database_path)


def get_approved_canary_alert_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ApprovedCanaryAlertStore:
    """Return the shared approved-canary alert store."""

    return ApprovedCanaryAlertStore(settings.database_path)


def get_launch_ready_canary_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> LaunchReadyCanaryStore:
    """Return the shared launch-ready canary snapshot store."""

    return LaunchReadyCanaryStore(settings.database_path)


def get_canary_basket_launch_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> CanaryBasketLaunchStore:
    """Return the shared canary basket launch store."""

    return CanaryBasketLaunchStore(settings.database_path)


def get_stable_launch_ready_alert_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> StableLaunchReadyAlertStore:
    """Return the shared stable launch-ready alert store."""

    return StableLaunchReadyAlertStore(settings.database_path)


def get_stable_canary_launch_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> StableCanaryLaunchStore:
    """Return the shared stable canary launch record store."""

    return StableCanaryLaunchStore(settings.database_path)


def get_route_stability_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> RouteStabilityService:
    """Return the repeated-scan route-stability service."""

    return _route_stability_service_for_path(settings.database_path)


def _validate_route_stability_filters(
    *,
    min_route_stability_weight: float,
    min_route_presence_ratio: float,
    min_route_samples: int,
) -> None:
    """Validate shared route-stability filter inputs."""

    if not 0.0 <= min_route_stability_weight <= 1.0:
        raise HTTPException(
            status_code=400,
            detail="min_route_stability_weight must be between 0 and 1",
        )
    if not 0.0 <= min_route_presence_ratio <= 1.0:
        raise HTTPException(
            status_code=400,
            detail="min_route_presence_ratio must be between 0 and 1",
        )
    if min_route_samples < 0:
        raise HTTPException(
            status_code=400,
            detail="min_route_samples must be non-negative",
        )


def get_system_state_service() -> SystemStateService:
    """Return the live venue system-state service."""

    return SystemStateService()


def get_candidate_alert_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> CandidateAlertStore:
    """Return the shared candidate alert store."""

    return _candidate_alert_store_for_path(settings.database_path)


@lru_cache
def _execution_alert_store_for_path(database_path: str) -> ExecutionAlertStore:
    """Return the shared execution alert store for the configured SQLite path."""

    return ExecutionAlertStore(database_path)


def get_execution_alert_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExecutionAlertStore:
    """Return the shared execution alert store."""

    return _execution_alert_store_for_path(settings.database_path)


def get_system_state_alert_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> SystemStateAlertStore:
    """Return the shared system-state alert store."""

    return SystemStateAlertStore(settings.database_path)


def get_execution_observation_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExecutionObservationStore:
    """Return the shared execution observation store."""

    return _execution_observation_store_for_path(settings.database_path)


def get_watchlist_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> WatchlistStore:
    """Return the shared watchlist store."""

    return _watchlist_store_for_path(settings.watchlist_path)


def get_paper_trade_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> PaperTradeStore:
    """Return the shared paper trade journal store."""

    return _paper_trade_store_for_path(settings.database_path)


def _validated_history_limit(name: str, value: int) -> int:
    """Validate bounded positive query parameters for history views."""

    if value < 1:
        raise HTTPException(status_code=400, detail=f"{name} must be at least 1")
    if value > MAX_HISTORY_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be at most {MAX_HISTORY_LIMIT}",
        )
    return value


def _validated_list_limit(name: str, value: int) -> int:
    """Validate bounded list query parameters that may intentionally request zero rows."""

    if value < 0:
        raise HTTPException(status_code=400, detail=f"{name} must be non-negative")
    if value > MAX_HISTORY_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be at most {MAX_HISTORY_LIMIT}",
        )
    return value


def _validated_non_negative_threshold(name: str, value: float) -> float:
    """Validate bounded numeric query parameters for candidate filters."""

    if not math.isfinite(value):
        raise HTTPException(status_code=400, detail=f"{name} must be finite")
    if value < 0:
        raise HTTPException(status_code=400, detail=f"{name} must be non-negative")
    return value


def _validated_positive_threshold(name: str, value: float) -> float:
    """Validate finite strictly positive query parameters."""

    if not math.isfinite(value):
        raise HTTPException(status_code=400, detail=f"{name} must be finite")
    if value <= 0:
        raise HTTPException(status_code=400, detail=f"{name} must be greater than zero")
    return value


def _validated_fraction(name: str, value: float) -> float:
    """Validate finite fractions constrained to the inclusive unit interval."""

    if not math.isfinite(value):
        raise HTTPException(status_code=400, detail=f"{name} must be finite")
    if value <= 0 or value > 1:
        raise HTTPException(status_code=400, detail=f"{name} must be within (0, 1]")
    return value


def get_execution_journal_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExecutionJournalStore:
    """Return the shared execution journal store."""

    return _execution_journal_store_for_path(settings.database_path)


def get_preview_confirmation_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> PreviewConfirmationStore:
    """Return the shared preview confirmation store."""

    return _preview_confirmation_store_for_path(settings.database_path)


def get_cleanup_preview_confirmation_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> CleanupPreviewConfirmationStore:
    """Return the shared cleanup preview confirmation store."""

    return _cleanup_preview_confirmation_store_for_path(settings.database_path)


def get_pair_close_preview_confirmation_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> PairClosePreviewConfirmationStore:
    """Return the shared pair-close preview confirmation store."""

    return _pair_close_preview_confirmation_store_for_path(settings.database_path)


# TODO: wire this provider to the future live execution adapter surface.
def get_execution_adapter() -> ExecutionAdapter:
    """Return the default explicitly simulated execution adapter."""

    return MockExecutionAdapter()


def get_execution_accounting_service(
    store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
    observation_store: Annotated[
        ExecutionObservationStore,
        Depends(get_execution_observation_store),
    ],
) -> ExecutionAccountingService:
    """Return the derived execution accounting service."""

    return ExecutionAccountingService(
        journal_store=store,
        observation_store=observation_store,
    )


def get_route_approval_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> RouteApprovalStore:
    """Return the shared route approval store."""

    return RouteApprovalStore(settings.database_path)


def get_route_approval_service(
    store: Annotated[RouteApprovalStore, Depends(get_route_approval_store)],
) -> RouteApprovalService:
    """Return the route approval service."""

    return RouteApprovalService(store=store)


def get_balance_snapshot_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> BalanceSnapshotStore:
    """Return the shared balance snapshot store."""

    return BalanceSnapshotStore(settings.database_path)


def get_balance_accounting_service(
    store: Annotated[BalanceSnapshotStore, Depends(get_balance_snapshot_store)],
) -> BalanceAccountingService:
    """Return the balance accounting service."""

    return BalanceAccountingService(store=store)


def get_paradex_live_execution_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ParadexLiveExecutionService:
    """Return the live Paradex execution service for manual submissions."""

    return ParadexLiveExecutionService(
        account_address=settings.paradex_account_address or "",
        private_key=settings.paradex_private_key or "",
        recv_window_ms=settings.paradex_recv_window_ms,
    )


def get_extended_live_execution_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExtendedLiveExecutionService:
    """Return the live Extended execution service for manual submissions."""

    api_key = settings.extended_api_key
    stark_private_key = settings.extended_stark_private_key
    if not api_key or not stark_private_key:
        raise HTTPException(
            status_code=500,
            detail=(
                "Extended live execution requires "
                "CARRYME_API_EXTENDED_API_KEY and "
                "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY"
            ),
        )
    return ExtendedLiveExecutionService(
        api_key=api_key,
        stark_private_key=stark_private_key,
    )


def get_hyperliquid_live_execution_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> HyperliquidLiveExecutionService:
    """Return the live Hyperliquid execution service for manual submissions."""

    return HyperliquidLiveExecutionService(
        account_address=settings.hyperliquid_account_address or "",
        vault_address=settings.hyperliquid_vault_address,
        api_wallet_private_key=settings.hyperliquid_api_wallet_private_key or "",
    )


def _normalize_unique_venues(venues: list[str]) -> list[str]:
    """Return normalized venues in stable order without duplicates."""

    selected: list[str] = []
    for venue in venues:
        normalized = venue.strip().lower()
        if normalized and normalized not in selected:
            selected.append(normalized)
    return selected


def _candidate_venues(candidate: FundingUniverseCanaryCandidate) -> list[str]:
    """Return the unique venues touched by one canary candidate."""

    opportunity = candidate.opportunity.opportunity
    return _normalize_unique_venues([opportunity.long_venue, opportunity.short_venue])


def _build_cleanup_preview_services_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> dict[str, Any]:
    """Build only the cleanup-preview services required by one canary route."""

    services: dict[str, Any] = {}
    for venue in _candidate_venues(candidate):
        if venue == "extended":
            services[venue] = ExtendedCleanupPreviewService(api_key=settings.extended_api_key or "")
        elif venue == "hyperliquid":
            services[venue] = HyperliquidCleanupPreviewService(
                account_address=settings.hyperliquid_account_address or "",
                vault_address=settings.hyperliquid_vault_address,
            )
        elif venue == "paradex":
            services[venue] = ParadexCleanupPreviewService(
                account_address=settings.paradex_account_address or "",
                private_key=settings.paradex_private_key,
                bearer_token=settings.paradex_bearer_token,
            )
        else:
            raise ValueError(f"Unsupported canary venue {venue!r}")
    return services


def _build_cleanup_preview_router_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> CleanupPreviewRouter:
    """Build a venue-filtered cleanup preview router for one canary route."""

    return CleanupPreviewRouter(
        services=_build_cleanup_preview_services_for_candidate(settings, candidate)
    )


def _build_pair_close_preview_service_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> PairClosePreviewService:
    """Build a venue-filtered pair-close preview service for one canary route."""

    return PairClosePreviewService(
        services=_build_cleanup_preview_services_for_candidate(settings, candidate)
    )


def _build_live_execution_services_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
    *,
    live_service_resolver: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Build only the live execution services required by one canary route."""

    services: dict[str, Any] = {}
    for venue in _candidate_venues(candidate):
        if live_service_resolver is not None:
            services[venue] = live_service_resolver(venue)
            continue
        if venue == "extended":
            services[venue] = get_extended_live_execution_service(settings)
        elif venue == "hyperliquid":
            services[venue] = get_hyperliquid_live_execution_service(settings)
        elif venue == "paradex":
            services[venue] = get_paradex_live_execution_service(settings)
        else:
            raise ValueError(f"Unsupported canary venue {venue!r}")
    return services


def _build_cleanup_live_execution_router_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
    *,
    live_service_resolver: Callable[[str], Any] | None = None,
) -> CleanupLiveExecutionRouter:
    """Build a venue-filtered cleanup live router for one canary route."""

    return CleanupLiveExecutionRouter(
        services=_build_live_execution_services_for_candidate(
            settings,
            candidate,
            live_service_resolver=live_service_resolver,
        )
    )


def _build_paired_live_execution_coordinator_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
    *,
    live_service_resolver: Callable[[str], Any] | None = None,
) -> PairedLiveExecutionCoordinator:
    """Build a venue-filtered paired live coordinator for one canary route."""

    return PairedLiveExecutionCoordinator(
        services=_build_live_execution_services_for_candidate(
            settings,
            candidate,
            live_service_resolver=live_service_resolver,
        )
    )


def _build_pair_close_live_execution_coordinator_for_candidate(
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
    *,
    live_service_resolver: Callable[[str], Any] | None = None,
) -> PairCloseLiveExecutionCoordinator:
    """Build a venue-filtered pair-close live coordinator for one canary route."""

    return PairCloseLiveExecutionCoordinator(
        services=_build_live_execution_services_for_candidate(
            settings,
            candidate,
            live_service_resolver=live_service_resolver,
        )
    )


def _resolve_request_dependency(
    request: Request,
    getter: object,
    builder: Callable[[], Any],
) -> Any:
    """Resolve one dependency override lazily without invoking unrelated builders."""

    override = request.app.dependency_overrides.get(getter)
    if override is not None:
        return override()
    return builder()


def _resolve_canary_execution_services_for_candidate(
    *,
    request: Request,
    settings: ApiSettings,
    candidate: FundingUniverseCanaryCandidate,
) -> _CanaryExecutionServices:
    """Resolve candidate-scoped execution services with request overrides applied."""

    cleanup_preview_service = _resolve_request_dependency(
        request,
        get_cleanup_preview_service,
        lambda: _build_cleanup_preview_router_for_candidate(settings, candidate),
    )
    pair_close_preview_service = _resolve_request_dependency(
        request,
        get_pair_close_preview_service,
        lambda: _build_pair_close_preview_service_for_candidate(settings, candidate),
    )

    def _resolve_live_execution_service(venue: str) -> Any:
        if venue == "extended":
            return _resolve_request_dependency(
                request,
                get_extended_live_execution_service,
                lambda: get_extended_live_execution_service(settings),
            )
        if venue == "hyperliquid":
            return _resolve_request_dependency(
                request,
                get_hyperliquid_live_execution_service,
                lambda: get_hyperliquid_live_execution_service(settings),
            )
        if venue == "paradex":
            return _resolve_request_dependency(
                request,
                get_paradex_live_execution_service,
                lambda: get_paradex_live_execution_service(settings),
            )
        raise ValueError(f"Unsupported canary venue {venue!r}")

    cleanup_live_router = _resolve_request_dependency(
        request,
        get_cleanup_live_execution_router,
        lambda: _build_cleanup_live_execution_router_for_candidate(
            settings,
            candidate,
            live_service_resolver=_resolve_live_execution_service,
        ),
    )
    paired_service = _resolve_request_dependency(
        request,
        get_paired_live_execution_coordinator,
        lambda: _build_paired_live_execution_coordinator_for_candidate(
            settings,
            candidate,
            live_service_resolver=_resolve_live_execution_service,
        ),
    )
    pair_close_live_service = _resolve_request_dependency(
        request,
        get_pair_close_live_execution_coordinator,
        lambda: _build_pair_close_live_execution_coordinator_for_candidate(
            settings,
            candidate,
            live_service_resolver=_resolve_live_execution_service,
        ),
    )
    return _CanaryExecutionServices(
        cleanup_preview_service=cleanup_preview_service,
        pair_close_preview_service=pair_close_preview_service,
        cleanup_live_router=cleanup_live_router,
        paired_service=paired_service,
        pair_close_live_service=pair_close_live_service,
    )


def get_cleanup_preview_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> CleanupPreviewRouter:
    """Return the cleanup-preview router for all supported live venues."""

    return CleanupPreviewRouter(
        services={
            "extended": ExtendedCleanupPreviewService(api_key=settings.extended_api_key or ""),
            "hyperliquid": HyperliquidCleanupPreviewService(
                account_address=settings.hyperliquid_account_address or "",
                vault_address=settings.hyperliquid_vault_address,
            ),
            "paradex": ParadexCleanupPreviewService(
                account_address=settings.paradex_account_address or "",
                private_key=settings.paradex_private_key,
                bearer_token=settings.paradex_bearer_token,
            ),
        }
    )


def get_pair_close_preview_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> PairClosePreviewService:
    """Return the pair-close preview service for all supported live venues."""

    return PairClosePreviewService(
        services={
            "extended": ExtendedCleanupPreviewService(api_key=settings.extended_api_key or ""),
            "hyperliquid": HyperliquidCleanupPreviewService(
                account_address=settings.hyperliquid_account_address or "",
                vault_address=settings.hyperliquid_vault_address,
            ),
            "paradex": ParadexCleanupPreviewService(
                account_address=settings.paradex_account_address or "",
                private_key=settings.paradex_private_key,
                bearer_token=settings.paradex_bearer_token,
            ),
        }
    )


def get_execution_order_state_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExecutionOrderStateService:
    """Return the venue order-state observation service."""

    observers: dict[str, ExecutionLegOrderObserver] = {}
    if settings.extended_api_key:
        observers["extended"] = ExtendedOrderStateObserver(
            api_key=settings.extended_api_key,
        )
    if settings.paradex_account_address and (
        settings.paradex_private_key or settings.paradex_bearer_token
    ):
        observers["paradex"] = ParadexOrderStateObserver(
            account_address=settings.paradex_account_address,
            private_key=settings.paradex_private_key,
            bearer_token=settings.paradex_bearer_token,
        )
    if settings.hyperliquid_account_address and settings.hyperliquid_api_wallet_private_key:
        observers["hyperliquid"] = HyperliquidOrderStateObserver(
            account_address=settings.hyperliquid_account_address,
            vault_address=settings.hyperliquid_vault_address,
        )
    return ExecutionOrderStateService(observers=observers)


def get_paired_live_execution_coordinator(
    extended_service: Annotated[
        ExtendedLiveExecutionService,
        Depends(get_extended_live_execution_service),
    ],
    hyperliquid_service: Annotated[
        HyperliquidLiveExecutionService,
        Depends(get_hyperliquid_live_execution_service),
    ],
    paradex_service: Annotated[
        ParadexLiveExecutionService,
        Depends(get_paradex_live_execution_service),
    ],
) -> PairedLiveExecutionCoordinator:
    """Return the paired manual live execution coordinator."""

    return PairedLiveExecutionCoordinator(
        services={
            "extended": extended_service,
            "hyperliquid": hyperliquid_service,
            "paradex": paradex_service,
        }
    )


def get_cleanup_live_execution_router(
    extended_service: Annotated[
        ExtendedLiveExecutionService,
        Depends(get_extended_live_execution_service),
    ],
    hyperliquid_service: Annotated[
        HyperliquidLiveExecutionService,
        Depends(get_hyperliquid_live_execution_service),
    ],
    paradex_service: Annotated[
        ParadexLiveExecutionService,
        Depends(get_paradex_live_execution_service),
    ],
) -> CleanupLiveExecutionRouter:
    """Return the cleanup live execution router for supported venues."""

    return CleanupLiveExecutionRouter(
        services={
            "extended": extended_service,
            "hyperliquid": hyperliquid_service,
            "paradex": paradex_service,
        }
    )


def get_mock_execution_adapter() -> MockExecutionAdapter:
    """Return the adapter allowed for mock execution journal submissions."""

    return MockExecutionAdapter()


def get_pair_close_live_execution_coordinator(
    extended_service: Annotated[
        ExtendedLiveExecutionService,
        Depends(get_extended_live_execution_service),
    ],
    hyperliquid_service: Annotated[
        HyperliquidLiveExecutionService,
        Depends(get_hyperliquid_live_execution_service),
    ],
    paradex_service: Annotated[
        ParadexLiveExecutionService,
        Depends(get_paradex_live_execution_service),
    ],
) -> PairCloseLiveExecutionCoordinator:
    """Return the paired close execution coordinator."""

    return PairCloseLiveExecutionCoordinator(
        services={
            "extended": extended_service,
            "hyperliquid": hyperliquid_service,
            "paradex": paradex_service,
        }
    )


@lru_cache
def get_account_preflight_service() -> AccountPreflightService:
    """Return the authenticated account-state preflight service."""

    return AccountPreflightService()


def get_order_preview_service() -> OrderPreviewService:
    """Return the unsigned live order preview service."""

    return OrderPreviewService()


def _build_account_preflight_configs(settings: ApiSettings) -> AccountPreflightConfigMap:
    """Build the authenticated-read account probe config map from API settings."""

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


def _build_system_state_configs(settings: ApiSettings) -> SystemStateConfigMap:
    """Build the public system-state config map from API settings."""

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


async def _build_readiness_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    preview_hash: str,
    settings: ApiSettings,
    confirmation_store: PreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
    system_state_service: SystemStateService,
) -> LiveSubmissionReadiness:
    normalized_preview_hash = preview_hash.strip()
    if not normalized_preview_hash:
        raise ValueError("preview_hash must be non-empty")
    execution_preflight = build_paper_trade_execution_preflight(
        paper_trade,
        build_live_execution_configs(settings),
    )
    account_preflight = await account_preflight_service.probe_paper_trade(
        paper_trade,
        _build_account_preflight_configs(settings),
    )
    system_state = await system_state_service.probe_paper_trade(
        paper_trade,
        _build_system_state_configs(settings),
    )
    confirmation = confirmation_store.find_latest_by_preview_hash(
        paper_trade_id=paper_trade.entry_id or 0,
        preview_hash=normalized_preview_hash,
    )
    return build_live_submission_readiness(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        preview_hash=normalized_preview_hash,
        confirmations=[] if confirmation is None else [confirmation],
        execution_preflight=execution_preflight,
        account_preflight=account_preflight,
        system_state=system_state,
    )


async def _build_venue_scoped_readiness_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    preview_hash: str,
    venue: str,
    settings: ApiSettings,
    confirmation_store: PreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
    system_state_service: SystemStateService,
) -> tuple[LiveSubmissionReadiness, PreviewConfirmationEntry | None]:
    normalized_preview_hash = preview_hash.strip()
    if not normalized_preview_hash:
        raise ValueError("preview_hash must be non-empty")

    execution_statuses = {
        item.venue: item
        for item in build_venue_execution_preflights(build_live_execution_configs(settings))
    }
    selected_execution = execution_statuses.get(venue)
    execution_blockers: list[str] = []
    execution_venues: list[VenueExecutionPreflight] = []
    if selected_execution is None:
        execution_blockers.append(f"Venue {venue} live execution is not configured")
    else:
        execution_venues.append(selected_execution)
        if not selected_execution.enabled:
            execution_blockers.append(f"Venue {venue} live execution is not enabled")
        if selected_execution.missing_env_vars:
            execution_blockers.append(
                f"Venue {venue} is missing required credentials: "
                + ", ".join(selected_execution.missing_env_vars)
            )
    execution_preflight = PaperTradeExecutionPreflight(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        ready=not execution_blockers,
        venues=execution_venues,
        blocking_reasons=execution_blockers,
    )

    account_preflight_all = await account_preflight_service.probe_paper_trade(
        paper_trade,
        _build_account_preflight_configs(settings),
    )
    account_statuses = {item.venue: item for item in account_preflight_all.venues}
    selected_account = account_statuses.get(venue)
    account_blockers: list[str] = []
    account_venues: list[VenueAccountPreflight] = []
    if selected_account is None:
        account_blockers.append(f"Venue {venue} account preflight did not return a status")
    else:
        account_venues.append(selected_account)
        if not selected_account.enabled:
            account_blockers.append(f"Venue {venue} account preflight is not enabled")
        account_blockers.extend(selected_account.blocking_reasons)
    account_preflight = PaperTradeAccountPreflight(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        ready=not account_blockers,
        venues=account_venues,
        blocking_reasons=account_blockers,
    )

    system_state_all = await system_state_service.probe_paper_trade(
        paper_trade,
        _build_system_state_configs(settings),
    )
    system_statuses = {item.venue: item for item in system_state_all.venues}
    selected_system = system_statuses.get(venue)
    system_blockers: list[str] = []
    system_venues: list[VenueSystemState] = []
    if selected_system is None:
        system_blockers.append(f"Venue {venue} system state did not return a status")
    else:
        system_venues.append(selected_system)
        if not selected_system.enabled:
            system_blockers.append(f"Venue {venue} system state check is not enabled")
        system_blockers.extend(selected_system.blocking_reasons)
    system_state = PaperTradeSystemState(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        ready=not system_blockers,
        venues=system_venues,
        blocking_reasons=system_blockers,
    )

    confirmation = confirmation_store.find_latest_by_preview_hash(
        paper_trade_id=paper_trade.entry_id or 0,
        preview_hash=normalized_preview_hash,
    )
    return (
        build_live_submission_readiness(
            paper_trade_id=paper_trade.entry_id or 0,
            label=paper_trade.intent.label,
            preview_hash=normalized_preview_hash,
            confirmations=[] if confirmation is None else [confirmation],
            execution_preflight=execution_preflight,
            account_preflight=account_preflight,
            system_state=system_state,
        ),
        confirmation,
    )


async def _build_cleanup_context_for_paper_trade(
    *,
    paper_trade_id: int,
    settings: ApiSettings,
    paper_store: PaperTradeStore,
    execution_store: ExecutionJournalStore,
    account_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    cleanup_service: CleanupPreviewRouter,
) -> tuple[PaperTradeEntry, ExecutionJournalEntry, ExecutionPairStatus, ExecutionCleanupPreview]:
    paper_trade = paper_store.get(paper_trade_id)
    if paper_trade is None:
        raise HTTPException(
            status_code=404,
            detail=f"Paper trade {paper_trade_id} was not found",
        )
    execution = execution_store.latest_for_paper_trade(paper_trade_id)
    if execution is None:
        raise HTTPException(
            status_code=404,
            detail=f"No execution journal entry matched paper trade {paper_trade_id}",
        )

    account_preflight = await account_service.probe_paper_trade(
        paper_trade,
        _build_account_preflight_configs(settings),
    )
    reconciliation = reconcile_execution(execution, account_preflight)
    order_state = await order_state_service.observe_execution(execution)
    pair_status = build_execution_pair_status(execution, order_state, reconciliation)
    try:
        cleanup_preview = await cleanup_service.preview_from_execution(
            entry=execution,
            pair_status=pair_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return paper_trade, execution, pair_status, cleanup_preview


async def _build_pair_close_context_for_paper_trade(
    *,
    paper_trade_id: int,
    settings: ApiSettings,
    paper_store: PaperTradeStore,
    execution_store: ExecutionJournalStore,
    account_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    pair_close_service: PairClosePreviewService,
) -> tuple[PaperTradeEntry, ExecutionJournalEntry, ExecutionPairStatus, ExecutionPairClosePreview]:
    paper_trade, execution, pair_status = await _build_pair_close_status_for_paper_trade(
        paper_trade_id=paper_trade_id,
        settings=settings,
        paper_store=paper_store,
        execution_store=execution_store,
        account_service=account_service,
        order_state_service=order_state_service,
    )
    try:
        pair_close_preview = await pair_close_service.preview_from_execution(
            entry=execution,
            pair_status=pair_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return paper_trade, execution, pair_status, pair_close_preview


async def _build_pair_close_status_for_paper_trade(
    *,
    paper_trade_id: int,
    settings: ApiSettings,
    paper_store: PaperTradeStore,
    execution_store: ExecutionJournalStore,
    account_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
) -> tuple[PaperTradeEntry, ExecutionJournalEntry, ExecutionPairStatus]:
    paper_trade = paper_store.get(paper_trade_id)
    if paper_trade is None:
        raise HTTPException(
            status_code=404,
            detail=f"Paper trade {paper_trade_id} was not found",
        )
    execution = execution_store.latest_for_paper_trade(paper_trade_id)
    if execution is None:
        raise HTTPException(
            status_code=404,
            detail=f"No execution journal entry matched paper trade {paper_trade_id}",
        )

    try:
        account_preflight = await account_service.probe_paper_trade(
            paper_trade,
            _build_account_preflight_configs(settings),
        )
        reconciliation = reconcile_execution(execution, account_preflight)
        order_state = await order_state_service.observe_execution(execution)
        pair_status = build_execution_pair_status(execution, order_state, reconciliation)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return paper_trade, execution, pair_status


def _pair_close_preview_matches_canonical(
    *,
    provided: ExecutionPairClosePreview,
    canonical: ExecutionPairClosePreview,
) -> bool:
    """Return whether a client-supplied pair-close preview matches the server preview."""

    return (
        provided.execution_entry_id == canonical.execution_entry_id
        and provided.paper_trade_id == canonical.paper_trade_id
        and provided.label == canonical.label
        and provided.slippage_tolerance_bps == canonical.slippage_tolerance_bps
        and provided.preview_hash == canonical.preview_hash
        and provided.reason == canonical.reason
        and [leg.model_dump(mode="json") for leg in provided.legs]
        == [leg.model_dump(mode="json") for leg in canonical.legs]
    )


def _opposite_trade_side(side: str) -> str:
    normalized = side.strip().lower()
    if normalized == "buy":
        return "sell"
    if normalized == "sell":
        return "buy"
    raise ValueError(f"Unsupported trade side {side!r}")


def _validate_client_pair_close_preview_for_confirmation(
    *,
    preview: ExecutionPairClosePreview,
    preview_hash: str,
    paper_trade: PaperTradeEntry,
    execution: ExecutionJournalEntry,
    expected_preview_venues: list[str],
) -> None:
    if paper_trade.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Paper trade entry_id is required before pair-close confirmation",
        )
    if preview.preview_hash != preview_hash:
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview hash did not match the requested preview hash",
        )
    expected_preview_hash = _pair_close_hash(
        execution_entry_id=preview.execution_entry_id,
        paper_trade_id=preview.paper_trade_id,
        legs=preview.legs,
    )
    if preview.preview_hash != expected_preview_hash:
        raise HTTPException(
            status_code=409,
            detail=(
                "Provided pair close preview hash did not match the preview body"
            ),
        )
    if preview.paper_trade_id != paper_trade.entry_id:
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview did not belong to this paper trade",
        )
    if execution.entry_id is not None and preview.execution_entry_id != execution.entry_id:
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview must target the latest execution entry",
        )
    if preview.label != paper_trade.intent.label:
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview label did not match the paper trade",
        )
    if preview.reason != "close_pair":
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview must have reason 'close_pair'",
        )
    preview_venues = [leg.venue.strip().lower() for leg in preview.legs]
    if len(preview.legs) != 2 or len(set(preview_venues)) != 2:
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview must contain exactly two legs on distinct venues",
        )
    if preview_venues != expected_preview_venues:
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview leg order did not match the current close order",
        )

    expected_legs = {
        paper_trade.intent.long_leg.venue.strip().lower(): (
            paper_trade.intent.long_leg.symbol,
            paper_trade.intent.long_leg.fee_profile,
            _opposite_trade_side(paper_trade.intent.long_leg.side),
        ),
        paper_trade.intent.short_leg.venue.strip().lower(): (
            paper_trade.intent.short_leg.symbol,
            paper_trade.intent.short_leg.fee_profile,
            _opposite_trade_side(paper_trade.intent.short_leg.side),
        ),
    }
    if {leg.venue.strip().lower() for leg in preview.legs} != set(expected_legs):
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview venues did not match the paper trade",
        )
    for leg in preview.legs:
        if leg.reduce_only is not True:
            raise HTTPException(
                status_code=409,
                detail="Provided pair close preview legs must be reduce-only",
            )
        expected_symbol, expected_fee_profile, expected_side = expected_legs[
            leg.venue.strip().lower()
        ]
        if (
            leg.symbol != expected_symbol
            or leg.fee_profile != expected_fee_profile
            or leg.side != expected_side
        ):
            raise HTTPException(
                status_code=409,
                detail="Provided pair close preview leg did not match the paper trade intent",
            )
        _validate_client_pair_close_preview_leg_payload(leg)


def _validate_client_pair_close_preview_leg_payload(leg: VenueOrderPreview) -> None:
    payload = leg.payload
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=409,
            detail="Provided pair close preview leg payload must be an object",
        )
    venue = leg.venue.strip().lower()
    if venue == "extended":
        if payload.get("side") != leg.side.upper():
            raise HTTPException(
                status_code=409,
                detail="Provided Extended pair close payload side did not match the preview leg",
            )
        if payload.get("size") != leg.quantity_text:
            raise HTTPException(
                status_code=409,
                detail="Provided Extended pair close payload size did not match the preview leg",
            )
        if payload.get("price") != leg.worst_price_text:
            raise HTTPException(
                status_code=409,
                detail="Provided Extended pair close payload price did not match the preview leg",
            )
        if payload.get("time_in_force") != leg.time_in_force.upper():
            raise HTTPException(
                status_code=409,
                detail=(
                    "Provided Extended pair close payload time_in_force "
                    "did not match the preview leg"
                ),
            )
        if payload.get("type") != leg.order_type.upper():
            raise HTTPException(
                status_code=409,
                detail="Provided Extended pair close payload type did not match the preview leg",
            )
        if payload.get("reduce_only") is not leg.reduce_only:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Provided Extended pair close payload reduce_only "
                    "did not match the preview leg"
                ),
            )
        if payload.get("post_only") is not leg.post_only:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Provided Extended pair close payload post_only "
                    "did not match the preview leg"
                ),
            )
        if not isinstance(payload.get("client_order_id"), str) or not payload["client_order_id"]:
            raise HTTPException(
                status_code=409,
                detail="Provided Extended pair close payload must include a client_order_id",
            )
        return
    if venue == "paradex":
        if payload.get("market") != leg.symbol:
            raise HTTPException(
                status_code=409,
                detail="Provided Paradex pair close payload market did not match the preview leg",
            )
        if payload.get("side") != leg.side.upper():
            raise HTTPException(
                status_code=409,
                detail="Provided Paradex pair close payload side did not match the preview leg",
            )
        if payload.get("type") != leg.order_type.upper():
            raise HTTPException(
                status_code=409,
                detail="Provided Paradex pair close payload type did not match the preview leg",
            )
        if payload.get("size") != leg.quantity_text:
            raise HTTPException(
                status_code=409,
                detail="Provided Paradex pair close payload size did not match the preview leg",
            )
        if payload.get("price") != leg.worst_price_text:
            raise HTTPException(
                status_code=409,
                detail="Provided Paradex pair close payload price did not match the preview leg",
            )
        if payload.get("instruction") != leg.time_in_force.upper():
            raise HTTPException(
                status_code=409,
                detail=(
                    "Provided Paradex pair close payload instruction "
                    "did not match the preview leg"
                ),
            )
        if payload.get("reduce_only") is not leg.reduce_only:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Provided Paradex pair close payload reduce_only "
                    "did not match the preview leg"
                ),
            )
        if not isinstance(payload.get("client_id"), str) or not payload["client_id"]:
            raise HTTPException(
                status_code=409,
                detail="Provided Paradex pair close payload must include a client_id",
            )
        return
    if venue == "hyperliquid":
        if "reduce_only" in payload and payload.get("reduce_only") is not leg.reduce_only:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Provided Hyperliquid pair close payload reduce_only "
                    "did not match the preview leg"
                ),
            )
        return


async def _ensure_cleanup_live_ready(
    *,
    venue: str,
    settings: ApiSettings,
    account_service: AccountPreflightService,
) -> None:
    execution_statuses = {
        item.venue: item
        for item in build_venue_execution_preflights(build_live_execution_configs(settings))
    }
    venue_execution = execution_statuses.get(venue)
    if venue_execution is None:
        raise HTTPException(
            status_code=409,
            detail=PaperTradeExecutionPreflight(
                paper_trade_id=0,
                label=f"{venue}_cleanup",
                ready=False,
                venues=[],
                blocking_reasons=[f"Venue {venue} live execution is not configured"],
            ).model_dump(mode="json"),
        )
    execution_blockers: list[str] = []
    if not venue_execution.enabled:
        execution_blockers.append(f"Venue {venue} live execution is not enabled")
    if venue_execution.missing_env_vars:
        execution_blockers.append(
            f"Venue {venue} is missing required credentials: "
            + ", ".join(venue_execution.missing_env_vars)
        )
    if execution_blockers:
        raise HTTPException(
            status_code=409,
            detail=PaperTradeExecutionPreflight(
                paper_trade_id=0,
                label=f"{venue}_cleanup",
                ready=False,
                venues=[venue_execution],
                blocking_reasons=execution_blockers,
            ).model_dump(mode="json"),
        )

    account_statuses = {
        item.venue: item
        for item in await account_service.probe_venues(_build_account_preflight_configs(settings))
    }
    venue_account = account_statuses.get(venue)
    if venue_account is None:
        raise HTTPException(
            status_code=409,
            detail=PaperTradeAccountPreflight(
                paper_trade_id=0,
                label=f"{venue}_cleanup",
                ready=False,
                venues=[],
                blocking_reasons=[f"Venue {venue} account preflight is not configured"],
            ).model_dump(mode="json"),
        )
    account_blockers: list[str] = []
    if not venue_account.enabled:
        account_blockers.append(f"Venue {venue} account preflight is not enabled")
    account_blockers.extend(venue_account.blocking_reasons)
    if account_blockers:
        raise HTTPException(
            status_code=409,
            detail=PaperTradeAccountPreflight(
                paper_trade_id=0,
                label=f"{venue}_cleanup",
                ready=False,
                venues=[venue_account],
                blocking_reasons=account_blockers,
            ).model_dump(mode="json"),
        )


def _build_cleanup_confirmation_note(
    *,
    note: str | None,
    requested_preview_hash: str,
    current_preview_hash: str,
) -> str | None:
    if requested_preview_hash == current_preview_hash:
        return note
    refresh_note = (
        "Refreshed cleanup preview from stale hash "
        f"{requested_preview_hash} to current hash {current_preview_hash}"
    )
    if note is None or not note.strip():
        return refresh_note
    if refresh_note in note:
        return note
    return f"{note}; {refresh_note}"


def _append_cleanup_confirmation_entry(
    *,
    paper_trade: PaperTradeEntry,
    cleanup_preview: ExecutionCleanupPreview,
    confirmation_store: CleanupPreviewConfirmationStore,
    note: str | None,
) -> CleanupPreviewConfirmationEntry:
    effective_paper_trade_id = (
        paper_trade.entry_id
        if paper_trade.entry_id is not None
        else cleanup_preview.paper_trade_id
    )
    if effective_paper_trade_id is None:
        raise ValueError("paper_trade_id is required for cleanup confirmation entries")
    return confirmation_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=effective_paper_trade_id,
            label=paper_trade.intent.label,
            preview_hash=cleanup_preview.preview_hash,
            preview=cleanup_preview,
            note=note,
        )
    )


def _resolve_cleanup_confirmation_for_live_submit(
    *,
    paper_trade: PaperTradeEntry,
    cleanup_preview: ExecutionCleanupPreview,
    confirmation_store: CleanupPreviewConfirmationStore,
    requested_preview_hash: str,
) -> CleanupPreviewConfirmationEntry:
    current_preview_hash = cleanup_preview.preview_hash
    effective_paper_trade_id = (
        paper_trade.entry_id
        if paper_trade.entry_id is not None
        else cleanup_preview.paper_trade_id
    )
    if effective_paper_trade_id is None:
        raise ValueError("paper_trade_id is required for cleanup confirmation entries")

    if requested_preview_hash == current_preview_hash:
        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=effective_paper_trade_id,
        )
        return require_confirmed_cleanup_preview(
            paper_trade_id=effective_paper_trade_id,
            preview_hash=current_preview_hash,
            confirmations=confirmations,
        )

    current_confirmation = confirmation_store.find_latest_by_preview_hash(
        paper_trade_id=effective_paper_trade_id,
        preview_hash=current_preview_hash,
    )
    if current_confirmation is not None:
        return current_confirmation

    stale_confirmation = confirmation_store.find_latest_by_preview_hash(
        paper_trade_id=effective_paper_trade_id,
        preview_hash=requested_preview_hash,
    )
    if stale_confirmation is None:
        raise ValueError(
            "No cleanup preview confirmation matched "
            f"paper_trade_id={effective_paper_trade_id} "
            f"and preview_hash={requested_preview_hash}"
        )

    refreshed_note = _build_cleanup_confirmation_note(
        note=stale_confirmation.note,
        requested_preview_hash=requested_preview_hash,
        current_preview_hash=current_preview_hash,
    )
    return _append_cleanup_confirmation_entry(
        paper_trade=paper_trade,
        cleanup_preview=cleanup_preview,
        confirmation_store=confirmation_store,
        note=refreshed_note,
    )


def _effective_cleanup_submission_paper_trade_id(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: CleanupPreviewConfirmationEntry,
) -> int:
    paper_trade_id = paper_trade.entry_id or confirmation.paper_trade_id
    if paper_trade_id < 1:
        raise HTTPException(
            status_code=500,
            detail="paper_trade_id is required before cleanup live submission",
        )
    return paper_trade_id


def _reserve_cleanup_live_submission_or_existing(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: CleanupPreviewConfirmationEntry,
    execution_store: ExecutionJournalStore,
) -> ExecutionJournalEntry | None:
    if confirmation.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Cleanup preview confirmation entry_id is required before live submission",
        )
    paper_trade_id = _effective_cleanup_submission_paper_trade_id(
        paper_trade=paper_trade,
        confirmation=confirmation,
    )
    existing_entry = execution_store.find_by_paper_trade_preview_hash(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
    )
    if existing_entry is not None:
        return existing_entry
    if not execution_store.reserve_cleanup_live_submission(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        confirmation_entry_id=confirmation.entry_id,
    ):
        existing_entry = execution_store.find_by_paper_trade_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=confirmation.preview_hash,
        )
        if existing_entry is not None:
            return existing_entry
        raise HTTPException(
            status_code=409,
            detail=(
                "Cleanup live submission was already reserved for this paper trade and "
                "preview without a matching journaled execution; manual reconciliation "
                "is required before retrying"
            ),
        )
    if not execution_store.reserve_live_submission(
        confirmation_entry_id=confirmation.entry_id,
        preview_hash=confirmation.preview_hash,
    ):
        existing_entry = execution_store.find_by_confirmation(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
        )
        if existing_entry is not None:
            return existing_entry
        raise HTTPException(
            status_code=409,
            detail=(
                "Cleanup live submission was already reserved without a matching "
                "journaled execution; manual reconciliation is required before retrying"
            ),
        )
    return None


def _mark_cleanup_live_submission_completed(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: CleanupPreviewConfirmationEntry,
    execution_store: ExecutionJournalStore,
    execution_entry_id: int,
) -> None:
    if confirmation.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Cleanup preview confirmation entry_id is required before live submission",
        )
    paper_trade_id = _effective_cleanup_submission_paper_trade_id(
        paper_trade=paper_trade,
        confirmation=confirmation,
    )
    execution_store.mark_live_submission_completed(
        confirmation_entry_id=confirmation.entry_id,
        preview_hash=confirmation.preview_hash,
        execution_entry_id=execution_entry_id,
    )
    execution_store.mark_cleanup_live_submission_completed(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        execution_entry_id=execution_entry_id,
    )


def _release_cleanup_live_submission_reservations(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: CleanupPreviewConfirmationEntry,
    execution_store: ExecutionJournalStore,
) -> None:
    """Release cleanup reservations only when submission definitely did not happen."""

    if confirmation.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Cleanup preview confirmation entry_id is required before live submission",
        )
    paper_trade_id = _effective_cleanup_submission_paper_trade_id(
        paper_trade=paper_trade,
        confirmation=confirmation,
    )
    released_live = execution_store.release_live_submission(
        confirmation_entry_id=confirmation.entry_id,
        preview_hash=confirmation.preview_hash,
    )
    released_cleanup = execution_store.release_cleanup_live_submission(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        confirmation_entry_id=confirmation.entry_id,
    )
    if released_live and released_cleanup:
        return
    raise HTTPException(
        status_code=409,
        detail=(
            "Cleanup live submission failed before journaling, but its "
            "reservations could not be safely released; manual reconciliation "
            "is required before retrying"
        ),
    )


async def _observe_pair_status_for_execution(
    *,
    paper_trade: PaperTradeEntry,
    execution: ExecutionJournalEntry,
    settings: ApiSettings,
    account_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    observation_store: ExecutionObservationStore | None = None,
    observation_context: str = "guarded_pair_poll",
    poll_attempts: int = 5,
    poll_interval_seconds: float = 2.0,
) -> ExecutionPairStatus:
    if poll_attempts <= 0:
        raise ValueError("poll_attempts must be positive")
    if poll_interval_seconds < 0:
        raise ValueError("poll_interval_seconds must be non-negative")

    last_status: ExecutionPairStatus | None = None
    for attempt in range(poll_attempts):
        if attempt > 0 and poll_interval_seconds > 0:
            await asyncio.sleep(poll_interval_seconds)
        try:
            account_preflight = await asyncio.wait_for(
                account_service.probe_paper_trade(
                    paper_trade,
                    _build_account_preflight_configs(settings),
                ),
                timeout=PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS,
            )
            reconciliation = reconcile_execution(execution, account_preflight)
            order_state = await asyncio.wait_for(
                order_state_service.observe_execution(execution),
                timeout=PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            continue
        last_status = build_execution_pair_status(execution, order_state, reconciliation)
        if observation_store is not None:
            try:
                observation_store.append(
                    ExecutionObservationEntry(
                        observed_at=datetime.now(UTC),
                        context=observation_context,
                        execution_entry_id=execution.entry_id,
                        paper_trade_id=execution.paper_trade_id,
                        preview_hash=execution.preview_hash,
                        order_state=order_state,
                        pair_status=last_status,
                    )
                )
            except Exception:
                logger.warning(
                    (
                        "Failed to persist execution observation for "
                        "entry_id=%s paper_trade_id=%s preview_hash=%s"
                    ),
                    execution.entry_id,
                    execution.paper_trade_id,
                    execution.preview_hash,
                    exc_info=True,
                )
        if last_status.derived_state in {
            "hedged",
            "unfilled",
            "cleanup_needed",
            "review_required",
        }:
            return last_status

    if last_status is None:
        raise UpstreamDataError("Timed out polling execution status from venue services")
    return last_status


async def _run_guarded_auto_cleanup_sequence(
    *,
    paper_trade: PaperTradeEntry,
    preview_hash: str,
    initial_execution: ExecutionJournalEntry,
    initial_pair_status: ExecutionPairStatus,
    settings: ApiSettings,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    cleanup_confirmation_store: CleanupPreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    cleanup_preview_service: CleanupPreviewRouter,
    cleanup_live_router: CleanupLiveExecutionRouter,
    poll_attempts: int,
    poll_interval_seconds: float,
    confirmation_note: str,
    observation_context_prefix: str,
) -> tuple[ExecutionJournalEntry | None, ExecutionPairStatus]:
    paper_trade_id = paper_trade.entry_id or 0
    current_execution = initial_execution
    pair_status = initial_pair_status
    latest_cleanup_execution: ExecutionJournalEntry | None = None

    for step in range(1, MAX_GUARDED_AUTO_CLEANUP_STEPS + 1):
        reused_existing_cleanup = False
        if pair_status.recommended_action not in {
            "close_open_leg",
            "complete_or_unwind_missing_leg",
        }:
            return latest_cleanup_execution, pair_status

        try:
            cleanup_preview = await cleanup_preview_service.preview_from_execution(
                entry=current_execution,
                pair_status=pair_status,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        try:
            await asyncio.wait_for(
                _ensure_cleanup_live_ready(
                    venue=cleanup_preview.leg.venue,
                    settings=settings,
                    account_service=account_preflight_service,
                ),
                timeout=PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail=(
                    "Timed out probing cleanup live readiness for "
                    f"{cleanup_preview.leg.venue}"
                ),
            ) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        cleanup_confirmation = cleanup_confirmation_store.find_latest_by_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=cleanup_preview.preview_hash,
        )
        if cleanup_confirmation is None:
            cleanup_confirmation = cleanup_confirmation_store.append(
                CleanupPreviewConfirmationEntry(
                    confirmed_at=datetime.now(UTC),
                    paper_trade_id=paper_trade_id,
                    label=paper_trade.intent.label,
                    preview_hash=cleanup_preview.preview_hash,
                    preview=cleanup_preview,
                    note=confirmation_note,
                )
            )
        existing_entry = _reserve_cleanup_live_submission_or_existing(
            paper_trade=paper_trade,
            confirmation=cleanup_confirmation,
            execution_store=execution_store,
        )
        if existing_entry is not None:
            latest_cleanup_execution = existing_entry
            reused_existing_cleanup = True
        else:
            try:
                latest_cleanup_execution = (
                    await cleanup_live_router.submit_confirmed_cleanup_preview(
                        paper_trade=paper_trade,
                        confirmation=cleanup_confirmation,
                    )
                )
            except ValueError as exc:
                _release_cleanup_live_submission_reservations(
                    paper_trade=paper_trade,
                    confirmation=cleanup_confirmation,
                    execution_store=execution_store,
                )
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc

            latest_cleanup_execution = execution_store.append(latest_cleanup_execution)
            if latest_cleanup_execution.entry_id is None:
                raise HTTPException(
                    status_code=500,
                    detail="Execution journal append did not return an id",
                )
            _mark_cleanup_live_submission_completed(
                paper_trade=paper_trade,
                confirmation=cleanup_confirmation,
                execution_store=execution_store,
                execution_entry_id=latest_cleanup_execution.entry_id,
            )

        try:
            pair_status = await _observe_pair_status_for_execution(
                paper_trade=paper_trade,
                execution=latest_cleanup_execution,
                settings=settings,
                account_service=account_preflight_service,
                order_state_service=order_state_service,
                observation_store=observation_store,
                observation_context=f"{observation_context_prefix}_cleanup_step_{step}",
                poll_attempts=poll_attempts,
                poll_interval_seconds=poll_interval_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if reused_existing_cleanup:
            pair_status = pair_status.model_copy(
                update={
                    "notes": [
                        *pair_status.notes,
                        "Existing cleanup execution reused for this confirmation",
                    ]
                }
            )
            return latest_cleanup_execution, pair_status

        current_execution = latest_cleanup_execution

    if pair_status.recommended_action in {"close_open_leg", "complete_or_unwind_missing_leg"}:
        pair_status = pair_status.model_copy(
            update={
                "notes": [
                    *pair_status.notes,
                    (
                        "Automatic cleanup stopped after reaching the maximum chained cleanup "
                        f"steps ({MAX_GUARDED_AUTO_CLEANUP_STEPS})."
                    ),
                ]
            }
        )
    return latest_cleanup_execution, pair_status


def _select_trade_intent_records(
    records: list[OpportunityRecord],
    *,
    limit: int,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
    max_break_even_days_entry: float | None,
) -> list[OpportunityRecord]:
    """Select ranked records that still clear the requested dry-run gates."""

    latest = latest_records_by_label(records, limit=len(records))
    candidates = filter_candidate_records(
        latest,
        min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
        min_capacity_notional=min_capacity_notional,
    )
    if max_break_even_days_entry is not None:
        candidates = [
            record
            for record in candidates
            if record.opportunity.break_even_days_entry is not None
            and record.opportunity.break_even_days_entry <= max_break_even_days_entry
        ]
    return rank_history_records(candidates, limit=limit)


def _record_capacity_notional(record: OpportunityRecord) -> float:
    """Return the saved max entry capacity as a sortable/filterable number."""

    capacity = record.opportunity.capacity
    if capacity is None or capacity.max_entry_notional is None:
        return 0.0
    return capacity.max_entry_notional


def _opportunity_spread_penalty(opportunity: FundingArbOpportunity) -> float:
    """Return a sortable penalty for wide or missing saved spreads."""

    penalty = 0.0
    for spread_rate in (opportunity.short_spread_rate, opportunity.long_spread_rate):
        if spread_rate is None:
            penalty += 0.01
        else:
            penalty += spread_rate
    return penalty


def _opportunity_stale_book_penalty(opportunity: FundingArbOpportunity) -> float:
    """Return a sortable penalty for stale saved order books."""

    penalty = 0.0
    if opportunity.short_stale_book:
        penalty += 1.0
    if opportunity.long_stale_book:
        penalty += 1.0
    return penalty


def _min_saved_liquidity_values(short_value: float | None, long_value: float | None) -> float:
    """Return the weaker saved metric, failing closed when either side is missing."""

    if short_value is None or long_value is None:
        return 0.0
    return min(short_value, long_value)


def _opportunity_liquidity_score(opportunity: FundingArbOpportunity) -> float:
    """Return a small sortable bonus for weaker-side saved liquidity."""

    min_daily_volume = _min_saved_liquidity_values(
        opportunity.short_daily_volume,
        opportunity.long_daily_volume,
    )
    min_open_interest = _min_saved_liquidity_values(
        opportunity.short_open_interest,
        opportunity.long_open_interest,
    )
    return math.log10(1.0 + min_daily_volume) * 1e-6 + math.log10(
        1.0 + min_open_interest
    ) * 1e-6


def _ensure_aware_utc(value: datetime) -> datetime:
    """Normalize persisted datetimes before freshness comparisons."""

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _safe_aware_utc(value: datetime) -> datetime:
    """Normalize datetimes without letting malformed persisted values abort ranking."""

    try:
        return _ensure_aware_utc(value)
    except (OSError, OverflowError, ValueError):
        return MIN_AWARE_UTC_DATETIME


def _opportunity_quality_key(
    opportunity: FundingArbOpportunity,
    *,
    capacity_notional: float,
    recency: datetime,
) -> tuple[float, float, float, float, float, float, datetime]:
    """Rank by edge while demoting poor execution quality and stale saved data."""

    return (
        opportunity.one_day_net_edge_after_round_trip,
        -_opportunity_spread_penalty(opportunity),
        _opportunity_liquidity_score(opportunity),
        -_opportunity_stale_book_penalty(opportunity),
        opportunity.one_day_net_edge_after_entry,
        capacity_notional,
        recency,
    )


def _canary_candidate_identity(candidate: FundingUniverseCanaryCandidate) -> tuple[str, ...]:
    """Return a stable identity for deduping canary candidates across scans."""

    opportunity = candidate.opportunity.opportunity
    venue_markets: list[str] = []
    for venue, market in sorted(candidate.opportunity.venue_markets.items()):
        venue_markets.extend([venue, market.venue, market.symbol])
    return (
        opportunity.canonical_symbol,
        opportunity.short_venue,
        opportunity.long_venue,
        opportunity.short_fee_profile,
        opportunity.long_fee_profile,
        *venue_markets,
    )


def _canary_candidate_quality_key(
    candidate: FundingUniverseCanaryCandidate,
) -> tuple[float, float, float, float, float, float, datetime]:
    """Return the same execution-aware key used for history shortlist ranking."""

    opportunity = candidate.opportunity.opportunity
    capacity_notional = 0.0
    if candidate.opportunity.deployable_notional is not None:
        capacity_notional = candidate.opportunity.deployable_notional
    elif opportunity.capacity is not None and opportunity.capacity.max_entry_notional is not None:
        capacity_notional = opportunity.capacity.max_entry_notional
    return _opportunity_quality_key(
        opportunity,
        capacity_notional=capacity_notional,
        recency=MIN_AWARE_UTC_DATETIME,
    )


def _merge_canary_candidates(
    first: list[FundingUniverseCanaryCandidate],
    second: list[FundingUniverseCanaryCandidate],
    *,
    limit: int,
) -> list[FundingUniverseCanaryCandidate]:
    """Merge shortlist and broad-scan candidates without duplicating routes."""

    merged: list[FundingUniverseCanaryCandidate] = []
    seen: set[tuple[str, ...]] = set()
    for candidate in [*first, *second]:
        identity = _canary_candidate_identity(candidate)
        if identity in seen:
            continue
        merged.append(candidate)
        seen.add(identity)
    return sorted(merged, key=_canary_candidate_quality_key, reverse=True)[:limit]


def _select_canary_reprice_symbols_from_history(
    store: OpportunityHistoryStore,
    *,
    sample: int,
    limit: int,
    now: datetime,
    max_age_seconds: int,
    venues: list[str],
    exclude_symbols: list[str] | None,
    exclude_tags: list[str] | None,
    min_roundtrip_edge: float,
    min_capacity_notional: float,
    min_daily_volume: float,
    min_open_interest: float,
) -> list[str]:
    """Select saved symbols worth live repricing before approval proposal generation."""

    selected_venues = {venue.strip().lower() for venue in venues if venue.strip()}
    if max_age_seconds < 1 or max_age_seconds > MAX_HISTORY_SHORTLIST_MAX_AGE_SECONDS:
        return []
    try:
        cutoff = _ensure_aware_utc(now) - timedelta(seconds=max_age_seconds)
    except OverflowError:
        return []
    records = [
        record
        for record in store.list_recent(limit=sample)
        if _safe_aware_utc(record.recorded_at) >= cutoff
    ]
    if not records:
        return []
    latest = latest_records_by_label(records, limit=len(records))
    candidates: list[OpportunityRecord] = []
    for record in latest:
        opportunity = record.opportunity
        route_venues = {
            opportunity.short_venue.strip().lower(),
            opportunity.long_venue.strip().lower(),
        }
        if not route_venues.issubset(selected_venues):
            continue
        if not passes_symbol_policy(
            opportunity.canonical_symbol,
            exclude_symbols=exclude_symbols,
            exclude_tags=exclude_tags,
        ):
            continue
        if opportunity.one_day_net_edge_after_round_trip < min_roundtrip_edge:
            continue
        if _record_capacity_notional(record) < min_capacity_notional:
            continue
        saved_daily_volume = _min_saved_liquidity_values(
            opportunity.short_daily_volume,
            opportunity.long_daily_volume,
        )
        if saved_daily_volume < min_daily_volume:
            continue
        saved_open_interest = _min_saved_liquidity_values(
            opportunity.short_open_interest,
            opportunity.long_open_interest,
        )
        if saved_open_interest < min_open_interest:
            continue
        candidates.append(record)

    ranked = sorted(
        candidates,
        key=lambda record: _opportunity_quality_key(
            record.opportunity,
            capacity_notional=_record_capacity_notional(record),
            recency=_safe_aware_utc(record.recorded_at),
        ),
        reverse=True,
    )
    symbols: list[str] = []
    seen_symbols: set[str] = set()
    for record in ranked:
        symbol = record.opportunity.canonical_symbol
        if symbol in seen_symbols:
            continue
        symbols.append(symbol)
        seen_symbols.add(symbol)
        if len(symbols) >= limit:
            break
    return symbols


def _build_trade_intent_candidates(
    store: OpportunityHistoryStore,
    *,
    limit: int,
    sample: int,
    label: str | None,
    capacity_fraction: float,
    max_target_notional: float,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
    max_break_even_days_entry: float | None,
) -> list[FundingPairTradeIntent]:
    """Build ranked dry-run trade intents from saved opportunity history."""

    limit = _validated_history_limit("limit", limit)
    sample = max(limit, _validated_history_limit("sample", sample))
    candidate_sample = min(MAX_HISTORY_LIMIT, max(sample, limit * 4))
    capacity_fraction = _validated_fraction("capacity_fraction", capacity_fraction)
    max_target_notional = _validated_positive_threshold(
        "max_target_notional",
        max_target_notional,
    )
    min_one_day_net_edge_after_entry = _validated_non_negative_threshold(
        "min_one_day_net_edge_after_entry",
        min_one_day_net_edge_after_entry,
    )
    min_capacity_notional = _validated_non_negative_threshold(
        "min_capacity_notional",
        min_capacity_notional,
    )
    if max_break_even_days_entry is not None:
        max_break_even_days_entry = _validated_non_negative_threshold(
            "max_break_even_days_entry",
            max_break_even_days_entry,
        )
    records = store.list_recent(limit=candidate_sample, label=label)
    selected = _select_trade_intent_records(
        records,
        limit=candidate_sample,
        min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
        min_capacity_notional=min_capacity_notional,
        max_break_even_days_entry=max_break_even_days_entry,
    )
    intents: list[FundingPairTradeIntent] = []
    for record in selected:
        try:
            intents.append(
                build_trade_intent(
                    record,
                    capacity_fraction=capacity_fraction,
                    max_target_notional=max_target_notional,
                    min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
                    min_capacity_notional=min_capacity_notional,
                    max_break_even_days_entry=max_break_even_days_entry,
                )
            )
        except InvalidTradeCandidateError:
            continue
        if len(intents) == limit:
            break
    return intents


async def _select_approved_canary_candidate(
    *,
    universe_service: OpportunityUniverseService,
    approval_service: RouteApprovalService,
    venues: list[str] | None,
    label: str | None,
    extended_fee_profile: str | None,
    paradex_fee_profile: str | None,
    hyperliquid_fee_profile: str | None,
    target_notional: float,
    canary_max_notional: float,
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
) -> tuple[FundingUniverseCanaryCandidate, RouteApprovalEntry]:
    selected_venues = venues or list(SUPPORTED_UNIVERSE_VENUES)
    _validate_route_stability_filters(
        min_route_stability_weight=min_route_stability_weight,
        min_route_presence_ratio=min_route_presence_ratio,
        min_route_samples=min_route_samples,
    )
    fee_profile_overrides = _build_fee_profile_overrides(
        extended_fee_profile=extended_fee_profile,
        paradex_fee_profile=paradex_fee_profile,
        hyperliquid_fee_profile=hyperliquid_fee_profile,
    )
    if label is not None:
        exact_matches: list[tuple[FundingUniverseCanaryCandidate, RouteApprovalEntry]] = []
        approvals = approval_service.list_recent(limit=1_000, label=label, approved=True)
        for approved_route in approvals:
            candidate, _ = await scan_exact_canary_candidate_for_approval(
                scanner=universe_service,
                approval_service=approval_service,
                approval=approved_route,
                venues=selected_venues,
                fee_profile_overrides=fee_profile_overrides,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=limit,
            )
            if candidate is not None:
                exact_matches.append((candidate, approved_route))
        if exact_matches:
            selected, _ = max(
                exact_matches,
                key=lambda item: _rank_approved_canary_candidate(item[0]),
            )
            matched_approval = approval_service.get_for_candidate(selected)
            if matched_approval is None or not matched_approval.approved:
                raise HTTPException(
                    status_code=409,
                    detail="Selected canary route is no longer approved for live execution",
                )
            return selected, matched_approval
        raise HTTPException(
            status_code=404,
            detail="No approved canary candidate matched the requested filters",
        )
    candidates = await _run_bounded_universe_scan(
        "approved canary candidate scan",
        lambda: universe_service.scan_canary_candidates(
            venues=selected_venues,
            fee_profile_overrides=fee_profile_overrides,
            target_notional=target_notional,
            canary_max_notional=canary_max_notional,
            min_capacity_notional=min_capacity_notional,
            min_daily_volume=min_daily_volume,
            min_open_interest=min_open_interest,
            min_roundtrip_edge=min_roundtrip_edge,
            min_execution_quality_score=min_execution_quality_score,
            min_execution_samples=min_execution_samples,
            min_route_stability_weight=min_route_stability_weight,
            min_route_presence_ratio=min_route_presence_ratio,
            min_route_samples=min_route_samples,
            include_symbols=include_symbols,
            exclude_symbols=exclude_symbols,
            exclude_tags=exclude_tags,
            limit=limit,
        ),
    )
    approved_candidates = approval_service.filter_approved_canary_candidates(candidates)
    if label is not None:
        approved_candidates = [
            item
            for item in approved_candidates
            if build_pair_spec_from_universe_opportunity(item.opportunity).label == label
        ]
    if not approved_candidates:
        raise HTTPException(
            status_code=404,
            detail="No approved canary candidate matched the requested filters",
        )
    selected = approved_candidates[0]
    matched_approval = approval_service.get_for_candidate(selected)
    if matched_approval is None or not matched_approval.approved:
        raise HTTPException(
            status_code=409,
            detail="Selected canary route is no longer approved for live execution",
        )
    return selected, matched_approval


async def _scan_approved_canary_basket_plan(
    *,
    universe_service: OpportunityUniverseService,
    approval_service: RouteApprovalService,
    venues: list[str] | None,
    extended_fee_profile: str | None,
    paradex_fee_profile: str | None,
    hyperliquid_fee_profile: str | None,
    target_notional: float,
    canary_max_notional: float,
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
) -> ApprovedCanaryBasketPlan:
    """Build one approved-canary basket plan from the live opportunity universe."""

    selected_venues = venues or list(SUPPORTED_UNIVERSE_VENUES)
    _validate_route_stability_filters(
        min_route_stability_weight=min_route_stability_weight,
        min_route_presence_ratio=min_route_presence_ratio,
        min_route_samples=min_route_samples,
    )
    fee_profile_overrides = (
        _build_fee_profile_overrides(
            extended_fee_profile=extended_fee_profile,
            paradex_fee_profile=paradex_fee_profile,
            hyperliquid_fee_profile=hyperliquid_fee_profile,
        )
        or {}
    )
    resolved_fee_profiles = universe_service.resolve_fee_profiles(
        venues=selected_venues,
        fee_profile_overrides=fee_profile_overrides,
    )
    resolved_venues = list(resolved_fee_profiles)
    candidates = await _run_bounded_universe_scan(
        "funding universe approved basket scan",
        lambda: universe_service.scan_canary_candidates(
            venues=resolved_venues,
            fee_profile_overrides=fee_profile_overrides or None,
            target_notional=target_notional,
            canary_max_notional=canary_max_notional,
            min_capacity_notional=min_capacity_notional,
            min_daily_volume=min_daily_volume,
            min_open_interest=min_open_interest,
            min_roundtrip_edge=min_roundtrip_edge,
            min_execution_quality_score=min_execution_quality_score,
            min_execution_samples=min_execution_samples,
            min_route_stability_weight=min_route_stability_weight,
            min_route_presence_ratio=min_route_presence_ratio,
            min_route_samples=min_route_samples,
            include_symbols=include_symbols,
            exclude_symbols=exclude_symbols,
            exclude_tags=exclude_tags,
            limit=limit,
        ),
    )
    return approval_service.build_approved_canary_basket_plan(
        candidates=candidates,
        venues=resolved_venues,
        fee_profiles=resolved_fee_profiles,
        target_notional=target_notional,
    )


def _select_latest_approved_canary_snapshot(
    *,
    store: ApprovedCanaryStore,
    approval_service: RouteApprovalService,
    label: str | None,
    max_snapshot_age_seconds: int,
    canary_max_notional: float | None = None,
    now: datetime | None = None,
) -> tuple[ApprovedCanarySnapshot, FundingUniverseCanaryCandidate, RouteApprovalEntry]:
    snapshot = store.latest(label=label)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No approved canary snapshot found")
    current_time = now or datetime.now(UTC)
    snapshot_age_seconds = max(
        0.0,
        (current_time - snapshot.captured_at).total_seconds(),
    )
    if snapshot_age_seconds > max_snapshot_age_seconds:
        raise HTTPException(
            status_code=409,
            detail=(
                "Approved canary snapshot is stale "
                f"({snapshot_age_seconds:.1f}s > {max_snapshot_age_seconds}s)"
            ),
        )
    approval = approval_service.get_for_candidate(snapshot.candidate)
    if approval is None or not approval.approved:
        raise HTTPException(
            status_code=409,
            detail="Latest approved canary snapshot is no longer approved for live execution",
        )
    capped_notional = min(
        snapshot.candidate.suggested_canary_notional,
        approval.max_live_notional,
        canary_max_notional
        if canary_max_notional is not None
        else snapshot.candidate.suggested_canary_notional,
    )
    if capped_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail=(
                "Approved canary snapshot does not satisfy the requested canary_max_notional"
                if canary_max_notional is not None
                else "Approved canary snapshot no longer permits a positive live notional"
            ),
        )
    return (
        snapshot,
        snapshot.candidate.model_copy(update={"suggested_canary_notional": capped_notional}),
        approval,
    )


def _validate_latest_approved_canary_snapshot_request(
    *,
    candidate: FundingUniverseCanaryCandidate,
    approval: RouteApprovalEntry,
    universe_service: OpportunityUniverseService,
    venues: list[str] | None,
    label: str,
    extended_fee_profile: str | None,
    paradex_fee_profile: str | None,
    hyperliquid_fee_profile: str | None,
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
) -> FundingUniverseCanaryCandidate:
    """Reject cached snapshots that do not satisfy the current canary request."""

    selected_venues = {venue.lower() for venue in (venues or list(SUPPORTED_UNIVERSE_VENUES))}
    route = candidate.opportunity.opportunity
    route_venues = {route.short_venue.lower(), route.long_venue.lower()}
    if not route_venues.issubset(selected_venues):
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested venues",
        )
    if build_pair_spec_from_universe_opportunity(candidate.opportunity).label != label:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested label",
        )
    if (
        route.canonical_symbol != approval.canonical_symbol
        or route.short_venue != approval.short_venue
        or route.long_venue != approval.long_venue
        or route.short_fee_profile != approval.short_fee_profile
        or route.long_fee_profile != approval.long_fee_profile
    ):
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot no longer matches the approved live route",
        )
    requested_fee_profiles = {
        "extended": extended_fee_profile,
        "paradex": paradex_fee_profile,
        "hyperliquid": hyperliquid_fee_profile,
    }
    for venue_name, route_fee_profile in (
        (route.short_venue.lower(), route.short_fee_profile),
        (route.long_venue.lower(), route.long_fee_profile),
    ):
        requested_fee_profile = requested_fee_profiles.get(venue_name)
        if requested_fee_profile is not None and route_fee_profile != requested_fee_profile:
            raise HTTPException(
                status_code=409,
                detail="Approved canary snapshot does not satisfy the requested fee profiles",
            )
    if not passes_symbol_policy(
        route.canonical_symbol,
        include_symbols=include_symbols,
        exclude_symbols=exclude_symbols,
        exclude_tags=DEFAULT_CANARY_EXCLUDE_TAGS if exclude_tags is None else exclude_tags,
    ):
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested symbol policy",
        )

    deployable_notional = candidate.opportunity.deployable_notional
    if route.capacity is not None and route.capacity.max_entry_notional is not None:
        deployable_notional = min(target_notional, route.capacity.max_entry_notional)
    elif deployable_notional is not None:
        deployable_notional = min(target_notional, deployable_notional)
    else:
        deployable_notional = max(0.0, target_notional)
    if deployable_notional < min_capacity_notional:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )
    if (candidate.opportunity.min_daily_volume or 0.0) < min_daily_volume:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )
    if (candidate.opportunity.min_open_interest or 0.0) < min_open_interest:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )
    modeled_roundtrip_edge = (
        route.gross_daily_edge - candidate.opportunity.modeled_round_trip_cost_rate
        if candidate.opportunity.modeled_round_trip_cost_rate is not None
        else route.one_day_net_edge_after_round_trip
    )
    if modeled_roundtrip_edge < min_roundtrip_edge:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )

    execution_quality_service = getattr(universe_service, "execution_quality_service", None)
    default_execution_quality_score = (
        execution_quality_service.prior_score if execution_quality_service is not None else 1.0
    )
    execution_quality_score = (
        candidate.opportunity.execution_quality.weighted_score
        if candidate.opportunity.execution_quality is not None
        else default_execution_quality_score
    )
    execution_sample_size = (
        candidate.opportunity.execution_quality.sample_size
        if candidate.opportunity.execution_quality is not None
        else 0
    )
    if execution_quality_score < min_execution_quality_score:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )
    if execution_sample_size < min_execution_samples:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )

    if candidate.opportunity.route_stability is not None:
        route_stability_weight = candidate.opportunity.route_stability.stability_weight
        route_presence_ratio = candidate.opportunity.route_stability.presence_ratio
        route_sample_size = candidate.opportunity.route_stability.sample_size
        if route_stability_weight < min_route_stability_weight:
            raise HTTPException(
                status_code=409,
                detail="Approved canary snapshot does not satisfy the requested filters",
            )
        if route_presence_ratio < min_route_presence_ratio:
            raise HTTPException(
                status_code=409,
                detail="Approved canary snapshot does not satisfy the requested filters",
            )
        if route_sample_size < min_route_samples:
            raise HTTPException(
                status_code=409,
                detail="Approved canary snapshot does not satisfy the requested filters",
            )
    elif (
        min_route_stability_weight > 0.0
        or min_route_presence_ratio > 0.0
        or min_route_samples > 0
    ):
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )

    adjusted_notional = min(
        candidate.suggested_canary_notional,
        approval.max_live_notional,
        deployable_notional,
    )
    if adjusted_notional < min_capacity_notional:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )
    if adjusted_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot does not satisfy the requested filters",
        )
    return candidate.model_copy(update={"suggested_canary_notional": adjusted_notional})


def _select_latest_launch_ready_canary_snapshot(
    *,
    store: LaunchReadyCanaryStore,
    approval_service: RouteApprovalService,
    label: str | None,
    max_snapshot_age_seconds: int,
    now: datetime | None = None,
) -> tuple[LaunchReadyCanarySnapshot, FundingUniverseCanaryCandidate, RouteApprovalEntry]:
    snapshot = store.latest(label=label)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No launch-ready canary snapshot found")
    current_time = now or datetime.now(UTC)
    snapshot_age_seconds = (current_time - snapshot.captured_at).total_seconds()
    if snapshot_age_seconds < 0:
        raise HTTPException(
            status_code=409,
            detail="Launch-ready canary snapshot timestamp is in the future",
        )
    effective_max_age_seconds = min(
        max_snapshot_age_seconds,
        snapshot.max_snapshot_age_seconds,
    )
    if snapshot_age_seconds > effective_max_age_seconds:
        raise HTTPException(
            status_code=409,
            detail=(
                "Launch-ready canary snapshot is stale "
                f"({snapshot_age_seconds:.1f}s > {effective_max_age_seconds}s)"
            ),
        )
    if not snapshot.system_state.ready:
        raise HTTPException(
            status_code=409,
            detail="Latest launch-ready canary snapshot is not ready for live execution",
        )
    approval = approval_service.get_for_candidate(snapshot.approved_snapshot.candidate)
    if approval is None or not approval.approved:
        raise HTTPException(
            status_code=409,
            detail="Latest launch-ready canary snapshot is no longer approved for live execution",
        )
    capped_notional = min(
        snapshot.approved_snapshot.candidate.suggested_canary_notional,
        approval.max_live_notional,
    )
    if capped_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail="Launch-ready canary snapshot no longer permits a positive live notional",
        )
    return (
        snapshot,
        snapshot.approved_snapshot.candidate.model_copy(
            update={"suggested_canary_notional": capped_notional}
        ),
        approval,
    )


def _build_launch_ready_automation_gate_policy(
    settings: ApiSettings,
) -> LaunchReadyAutomationGatePolicy:
    return LaunchReadyAutomationGatePolicy(
        min_edge_retention_ratio=settings.stable_launch_ready_min_edge_retention_ratio,
        max_entry_break_even_funding_windows=(
            settings.stable_launch_ready_max_entry_break_even_funding_windows
        ),
        max_round_trip_break_even_funding_windows=(
            settings.stable_launch_ready_max_round_trip_break_even_funding_windows
        ),
    )


async def _build_launch_ready_snapshot_for_approved_snapshot(
    *,
    settings: ApiSettings,
    approved_store: ApprovedCanaryStore,
    system_state_service: SystemStateService,
    snapshot: ApprovedCanarySnapshot,
    now: datetime,
) -> LaunchReadyCanarySnapshot:
    snapshot_age_seconds = max(0.0, (now - snapshot.captured_at).total_seconds())
    if snapshot.captured_at > now:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot timestamp is in the future",
        )
    if snapshot_age_seconds > settings.launch_ready_canary_max_snapshot_age_seconds:
        raise HTTPException(
            status_code=409,
            detail=(
                "Approved canary snapshot is stale "
                f"({snapshot_age_seconds:.1f}s > "
                f"{settings.launch_ready_canary_max_snapshot_age_seconds}s)"
            ),
        )
    if not snapshot.approval.approved:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot is not approved for live execution",
        )

    capped_notional = min(
        snapshot.candidate.suggested_canary_notional,
        snapshot.approval.max_live_notional,
    )
    if capped_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot no longer permits a positive live notional",
        )

    refreshed_candidate = snapshot.candidate.model_copy(
        update={"suggested_canary_notional": capped_notional}
    )
    refreshed_snapshot = snapshot.model_copy(
        update={"candidate": refreshed_candidate}
    )
    recent_approved_chain = list_recent_approved_snapshot_chain(
        recent_snapshots=[
            refreshed_snapshot,
            *approved_store.list_recent(limit=20, label=snapshot.label),
        ],
        snapshot=refreshed_snapshot,
        max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
    )
    automation_gate_reason = build_approved_snapshot_automation_gate_reason(
        snapshot=refreshed_snapshot,
        recent_chain=recent_approved_chain or [refreshed_snapshot],
        policy=_build_launch_ready_automation_gate_policy(settings),
    )
    if automation_gate_reason is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                "Approved canary snapshot does not satisfy automated launch gates: "
                f"{automation_gate_reason}"
            ),
        )

    execution_preflight = build_candidate_live_execution_preflight(
        venue_preflights=build_venue_execution_preflights(
            build_live_execution_configs(settings)
        ),
        candidate=refreshed_candidate,
        label=snapshot.label,
    )
    if not execution_preflight.ready:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Approved canary snapshot live execution preflight is not ready",
                "blocking_reasons": execution_preflight.blocking_reasons,
                "execution_preflight": execution_preflight.model_dump(mode="json"),
            },
        )

    system_state = await probe_candidate_system_state(
        service=system_state_service,
        configs=_build_system_state_configs(settings),
        candidate=refreshed_candidate,
        label=snapshot.label,
    )
    if not system_state.ready:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Approved canary snapshot system state is not ready",
                "blocking_reasons": system_state.blocking_reasons,
                "system_state": system_state.model_dump(mode="json"),
            },
        )

    return LaunchReadyCanarySnapshot(
        captured_at=now,
        label=snapshot.label,
        max_snapshot_age_seconds=settings.launch_ready_canary_max_snapshot_age_seconds,
        approved_snapshot=refreshed_snapshot,
        system_state=system_state,
    )


async def _approve_refresh_and_cache_launch_ready_canary_proposal(
    *,
    label: str,
    universe_service: OpportunityUniverseService,
    approval_service: RouteApprovalService,
    approved_store: ApprovedCanaryStore,
    launch_ready_store: LaunchReadyCanaryStore,
    system_state_service: SystemStateService,
    settings: ApiSettings,
    payload: FundingUniverseCanaryApprovalProposalSummary,
    current_time: datetime,
    max_proposal_age_seconds: int,
    target_notional: float,
    canary_max_notional: float,
    min_capacity_notional: float,
    min_daily_volume: float,
    min_open_interest: float,
    min_roundtrip_edge: float,
    min_execution_quality_score: float,
    min_execution_samples: int,
    min_route_stability_weight: float,
    min_route_presence_ratio: float,
    min_route_samples: int,
    exclude_tags: list[str] | None,
) -> LaunchReadyCanarySnapshot:
    """Promote one current proposal and cache it only if launch-ready gates pass."""

    approval_payload = _build_approved_route_payload_from_canary_proposal(
        label=label,
        proposal=payload,
        current_time=current_time,
        max_proposal_age_seconds=max_proposal_age_seconds,
    )
    candidate_approval = RouteApprovalEntry(
        updated_at=current_time,
        label=label,
        canonical_symbol=approval_payload.canonical_symbol,
        short_venue=approval_payload.short_venue,
        long_venue=approval_payload.long_venue,
        short_fee_profile=approval_payload.short_fee_profile,
        long_fee_profile=approval_payload.long_fee_profile,
        approved=True,
        max_live_notional=approval_payload.max_live_notional,
        note=approval_payload.note,
    )
    snapshot = await _scan_current_approved_canary_snapshot_for_promoted_proposal(
        universe_service=universe_service,
        approval=candidate_approval,
        now=current_time,
        target_notional=target_notional,
        canary_max_notional=canary_max_notional,
        min_capacity_notional=min_capacity_notional,
        min_daily_volume=min_daily_volume,
        min_open_interest=min_open_interest,
        min_roundtrip_edge=min_roundtrip_edge,
        min_execution_quality_score=min_execution_quality_score,
        min_execution_samples=min_execution_samples,
        min_route_stability_weight=min_route_stability_weight,
        min_route_presence_ratio=min_route_presence_ratio,
        min_route_samples=min_route_samples,
        exclude_tags=DEFAULT_CANARY_EXCLUDE_TAGS if exclude_tags is None else exclude_tags,
    )
    launch_ready_snapshot = await _build_launch_ready_snapshot_for_approved_snapshot(
        settings=settings,
        approved_store=approved_store,
        system_state_service=system_state_service,
        snapshot=snapshot,
        now=current_time,
    )
    return _persist_launch_ready_promotion_atomically(
        approval_service=approval_service,
        approved_store=approved_store,
        launch_ready_store=launch_ready_store,
        approval_payload=approval_payload,
        snapshot=snapshot,
        launch_ready_snapshot=launch_ready_snapshot,
    )


def _persist_launch_ready_promotion_atomically(
    *,
    approval_service: RouteApprovalService,
    approved_store: ApprovedCanaryStore,
    launch_ready_store: LaunchReadyCanaryStore,
    approval_payload: RouteApprovalUpsert,
    snapshot: ApprovedCanarySnapshot,
    launch_ready_snapshot: LaunchReadyCanarySnapshot,
) -> LaunchReadyCanarySnapshot:
    """Persist approval, approved snapshot, and launch-ready snapshot in one transaction."""

    database_urls = {
        approval_service.store.database.url,
        approved_store.database.url,
        launch_ready_store.database.url,
    }
    if len(database_urls) != 1:
        raise RuntimeError("launch-ready promotion stores must share one database target")

    approval_service.store.initialize()
    approved_store.initialize()
    launch_ready_store.initialize()

    approval_entry = RouteApprovalEntry(
        label=snapshot.label,
        updated_at=datetime.now(UTC),
        canonical_symbol=approval_payload.canonical_symbol,
        short_venue=approval_payload.short_venue,
        long_venue=approval_payload.long_venue,
        short_fee_profile=approval_payload.short_fee_profile,
        long_fee_profile=approval_payload.long_fee_profile,
        approved=approval_payload.approved,
        max_live_notional=approval_payload.max_live_notional,
        note=approval_payload.note,
    )
    approved_snapshot_to_persist = snapshot.model_copy(
        update={"approval": approval_entry}
    )

    with approval_service.store.database.begin() as connection:
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
                approval_entry.updated_at.isoformat(),
                approval_entry.label,
                approval_entry.canonical_symbol,
                approval_entry.short_venue,
                approval_entry.long_venue,
                approval_entry.short_fee_profile,
                approval_entry.long_fee_profile,
                int(approval_entry.approved),
                approval_entry.max_live_notional,
                approval_entry.model_dump_json(),
            ),
        )
        approved_snapshot_id = connection.insert_returning_id(
            """
            INSERT INTO approved_canary_snapshots (
                captured_at,
                label,
                snapshot_json
            ) VALUES (?, ?, ?)
            """,
            (
                approved_snapshot_to_persist.captured_at.isoformat(),
                approved_snapshot_to_persist.label,
                approved_snapshot_to_persist.model_dump_json(),
            ),
        )
        persisted_approved_snapshot = ApprovedCanarySnapshot.model_validate(
            {
                **approved_snapshot_to_persist.model_dump(mode="json"),
                "snapshot_id": approved_snapshot_id,
            }
        )
        launch_ready_to_persist = launch_ready_snapshot.model_copy(
            update={"approved_snapshot": persisted_approved_snapshot}
        )
        launch_ready_snapshot_id = connection.insert_returning_id(
            """
            INSERT INTO launch_ready_canary_snapshots (
                captured_at,
                label,
                approved_snapshot_id,
                snapshot_json
            ) VALUES (?, ?, ?, ?)
            """,
            (
                launch_ready_to_persist.captured_at.isoformat(),
                launch_ready_to_persist.label,
                launch_ready_to_persist.approved_snapshot.snapshot_id,
                launch_ready_to_persist.model_dump_json(),
            ),
        )

    return LaunchReadyCanarySnapshot.model_validate(
        {
            **launch_ready_to_persist.model_dump(mode="python"),
            "launch_ready_snapshot_id": launch_ready_snapshot_id,
        }
    )


def _launch_ready_snapshot_payload_changed(
    previous_snapshot: LaunchReadyCanarySnapshot,
    current_snapshot: LaunchReadyCanarySnapshot,
) -> bool:
    previous_payload = _normalized_launch_ready_snapshot_payload(previous_snapshot)
    current_payload = _normalized_launch_ready_snapshot_payload(current_snapshot)
    return previous_payload != current_payload


def _normalized_launch_ready_snapshot_payload(
    snapshot: LaunchReadyCanarySnapshot,
) -> dict[str, object]:
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
    payload = system_state.model_dump(mode="python")
    venues = payload.get("venues")
    if isinstance(venues, list):
        payload["venues"] = sorted(
            venues,
            key=lambda venue: str(cast(dict[str, object], venue)["venue"]),
        )
    return payload


def _build_launch_ready_canary_stability(
    *,
    store: LaunchReadyCanaryStore,
    label: str | None,
    max_snapshot_age_seconds: int,
    min_snapshot_count: int,
    min_stable_seconds: float,
    now: datetime | None = None,
) -> LaunchReadyCanaryStability:
    if min_snapshot_count <= 0:
        raise HTTPException(status_code=400, detail="min_snapshot_count must be positive")
    if min_stable_seconds < 0:
        raise HTTPException(status_code=400, detail="min_stable_seconds must be non-negative")

    latest_snapshot = store.latest(label=label)
    if latest_snapshot is None:
        raise HTTPException(status_code=404, detail="No launch-ready canary snapshot found")

    current_time = now or datetime.now(UTC)
    snapshot_age_seconds = (current_time - latest_snapshot.captured_at).total_seconds()
    if snapshot_age_seconds < 0:
        raise HTTPException(
            status_code=409,
            detail="Launch-ready canary snapshot timestamp is in the future",
        )
    effective_max_age_seconds = min(
        max_snapshot_age_seconds,
        latest_snapshot.max_snapshot_age_seconds,
    )
    if snapshot_age_seconds > effective_max_age_seconds:
        raise HTTPException(
            status_code=409,
            detail=(
                "Launch-ready canary snapshot is stale "
                f"({snapshot_age_seconds:.1f}s > {effective_max_age_seconds}s)"
            ),
        )

    snapshots = store.list_recent(limit=max(min_snapshot_count + 5, 20), label=label)
    chain: list[LaunchReadyCanarySnapshot] = []
    for snapshot in snapshots:
        if _launch_ready_snapshot_payload_changed(snapshot, latest_snapshot):
            break
        chain.append(snapshot)

    consecutive_snapshots = len(chain)
    oldest_snapshot = chain[-1]
    stable_seconds = max(
        0.0,
        (latest_snapshot.captured_at - oldest_snapshot.captured_at).total_seconds(),
    )
    if consecutive_snapshots < min_snapshot_count:
        raise HTTPException(
            status_code=409,
            detail=(
                "Launch-ready canary snapshot is not yet stable "
                f"({consecutive_snapshots} < {min_snapshot_count} consecutive snapshots)"
            ),
        )
    if stable_seconds < min_stable_seconds:
        raise HTTPException(
            status_code=409,
            detail=(
                "Launch-ready canary snapshot has not persisted long enough "
                f"({stable_seconds:.1f}s < {min_stable_seconds:.1f}s)"
            ),
        )
    return LaunchReadyCanaryStability(
        snapshot=latest_snapshot,
        consecutive_snapshots=consecutive_snapshots,
        stable_seconds=stable_seconds,
        min_snapshot_count=min_snapshot_count,
        min_stable_seconds=min_stable_seconds,
    )


def _append_unique_blocking_reason(reasons: list[str], reason: object) -> None:
    normalized = str(reason)
    if normalized not in reasons:
        reasons.append(normalized)


def _automation_snapshot_summary(
    *,
    snapshot_id: int | None,
    label: str,
    captured_at: datetime,
    now: datetime,
) -> AutomationSnapshotSummary:
    return AutomationSnapshotSummary(
        snapshot_id=snapshot_id,
        label=label,
        captured_at=captured_at,
        age_seconds=(now - captured_at).total_seconds(),
    )


def _count_blocking_live_executions_for_automation_readiness(
    *,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    now: datetime,
    max_observation_age_seconds: int,
    scan_limit: int,
) -> int:
    blocking_count = 0
    for execution in _list_recent_live_executions_for_automation_readiness(
        execution_store=execution_store,
        observation_store=observation_store,
        now=now,
        max_age_seconds=max_observation_age_seconds,
        limit=scan_limit,
    ):
        paper_trade_id = execution.paper_trade_id
        if paper_trade_id is None:
            blocking_count += 1
            continue
        latest_observation = observation_store.latest_for_paper_trade(paper_trade_id)
        if latest_observation is None:
            blocking_count += 1
            continue
        observation_age_seconds = (now - latest_observation.observed_at).total_seconds()
        if observation_age_seconds < 0:
            blocking_count += 1
            continue
        if observation_age_seconds > max_observation_age_seconds:
            blocking_count += 1
            continue
        pair_status = latest_observation.pair_status
        if pair_status is None:
            blocking_count += 1
            continue
        if (
            pair_status.derived_state in {"hedged", "cleanup_needed", "review_required"}
            or pair_status.recommended_action != "no_action"
        ):
            blocking_count += 1
    return blocking_count


def _list_recent_live_executions_for_automation_readiness(
    *,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    now: datetime,
    max_age_seconds: int,
    limit: int,
) -> list[ExecutionJournalEntry]:
    """Return recent unique live executions using the same paging semantics as the worker."""

    page_size = max(min(limit, 100), 20)
    offset = 0
    seen_paper_trade_ids: set[int] = set()
    selected: list[ExecutionJournalEntry] = []

    while len(selected) < limit:
        batch = execution_store.list_recent_active_live(limit=page_size, offset=offset)
        if not batch:
            break
        offset += len(batch)

        for execution in batch:
            age_seconds = (now - execution.executed_at).total_seconds()
            if age_seconds > max_age_seconds and not _execution_requires_continued_monitoring(
                observation_store,
                execution=execution,
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
) -> bool:
    """Return whether an older live execution still has an active monitoring state."""

    paper_trade_id = execution.paper_trade_id
    if paper_trade_id is None:
        return False
    latest = latest_observation
    if latest is None:
        latest = observation_store.latest_for_paper_trade(paper_trade_id)
    if latest is None:
        return False
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


def _append_paper_trade_from_canary_candidate(
    *,
    candidate: FundingUniverseCanaryCandidate,
    paper_store: PaperTradeStore,
    desired_notional: float | None,
    note: str | None,
) -> PaperTradeEntry:
    capped_notional = candidate.suggested_canary_notional
    if desired_notional is not None:
        capped_notional = min(capped_notional, desired_notional)
    if capped_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail="Approved route does not allow a positive live notional",
        )
    record = build_opportunity_record_from_universe_opportunity(
        recorded_at=datetime.now(UTC),
        opportunity=candidate.opportunity,
    )
    intent = build_trade_intent(
        record,
        capacity_fraction=1.0,
        max_target_notional=capped_notional,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=0.0,
    )
    entry = PaperTradeEntry(
        created_at=datetime.now(UTC),
        intent=intent,
        note=note,
    )
    return paper_store.append(entry)


async def _run_guarded_canary_lifecycle(
    *,
    candidate: FundingUniverseCanaryCandidate,
    approval: RouteApprovalEntry,
    desired_notional: float | None,
    note: str | None,
    lifecycle_note: str | None,
    paper_store: PaperTradeStore,
    confirmation_store: PreviewConfirmationStore,
    pair_close_confirmation_store: PairClosePreviewConfirmationStore,
    cleanup_confirmation_store: CleanupPreviewConfirmationStore,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    settings: ApiSettings,
    account_preflight_service: AccountPreflightService,
    system_state_service: SystemStateService,
    balance_service: BalanceAccountingService,
    order_preview_service: OrderPreviewService,
    order_state_service: ExecutionOrderStateService,
    cleanup_preview_service: CleanupPreviewRouter,
    pair_close_preview_service: PairClosePreviewService,
    cleanup_live_router: CleanupLiveExecutionRouter,
    paired_service: PairedLiveExecutionCoordinator,
    pair_close_live_service: PairCloseLiveExecutionCoordinator,
    approval_service: RouteApprovalService,
    slippage_tolerance_bps: int,
    open_first_venue: str,
    close_first_venue: str,
    poll_attempts: int,
    poll_interval_seconds: float,
    auto_cleanup: bool,
    close_position: bool,
    before_open_submission: Callable[[], Awaitable[None] | None] | None = None,
) -> CanaryLifecycleResult:
    paper_trade = _append_paper_trade_from_canary_candidate(
        candidate=candidate,
        paper_store=paper_store,
        desired_notional=desired_notional,
        note=note or "guarded canary lifecycle",
    )
    _require_live_route_approval(
        paper_trade=paper_trade,
        approval_service=approval_service,
    )

    pre_open_snapshots = await _capture_authenticated_balance_snapshots_for_paper_trade(
        paper_trade=paper_trade,
        stage="pre_open",
        note="guarded canary lifecycle pre-open",
        settings=settings,
        account_service=account_preflight_service,
        balance_service=balance_service,
    )
    open_confirmation = await _append_preview_confirmation_for_paper_trade(
        paper_trade=paper_trade,
        confirmation_store=confirmation_store,
        service=order_preview_service,
        slippage_tolerance_bps=slippage_tolerance_bps,
        note="guarded canary lifecycle auto-confirm open preview",
    )
    readiness = await _build_readiness_for_paper_trade(
        paper_trade=paper_trade,
        preview_hash=open_confirmation.preview_hash,
        settings=settings,
        confirmation_store=confirmation_store,
        account_preflight_service=account_preflight_service,
        system_state_service=system_state_service,
    )
    if not readiness.ready:
        raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))
    if before_open_submission is not None:
        callback_result = before_open_submission()
        if inspect.isawaitable(callback_result):
            await callback_result
    open_execution = await _execute_guarded_pair_from_confirmation(
        paper_trade=paper_trade,
        confirmation=open_confirmation,
        settings=settings,
        execution_store=execution_store,
        observation_store=observation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        account_preflight_service=account_preflight_service,
        order_state_service=order_state_service,
        cleanup_preview_service=cleanup_preview_service,
        cleanup_live_router=cleanup_live_router,
        service=paired_service,
        first_venue=open_first_venue,
        poll_attempts=poll_attempts,
        poll_interval_seconds=poll_interval_seconds,
        auto_cleanup=auto_cleanup,
    )
    post_open_snapshots = await _capture_authenticated_balance_snapshots_for_paper_trade(
        paper_trade=paper_trade,
        stage="post_open",
        note="guarded canary lifecycle post-open",
        settings=settings,
        account_service=account_preflight_service,
        balance_service=balance_service,
    )

    close_confirmation: PairClosePreviewConfirmationEntry | None = None
    close_execution: GuardedPairExecutionResult | None = None
    post_close_snapshots: list[VenueBalanceSnapshot] = []
    notes: list[str] = []
    if lifecycle_note:
        notes.append(lifecycle_note)
    final_pair_status = open_execution.pair_status

    if close_position and open_execution.pair_status.derived_state == "hedged":
        (
            _paper_trade,
            _execution,
            _pair_status,
            pair_close_preview,
        ) = await _build_pair_close_context_for_paper_trade(
            paper_trade_id=paper_trade.entry_id or 0,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_preflight_service,
            order_state_service=order_state_service,
            pair_close_service=pair_close_preview_service,
        )
        close_confirmation = _append_pair_close_confirmation_for_preview(
            paper_trade=paper_trade,
            confirmation_store=pair_close_confirmation_store,
            preview=pair_close_preview,
            note="guarded canary lifecycle auto-confirm close preview",
        )
        close_execution = await _execute_guarded_pair_close_from_confirmation(
            paper_trade=paper_trade,
            confirmation=close_confirmation,
            settings=settings,
            execution_store=execution_store,
            observation_store=observation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            account_preflight_service=account_preflight_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            cleanup_live_router=cleanup_live_router,
            service=pair_close_live_service,
            first_venue=close_first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
        )
        post_close_snapshots = await _capture_authenticated_balance_snapshots_for_paper_trade(
            paper_trade=paper_trade,
            stage="post_close",
            note="guarded canary lifecycle post-close",
            settings=settings,
            account_service=account_preflight_service,
            balance_service=balance_service,
        )
        final_pair_status = close_execution.pair_status
    elif close_position:
        notes.append("Close step was skipped because the open step did not end in a hedged state.")
    else:
        notes.append("Close step was disabled for this canary cycle.")

    return CanaryLifecycleResult(
        candidate=candidate,
        approval=approval,
        paper_trade=paper_trade,
        open_confirmation=open_confirmation,
        open_execution=open_execution,
        close_confirmation=close_confirmation,
        close_execution=close_execution,
        pre_open_snapshots=pre_open_snapshots,
        post_open_snapshots=post_open_snapshots,
        post_close_snapshots=post_close_snapshots,
        balance_delta=balance_service.summarize_paper_trade(paper_trade.entry_id or 0),
        final_pair_status=final_pair_status,
        notes=notes,
    )


def _http_exception_detail_string(detail: object) -> str:
    """Normalize FastAPI exception detail payloads for persisted basket outcomes."""

    if isinstance(detail, str):
        return detail
    return str(detail)


def _summarize_canary_basket_balance_delta(
    route_outcomes: list[CanaryBasketRouteOutcome],
) -> CanaryBasketBalanceDelta | None:
    """Aggregate route-level balance deltas for one basket execution."""

    route_deltas = [
        outcome.result.balance_delta
        for outcome in route_outcomes
        if outcome.result is not None and outcome.result.balance_delta is not None
    ]
    if not route_deltas:
        return None

    collateral_total = 0.0
    available_total = 0.0
    free_total = 0.0
    have_collateral = False
    have_available = False
    have_free = False
    for delta in route_deltas:
        if delta.total_collateral_delta is not None:
            collateral_total += delta.total_collateral_delta
            have_collateral = True
        if delta.total_available_to_trade_delta is not None:
            available_total += delta.total_available_to_trade_delta
            have_available = True
        if delta.total_free_collateral_delta is not None:
            free_total += delta.total_free_collateral_delta
            have_free = True

    return CanaryBasketBalanceDelta(
        route_count=len(route_outcomes),
        successful_route_count=sum(1 for item in route_outcomes if item.status == "completed"),
        total_collateral_delta=collateral_total if have_collateral else None,
        total_available_to_trade_delta=available_total if have_available else None,
        total_free_collateral_delta=free_total if have_free else None,
        routes=route_deltas,
    )


async def _execute_approved_canary_basket_plan(
    *,
    request: Request,
    settings: ApiSettings,
    basket_plan: ApprovedCanaryBasketPlan,
    note: str | None,
    paper_store: PaperTradeStore,
    confirmation_store: PreviewConfirmationStore,
    pair_close_confirmation_store: PairClosePreviewConfirmationStore,
    cleanup_confirmation_store: CleanupPreviewConfirmationStore,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    account_preflight_service: AccountPreflightService,
    system_state_service: SystemStateService,
    balance_service: BalanceAccountingService,
    order_preview_service: OrderPreviewService,
    order_state_service: ExecutionOrderStateService,
    approval_service: RouteApprovalService,
    basket_store: CanaryBasketLaunchStore,
    slippage_tolerance_bps: int,
    open_first_venue: str,
    close_first_venue: str,
    poll_attempts: int,
    poll_interval_seconds: float,
    auto_cleanup: bool,
    close_position: bool,
    continue_on_failure: bool,
) -> CanaryBasketLaunchResult:
    """Launch one approved basket sequentially and persist the aggregated result."""

    launched_at = datetime.now(UTC)
    route_outcomes: list[CanaryBasketRouteOutcome] = []
    notes: list[str] = []
    for index, entry in enumerate(basket_plan.entries, start=1):
        candidate = entry.candidate.model_copy(
            update={"suggested_canary_notional": entry.selected_notional}
        )
        services = _resolve_canary_execution_services_for_candidate(
            request=request,
            settings=settings,
            candidate=candidate,
        )
        route_note = f"approved canary basket route {index}/{len(basket_plan.entries)}" + (
            f" :: {note}" if note else ""
        )
        try:
            result = await _run_guarded_canary_lifecycle(
                candidate=candidate,
                approval=entry.approval,
                desired_notional=entry.selected_notional,
                note=route_note,
                lifecycle_note=(
                    f"Approved canary basket launch started at {launched_at.isoformat()}."
                ),
                paper_store=paper_store,
                confirmation_store=confirmation_store,
                pair_close_confirmation_store=pair_close_confirmation_store,
                cleanup_confirmation_store=cleanup_confirmation_store,
                execution_store=execution_store,
                observation_store=observation_store,
                settings=settings,
                account_preflight_service=account_preflight_service,
                system_state_service=system_state_service,
                balance_service=balance_service,
                order_preview_service=order_preview_service,
                order_state_service=order_state_service,
                cleanup_preview_service=services.cleanup_preview_service,
                pair_close_preview_service=services.pair_close_preview_service,
                cleanup_live_router=services.cleanup_live_router,
                paired_service=services.paired_service,
                pair_close_live_service=services.pair_close_live_service,
                approval_service=approval_service,
                slippage_tolerance_bps=slippage_tolerance_bps,
                open_first_venue=open_first_venue,
                close_first_venue=close_first_venue,
                poll_attempts=poll_attempts,
                poll_interval_seconds=poll_interval_seconds,
                auto_cleanup=auto_cleanup,
                close_position=close_position,
            )
        except HTTPException as exc:
            route_outcomes.append(
                CanaryBasketRouteOutcome(
                    label=entry.label,
                    selected_notional=entry.selected_notional,
                    status="failed",
                    error=_http_exception_detail_string(exc.detail),
                )
            )
            notes.append(
                f"Route {entry.label} failed with HTTP {exc.status_code}: "
                f"{_http_exception_detail_string(exc.detail)}"
            )
            if not continue_on_failure:
                break
            continue
        except Exception as exc:
            route_outcomes.append(
                CanaryBasketRouteOutcome(
                    label=entry.label,
                    selected_notional=entry.selected_notional,
                    status="failed",
                    error=str(exc),
                )
            )
            notes.append(f"Route {entry.label} failed: {exc}")
            if not continue_on_failure:
                break
            continue

        route_outcomes.append(
            CanaryBasketRouteOutcome(
                label=entry.label,
                selected_notional=entry.selected_notional,
                status="completed",
                paper_trade_id=result.paper_trade.entry_id,
                result=result,
            )
        )

    successful_route_count = sum(1 for item in route_outcomes if item.status == "completed")
    failed_route_count = sum(1 for item in route_outcomes if item.status == "failed")
    record = CanaryBasketLaunchResult(
        launched_at=launched_at,
        status="completed" if failed_route_count == 0 else "completed_with_failures",
        basket_plan=basket_plan,
        route_count=len(route_outcomes),
        successful_route_count=successful_route_count,
        failed_route_count=failed_route_count,
        routes=route_outcomes,
        balance_delta=_summarize_canary_basket_balance_delta(route_outcomes),
        notes=notes,
    )
    return basket_store.append(record)


async def _capture_authenticated_balance_snapshots_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    stage: str,
    note: str | None,
    settings: ApiSettings,
    account_service: AccountPreflightService,
    balance_service: BalanceAccountingService,
) -> list[VenueBalanceSnapshot]:
    preflight = await account_service.probe_paper_trade(
        paper_trade,
        _build_account_preflight_configs(settings),
    )
    unauthenticated = [item.venue for item in preflight.venues if not item.authenticated]
    if unauthenticated:
        raise HTTPException(
            status_code=409,
            detail=(
                "Balance snapshot capture requires authenticated venue reads for: "
                + ", ".join(sorted(unauthenticated))
            ),
        )
    return balance_service.capture_paper_trade(
        paper_trade=paper_trade,
        preflight=preflight,
        stage=stage,
        note=note,
    )


async def _append_preview_confirmation_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    confirmation_store: PreviewConfirmationStore,
    service: OrderPreviewService,
    slippage_tolerance_bps: int,
    note: str | None,
) -> PreviewConfirmationEntry:
    try:
        preview = await service.preview_paper_trade(
            paper_trade,
            slippage_tolerance_bps=slippage_tolerance_bps,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label=paper_trade.intent.label,
            preview_hash=preview.preview_hash,
            preview=preview,
            note=note,
        )
    )


def _append_pair_close_confirmation_for_preview(
    *,
    paper_trade: PaperTradeEntry,
    confirmation_store: PairClosePreviewConfirmationStore,
    preview: ExecutionPairClosePreview,
    note: str | None,
) -> PairClosePreviewConfirmationEntry:
    return confirmation_store.append(
        PairClosePreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label=paper_trade.intent.label,
            preview_hash=preview.preview_hash,
            preview=preview,
            note=note,
        )
    )


def _effective_pair_open_submission_paper_trade_id(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: PreviewConfirmationEntry,
) -> int:
    paper_trade_id = paper_trade.entry_id or confirmation.paper_trade_id
    if paper_trade_id < 1:
        raise HTTPException(
            status_code=500,
            detail="paper_trade_id is required before paired live submission",
        )
    return paper_trade_id


def _reserve_pair_open_live_submission_or_existing(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: PreviewConfirmationEntry,
    execution_store: ExecutionJournalStore,
) -> ExecutionJournalEntry | None:
    if confirmation.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Preview confirmation entry_id is required before live submission",
        )
    paper_trade_id = _effective_pair_open_submission_paper_trade_id(
        paper_trade=paper_trade,
        confirmation=confirmation,
    )
    existing_entry = execution_store.find_by_paper_trade_preview_hash(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
    )
    if existing_entry is not None:
        return existing_entry
    reservation_result = execution_store.reserve_pair_open_and_live_submission(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        confirmation_entry_id=confirmation.entry_id,
    )
    if reservation_result == "pair_open_conflict":
        existing_entry = execution_store.find_by_paper_trade_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=confirmation.preview_hash,
        )
        if existing_entry is not None:
            return existing_entry
        raise HTTPException(
            status_code=409,
            detail=(
                "A paired live submission is already reserved for this paper trade "
                "and preview hash; manual reconciliation is required before retrying"
            ),
        )
    if reservation_result == "live_conflict":
        existing_entry = execution_store.find_by_confirmation(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
        )
        if existing_entry is not None:
            return existing_entry
        raise HTTPException(
            status_code=409,
            detail=(
                "A live submission is already reserved for this confirmed preview; "
                "manual reconciliation is required before retrying"
            ),
        )
    return None


def _mark_pair_open_live_submission_completed(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: PreviewConfirmationEntry,
    execution_store: ExecutionJournalStore,
    execution_entry_id: int,
) -> None:
    if confirmation.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Preview confirmation entry_id is required before live submission",
        )
    paper_trade_id = _effective_pair_open_submission_paper_trade_id(
        paper_trade=paper_trade,
        confirmation=confirmation,
    )
    execution_store.mark_live_submission_completed(
        confirmation_entry_id=confirmation.entry_id,
        preview_hash=confirmation.preview_hash,
        execution_entry_id=execution_entry_id,
    )
    execution_store.mark_pair_open_live_submission_completed(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        execution_entry_id=execution_entry_id,
    )


async def _execute_guarded_pair_from_confirmation(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: PreviewConfirmationEntry,
    settings: ApiSettings,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    cleanup_confirmation_store: CleanupPreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    cleanup_preview_service: CleanupPreviewRouter,
    cleanup_live_router: CleanupLiveExecutionRouter,
    service: PairedLiveExecutionCoordinator,
    first_venue: str,
    poll_attempts: int,
    poll_interval_seconds: float,
    auto_cleanup: bool,
) -> GuardedPairExecutionResult:
    paper_trade_id = paper_trade.entry_id or 0
    existing_entry = _reserve_pair_open_live_submission_or_existing(
        paper_trade=paper_trade,
        confirmation=confirmation,
        execution_store=execution_store,
    )
    if existing_entry is not None:
        raise HTTPException(
            status_code=409,
            detail=existing_entry.model_dump(mode="json"),
        )
    try:
        primary_execution = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            first_venue=first_venue,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    primary_execution = execution_store.append(primary_execution)
    if primary_execution.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Execution journal append did not return an id",
        )
    _mark_pair_open_live_submission_completed(
        paper_trade=paper_trade,
        confirmation=confirmation,
        execution_store=execution_store,
        execution_entry_id=primary_execution.entry_id,
    )
    try:
        pair_status = await _observe_pair_status_for_execution(
            paper_trade=paper_trade,
            execution=primary_execution,
            settings=settings,
            account_service=account_preflight_service,
            order_state_service=order_state_service,
            observation_store=observation_store,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    cleanup_execution: ExecutionJournalEntry | None = None
    if auto_cleanup:
        cleanup_execution, pair_status = await _run_guarded_auto_cleanup_sequence(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            initial_execution=primary_execution,
            initial_pair_status=pair_status,
            settings=settings,
            execution_store=execution_store,
            observation_store=observation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            account_preflight_service=account_preflight_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            cleanup_live_router=cleanup_live_router,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            confirmation_note="guarded pair auto-cleanup",
            observation_context_prefix="guarded_pair",
        )

    return GuardedPairExecutionResult(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        primary_execution=primary_execution,
        cleanup_execution=cleanup_execution,
        pair_status=pair_status,
    )


async def _execute_guarded_pair_close_from_confirmation(
    *,
    paper_trade: PaperTradeEntry,
    confirmation: PairClosePreviewConfirmationEntry,
    settings: ApiSettings,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    cleanup_confirmation_store: CleanupPreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    cleanup_preview_service: CleanupPreviewRouter,
    cleanup_live_router: CleanupLiveExecutionRouter,
    service: PairCloseLiveExecutionCoordinator,
    first_venue: str,
    poll_attempts: int,
    poll_interval_seconds: float,
    auto_cleanup: bool,
) -> GuardedPairExecutionResult:
    if confirmation.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Pair-close confirmation entry_id is required before live submission",
        )
    confirmation_entry_id = confirmation.entry_id
    paper_trade_id = paper_trade.entry_id or confirmation.paper_trade_id
    if paper_trade_id < 1:
        raise HTTPException(
            status_code=500,
            detail="paper_trade_id is required before pair-close live submission",
        )
    existing_entry = execution_store.find_by_paper_trade_preview_hash(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
    )
    if existing_entry is not None:
        raise HTTPException(
            status_code=409,
            detail=existing_entry.model_dump(mode="json"),
        )
    for venue in {leg.venue for leg in confirmation.preview.legs}:
        try:
            await asyncio.wait_for(
                _ensure_cleanup_live_ready(
                    venue=venue,
                    settings=settings,
                    account_service=account_preflight_service,
                ),
                timeout=PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail=f"Timed out probing cleanup live readiness for {venue}",
            ) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    if not execution_store.reserve_pair_close_live_submission(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        confirmation_entry_id=confirmation_entry_id,
    ):
        existing_entry = execution_store.find_by_paper_trade_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=confirmation.preview_hash,
        )
        if existing_entry is not None:
            raise HTTPException(
                status_code=409,
                detail=existing_entry.model_dump(mode="json"),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "A pair-close live submission is already reserved for this paper trade "
                "and preview hash; manual reconciliation is required before retrying"
            ),
        )

    if not execution_store.reserve_live_submission(
        confirmation_entry_id=confirmation_entry_id,
        preview_hash=confirmation.preview_hash,
    ):
        existing_entry = execution_store.find_by_confirmation(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
        )
        if existing_entry is not None:
            raise HTTPException(
                status_code=409,
                detail=existing_entry.model_dump(mode="json"),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "A live submission is already reserved for this confirmed preview; "
                "manual reconciliation is required before retrying"
            ),
        )

    def _release_pair_close_reservations_or_raise() -> None:
        released_live = execution_store.release_live_submission(
            confirmation_entry_id=confirmation_entry_id,
            preview_hash=confirmation.preview_hash,
        )
        released_pair_close = execution_store.release_pair_close_live_submission(
            paper_trade_id=paper_trade_id,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation_entry_id,
        )
        if released_live and released_pair_close:
            return
        raise HTTPException(
            status_code=409,
            detail=(
                "Pair-close live submission failed before journaling, but its "
                "reservations could not be safely released; manual reconciliation "
                "is required before retrying"
            ),
        )

    try:
        primary_execution = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            first_venue=first_venue,
        )
    except ValueError as exc:
        _release_pair_close_reservations_or_raise()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    primary_execution = execution_store.append(primary_execution)
    if primary_execution.entry_id is None:
        raise HTTPException(
            status_code=500,
            detail="Execution journal append did not return an id",
        )
    execution_store.mark_live_submission_completed(
        confirmation_entry_id=confirmation_entry_id,
        preview_hash=confirmation.preview_hash,
        execution_entry_id=primary_execution.entry_id,
    )
    execution_store.mark_pair_close_live_submission_completed(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        execution_entry_id=primary_execution.entry_id,
    )
    try:
        pair_status = await _observe_pair_status_for_execution(
            paper_trade=paper_trade,
            execution=primary_execution,
            settings=settings,
            account_service=account_preflight_service,
            order_state_service=order_state_service,
            observation_store=observation_store,
            observation_context="guarded_pair_close_poll",
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    cleanup_execution: ExecutionJournalEntry | None = None
    if auto_cleanup:
        cleanup_execution, pair_status = await _run_guarded_auto_cleanup_sequence(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            initial_execution=primary_execution,
            initial_pair_status=pair_status,
            settings=settings,
            execution_store=execution_store,
            observation_store=observation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            account_preflight_service=account_preflight_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            cleanup_live_router=cleanup_live_router,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            confirmation_note="guarded pair close auto-cleanup",
            observation_context_prefix="guarded_pair_close",
        )

    return GuardedPairExecutionResult(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        primary_execution=primary_execution,
        cleanup_execution=cleanup_execution,
        pair_status=pair_status,
    )


def create_app() -> FastAPI:
    """Create the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Warm the shared execution journal store before serving live requests."""

        settings = await _resolve_api_settings_for_app(app)
        if _should_prewarm_execution_journal(settings):
            _execution_journal_store_for_path(settings.database_path).initialize()
        yield

    app = FastAPI(title="carryme", version=APP_VERSION, lifespan=lifespan)

    @app.middleware("http")
    async def require_operator_auth_for_mutations(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.method not in MUTATING_HTTP_METHODS:
            return await call_next(request)

        settings = await _resolve_api_settings_for_request(request)
        if settings.operator_api_key is None:
            return await call_next(request)

        authorization = request.headers.get("Authorization")
        if authorization is None:
            return _operator_auth_error(401, "Missing operator authorization header")

        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            return _operator_auth_error(401, "Malformed operator authorization header")

        if not secrets.compare_digest(
            token,
            settings.operator_api_key.get_secret_value(),
        ):
            return _operator_auth_error(403, "Invalid operator authorization")

        return await call_next(request)

    @app.get("/health", response_model=ServiceHealth)
    def health() -> ServiceHealth:
        return ServiceHealth(
            service=AppDescriptor(
                name=APP_NAME,
                version=APP_VERSION,
                environment=get_app_environment(),
            )
        )

    @app.get("/v1/health", response_model=ServiceHealth)
    def versioned_health() -> ServiceHealth:
        return health()

    def _database_readiness_payload(settings: ApiSettings) -> ServiceReadiness:
        ready = True
        try:
            Database(settings.database_path).ping_with_timeout(
                DEFAULT_DATABASE_PING_TIMEOUT_SECONDS
            )
        except Exception:
            ready = False
        return ServiceReadiness(
            service=AppDescriptor(
                name=APP_NAME,
                version=APP_VERSION,
                environment=settings.environment,
            ),
            status="ready" if ready else "degraded",
            database=DatabaseReadiness(
                target=settings.database_target,
                ready=ready,
            ),
        )

    @app.get("/ready", response_model=ServiceReadiness)
    def readiness(
        response: Response,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
    ) -> ServiceReadiness:
        payload = _database_readiness_payload(settings)
        if payload.status != "ready":
            response.status_code = 503
        return payload

    @app.get("/v1/ready", response_model=ServiceReadiness)
    def versioned_readiness(
        response: Response,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
    ) -> ServiceReadiness:
        return readiness(response, settings)

    @app.get("/v1/reference/fees/{venue}", response_model=list[TradingFeeProfile])
    def fee_profiles(venue: str) -> list[TradingFeeProfile]:
        try:
            return list_fee_profiles(venue)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/watchlist", response_model=WatchlistDocument)
    def watchlist(
        store: Annotated[WatchlistStore, Depends(get_watchlist_store)],
    ) -> WatchlistDocument:
        try:
            pairs = store.load()
        except FileNotFoundError:
            pairs = []
        return WatchlistDocument(pairs=pairs)

    @app.put("/v1/watchlist", response_model=WatchlistDocument)
    def replace_watchlist(
        document: Annotated[WatchlistDocument, Body(...)],
        store: Annotated[WatchlistStore, Depends(get_watchlist_store)],
    ) -> WatchlistDocument:
        return WatchlistDocument(pairs=store.replace(document.pairs))

    @app.get("/v1/history/funding-pairs", response_model=list[OpportunityRecord])
    def history(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[OpportunityRecord]:
        limit = _validated_history_limit("limit", limit)
        try:
            return store.list_recent(limit=limit, label=label)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/history/funding-pairs/latest", response_model=list[OpportunityRecord])
    def latest_history(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 20,
        sample: int = 200,
        label: str | None = None,
    ) -> list[OpportunityRecord]:
        limit = _validated_history_limit("limit", limit)
        sample = max(limit, _validated_history_limit("sample", sample))
        records = store.list_recent(limit=sample, label=label)
        return latest_records_by_label(records, limit=limit)

    @app.get("/v1/history/funding-pairs/ranked", response_model=list[OpportunityRecord])
    def ranked_history(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 20,
        sample: int = 200,
        label: str | None = None,
    ) -> list[OpportunityRecord]:
        limit = _validated_history_limit("limit", limit)
        sample = max(limit, _validated_history_limit("sample", sample))
        records = store.list_recent(limit=sample, label=label)
        latest = latest_records_by_label(records, limit=sample)
        return rank_history_records(latest, limit=limit)

    @app.get("/v1/history/funding-pairs/candidates", response_model=list[OpportunityRecord])
    def candidate_history(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 20,
        sample: int = 200,
        label: str | None = None,
        min_one_day_net_edge_after_entry: float = 0.0,
        min_capacity_notional: float = 0.0,
    ) -> list[OpportunityRecord]:
        limit = _validated_history_limit("limit", limit)
        sample = max(limit, _validated_history_limit("sample", sample))
        min_one_day_net_edge_after_entry = _validated_non_negative_threshold(
            "min_one_day_net_edge_after_entry",
            min_one_day_net_edge_after_entry,
        )
        min_capacity_notional = _validated_non_negative_threshold(
            "min_capacity_notional",
            min_capacity_notional,
        )
        records = store.list_recent(limit=sample, label=label)
        latest = latest_records_by_label(records, limit=sample)
        candidates = filter_candidate_records(
            latest,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
        )
        return rank_history_records(candidates, limit=limit)

    @app.get("/v1/alerts/candidates", response_model=list[CandidateAlertEvent])
    def candidate_alerts(
        store: Annotated[CandidateAlertStore, Depends(get_candidate_alert_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[CandidateAlertEvent]:
        limit = _validated_history_limit("limit", limit)
        try:
            return store.list_recent(limit=limit, label=label)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/alerts/executions", response_model=list[ExecutionAlertEvent])
    def execution_alerts(
        store: Annotated[ExecutionAlertStore, Depends(get_execution_alert_store)],
        limit: int = 50,
        paper_trade_id: int | None = None,
    ) -> list[ExecutionAlertEvent]:
        limit = _validated_history_limit("limit", limit)
        try:
            return store.list_recent(limit=limit, paper_trade_id=paper_trade_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/alerts/system-state", response_model=list[SystemStateAlertEvent])
    def system_state_alerts(
        store: Annotated[SystemStateAlertStore, Depends(get_system_state_alert_store)],
        limit: int = 50,
        venue: str | None = None,
    ) -> list[SystemStateAlertEvent]:
        try:
            return store.list_recent(limit=limit, venue=venue)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/intents/funding-pairs", response_model=list[FundingPairTradeIntent])
    def funding_pair_intents(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 20,
        sample: int = 200,
        label: str | None = None,
        capacity_fraction: float = 0.25,
        max_target_notional: float = 1000.0,
        min_one_day_net_edge_after_entry: float = 0.0,
        min_capacity_notional: float = 0.0,
        max_break_even_days_entry: float | None = None,
    ) -> list[FundingPairTradeIntent]:
        return _build_trade_intent_candidates(
            store,
            limit=limit,
            sample=sample,
            label=label,
            capacity_fraction=capacity_fraction,
            max_target_notional=max_target_notional,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
            max_break_even_days_entry=max_break_even_days_entry,
        )

    @app.get("/v1/intents/funding-pair", response_model=FundingPairTradeIntent)
    def funding_pair_intent(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        sample: int = 200,
        label: str | None = None,
        capacity_fraction: float = 0.25,
        max_target_notional: float = 1000.0,
        min_one_day_net_edge_after_entry: float = 0.0,
        min_capacity_notional: float = 0.0,
        max_break_even_days_entry: float | None = None,
    ) -> FundingPairTradeIntent:
        selected = funding_pair_intents(
            store=store,
            limit=1,
            sample=sample,
            label=label,
            capacity_fraction=capacity_fraction,
            max_target_notional=max_target_notional,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
            max_break_even_days_entry=max_break_even_days_entry,
        )
        if not selected:
            raise HTTPException(
                status_code=404,
                detail="No trade intent candidate matched the requested filters",
            )
        return selected[0]

    @app.get("/v1/paper-trades", response_model=list[PaperTradeEntry])
    def paper_trades(
        store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[PaperTradeEntry]:
        limit = _validated_history_limit("limit", limit)
        try:
            return store.list_recent(limit=limit, label=label)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/opportunities/route-approvals", response_model=list[RouteApprovalEntry])
    def route_approvals(
        service: Annotated[RouteApprovalService, Depends(get_route_approval_service)],
        limit: int = 50,
        label: str | None = None,
        canonical_symbol: str | None = None,
        approved: bool | None = None,
    ) -> list[RouteApprovalEntry]:
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return service.list_recent(
            limit=limit,
            label=label,
            canonical_symbol=canonical_symbol,
            approved=approved,
        )

    @app.get(
        "/v1/opportunities/funding-universe/canary/approved-basket",
        response_model=ApprovedCanaryBasketPlan,
    )
    async def approved_canary_basket(
        service: Annotated[OpportunityUniverseService, Depends(get_opportunity_universe_service)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        venues: Annotated[list[str] | None, Query()] = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 10,
    ) -> ApprovedCanaryBasketPlan:
        try:
            return await _scan_approved_canary_basket_plan(
                universe_service=service,
                approval_service=approval_service,
                venues=venues,
                extended_fee_profile=extended_fee_profile,
                paradex_fee_profile=paradex_fee_profile,
                hyperliquid_fee_profile=hyperliquid_fee_profile,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/opportunities/funding-universe/canary/snapshots",
        response_model=list[ApprovedCanarySnapshot],
    )
    def approved_canary_snapshots(
        store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[ApprovedCanarySnapshot]:
        limit = _validated_list_limit("limit", limit)
        return store.list_recent(limit=limit, label=label)

    @app.get(
        "/v1/opportunities/funding-universe/canary/snapshot-summaries",
        response_model=list[ApprovedCanarySnapshotSummary],
    )
    def approved_canary_snapshot_summaries(
        store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[ApprovedCanarySnapshotSummary]:
        limit = _validated_list_limit("limit", limit)
        return [
            _summarize_approved_canary_snapshot_payload(
                snapshot_id=stored_id,
                snapshot_payload=snapshot_payload,
            )
            for stored_id, snapshot_payload in store.list_recent_payloads(
                limit=limit,
                label=label,
            )
        ]

    @app.get(
        "/v1/opportunities/funding-universe/canary/latest-approved",
        response_model=ApprovedCanarySnapshot,
    )
    def latest_approved_canary_snapshot(
        store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        label: str | None = None,
    ) -> ApprovedCanarySnapshot:
        snapshot = store.latest(label=label)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="No approved canary snapshot found")
        return snapshot

    @app.get(
        "/v1/executions/live/canary-cycle/launch-ready-snapshots",
        response_model=list[LaunchReadyCanarySnapshot],
    )
    def launch_ready_canary_snapshots(
        store: Annotated[LaunchReadyCanaryStore, Depends(get_launch_ready_canary_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[LaunchReadyCanarySnapshot]:
        limit = _validated_list_limit("limit", limit)
        return store.list_recent(limit=limit, label=label)

    @app.get(
        "/v1/executions/live/canary-cycle/launch-ready-snapshot-summaries",
        response_model=list[LaunchReadyCanarySnapshotSummary],
    )
    def launch_ready_canary_snapshot_summaries(
        store: Annotated[LaunchReadyCanaryStore, Depends(get_launch_ready_canary_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[LaunchReadyCanarySnapshotSummary]:
        limit = _validated_list_limit("limit", limit)
        return [
            _summarize_launch_ready_canary_snapshot_payload(
                launch_ready_snapshot_id=stored_id,
                snapshot_payload=snapshot_payload,
            )
            for stored_id, snapshot_payload in store.list_recent_payloads(
                limit=limit,
                label=label,
            )
        ]

    @app.get(
        "/v1/executions/live/canary-cycle/latest-launch-ready",
        response_model=LaunchReadyCanarySnapshot,
    )
    def latest_launch_ready_canary_snapshot(
        store: Annotated[LaunchReadyCanaryStore, Depends(get_launch_ready_canary_store)],
        label: str | None = None,
    ) -> LaunchReadyCanarySnapshot:
        snapshot = store.latest(label=label)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="No launch-ready canary snapshot found")
        return snapshot

    @app.get(
        "/v1/executions/live/canary-cycle/latest-stable-launch-ready",
        response_model=LaunchReadyCanaryStability,
    )
    def latest_stable_launch_ready_canary_snapshot(
        store: Annotated[LaunchReadyCanaryStore, Depends(get_launch_ready_canary_store)],
        label: str | None = None,
        max_snapshot_age_seconds: int = 300,
        min_snapshot_count: int = 2,
        min_stable_seconds: float = 30.0,
    ) -> LaunchReadyCanaryStability:
        return _build_launch_ready_canary_stability(
            store=store,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            min_snapshot_count=min_snapshot_count,
            min_stable_seconds=min_stable_seconds,
        )

    @app.get(
        "/v1/automation/readiness",
        response_model=ProductionAutomationReadiness,
    )
    def production_automation_readiness(
        approved_store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        launch_ready_store: Annotated[
            LaunchReadyCanaryStore,
            Depends(get_launch_ready_canary_store),
        ],
        stable_launch_store: Annotated[
            StableCanaryLaunchStore,
            Depends(get_stable_canary_launch_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        checked_at: Annotated[datetime, Depends(get_automation_readiness_checked_at)],
        label: str | None = None,
    ) -> ProductionAutomationReadiness:
        blocking_reasons: list[str] = []

        latest_approved = approved_store.latest(label=label)
        approved_summary: AutomationSnapshotSummary | None = None
        if latest_approved is None:
            _append_unique_blocking_reason(blocking_reasons, "No approved canary snapshot found")
        else:
            approved_summary = _automation_snapshot_summary(
                snapshot_id=latest_approved.snapshot_id,
                label=latest_approved.label,
                captured_at=latest_approved.captured_at,
                now=checked_at,
            )
            if approved_summary.age_seconds < 0:
                _append_unique_blocking_reason(
                    blocking_reasons,
                    "Approved canary snapshot timestamp is in the future",
                )
            elif approved_summary.age_seconds > AUTOMATION_READINESS_MAX_SNAPSHOT_AGE_SECONDS:
                _append_unique_blocking_reason(
                    blocking_reasons,
                    (
                        "Approved canary snapshot is stale "
                        f"({approved_summary.age_seconds:.1f}s > "
                        f"{AUTOMATION_READINESS_MAX_SNAPSHOT_AGE_SECONDS}s)"
                    ),
                )

        latest_launch_ready = launch_ready_store.latest(label=label)
        launch_ready_summary: AutomationSnapshotSummary | None = None
        if latest_launch_ready is None:
            _append_unique_blocking_reason(
                blocking_reasons,
                "No launch-ready canary snapshot found",
            )
        else:
            launch_ready_summary = _automation_snapshot_summary(
                snapshot_id=latest_launch_ready.launch_ready_snapshot_id,
                label=latest_launch_ready.label,
                captured_at=latest_launch_ready.captured_at,
                now=checked_at,
            )
            effective_max_age_seconds = min(
                AUTOMATION_READINESS_MAX_SNAPSHOT_AGE_SECONDS,
                latest_launch_ready.max_snapshot_age_seconds,
            )
            if launch_ready_summary.age_seconds < 0:
                _append_unique_blocking_reason(
                    blocking_reasons,
                    "Launch-ready canary snapshot timestamp is in the future",
                )
            elif launch_ready_summary.age_seconds > effective_max_age_seconds:
                _append_unique_blocking_reason(
                    blocking_reasons,
                    (
                        "Launch-ready canary snapshot is stale "
                        f"({launch_ready_summary.age_seconds:.1f}s > "
                        f"{effective_max_age_seconds}s)"
                    ),
                )
            if not latest_launch_ready.system_state.ready:
                _append_unique_blocking_reason(
                    blocking_reasons,
                    "Latest launch-ready canary snapshot is not ready for live execution",
                )

        stability: LaunchReadyCanaryStability | None = None
        try:
            stability = _build_launch_ready_canary_stability(
                store=launch_ready_store,
                label=label,
                max_snapshot_age_seconds=AUTOMATION_READINESS_MAX_SNAPSHOT_AGE_SECONDS,
                min_snapshot_count=AUTOMATION_READINESS_MIN_SNAPSHOT_COUNT,
                min_stable_seconds=AUTOMATION_READINESS_MIN_STABLE_SECONDS,
                now=checked_at,
            )
        except HTTPException as exc:
            if exc.status_code in {404, 409}:
                _append_unique_blocking_reason(blocking_reasons, exc.detail)
            else:
                raise

        latest_stable_launch = None
        if (
            stability is not None
            and stability.snapshot.launch_ready_snapshot_id is not None
        ):
            latest_stable_launch = stable_launch_store.latest_for_snapshot(
                stability.snapshot.launch_ready_snapshot_id
            )
            if latest_stable_launch is not None and latest_stable_launch.status != "shadowed":
                _append_unique_blocking_reason(
                    blocking_reasons,
                    (
                        "Latest stable launch-ready snapshot already launched by worker "
                        f"as paper trade {latest_stable_launch.paper_trade_id}"
                    ),
                )

        active_live_execution_count = (
            _count_blocking_live_executions_for_automation_readiness(
                execution_store=execution_store,
                observation_store=observation_store,
                now=checked_at,
                max_observation_age_seconds=AUTOMATION_READINESS_MAX_OBSERVATION_AGE_SECONDS,
                scan_limit=AUTOMATION_READINESS_ACTIVE_EXECUTION_SCAN_LIMIT,
            )
        )
        if active_live_execution_count > AUTOMATION_READINESS_MAX_ACTIVE_LIVE_EXECUTIONS:
            _append_unique_blocking_reason(
                blocking_reasons,
                (
                    "Active live executions still require monitoring before unattended "
                    f"launch (max_allowed={AUTOMATION_READINESS_MAX_ACTIVE_LIVE_EXECUTIONS}, "
                    f"current={active_live_execution_count})"
                ),
            )

        ready = not blocking_reasons
        return ProductionAutomationReadiness(
            checked_at=checked_at,
            ready=ready,
            status="ready" if ready else "blocked",
            blocking_reasons=blocking_reasons,
            approved_snapshot=approved_summary,
            launch_ready_snapshot=launch_ready_summary,
            stable_launch_ready=stability is not None,
            stable_launch_consecutive_snapshots=(
                stability.consecutive_snapshots if stability is not None else None
            ),
            stable_launch_stable_seconds=(
                stability.stable_seconds if stability is not None else None
            ),
            latest_stable_launch=latest_stable_launch,
            active_live_execution_count=active_live_execution_count,
            max_active_live_executions=AUTOMATION_READINESS_MAX_ACTIVE_LIVE_EXECUTIONS,
        )

    @app.get(
        "/v1/alerts/approved-canaries",
        response_model=list[ApprovedCanaryAlertEvent],
    )
    def approved_canary_alerts(
        store: Annotated[
            ApprovedCanaryAlertStore,
            Depends(get_approved_canary_alert_store),
        ],
        limit: int = 50,
        label: str | None = None,
    ) -> list[ApprovedCanaryAlertEvent]:
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return store.list_recent(limit=limit, label=label)

    @app.get(
        "/v1/alerts/stable-launch-ready",
        response_model=list[StableLaunchReadyAlertEvent],
    )
    def stable_launch_ready_alerts(
        store: Annotated[
            StableLaunchReadyAlertStore,
            Depends(get_stable_launch_ready_alert_store),
        ],
        limit: int = 50,
        label: str | None = None,
    ) -> list[StableLaunchReadyAlertEvent]:
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return store.list_recent(limit=limit, label=label)

    @app.put(
        "/v1/opportunities/route-approvals/{label}",
        response_model=RouteApprovalEntry,
    )
    def upsert_route_approval(
        label: str,
        service: Annotated[RouteApprovalService, Depends(get_route_approval_service)],
        payload: Annotated[RouteApprovalUpsert, Body()],
    ) -> RouteApprovalEntry:
        return service.upsert(label=label, payload=payload)

    @app.get(
        "/v1/accounting/balance-snapshots",
        response_model=list[VenueBalanceSnapshot],
    )
    def balance_snapshots(
        service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        limit: int = 100,
        paper_trade_id: int | None = None,
        label: str | None = None,
        stage: str | None = None,
        venue: str | None = None,
    ) -> list[VenueBalanceSnapshot]:
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return service.list_snapshots(
            limit=limit,
            paper_trade_id=paper_trade_id,
            label=label,
            stage=stage,
            venue=venue,
        )

    @app.post(
        "/v1/accounting/balance-snapshots/from-paper-trade/{paper_trade_id}",
        response_model=list[VenueBalanceSnapshot],
    )
    async def capture_balance_snapshots_for_paper_trade(
        paper_trade_id: int,
        stage: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        note: str | None = None,
    ) -> list[VenueBalanceSnapshot]:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        preflight = await account_service.probe_paper_trade(
            paper_trade,
            _build_account_preflight_configs(settings),
        )
        unauthenticated = [item.venue for item in preflight.venues if not item.authenticated]
        if unauthenticated:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Balance snapshot capture requires authenticated venue reads for: "
                    + ", ".join(sorted(unauthenticated))
                ),
            )
        return service.capture_paper_trade(
            paper_trade=paper_trade,
            preflight=preflight,
            stage=stage,
            note=note,
        )

    @app.get(
        "/v1/accounting/balance-delta/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeBalanceDelta,
    )
    def balance_delta_for_paper_trade(
        paper_trade_id: int,
        service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
    ) -> PaperTradeBalanceDelta:
        summary = service.summarize_paper_trade(paper_trade_id)
        if summary is None:
            raise HTTPException(status_code=404, detail="No balance snapshots found")
        return summary

    @app.get(
        "/v1/accounting/balance-attribution/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeBalanceAttribution,
    )
    def balance_attribution_for_paper_trade(
        paper_trade_id: int,
        service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
    ) -> PaperTradeBalanceAttribution:
        summary = service.summarize_paper_trade_attribution(paper_trade_id)
        if summary is None:
            raise HTTPException(status_code=404, detail="No balance snapshots found")
        return summary

    @app.post("/v1/paper-trades/from-intent", response_model=PaperTradeEntry)
    def create_paper_trade_from_intent(
        history_store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        sample: int = 200,
        label: str | None = None,
        capacity_fraction: float = 0.25,
        max_target_notional: float = 1000.0,
        min_one_day_net_edge_after_entry: float = 0.0,
        min_capacity_notional: float = 0.0,
        max_break_even_days_entry: float | None = None,
        note: str | None = None,
    ) -> PaperTradeEntry:
        intents = _build_trade_intent_candidates(
            history_store,
            limit=1,
            sample=sample,
            label=label,
            capacity_fraction=capacity_fraction,
            max_target_notional=max_target_notional,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
            max_break_even_days_entry=max_break_even_days_entry,
        )
        if not intents:
            raise HTTPException(
                status_code=404,
                detail="No paper trade intent matched the requested filters",
            )
        entry = PaperTradeEntry(
            created_at=datetime.now(UTC),
            intent=intents[0],
            note=note,
        )
        return paper_store.append(entry)

    @app.post("/v1/paper-trades/from-canary", response_model=PaperTradeEntry)
    async def create_paper_trade_from_canary(
        universe_service: Annotated[
            OpportunityUniverseService, Depends(get_opportunity_universe_service)
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        venues: Annotated[list[str] | None, Query()] = None,
        label: str | None = None,
        desired_notional: float | None = None,
        note: str | None = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 10,
    ) -> PaperTradeEntry:
        selected_venues = venues or ["extended", "paradex", "hyperliquid"]
        candidates = await _run_bounded_universe_scan(
            "paper trade approved canary scan",
            lambda: universe_service.scan_canary_candidates(
                venues=selected_venues,
                fee_profile_overrides=_build_fee_profile_overrides(
                    extended_fee_profile=extended_fee_profile,
                    paradex_fee_profile=paradex_fee_profile,
                    hyperliquid_fee_profile=hyperliquid_fee_profile,
                ),
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=limit,
            ),
        )
        approved_candidates = approval_service.filter_approved_canary_candidates(candidates)
        if label is not None:
            approved_candidates = [
                item
                for item in approved_candidates
                if build_pair_spec_from_universe_opportunity(item.opportunity).label == label
            ]
        if not approved_candidates:
            raise HTTPException(
                status_code=404,
                detail="No approved canary candidate matched the requested filters",
            )
        selected = approved_candidates[0]
        capped_notional = selected.suggested_canary_notional
        if desired_notional is not None:
            capped_notional = min(capped_notional, desired_notional)
        if capped_notional <= 0:
            raise HTTPException(
                status_code=409,
                detail="Approved route does not allow a positive live notional",
            )
        record = build_opportunity_record_from_universe_opportunity(
            recorded_at=datetime.now(UTC),
            opportunity=selected.opportunity,
        )
        intent = build_trade_intent(
            record,
            capacity_fraction=1.0,
            max_target_notional=capped_notional,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=0.0,
        )
        entry = PaperTradeEntry(
            created_at=datetime.now(UTC),
            intent=intent,
            note=note,
        )
        return paper_store.append(entry)

    @app.get("/v1/executions", response_model=list[ExecutionJournalEntry])
    def executions(
        store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[ExecutionJournalEntry]:
        limit = _validated_history_limit("limit", limit)
        try:
            return store.list_recent(limit=limit, label=label)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get(
        "/v1/executions/order-state/latest/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionOrderState,
    )
    async def latest_execution_order_state_for_paper_trade(
        paper_trade_id: int,
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
    ) -> ExecutionOrderState:
        execution = execution_store.latest_for_paper_trade(paper_trade_id)
        if execution is None:
            raise HTTPException(
                status_code=404,
                detail=f"No execution journal entry matched paper trade {paper_trade_id}",
            )
        return await service.observe_execution(execution)

    @app.get(
        "/v1/executions/observations/latest/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionObservationEntry,
    )
    def latest_execution_observation_for_paper_trade(
        paper_trade_id: int,
        store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
    ) -> ExecutionObservationEntry:
        observation = store.latest_for_paper_trade(paper_trade_id)
        if observation is None:
            raise HTTPException(
                status_code=404,
                detail=f"No execution observation matched paper trade {paper_trade_id}",
            )
        return observation

    @app.get(
        "/v1/executions/reconciliation/latest/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionReconciliation,
    )
    async def reconcile_latest_execution_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        service: Annotated[AccountPreflightService, Depends(get_account_preflight_service)],
    ) -> ExecutionReconciliation:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        execution = execution_store.latest_for_paper_trade(paper_trade_id)
        if execution is None:
            raise HTTPException(
                status_code=404,
                detail=f"No execution journal entry matched paper trade {paper_trade_id}",
            )
        account_preflight = await service.probe_paper_trade(
            paper_trade,
            _build_account_preflight_configs(settings),
        )
        return reconcile_execution(execution, account_preflight)

    @app.get(
        "/v1/executions/pair-status/latest/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionPairStatus,
    )
    async def latest_execution_pair_status_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
    ) -> ExecutionPairStatus:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        execution = execution_store.latest_for_paper_trade(paper_trade_id)
        if execution is None:
            raise HTTPException(
                status_code=404,
                detail=f"No execution journal entry matched paper trade {paper_trade_id}",
            )
        account_preflight = await account_service.probe_paper_trade(
            paper_trade,
            _build_account_preflight_configs(settings),
        )
        reconciliation = reconcile_execution(execution, account_preflight)
        order_state = await order_state_service.observe_execution(execution)
        return build_execution_pair_status(execution, order_state, reconciliation)

    @app.get(
        "/v1/executions/cleanup-preview/latest/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionCleanupPreview,
    )
    async def latest_execution_cleanup_preview_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
    ) -> ExecutionCleanupPreview:
        _, _, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        return cleanup_preview

    @app.get(
        "/v1/executions/pair-close-preview/latest/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionPairClosePreview,
    )
    async def latest_pair_close_preview_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        pair_close_service: Annotated[
            PairClosePreviewService,
            Depends(get_pair_close_preview_service),
        ],
    ) -> ExecutionPairClosePreview:
        _, _, _, pair_close_preview = await _build_pair_close_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            pair_close_service=pair_close_service,
        )
        return pair_close_preview

    @app.get(
        "/v1/executions/cleanup-preview-confirmations",
        response_model=list[CleanupPreviewConfirmationEntry],
    )
    def cleanup_preview_confirmations(
        store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        limit: int = 50,
        label: str | None = None,
        paper_trade_id: int | None = None,
    ) -> list[CleanupPreviewConfirmationEntry]:
        limit = _validated_history_limit("limit", limit)
        return store.list_recent(limit=limit, label=label, paper_trade_id=paper_trade_id)

    @app.get(
        "/v1/executions/pair-close-preview-confirmations",
        response_model=list[PairClosePreviewConfirmationEntry],
    )
    def pair_close_preview_confirmations(
        store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        limit: int = 50,
        label: str | None = None,
        paper_trade_id: int | None = None,
    ) -> list[PairClosePreviewConfirmationEntry]:
        limit = _validated_history_limit("limit", limit)
        return store.list_recent(limit=limit, label=label, paper_trade_id=paper_trade_id)

    @app.post(
        "/v1/executions/cleanup-preview-confirmations/latest/from-paper-trade/{paper_trade_id}",
        response_model=CleanupPreviewConfirmationEntry,
    )
    async def confirm_execution_cleanup_preview(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        note: str | None = None,
    ) -> CleanupPreviewConfirmationEntry:
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        paper_trade, _execution, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        refreshed_note = _build_cleanup_confirmation_note(
            note=note,
            requested_preview_hash=normalized_preview_hash,
            current_preview_hash=cleanup_preview.preview_hash,
        )
        return _append_cleanup_confirmation_entry(
            paper_trade=paper_trade,
            cleanup_preview=cleanup_preview,
            confirmation_store=confirmation_store,
            note=refreshed_note,
        )

    @app.post(
        "/v1/executions/pair-close-preview-confirmations/latest/from-paper-trade/{paper_trade_id}",
        response_model=PairClosePreviewConfirmationEntry,
    )
    async def confirm_execution_pair_close_preview(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        pair_close_service: Annotated[
            PairClosePreviewService,
            Depends(get_pair_close_preview_service),
        ],
        confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        preview: Annotated[ExecutionPairClosePreview | None, Body()] = None,
        note: str | None = None,
    ) -> PairClosePreviewConfirmationEntry:
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        paper_trade, execution, pair_status = await _build_pair_close_status_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
        )
        if paper_trade.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Paper trade entry_id is required before pair-close confirmation",
            )
        resolved_paper_trade_id = paper_trade.entry_id
        if preview is not None:
            if (
                pair_status.derived_state != "hedged"
                or pair_status.recommended_action != "monitor_open_hedge"
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Current execution state no longer supports confirming a pair-close preview"
                    ),
                )
            try:
                expected_preview_venues = select_pair_close_preview_venues(
                    execution,
                    pair_status,
                )
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            _validate_client_pair_close_preview_for_confirmation(
                preview=preview,
                preview_hash=normalized_preview_hash,
                paper_trade=paper_trade,
                execution=execution,
                expected_preview_venues=expected_preview_venues,
            )
            confirmation = PairClosePreviewConfirmationEntry(
                confirmed_at=datetime.now(UTC),
                paper_trade_id=resolved_paper_trade_id,
                label=paper_trade.intent.label,
                preview_hash=normalized_preview_hash,
                preview=preview,
                note=note,
            )
            return confirmation_store.append(confirmation)
        _, _, _, canonical_preview = await _build_pair_close_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            pair_close_service=pair_close_service,
        )
        if canonical_preview.preview_hash != normalized_preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Preview hash did not match the current pair close preview",
            )
        confirmation = PairClosePreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=resolved_paper_trade_id,
            label=paper_trade.intent.label,
            preview_hash=canonical_preview.preview_hash,
            preview=canonical_preview,
            note=note,
        )
        return confirmation_store.append(confirmation)

    @app.get("/v1/executions/preflight/venues", response_model=list[VenueExecutionPreflight])
    def execution_preflight_venues(
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
    ) -> list[VenueExecutionPreflight]:
        return build_venue_execution_preflights(build_live_execution_configs(settings))

    @app.get(
        "/v1/executions/account-preflight/venues",
        response_model=list[VenueAccountPreflight],
    )
    async def execution_account_preflight_venues(
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        service: Annotated[AccountPreflightService, Depends(get_account_preflight_service)],
        response: Response,
    ) -> list[VenueAccountPreflight]:
        response.headers["Cache-Control"] = "no-store"
        try:
            return await service.probe_venues(_build_account_preflight_configs(settings))
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/executions/preflight/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeExecutionPreflight,
    )
    def execution_preflight_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
    ) -> PaperTradeExecutionPreflight:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        return build_paper_trade_execution_preflight(
            paper_trade,
            build_live_execution_configs(settings),
        )

    @app.get(
        "/v1/executions/account-preflight/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeAccountPreflight,
    )
    async def execution_account_preflight_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        service: Annotated[AccountPreflightService, Depends(get_account_preflight_service)],
        response: Response,
    ) -> PaperTradeAccountPreflight:
        response.headers["Cache-Control"] = "no-store"
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        try:
            return await service.probe_paper_trade(
                paper_trade,
                _build_account_preflight_configs(settings),
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/executions/system-state/venues",
        response_model=list[VenueSystemState],
    )
    async def execution_system_state_venues(
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        service: Annotated[SystemStateService, Depends(get_system_state_service)],
    ) -> list[VenueSystemState]:
        return await service.probe_venues(_build_system_state_configs(settings))

    @app.get(
        "/v1/executions/system-state/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeSystemState,
    )
    async def execution_system_state_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        service: Annotated[SystemStateService, Depends(get_system_state_service)],
    ) -> PaperTradeSystemState:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        return await service.probe_paper_trade(
            paper_trade,
            _build_system_state_configs(settings),
        )

    @app.get(
        "/v1/executions/readiness/from-paper-trade/{paper_trade_id}",
        response_model=LiveSubmissionReadiness,
    )
    async def execution_readiness_for_paper_trade(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        service: Annotated[AccountPreflightService, Depends(get_account_preflight_service)],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        response: Response,
    ) -> LiveSubmissionReadiness:
        response.headers["Cache-Control"] = "no-store"
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        try:
            return await _build_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=service,
                system_state_service=system_state_service,
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get(
        "/v1/executions/preview/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeOrderPreview,
    )
    async def execution_preview_for_paper_trade(
        paper_trade_id: int,
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        service: Annotated[OrderPreviewService, Depends(get_order_preview_service)],
        slippage_tolerance_bps: int = 10,
    ) -> PaperTradeOrderPreview:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        try:
            return await service.preview_paper_trade(
                paper_trade,
                slippage_tolerance_bps=slippage_tolerance_bps,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/executions/preview-confirmations",
        response_model=list[PreviewConfirmationEntry],
    )
    def preview_confirmations(
        store: Annotated[PreviewConfirmationStore, Depends(get_preview_confirmation_store)],
        limit: int = 50,
        label: str | None = None,
        paper_trade_id: int | None = None,
    ) -> list[PreviewConfirmationEntry]:
        limit = _validated_history_limit("limit", limit)
        try:
            return store.list_recent(limit=limit, label=label, paper_trade_id=paper_trade_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v1/executions/preview-confirmations/from-paper-trade/{paper_trade_id}",
        response_model=PreviewConfirmationEntry,
    )
    async def confirm_paper_trade_preview(
        paper_trade_id: int,
        request: Annotated[ConfirmPreviewRequest, Body(...)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        service: Annotated[OrderPreviewService, Depends(get_order_preview_service)],
    ) -> PreviewConfirmationEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        normalized_preview_hash = request.preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        try:
            preview = await service.preview_paper_trade(
                paper_trade,
                slippage_tolerance_bps=request.slippage_tolerance_bps,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if preview.preview_hash != normalized_preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Preview hash did not match the current unsigned order preview",
            )

        confirmation = PreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=paper_trade_id,
            label=paper_trade.intent.label,
            preview_hash=preview.preview_hash,
            preview=preview,
            note=request.note,
        )
        try:
            return confirmation_store.append(confirmation)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post(
        "/v1/executions/mock/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    def execute_saved_paper_trade(
        paper_trade_id: int,
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        adapter: Annotated[MockExecutionAdapter, Depends(get_mock_execution_adapter)],
    ) -> ExecutionJournalEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        journal_entry = adapter.submit(paper_trade)
        return execution_store.append(journal_entry)

    @app.post(
        "/v1/executions/live/paradex/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_saved_paper_trade_on_paradex(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        service: Annotated[
            ParadexLiveExecutionService,
            Depends(get_paradex_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )

        try:
            readiness, confirmation = await _build_venue_scoped_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                venue="paradex",
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
                system_state_service=system_state_service,
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        if confirmation is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            )
        existing_entry = _reserve_pair_open_live_submission_or_existing(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
        )
        if existing_entry is not None:
            raise HTTPException(
                status_code=409,
                detail=existing_entry.model_dump(mode="json"),
            )

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        _mark_pair_open_live_submission_completed(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/extended/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_saved_paper_trade_on_extended(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        service: Annotated[
            ExtendedLiveExecutionService,
            Depends(get_extended_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )

        try:
            readiness, confirmation = await _build_venue_scoped_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                venue="extended",
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
                system_state_service=system_state_service,
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        if confirmation is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            )
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Preview confirmation entry_id is required before live submission",
            )
        if not execution_store.reserve_live_submission(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
        ):
            existing_entry = execution_store.find_by_confirmation(
                confirmation_entry_id=confirmation.entry_id,
                preview_hash=confirmation.preview_hash,
            )
            if existing_entry is not None:
                raise HTTPException(
                    status_code=409,
                    detail=existing_entry.model_dump(mode="json"),
                )
            raise HTTPException(
                status_code=409,
                detail=(
                    "A live submission is already reserved for this confirmed preview; "
                    "manual reconciliation is required before retrying"
                ),
            )

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        execution_store.mark_live_submission_completed(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/hyperliquid/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_saved_paper_trade_on_hyperliquid(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        service: Annotated[
            HyperliquidLiveExecutionService,
            Depends(get_hyperliquid_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )

        try:
            readiness, confirmation = await _build_venue_scoped_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                venue="hyperliquid",
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
                system_state_service=system_state_service,
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        if confirmation is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            )
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Preview confirmation entry_id is required before live submission",
            )
        if not execution_store.reserve_live_submission(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
        ):
            existing_entry = execution_store.find_by_confirmation(
                confirmation_entry_id=confirmation.entry_id,
                preview_hash=confirmation.preview_hash,
            )
            if existing_entry is not None:
                raise HTTPException(
                    status_code=409,
                    detail=existing_entry.model_dump(mode="json"),
                )
            raise HTTPException(
                status_code=409,
                detail=(
                    "A live submission is already reserved for this confirmed preview; "
                    "manual reconciliation is required before retrying"
                ),
            )

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        execution_store.mark_live_submission_completed(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/extended/cleanup/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_extended_cleanup_for_paper_trade(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        live_service: Annotated[
            ExtendedLiveExecutionService,
            Depends(get_extended_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )
        await _ensure_cleanup_live_ready(
            venue="extended",
            settings=settings,
            account_service=account_service,
        )
        paper_trade, _, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        if cleanup_preview.leg.venue != "extended":
            raise HTTPException(
                status_code=409,
                detail="Current cleanup preview targets paradex, not extended",
            )
        try:
            confirmation = _resolve_cleanup_confirmation_for_live_submit(
                paper_trade=paper_trade,
                cleanup_preview=cleanup_preview,
                confirmation_store=confirmation_store,
                requested_preview_hash=normalized_preview_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        existing_entry = _reserve_cleanup_live_submission_or_existing(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
        )
        if existing_entry is not None:
            raise HTTPException(status_code=409, detail=existing_entry.model_dump(mode="json"))
        try:
            journal_entry = await live_service.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            _release_cleanup_live_submission_reservations(
                paper_trade=paper_trade,
                confirmation=confirmation,
                execution_store=execution_store,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        _mark_cleanup_live_submission_completed(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/paradex/cleanup/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_paradex_cleanup_for_paper_trade(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        live_service: Annotated[
            ParadexLiveExecutionService,
            Depends(get_paradex_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )
        await _ensure_cleanup_live_ready(
            venue="paradex",
            settings=settings,
            account_service=account_service,
        )
        paper_trade, _, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        if cleanup_preview.leg.venue != "paradex":
            raise HTTPException(
                status_code=409,
                detail="Current cleanup preview targets extended, not paradex",
            )
        try:
            confirmation = _resolve_cleanup_confirmation_for_live_submit(
                paper_trade=paper_trade,
                cleanup_preview=cleanup_preview,
                confirmation_store=confirmation_store,
                requested_preview_hash=normalized_preview_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        existing_entry = _reserve_cleanup_live_submission_or_existing(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
        )
        if existing_entry is not None:
            raise HTTPException(status_code=409, detail=existing_entry.model_dump(mode="json"))
        try:
            journal_entry = await live_service.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            _release_cleanup_live_submission_reservations(
                paper_trade=paper_trade,
                confirmation=confirmation,
                execution_store=execution_store,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        _mark_cleanup_live_submission_completed(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/hyperliquid/cleanup/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_hyperliquid_cleanup_for_paper_trade(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        live_service: Annotated[
            HyperliquidLiveExecutionService,
            Depends(get_hyperliquid_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )
        await _ensure_cleanup_live_ready(
            venue="hyperliquid",
            settings=settings,
            account_service=account_service,
        )
        paper_trade, _, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        if cleanup_preview.leg.venue != "hyperliquid":
            raise HTTPException(
                status_code=409,
                detail="Current cleanup preview targets a different venue, not hyperliquid",
            )
        try:
            confirmation = _resolve_cleanup_confirmation_for_live_submit(
                paper_trade=paper_trade,
                cleanup_preview=cleanup_preview,
                confirmation_store=confirmation_store,
                requested_preview_hash=normalized_preview_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        existing_entry = _reserve_cleanup_live_submission_or_existing(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
        )
        if existing_entry is not None:
            raise HTTPException(status_code=409, detail=existing_entry.model_dump(mode="json"))
        try:
            journal_entry = await live_service.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            _release_cleanup_live_submission_reservations(
                paper_trade=paper_trade,
                confirmation=confirmation,
                execution_store=execution_store,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        _mark_cleanup_live_submission_completed(
            paper_trade=paper_trade,
            confirmation=confirmation,
            execution_store=execution_store,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/pair/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_saved_paper_trade_as_pair(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        service: Annotated[
            PairedLiveExecutionCoordinator,
            Depends(get_paired_live_execution_coordinator),
        ],
        first_venue: str = "auto",
    ) -> ExecutionJournalEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )

        try:
            readiness = await _build_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
                system_state_service=system_state_service,
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        confirmation = confirmation_store.find_latest_by_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=preview_hash,
        )
        if confirmation is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            )
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Preview confirmation entry_id is required before live submission",
            )
        if not execution_store.reserve_live_submission(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
        ):
            existing_entry = execution_store.find_by_confirmation(
                confirmation_entry_id=confirmation.entry_id,
                preview_hash=confirmation.preview_hash,
            )
            if existing_entry is not None:
                raise HTTPException(
                    status_code=409,
                    detail=existing_entry.model_dump(mode="json"),
                )
            raise HTTPException(
                status_code=409,
                detail=(
                    "A live submission is already reserved for this confirmed preview; "
                    "manual reconciliation is required before retrying"
                ),
            )

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
                first_venue=first_venue,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        saved_entry = execution_store.append(journal_entry)
        if saved_entry.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        execution_store.mark_live_submission_completed(
            confirmation_entry_id=confirmation.entry_id,
            preview_hash=confirmation.preview_hash,
            execution_entry_id=saved_entry.entry_id,
        )
        return saved_entry

    @app.post(
        "/v1/executions/live/pair/guarded/from-paper-trade/{paper_trade_id}",
        response_model=GuardedPairExecutionResult,
    )
    async def execute_saved_paper_trade_as_guarded_pair(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_preview_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        cleanup_live_router: Annotated[
            CleanupLiveExecutionRouter,
            Depends(get_cleanup_live_execution_router),
        ],
        service: Annotated[
            PairedLiveExecutionCoordinator,
            Depends(get_paired_live_execution_coordinator),
        ],
        first_venue: str = "auto",
        poll_attempts: int = Query(default=5, ge=1, le=10),
        poll_interval_seconds: float = Query(default=2.0, ge=0.0, le=10.0),
        auto_cleanup: bool = True,
    ) -> GuardedPairExecutionResult:
        if not math.isfinite(poll_interval_seconds):
            raise HTTPException(
                status_code=400,
                detail="poll_interval_seconds must be finite",
            )
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        _require_live_route_approval(
            paper_trade=paper_trade,
            approval_service=approval_service,
        )

        try:
            readiness = await _build_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=normalized_preview_hash,
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
                system_state_service=system_state_service,
            )
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        confirmation = confirmation_store.find_latest_by_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=normalized_preview_hash,
        )
        if confirmation is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            )
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Preview confirmation entry_id is required before live submission",
            )
        return await _execute_guarded_pair_from_confirmation(
            paper_trade=paper_trade,
            confirmation=confirmation,
            settings=settings,
            execution_store=execution_store,
            observation_store=observation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            account_preflight_service=account_preflight_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            cleanup_live_router=cleanup_live_router,
            service=service,
            first_venue=first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
        )

    @app.post(
        "/v1/executions/live/pair/close/from-paper-trade/{paper_trade_id}",
        response_model=GuardedPairExecutionResult,
    )
    async def execute_saved_paper_trade_as_guarded_pair_close(
        paper_trade_id: int,
        preview_hash: str,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        pair_close_service: Annotated[
            PairClosePreviewService,
            Depends(get_pair_close_preview_service),
        ],
        cleanup_preview_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        cleanup_live_router: Annotated[
            CleanupLiveExecutionRouter,
            Depends(get_cleanup_live_execution_router),
        ],
        service: Annotated[
            PairCloseLiveExecutionCoordinator,
            Depends(get_pair_close_live_execution_coordinator),
        ],
        first_venue: str = "auto",
        poll_attempts: int = Query(default=5, ge=1, le=10),
        poll_interval_seconds: float = Query(default=2.0, ge=0.0, le=10.0),
        auto_cleanup: bool = True,
    ) -> GuardedPairExecutionResult:
        if not math.isfinite(poll_interval_seconds):
            raise HTTPException(
                status_code=400,
                detail="poll_interval_seconds must be finite",
            )
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
        confirmation = confirmation_store.find_latest_by_preview_hash(
            paper_trade_id=paper_trade_id,
            preview_hash=normalized_preview_hash,
        )
        if confirmation is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No pair-close preview confirmation matched the requested paper trade "
                    "and preview hash"
                ),
            )
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Pair-close confirmation entry_id is required before live submission",
            )
        for venue in {leg.venue for leg in confirmation.preview.legs}:
            try:
                await _ensure_cleanup_live_ready(
                    venue=venue,
                    settings=settings,
                    account_service=account_preflight_service,
                )
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc

        return await _execute_guarded_pair_close_from_confirmation(
            paper_trade=paper_trade,
            confirmation=confirmation,
            settings=settings,
            execution_store=execution_store,
            observation_store=observation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            account_preflight_service=account_preflight_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            cleanup_live_router=cleanup_live_router,
            service=service,
            first_venue=first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
        )

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        sample: int = 200,
        label: str | None = None,
    ) -> HTMLResponse:
        sample = _validated_history_limit("sample", sample)
        records = store.list_recent(limit=sample, label=label)
        return HTMLResponse(render_dashboard(records))

    @app.get("/dashboard/candidates", response_class=HTMLResponse)
    def candidate_dashboard(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        sample: int = 200,
        label: str | None = None,
        min_one_day_net_edge_after_entry: float = 0.0,
        min_capacity_notional: float = 0.0,
    ) -> HTMLResponse:
        sample = _validated_history_limit("sample", sample)
        min_one_day_net_edge_after_entry = _validated_non_negative_threshold(
            "min_one_day_net_edge_after_entry",
            min_one_day_net_edge_after_entry,
        )
        min_capacity_notional = _validated_non_negative_threshold(
            "min_capacity_notional",
            min_capacity_notional,
        )
        records = store.list_recent(limit=sample, label=label)
        return HTMLResponse(
            render_candidate_dashboard(
                records,
                min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
                min_capacity_notional=min_capacity_notional,
            )
        )

    @app.get("/v1/opportunities/funding-pair", response_model=FundingArbOpportunity)
    async def funding_pair(
        left_venue: str,
        left_symbol: str,
        left_fee_profile: str,
        right_venue: str,
        right_symbol: str,
        right_fee_profile: str,
        response: Response,
        service: Annotated[OpportunityService, Depends(get_opportunity_service)],
    ) -> FundingArbOpportunity:
        try:
            opportunity = await service.score_pair(
                left_venue=left_venue,
                left_symbol=left_symbol,
                left_fee_profile=left_fee_profile,
                right_venue=right_venue,
                right_symbol=right_symbol,
                right_fee_profile=right_fee_profile,
            )
            response.headers["Cache-Control"] = "no-store"
            return opportunity
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/opportunities/funding-universe", response_model=FundingUniverseScan)
    async def funding_universe(
        service: Annotated[OpportunityUniverseService, Depends(get_opportunity_universe_service)],
        venues: Annotated[list[str] | None, Query()] = None,
        ranking: str = "route_adjusted_quality_pnl",
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = None,
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        min_capacity_notional: float = 0.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.0,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.0,
        min_route_presence_ratio: float = 0.0,
        min_route_samples: int = 0,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 20,
    ) -> FundingUniverseScan:
        try:
            selected_venues = venues or list(SUPPORTED_UNIVERSE_VENUES)
            _validate_route_stability_filters(
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
            )
            return await _run_bounded_universe_scan(
                "funding universe scan",
                lambda: service.scan(
                    venues=selected_venues,
                    ranking=ranking,  # type: ignore[arg-type]
                    fee_profile_overrides=_build_fee_profile_overrides(
                        extended_fee_profile=extended_fee_profile,
                        paradex_fee_profile=paradex_fee_profile,
                        hyperliquid_fee_profile=hyperliquid_fee_profile,
                    ),
                    target_notional=target_notional,
                    min_capacity_notional=min_capacity_notional,
                    min_daily_volume=min_daily_volume,
                    min_open_interest=min_open_interest,
                    min_roundtrip_edge=min_roundtrip_edge,
                    min_execution_quality_score=min_execution_quality_score,
                    min_execution_samples=min_execution_samples,
                    min_route_stability_weight=min_route_stability_weight,
                    min_route_presence_ratio=min_route_presence_ratio,
                    min_route_samples=min_route_samples,
                    include_symbols=include_symbols,
                    exclude_symbols=exclude_symbols,
                    exclude_tags=exclude_tags,
                    limit=limit,
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/opportunities/funding-universe/canary",
        response_model=list[FundingUniverseCanaryCandidate],
    )
    async def funding_universe_canary_candidates(
        service: Annotated[OpportunityUniverseService, Depends(get_opportunity_universe_service)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        venues: Annotated[list[str] | None, Query()] = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        approved_only: bool = False,
        limit: int = 10,
    ) -> list[FundingUniverseCanaryCandidate]:
        try:
            limit = _validated_history_limit("limit", limit)
            selected_venues = venues or ["extended", "paradex", "hyperliquid"]
            candidates = await _run_bounded_universe_scan(
                "funding universe canary scan",
                lambda: service.scan_canary_candidates(
                    venues=selected_venues,
                    fee_profile_overrides=_build_fee_profile_overrides(
                        extended_fee_profile=extended_fee_profile,
                        paradex_fee_profile=paradex_fee_profile,
                        hyperliquid_fee_profile=hyperliquid_fee_profile,
                    ),
                    target_notional=target_notional,
                    canary_max_notional=canary_max_notional,
                    min_capacity_notional=min_capacity_notional,
                    min_daily_volume=min_daily_volume,
                    min_open_interest=min_open_interest,
                    min_roundtrip_edge=min_roundtrip_edge,
                    min_execution_quality_score=min_execution_quality_score,
                    min_execution_samples=min_execution_samples,
                    min_route_stability_weight=min_route_stability_weight,
                    min_route_presence_ratio=min_route_presence_ratio,
                    min_route_samples=min_route_samples,
                    include_symbols=include_symbols,
                    exclude_symbols=exclude_symbols,
                    exclude_tags=exclude_tags,
                    limit=limit,
                ),
            )
            if approved_only:
                return approval_service.filter_approved_canary_candidates(candidates)[:limit]
            return candidates
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/opportunities/funding-universe/canary/approval-proposals",
        response_model=list[FundingUniverseCanaryApprovalProposalSummary],
    )
    async def funding_universe_canary_approval_proposals(
        service: Annotated[OpportunityUniverseService, Depends(get_opportunity_universe_service)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        history_store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        current_time: Annotated[datetime, Depends(get_current_utc_time)],
        venues: Annotated[list[str] | None, Query()] = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 10,
        candidate_sample: int = 50,
        use_history_shortlist: bool = True,
        history_shortlist_sample: int = 500,
        history_shortlist_limit: int = 50,
        history_shortlist_max_age_seconds: int = DEFAULT_HISTORY_SHORTLIST_MAX_AGE_SECONDS,
    ) -> list[FundingUniverseCanaryApprovalProposalSummary]:
        try:
            return await _generate_canary_approval_proposal_summaries(
                universe_service=service,
                approval_service=approval_service,
                history_store=history_store,
                current_time=current_time,
                venues=venues,
                extended_fee_profile=extended_fee_profile,
                paradex_fee_profile=paradex_fee_profile,
                hyperliquid_fee_profile=hyperliquid_fee_profile,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=limit,
                candidate_sample=candidate_sample,
                use_history_shortlist=use_history_shortlist,
                history_shortlist_sample=history_shortlist_sample,
                history_shortlist_limit=history_shortlist_limit,
                history_shortlist_max_age_seconds=history_shortlist_max_age_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post(
        "/v1/opportunities/funding-universe/canary/approval-proposals/{label}/approve",
        response_model=RouteApprovalEntry,
    )
    def approve_funding_universe_canary_approval_proposal(
        label: str,
        service: Annotated[RouteApprovalService, Depends(get_route_approval_service)],
        payload: Annotated[FundingUniverseCanaryApprovalProposalSummary, Body()],
        current_time: Annotated[datetime, Depends(get_current_utc_time)],
        max_proposal_age_seconds: int = DEFAULT_APPROVAL_PROPOSAL_MAX_AGE_SECONDS,
    ) -> RouteApprovalEntry:
        approval_payload = _build_approved_route_payload_from_canary_proposal(
            label=label,
            proposal=payload,
            current_time=current_time,
            max_proposal_age_seconds=max_proposal_age_seconds,
        )
        return service.upsert(label=label, payload=approval_payload)

    @app.post(
        "/v1/opportunities/funding-universe/canary/approval-proposals/"
        "{label}/approve-and-refresh",
        response_model=ApprovedCanarySnapshot,
    )
    async def approve_and_refresh_funding_universe_canary_approval_proposal(
        label: str,
        universe_service: Annotated[
            OpportunityUniverseService,
            Depends(get_opportunity_universe_service),
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        approved_store: Annotated[
            ApprovedCanaryStore,
            Depends(get_approved_canary_store),
        ],
        payload: Annotated[FundingUniverseCanaryApprovalProposalSummary, Body()],
        current_time: Annotated[datetime, Depends(get_current_utc_time)],
        max_proposal_age_seconds: int = DEFAULT_APPROVAL_PROPOSAL_MAX_AGE_SECONDS,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
    ) -> ApprovedCanarySnapshot:
        approval_payload = _build_approved_route_payload_from_canary_proposal(
            label=label,
            proposal=payload,
            current_time=current_time,
            max_proposal_age_seconds=max_proposal_age_seconds,
        )
        candidate_approval = RouteApprovalEntry(
            updated_at=current_time,
            label=label,
            canonical_symbol=approval_payload.canonical_symbol,
            short_venue=approval_payload.short_venue,
            long_venue=approval_payload.long_venue,
            short_fee_profile=approval_payload.short_fee_profile,
            long_fee_profile=approval_payload.long_fee_profile,
            approved=True,
            max_live_notional=approval_payload.max_live_notional,
            note=approval_payload.note,
        )
        snapshot = await _scan_current_approved_canary_snapshot_for_promoted_proposal(
            universe_service=universe_service,
            approval=candidate_approval,
            now=current_time,
            target_notional=target_notional,
            canary_max_notional=canary_max_notional,
            min_capacity_notional=min_capacity_notional,
            min_daily_volume=min_daily_volume,
            min_open_interest=min_open_interest,
            min_roundtrip_edge=min_roundtrip_edge,
            min_execution_quality_score=min_execution_quality_score,
            min_execution_samples=min_execution_samples,
            min_route_stability_weight=min_route_stability_weight,
            min_route_presence_ratio=min_route_presence_ratio,
            min_route_samples=min_route_samples,
            exclude_tags=DEFAULT_CANARY_EXCLUDE_TAGS if exclude_tags is None else exclude_tags,
        )
        persisted_approval = approval_service.upsert(label=label, payload=approval_payload)
        return approved_store.append(snapshot.model_copy(update={"approval": persisted_approval}))

    @app.post(
        "/v1/opportunities/funding-universe/canary/approval-proposals/"
        "{label}/approve-refresh-and-cache-launch-ready",
        response_model=LaunchReadyCanarySnapshot,
    )
    async def approve_refresh_and_cache_launch_ready_canary_proposal(
        label: str,
        universe_service: Annotated[
            OpportunityUniverseService,
            Depends(get_opportunity_universe_service),
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        approved_store: Annotated[
            ApprovedCanaryStore,
            Depends(get_approved_canary_store),
        ],
        launch_ready_store: Annotated[
            LaunchReadyCanaryStore,
            Depends(get_launch_ready_canary_store),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        payload: Annotated[FundingUniverseCanaryApprovalProposalSummary, Body()],
        current_time: Annotated[datetime, Depends(get_current_utc_time)],
        max_proposal_age_seconds: int = DEFAULT_APPROVAL_PROPOSAL_MAX_AGE_SECONDS,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
    ) -> LaunchReadyCanarySnapshot:
        return await _approve_refresh_and_cache_launch_ready_canary_proposal(
            label=label,
            universe_service=universe_service,
            approval_service=approval_service,
            approved_store=approved_store,
            launch_ready_store=launch_ready_store,
            system_state_service=system_state_service,
            settings=settings,
            payload=payload,
            current_time=current_time,
            max_proposal_age_seconds=max_proposal_age_seconds,
            target_notional=target_notional,
            canary_max_notional=canary_max_notional,
            min_capacity_notional=min_capacity_notional,
            min_daily_volume=min_daily_volume,
            min_open_interest=min_open_interest,
            min_roundtrip_edge=min_roundtrip_edge,
            min_execution_quality_score=min_execution_quality_score,
            min_execution_samples=min_execution_samples,
            min_route_stability_weight=min_route_stability_weight,
            min_route_presence_ratio=min_route_presence_ratio,
            min_route_samples=min_route_samples,
            exclude_tags=exclude_tags,
        )

    @app.post(
        "/v1/opportunities/funding-universe/canary/approval-proposals/"
        "approve-best-launch-ready",
        response_model=LaunchReadyCanarySnapshot,
    )
    async def approve_best_launch_ready_canary_approval_proposal(
        universe_service: Annotated[
            OpportunityUniverseService,
            Depends(get_opportunity_universe_service),
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        approved_store: Annotated[
            ApprovedCanaryStore,
            Depends(get_approved_canary_store),
        ],
        launch_ready_store: Annotated[
            LaunchReadyCanaryStore,
            Depends(get_launch_ready_canary_store),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        history_store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        current_time: Annotated[datetime, Depends(get_current_utc_time)],
        venues: Annotated[list[str] | None, Query()] = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        max_proposal_age_seconds: int = DEFAULT_APPROVAL_PROPOSAL_MAX_AGE_SECONDS,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        candidate_sample: int = 50,
        use_history_shortlist: bool = True,
        history_shortlist_sample: int = 500,
        history_shortlist_limit: int = 50,
        history_shortlist_max_age_seconds: int = DEFAULT_HISTORY_SHORTLIST_MAX_AGE_SECONDS,
    ) -> LaunchReadyCanarySnapshot:
        try:
            proposals = await _generate_canary_approval_proposal_summaries(
                universe_service=universe_service,
                approval_service=approval_service,
                history_store=history_store,
                current_time=current_time,
                venues=venues or ["extended", "paradex"],
                extended_fee_profile=extended_fee_profile,
                paradex_fee_profile=paradex_fee_profile,
                hyperliquid_fee_profile=hyperliquid_fee_profile,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=1,
                candidate_sample=candidate_sample,
                use_history_shortlist=use_history_shortlist,
                history_shortlist_sample=history_shortlist_sample,
                history_shortlist_limit=history_shortlist_limit,
                history_shortlist_max_age_seconds=history_shortlist_max_age_seconds,
            )
            if not proposals:
                raise HTTPException(
                    status_code=404,
                    detail="No current canary approval proposal found",
                )
            proposal = proposals[0]
            return await _approve_refresh_and_cache_launch_ready_canary_proposal(
                label=proposal.label,
                universe_service=universe_service,
                approval_service=approval_service,
                approved_store=approved_store,
                launch_ready_store=launch_ready_store,
                system_state_service=system_state_service,
                settings=settings,
                payload=proposal,
                current_time=current_time,
                max_proposal_age_seconds=max_proposal_age_seconds,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                exclude_tags=exclude_tags,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post(
        "/v1/executions/live/canary-cycle/approved-basket",
        response_model=CanaryBasketLaunchResult,
    )
    async def execute_guarded_approved_canary_basket(
        request: Request,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        universe_service: Annotated[
            OpportunityUniverseService, Depends(get_opportunity_universe_service)
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        basket_store: Annotated[
            CanaryBasketLaunchStore,
            Depends(get_canary_basket_launch_store),
        ],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        pair_close_confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        balance_service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        order_preview_service: Annotated[
            OrderPreviewService,
            Depends(get_order_preview_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        venues: Annotated[list[str] | None, Query()] = None,
        note: str | None = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 10,
        slippage_tolerance_bps: int = 20,
        open_first_venue: str = "auto",
        close_first_venue: str = "auto",
        poll_attempts: int = 5,
        poll_interval_seconds: float = 2.0,
        auto_cleanup: bool = True,
        close_position: bool = True,
        continue_on_failure: bool = False,
    ) -> CanaryBasketLaunchResult:
        try:
            basket_plan = await _scan_approved_canary_basket_plan(
                universe_service=universe_service,
                approval_service=approval_service,
                venues=venues,
                extended_fee_profile=extended_fee_profile,
                paradex_fee_profile=paradex_fee_profile,
                hyperliquid_fee_profile=hyperliquid_fee_profile,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if not basket_plan.entries:
            raise HTTPException(
                status_code=404,
                detail="No approved canary basket routes matched the requested filters",
            )
        return await _execute_approved_canary_basket_plan(
            request=request,
            settings=settings,
            basket_plan=basket_plan,
            note=note,
            paper_store=paper_store,
            confirmation_store=confirmation_store,
            pair_close_confirmation_store=pair_close_confirmation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            execution_store=execution_store,
            observation_store=observation_store,
            account_preflight_service=account_preflight_service,
            system_state_service=system_state_service,
            balance_service=balance_service,
            order_preview_service=order_preview_service,
            order_state_service=order_state_service,
            approval_service=approval_service,
            basket_store=basket_store,
            slippage_tolerance_bps=slippage_tolerance_bps,
            open_first_venue=open_first_venue,
            close_first_venue=close_first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
            close_position=close_position,
            continue_on_failure=continue_on_failure,
        )

    @app.get(
        "/v1/executions/live/canary-cycle/approved-baskets",
        response_model=list[CanaryBasketLaunchResult],
    )
    def approved_canary_basket_launches(
        store: Annotated[
            CanaryBasketLaunchStore,
            Depends(get_canary_basket_launch_store),
        ],
        limit: int = 50,
        status: str | None = None,
    ) -> list[CanaryBasketLaunchResult]:
        limit = _validated_history_limit("limit", limit)
        return store.list_recent(limit=limit, status=status)

    @app.post(
        "/v1/executions/live/canary-cycle",
        response_model=CanaryLifecycleResult,
    )
    async def execute_guarded_canary_cycle(
        request: Request,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        approved_store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        universe_service: Annotated[
            OpportunityUniverseService, Depends(get_opportunity_universe_service)
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        pair_close_confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        balance_service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        order_preview_service: Annotated[
            OrderPreviewService,
            Depends(get_order_preview_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        venues: Annotated[list[str] | None, Query()] = None,
        label: str | None = None,
        desired_notional: float | None = None,
        note: str | None = None,
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = "pro_fastfills",
        hyperliquid_fee_profile: str | None = None,
        max_snapshot_age_seconds: int = 300,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 10,
        slippage_tolerance_bps: int = 20,
        open_first_venue: str = "auto",
        close_first_venue: str = "auto",
        poll_attempts: int = 5,
        poll_interval_seconds: float = 2.0,
        auto_cleanup: bool = True,
        close_position: bool = True,
    ) -> CanaryLifecycleResult:
        lifecycle_note: str | None = None
        normalized_label = label.strip() if label is not None else None
        if normalized_label == "":
            raise HTTPException(status_code=400, detail="label must be non-empty")
        if max_snapshot_age_seconds < 0:
            raise HTTPException(
                status_code=400,
                detail="max_snapshot_age_seconds must be non-negative",
            )
        _validate_route_stability_filters(
            min_route_stability_weight=min_route_stability_weight,
            min_route_presence_ratio=min_route_presence_ratio,
            min_route_samples=min_route_samples,
        )
        if normalized_label is not None:
            try:
                snapshot, selected, approval = await asyncio.to_thread(
                    _select_latest_approved_canary_snapshot,
                    store=approved_store,
                    approval_service=approval_service,
                    label=normalized_label,
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                    canary_max_notional=canary_max_notional,
                )
                selected = _validate_latest_approved_canary_snapshot_request(
                    candidate=selected,
                    approval=approval,
                    universe_service=universe_service,
                    venues=venues,
                    label=normalized_label,
                    extended_fee_profile=extended_fee_profile,
                    paradex_fee_profile=paradex_fee_profile,
                    hyperliquid_fee_profile=hyperliquid_fee_profile,
                    target_notional=target_notional,
                    min_capacity_notional=min_capacity_notional,
                    min_daily_volume=min_daily_volume,
                    min_open_interest=min_open_interest,
                    min_roundtrip_edge=min_roundtrip_edge,
                    min_execution_quality_score=min_execution_quality_score,
                    min_execution_samples=min_execution_samples,
                    min_route_stability_weight=min_route_stability_weight,
                    min_route_presence_ratio=min_route_presence_ratio,
                    min_route_samples=min_route_samples,
                    include_symbols=include_symbols,
                    exclude_symbols=exclude_symbols,
                    exclude_tags=exclude_tags,
                )
            except HTTPException as exc:
                if exc.status_code not in {404, 409}:
                    raise
                selected, approval = await _select_approved_canary_candidate(
                    universe_service=universe_service,
                    approval_service=approval_service,
                    venues=venues,
                    label=normalized_label,
                    extended_fee_profile=extended_fee_profile,
                    paradex_fee_profile=paradex_fee_profile,
                    hyperliquid_fee_profile=hyperliquid_fee_profile,
                    target_notional=target_notional,
                    canary_max_notional=canary_max_notional,
                    min_capacity_notional=min_capacity_notional,
                    min_daily_volume=min_daily_volume,
                    min_open_interest=min_open_interest,
                    min_roundtrip_edge=min_roundtrip_edge,
                    min_execution_quality_score=min_execution_quality_score,
                    min_execution_samples=min_execution_samples,
                    min_route_stability_weight=min_route_stability_weight,
                    min_route_presence_ratio=min_route_presence_ratio,
                    min_route_samples=min_route_samples,
                    include_symbols=include_symbols,
                    exclude_symbols=exclude_symbols,
                    exclude_tags=exclude_tags,
                    limit=limit,
                )
                fallback_reason = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
                lifecycle_note = (
                    "Approved canary snapshot unavailable; fell back to exact live scan "
                    f"({fallback_reason})."
                )
            else:
                lifecycle_note = (
                    "Launched from approved canary snapshot "
                    f"{snapshot.snapshot_id} captured at {snapshot.captured_at.isoformat()}."
                )
        else:
            selected, approval = await _select_approved_canary_candidate(
                universe_service=universe_service,
                approval_service=approval_service,
                venues=venues,
                label=label,
                extended_fee_profile=extended_fee_profile,
                paradex_fee_profile=paradex_fee_profile,
                hyperliquid_fee_profile=hyperliquid_fee_profile,
                target_notional=target_notional,
                canary_max_notional=canary_max_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                min_execution_quality_score=min_execution_quality_score,
                min_execution_samples=min_execution_samples,
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
                limit=limit,
            )
        cleanup_preview_service = _resolve_request_dependency(
            request,
            get_cleanup_preview_service,
            lambda: _build_cleanup_preview_router_for_candidate(settings, selected),
        )
        pair_close_preview_service = _resolve_request_dependency(
            request,
            get_pair_close_preview_service,
            lambda: _build_pair_close_preview_service_for_candidate(settings, selected),
        )

        def _resolve_live_execution_service(venue: str) -> Any:
            if venue == "extended":
                return _resolve_request_dependency(
                    request,
                    get_extended_live_execution_service,
                    lambda: get_extended_live_execution_service(settings),
                )
            if venue == "hyperliquid":
                return _resolve_request_dependency(
                    request,
                    get_hyperliquid_live_execution_service,
                    lambda: get_hyperliquid_live_execution_service(settings),
                )
            if venue == "paradex":
                return _resolve_request_dependency(
                    request,
                    get_paradex_live_execution_service,
                    lambda: get_paradex_live_execution_service(settings),
                )
            raise ValueError(f"Unsupported canary venue {venue!r}")

        cleanup_live_router = _resolve_request_dependency(
            request,
            get_cleanup_live_execution_router,
            lambda: _build_cleanup_live_execution_router_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        paired_service = _resolve_request_dependency(
            request,
            get_paired_live_execution_coordinator,
            lambda: _build_paired_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        pair_close_live_service = _resolve_request_dependency(
            request,
            get_pair_close_live_execution_coordinator,
            lambda: _build_pair_close_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        return await _run_guarded_canary_lifecycle(
            candidate=selected,
            approval=approval,
            desired_notional=desired_notional,
            note=note,
            lifecycle_note=lifecycle_note,
            paper_store=paper_store,
            confirmation_store=confirmation_store,
            pair_close_confirmation_store=pair_close_confirmation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            execution_store=execution_store,
            observation_store=observation_store,
            settings=settings,
            account_preflight_service=account_preflight_service,
            system_state_service=system_state_service,
            balance_service=balance_service,
            order_preview_service=order_preview_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            pair_close_preview_service=pair_close_preview_service,
            cleanup_live_router=cleanup_live_router,
            paired_service=paired_service,
            pair_close_live_service=pair_close_live_service,
            approval_service=approval_service,
            slippage_tolerance_bps=slippage_tolerance_bps,
            open_first_venue=open_first_venue,
            close_first_venue=close_first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
            close_position=close_position,
        )

    @app.post(
        "/v1/executions/live/canary-cycle/latest-approved",
        response_model=CanaryLifecycleResult,
    )
    async def execute_guarded_canary_cycle_from_latest_approved(
        request: Request,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        pair_close_confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        balance_service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        order_preview_service: Annotated[
            OrderPreviewService,
            Depends(get_order_preview_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        label: str | None = None,
        desired_notional: float | None = None,
        note: str | None = None,
        max_snapshot_age_seconds: int = 300,
        slippage_tolerance_bps: int = 20,
        open_first_venue: str = "auto",
        close_first_venue: str = "auto",
        poll_attempts: int = 5,
        poll_interval_seconds: float = 2.0,
        auto_cleanup: bool = True,
        close_position: bool = True,
    ) -> CanaryLifecycleResult:
        snapshot, selected, approval = _select_latest_approved_canary_snapshot(
            store=store,
            approval_service=approval_service,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
        )
        cleanup_preview_service = _resolve_request_dependency(
            request,
            get_cleanup_preview_service,
            lambda: _build_cleanup_preview_router_for_candidate(settings, selected),
        )
        pair_close_preview_service = _resolve_request_dependency(
            request,
            get_pair_close_preview_service,
            lambda: _build_pair_close_preview_service_for_candidate(settings, selected),
        )

        def _resolve_live_execution_service(venue: str) -> Any:
            if venue == "extended":
                return _resolve_request_dependency(
                    request,
                    get_extended_live_execution_service,
                    lambda: get_extended_live_execution_service(settings),
                )
            if venue == "hyperliquid":
                return _resolve_request_dependency(
                    request,
                    get_hyperliquid_live_execution_service,
                    lambda: get_hyperliquid_live_execution_service(settings),
                )
            if venue == "paradex":
                return _resolve_request_dependency(
                    request,
                    get_paradex_live_execution_service,
                    lambda: get_paradex_live_execution_service(settings),
                )
            raise ValueError(f"Unsupported canary venue {venue!r}")

        cleanup_live_router = _resolve_request_dependency(
            request,
            get_cleanup_live_execution_router,
            lambda: _build_cleanup_live_execution_router_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        paired_service = _resolve_request_dependency(
            request,
            get_paired_live_execution_coordinator,
            lambda: _build_paired_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        pair_close_live_service = _resolve_request_dependency(
            request,
            get_pair_close_live_execution_coordinator,
            lambda: _build_pair_close_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )

        return await _run_guarded_canary_lifecycle(
            candidate=selected,
            approval=approval,
            desired_notional=desired_notional,
            note=note,
            lifecycle_note=(
                "Launched from approved canary snapshot "
                f"{snapshot.snapshot_id} captured at {snapshot.captured_at.isoformat()}."
            ),
            paper_store=paper_store,
            confirmation_store=confirmation_store,
            pair_close_confirmation_store=pair_close_confirmation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            execution_store=execution_store,
            observation_store=observation_store,
            settings=settings,
            account_preflight_service=account_preflight_service,
            system_state_service=system_state_service,
            balance_service=balance_service,
            order_preview_service=order_preview_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            pair_close_preview_service=pair_close_preview_service,
            cleanup_live_router=cleanup_live_router,
            paired_service=paired_service,
            pair_close_live_service=pair_close_live_service,
            approval_service=approval_service,
            slippage_tolerance_bps=slippage_tolerance_bps,
            open_first_venue=open_first_venue,
            close_first_venue=close_first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
            close_position=close_position,
        )

    @app.post(
        "/v1/executions/live/canary-cycle/latest-launch-ready",
        response_model=CanaryLifecycleResult,
    )
    async def execute_guarded_canary_cycle_from_latest_launch_ready(
        request: Request,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        store: Annotated[
            LaunchReadyCanaryStore,
            Depends(get_launch_ready_canary_store),
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        pair_close_confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        execution_store: Annotated[
            ExecutionJournalStore,
            Depends(get_execution_journal_store),
        ],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        balance_service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        order_preview_service: Annotated[
            OrderPreviewService,
            Depends(get_order_preview_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        cleanup_preview_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        pair_close_preview_service: Annotated[
            PairClosePreviewService,
            Depends(get_pair_close_preview_service),
        ],
        label: str | None = None,
        desired_notional: float | None = None,
        note: str | None = None,
        max_snapshot_age_seconds: int = 300,
        slippage_tolerance_bps: int = 20,
        open_first_venue: str = "auto",
        close_first_venue: str = "auto",
        poll_attempts: int = 5,
        poll_interval_seconds: float = 2.0,
        auto_cleanup: bool = True,
        close_position: bool = True,
    ) -> CanaryLifecycleResult:
        snapshot, selected, approval = _select_latest_launch_ready_canary_snapshot(
            store=store,
            approval_service=approval_service,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
        )
        cleanup_preview_service = _resolve_request_dependency(
            request,
            get_cleanup_preview_service,
            lambda: _build_cleanup_preview_router_for_candidate(settings, selected),
        )
        pair_close_preview_service = _resolve_request_dependency(
            request,
            get_pair_close_preview_service,
            lambda: _build_pair_close_preview_service_for_candidate(settings, selected),
        )

        def _resolve_live_execution_service(venue: str) -> Any:
            if venue == "extended":
                return _resolve_request_dependency(
                    request,
                    get_extended_live_execution_service,
                    lambda: get_extended_live_execution_service(settings),
                )
            if venue == "hyperliquid":
                return _resolve_request_dependency(
                    request,
                    get_hyperliquid_live_execution_service,
                    lambda: get_hyperliquid_live_execution_service(settings),
                )
            if venue == "paradex":
                return _resolve_request_dependency(
                    request,
                    get_paradex_live_execution_service,
                    lambda: get_paradex_live_execution_service(settings),
                )
            raise ValueError(f"Unsupported canary venue {venue!r}")

        cleanup_live_router = _resolve_request_dependency(
            request,
            get_cleanup_live_execution_router,
            lambda: _build_cleanup_live_execution_router_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        paired_service = _resolve_request_dependency(
            request,
            get_paired_live_execution_coordinator,
            lambda: _build_paired_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        pair_close_live_service = _resolve_request_dependency(
            request,
            get_pair_close_live_execution_coordinator,
            lambda: _build_pair_close_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )

        return await _run_guarded_canary_lifecycle(
            candidate=selected,
            approval=approval,
            desired_notional=desired_notional,
            note=note,
            lifecycle_note=(
                "Launched from launch-ready canary snapshot "
                f"{snapshot.launch_ready_snapshot_id} derived from approved snapshot "
                f"{snapshot.approved_snapshot.snapshot_id} captured at "
                f"{snapshot.captured_at.isoformat()}."
            ),
            paper_store=paper_store,
            confirmation_store=confirmation_store,
            pair_close_confirmation_store=pair_close_confirmation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            execution_store=execution_store,
            observation_store=observation_store,
            settings=settings,
            account_preflight_service=account_preflight_service,
            system_state_service=system_state_service,
            balance_service=balance_service,
            order_preview_service=order_preview_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            pair_close_preview_service=pair_close_preview_service,
            cleanup_live_router=cleanup_live_router,
            paired_service=paired_service,
            pair_close_live_service=pair_close_live_service,
            approval_service=approval_service,
            slippage_tolerance_bps=slippage_tolerance_bps,
            open_first_venue=open_first_venue,
            close_first_venue=close_first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
            close_position=close_position,
        )

    @app.post(
        "/v1/executions/live/canary-cycle/latest-stable-launch-ready",
        response_model=CanaryLifecycleResult,
    )
    async def execute_guarded_canary_cycle_from_latest_stable_launch_ready(
        request: Request,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        store: Annotated[
            LaunchReadyCanaryStore,
            Depends(get_launch_ready_canary_store),
        ],
        approval_service: Annotated[
            RouteApprovalService,
            Depends(get_route_approval_service),
        ],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        pair_close_confirmation_store: Annotated[
            PairClosePreviewConfirmationStore,
            Depends(get_pair_close_preview_confirmation_store),
        ],
        cleanup_confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        execution_store: Annotated[
            ExecutionJournalStore,
            Depends(get_execution_journal_store),
        ],
        observation_store: Annotated[
            ExecutionObservationStore,
            Depends(get_execution_observation_store),
        ],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
        ],
        system_state_service: Annotated[
            SystemStateService,
            Depends(get_system_state_service),
        ],
        balance_service: Annotated[
            BalanceAccountingService,
            Depends(get_balance_accounting_service),
        ],
        order_preview_service: Annotated[
            OrderPreviewService,
            Depends(get_order_preview_service),
        ],
        order_state_service: Annotated[
            ExecutionOrderStateService,
            Depends(get_execution_order_state_service),
        ],
        label: str | None = None,
        desired_notional: float | None = None,
        note: str | None = None,
        max_snapshot_age_seconds: int = 300,
        min_snapshot_count: int = 2,
        min_stable_seconds: float = 30.0,
        slippage_tolerance_bps: int = 20,
        open_first_venue: str = "auto",
        close_first_venue: str = "auto",
        poll_attempts: int = 5,
        poll_interval_seconds: float = 2.0,
        auto_cleanup: bool = True,
        close_position: bool = True,
    ) -> CanaryLifecycleResult:
        stability = _build_launch_ready_canary_stability(
            store=store,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            min_snapshot_count=min_snapshot_count,
            min_stable_seconds=min_stable_seconds,
        )
        snapshot, selected, approval = _select_latest_launch_ready_canary_snapshot(
            store=store,
            approval_service=approval_service,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
        )
        cleanup_preview_service = _resolve_request_dependency(
            request,
            get_cleanup_preview_service,
            lambda: _build_cleanup_preview_router_for_candidate(settings, selected),
        )
        pair_close_preview_service = _resolve_request_dependency(
            request,
            get_pair_close_preview_service,
            lambda: _build_pair_close_preview_service_for_candidate(settings, selected),
        )

        def _resolve_live_execution_service(venue: str) -> Any:
            if venue == "extended":
                return _resolve_request_dependency(
                    request,
                    get_extended_live_execution_service,
                    lambda: get_extended_live_execution_service(settings),
                )
            if venue == "hyperliquid":
                return _resolve_request_dependency(
                    request,
                    get_hyperliquid_live_execution_service,
                    lambda: get_hyperliquid_live_execution_service(settings),
                )
            if venue == "paradex":
                return _resolve_request_dependency(
                    request,
                    get_paradex_live_execution_service,
                    lambda: get_paradex_live_execution_service(settings),
                )
            raise ValueError(f"Unsupported canary venue {venue!r}")

        cleanup_live_router = _resolve_request_dependency(
            request,
            get_cleanup_live_execution_router,
            lambda: _build_cleanup_live_execution_router_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        paired_service = _resolve_request_dependency(
            request,
            get_paired_live_execution_coordinator,
            lambda: _build_paired_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )
        pair_close_live_service = _resolve_request_dependency(
            request,
            get_pair_close_live_execution_coordinator,
            lambda: _build_pair_close_live_execution_coordinator_for_candidate(
                settings,
                selected,
                live_service_resolver=_resolve_live_execution_service,
            ),
        )

        return await _run_guarded_canary_lifecycle(
            candidate=selected,
            approval=approval,
            desired_notional=desired_notional,
            note=note,
            lifecycle_note=(
                "Launched from stable launch-ready canary snapshot "
                f"{stability.snapshot.launch_ready_snapshot_id} after "
                f"{stability.consecutive_snapshots} consecutive snapshots and "
                f"{stability.stable_seconds:.1f}s of stability."
            ),
            paper_store=paper_store,
            confirmation_store=confirmation_store,
            pair_close_confirmation_store=pair_close_confirmation_store,
            cleanup_confirmation_store=cleanup_confirmation_store,
            execution_store=execution_store,
            observation_store=observation_store,
            settings=settings,
            account_preflight_service=account_preflight_service,
            system_state_service=system_state_service,
            balance_service=balance_service,
            order_preview_service=order_preview_service,
            order_state_service=order_state_service,
            cleanup_preview_service=cleanup_preview_service,
            pair_close_preview_service=pair_close_preview_service,
            cleanup_live_router=cleanup_live_router,
            paired_service=paired_service,
            pair_close_live_service=pair_close_live_service,
            approval_service=approval_service,
            slippage_tolerance_bps=slippage_tolerance_bps,
            open_first_venue=open_first_venue,
            close_first_venue=close_first_venue,
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
            auto_cleanup=auto_cleanup,
            close_position=close_position,
        )

    @app.get(
        "/v1/opportunities/funding-universe/portfolio",
        response_model=FundingUniversePortfolioPlan,
    )
    async def funding_universe_portfolio(
        service: Annotated[OpportunityUniverseService, Depends(get_opportunity_universe_service)],
        venues: Annotated[list[str] | None, Query()] = None,
        ranking: str = "route_adjusted_quality_pnl",
        extended_fee_profile: str | None = None,
        paradex_fee_profile: str | None = None,
        hyperliquid_fee_profile: str | None = None,
        target_notional: float = 5_000.0,
        min_capacity_notional: float = 0.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.0,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.0,
        min_route_presence_ratio: float = 0.0,
        min_route_samples: int = 0,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        max_positions: int = 5,
        min_selected_notional: float = 0.0,
    ) -> FundingUniversePortfolioPlan:
        try:
            selected_venues = venues or list(SUPPORTED_UNIVERSE_VENUES)
            _validate_route_stability_filters(
                min_route_stability_weight=min_route_stability_weight,
                min_route_presence_ratio=min_route_presence_ratio,
                min_route_samples=min_route_samples,
            )
            scan = await _run_bounded_universe_scan(
                "funding universe portfolio scan",
                lambda: service.scan(
                    venues=selected_venues,
                    ranking=ranking,  # type: ignore[arg-type]
                    fee_profile_overrides=_build_fee_profile_overrides(
                        extended_fee_profile=extended_fee_profile,
                        paradex_fee_profile=paradex_fee_profile,
                        hyperliquid_fee_profile=hyperliquid_fee_profile,
                    ),
                    target_notional=target_notional,
                    min_capacity_notional=min_capacity_notional,
                    min_daily_volume=min_daily_volume,
                    min_open_interest=min_open_interest,
                    min_roundtrip_edge=min_roundtrip_edge,
                    min_execution_quality_score=min_execution_quality_score,
                    min_execution_samples=min_execution_samples,
                    min_route_stability_weight=min_route_stability_weight,
                    min_route_presence_ratio=min_route_presence_ratio,
                    min_route_samples=min_route_samples,
                    include_symbols=include_symbols,
                    exclude_symbols=exclude_symbols,
                    exclude_tags=exclude_tags,
                    limit=max_positions * 5,
                ),
            )
            return build_portfolio_plan(
                scan,
                target_notional=target_notional,
                max_positions=max_positions,
                min_selected_notional=min_selected_notional,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/opportunities/execution-quality",
        response_model=list[ExecutionQualitySummary],
    )
    def execution_quality_routes(
        service: Annotated[ExecutionQualityService, Depends(get_execution_quality_service)],
        canonical_symbol: str | None = None,
        short_venue: str | None = None,
        long_venue: str | None = None,
        min_sample_size: int = 0,
        limit: int = 50,
    ) -> list[ExecutionQualitySummary]:
        if min_sample_size < 0:
            raise HTTPException(
                status_code=400,
                detail="min_sample_size must be non-negative",
            )
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return service.list_summaries(
            canonical_symbol=canonical_symbol,
            short_venue=short_venue,
            long_venue=long_venue,
            min_sample_size=min_sample_size,
            limit=limit,
        )

    @app.get(
        "/v1/executions/accounting/latest/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeAccountingSummary,
    )
    async def execution_accounting_for_paper_trade(
        paper_trade_id: int,
        service: Annotated[ExecutionAccountingService, Depends(get_execution_accounting_service)],
    ) -> PaperTradeAccountingSummary:
        summary = service.latest_for_paper_trade(paper_trade_id)
        if summary is None:
            raise HTTPException(status_code=404, detail="No execution accounting found")
        return summary

    @app.get(
        "/v1/executions/accounting/routes",
        response_model=list[RouteAccountingSummary],
    )
    async def execution_accounting_routes(
        service: Annotated[ExecutionAccountingService, Depends(get_execution_accounting_service)],
        canonical_symbol: str | None = None,
        label: str | None = None,
        limit: int = 50,
    ) -> list[RouteAccountingSummary]:
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return service.list_route_summaries(
            canonical_symbol=canonical_symbol,
            label=label,
            limit=limit,
        )

    @app.get(
        "/v1/opportunities/route-stability",
        response_model=list[RouteStabilitySummary],
    )
    def route_stability_routes(
        service: Annotated[RouteStabilityService, Depends(get_route_stability_service)],
        canonical_symbol: str | None = None,
        short_venue: str | None = None,
        long_venue: str | None = None,
        min_sample_size: int = 0,
        min_presence_ratio: float = 0.0,
        limit: int = 50,
    ) -> list[RouteStabilitySummary]:
        if min_sample_size < 0:
            raise HTTPException(
                status_code=400,
                detail="min_sample_size must be non-negative",
            )
        if min_presence_ratio < 0 or min_presence_ratio > 1:
            raise HTTPException(
                status_code=400,
                detail="min_presence_ratio must be between 0 and 1",
            )
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return service.list_summaries(
            canonical_symbol=canonical_symbol,
            short_venue=short_venue,
            long_venue=long_venue,
            min_sample_size=min_sample_size,
            min_presence_ratio=min_presence_ratio,
            limit=limit,
        )

    return app


app = create_app()
