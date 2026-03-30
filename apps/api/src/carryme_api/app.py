"""FastAPI application factory for carryme."""

from __future__ import annotations

import asyncio
import logging
import math
import os
from collections.abc import Callable
from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated, Any

import httpx
from carryme_models import (
    SUPPORTED_UNIVERSE_VENUES,
    AppDescriptor,
    ApprovedCanaryAlertEvent,
    ApprovedCanarySnapshot,
    CanaryLifecycleResult,
    CandidateAlertEvent,
    CleanupPreviewConfirmationEntry,
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
    PaperTradeBalanceDelta,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    PaperTradeOrderPreview,
    PaperTradeSystemState,
    PreviewConfirmationEntry,
    RouteAccountingSummary,
    RouteApprovalEntry,
    RouteApprovalUpsert,
    RouteStabilitySummary,
    ServiceHealth,
    SystemStateAlertEvent,
    TradingFeeProfile,
    VenueAccountPreflight,
    VenueBalanceSnapshot,
    VenueExecutionPreflight,
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
    build_execution_pair_status,
    build_live_execution_configs,
    build_live_submission_readiness,
    build_opportunity_record_from_universe_opportunity,
    build_pair_spec_from_universe_opportunity,
    build_paper_trade_execution_preflight,
    build_portfolio_plan,
    build_trade_intent,
    build_venue_execution_preflights,
    reconcile_execution,
    require_confirmed_cleanup_preview,
)
from carryme_runtime.execution_order_state import ExecutionLegOrderObserver
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
    SystemStateAlertStore,
    WatchlistStore,
)
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse
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
MAX_HISTORY_LIMIT = 1000
PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS = 10.0
logger = logging.getLogger(__name__)


class ConfirmPreviewRequest(BaseModel):
    """Operator confirmation payload for one unsigned order preview."""

    preview_hash: str = Field(min_length=1)
    slippage_tolerance_bps: int = Field(default=10, ge=0)
    note: str | None = None


def get_app_environment() -> str:
    """Return the runtime environment exposed by the API health endpoints."""

    return (
        os.getenv(APP_ENVIRONMENT_VARIABLE, DEFAULT_APP_ENVIRONMENT).strip()
        or DEFAULT_APP_ENVIRONMENT
    )


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
) -> dict[str, str] | None:
    configured_profiles = {
        "extended": extended_fee_profile,
        "paradex": paradex_fee_profile,
        "hyperliquid": hyperliquid_fee_profile,
    }
    overrides = {
        venue: profile
        for venue in SUPPORTED_UNIVERSE_VENUES
        if (profile := configured_profiles.get(venue))
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
) -> ExecutionAccountingService:
    """Return the derived execution accounting service."""

    return ExecutionAccountingService(journal_store=store)


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
        pair_close_preview = await pair_close_service.preview_from_execution(
            entry=execution,
            pair_status=pair_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return paper_trade, execution, pair_status, pair_close_preview


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
    candidates = await universe_service.scan_canary_candidates(
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
    approval = approval_service.get_for_candidate(selected)
    if approval is None or not approval.approved:
        raise HTTPException(
            status_code=409,
            detail="Selected canary route is no longer approved for live execution",
        )
    return selected, approval


def _select_latest_approved_canary_snapshot(
    *,
    store: ApprovedCanaryStore,
    approval_service: RouteApprovalService,
    label: str | None,
    max_snapshot_age_seconds: int,
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
    )
    if capped_notional <= 0:
        raise HTTPException(
            status_code=409,
            detail="Approved canary snapshot no longer permits a positive live notional",
        )
    return (
        snapshot,
        snapshot.candidate.model_copy(update={"suggested_canary_notional": capped_notional}),
        approval,
    )


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
    snapshot_age_seconds = max(
        0.0,
        (current_time - snapshot.captured_at).total_seconds(),
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


def _launch_ready_snapshot_payload_changed(
    previous_snapshot: LaunchReadyCanarySnapshot,
    current_snapshot: LaunchReadyCanarySnapshot,
) -> bool:
    previous_payload = previous_snapshot.model_dump(
        mode="python",
        exclude={
            "launch_ready_snapshot_id": True,
            "captured_at": True,
            "approved_snapshot": {"snapshot_id", "captured_at"},
        },
    )
    current_payload = current_snapshot.model_dump(
        mode="python",
        exclude={
            "launch_ready_snapshot_id": True,
            "captured_at": True,
            "approved_snapshot": {"snapshot_id", "captured_at"},
        },
    )
    return previous_payload != current_payload


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
    snapshot_age_seconds = max(
        0.0,
        (current_time - latest_snapshot.captured_at).total_seconds(),
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
    execution_store.mark_live_submission_completed(
        confirmation_entry_id=confirmation.entry_id,
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
            poll_attempts=poll_attempts,
            poll_interval_seconds=poll_interval_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    cleanup_execution: ExecutionJournalEntry | None = None
    if auto_cleanup and pair_status.recommended_action == "close_open_leg":
        try:
            cleanup_preview = await cleanup_preview_service.preview_from_execution(
                entry=primary_execution,
                pair_status=pair_status,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        try:
            await _ensure_cleanup_live_ready(
                venue=cleanup_preview.leg.venue,
                settings=settings,
                account_service=account_preflight_service,
            )
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
                    note="guarded pair auto-cleanup",
                )
            )
        if cleanup_confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Cleanup preview confirmation entry_id is required before live submission",
            )
        if not execution_store.reserve_live_submission(
            confirmation_entry_id=cleanup_confirmation.entry_id,
            preview_hash=cleanup_confirmation.preview_hash,
        ):
            existing_entry = execution_store.find_by_confirmation(
                confirmation_entry_id=cleanup_confirmation.entry_id,
                preview_hash=cleanup_confirmation.preview_hash,
            )
            if existing_entry is not None:
                return GuardedPairExecutionResult(
                    paper_trade_id=paper_trade_id,
                    preview_hash=confirmation.preview_hash,
                    primary_execution=primary_execution,
                    cleanup_execution=existing_entry,
                    pair_status=pair_status.model_copy(
                        update={
                            "notes": [
                                *pair_status.notes,
                                "Existing cleanup execution reused for this confirmation",
                            ]
                        }
                    ),
                )
            return GuardedPairExecutionResult(
                paper_trade_id=paper_trade_id,
                preview_hash=confirmation.preview_hash,
                primary_execution=primary_execution,
                cleanup_execution=None,
                pair_status=pair_status.model_copy(
                    update={
                        "notes": [
                            *pair_status.notes,
                            (
                                "Cleanup live submission was already reserved; "
                                "manual reconciliation is required before retrying"
                            ),
                        ]
                    }
                ),
            )
        try:
            cleanup_execution = await cleanup_live_router.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=cleanup_confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        cleanup_execution = execution_store.append(cleanup_execution)
        if cleanup_execution.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        execution_store.mark_live_submission_completed(
            confirmation_entry_id=cleanup_confirmation.entry_id,
            preview_hash=cleanup_confirmation.preview_hash,
            execution_entry_id=cleanup_execution.entry_id,
        )
        try:
            pair_status = await _observe_pair_status_for_execution(
                paper_trade=paper_trade,
                execution=cleanup_execution,
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
    paper_trade_id = paper_trade.entry_id or 0
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
        primary_execution = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            first_venue=first_venue,
        )
    except ValueError as exc:
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
        confirmation_entry_id=confirmation.entry_id,
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
    if auto_cleanup and pair_status.recommended_action == "close_open_leg":
        try:
            cleanup_preview = await cleanup_preview_service.preview_from_execution(
                entry=primary_execution,
                pair_status=pair_status,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        try:
            await _ensure_cleanup_live_ready(
                venue=cleanup_preview.leg.venue,
                settings=settings,
                account_service=account_preflight_service,
            )
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
                    note="guarded pair close auto-cleanup",
                )
            )
        if cleanup_confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Cleanup preview confirmation entry_id is required before live submission",
            )
        if not execution_store.reserve_live_submission(
            confirmation_entry_id=cleanup_confirmation.entry_id,
            preview_hash=cleanup_confirmation.preview_hash,
        ):
            existing_entry = execution_store.find_by_confirmation(
                confirmation_entry_id=cleanup_confirmation.entry_id,
                preview_hash=cleanup_confirmation.preview_hash,
            )
            if existing_entry is not None:
                return GuardedPairExecutionResult(
                    paper_trade_id=paper_trade_id,
                    preview_hash=confirmation.preview_hash,
                    primary_execution=primary_execution,
                    cleanup_execution=existing_entry,
                    pair_status=pair_status.model_copy(
                        update={
                            "notes": [
                                *pair_status.notes,
                                "Existing cleanup execution reused for this confirmation",
                            ]
                        }
                    ),
                )
            return GuardedPairExecutionResult(
                paper_trade_id=paper_trade_id,
                preview_hash=confirmation.preview_hash,
                primary_execution=primary_execution,
                cleanup_execution=None,
                pair_status=pair_status.model_copy(
                    update={
                        "notes": [
                            *pair_status.notes,
                            (
                                "Cleanup live submission was already reserved; "
                                "manual reconciliation is required before retrying"
                            ),
                        ]
                    }
                ),
            )
        try:
            cleanup_execution = await cleanup_live_router.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=cleanup_confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        cleanup_execution = execution_store.append(cleanup_execution)
        if cleanup_execution.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Execution journal append did not return an id",
            )
        execution_store.mark_live_submission_completed(
            confirmation_entry_id=cleanup_confirmation.entry_id,
            preview_hash=cleanup_confirmation.preview_hash,
            execution_entry_id=cleanup_execution.entry_id,
        )
        try:
            pair_status = await _observe_pair_status_for_execution(
                paper_trade=paper_trade,
                execution=cleanup_execution,
                settings=settings,
                account_service=account_preflight_service,
                order_state_service=order_state_service,
                observation_store=observation_store,
                observation_context="guarded_pair_close_cleanup_poll",
                poll_attempts=poll_attempts,
                poll_interval_seconds=poll_interval_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    return GuardedPairExecutionResult(
        paper_trade_id=paper_trade_id,
        preview_hash=confirmation.preview_hash,
        primary_execution=primary_execution,
        cleanup_execution=cleanup_execution,
        pair_status=pair_status,
    )


def create_app() -> FastAPI:
    """Create the FastAPI application."""

    app = FastAPI(title="carryme", version=APP_VERSION)

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
        "/v1/opportunities/funding-universe/canary/snapshots",
        response_model=list[ApprovedCanarySnapshot],
    )
    def approved_canary_snapshots(
        store: Annotated[ApprovedCanaryStore, Depends(get_approved_canary_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[ApprovedCanarySnapshot]:
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return store.list_recent(limit=limit, label=label)

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
        if limit < 0:
            raise HTTPException(status_code=400, detail="limit must be non-negative")
        return store.list_recent(limit=limit, label=label)

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
        min_route_stability_weight: float = 0.35,
        min_route_presence_ratio: float = 0.35,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        limit: int = 10,
    ) -> PaperTradeEntry:
        selected_venues = venues or ["extended", "paradex", "hyperliquid"]
        candidates = await universe_service.scan_canary_candidates(
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
        paper_trade, execution, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        if cleanup_preview.preview_hash != normalized_preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Preview hash did not match the current cleanup preview",
            )
        confirmation = CleanupPreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=paper_trade.entry_id or paper_trade_id,
            label=paper_trade.intent.label,
            preview_hash=normalized_preview_hash,
            preview=cleanup_preview,
            note=note,
        )
        return confirmation_store.append(confirmation)

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
        paper_trade, _, _, canonical_preview = await _build_pair_close_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            pair_close_service=pair_close_service,
        )
        if preview is not None and not _pair_close_preview_matches_canonical(
            provided=preview,
            canonical=canonical_preview,
        ):
            raise HTTPException(
                status_code=409,
                detail="Provided pair close preview did not match the current server preview",
            )
        if canonical_preview.preview_hash != normalized_preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Preview hash did not match the current pair close preview",
            )
        confirmation = PairClosePreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=paper_trade.entry_id or paper_trade_id,
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
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
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
        if cleanup_preview.preview_hash != normalized_preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Cleanup preview hash did not match the current cleanup preview",
            )
        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=paper_trade_id,
        )
        try:
            confirmation = require_confirmed_cleanup_preview(
                paper_trade_id=paper_trade_id,
                preview_hash=normalized_preview_hash,
                confirmations=confirmations,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Cleanup preview confirmation entry_id is required before live submission",
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
                raise HTTPException(status_code=409, detail=existing_entry.model_dump(mode="json"))
            raise HTTPException(
                status_code=409,
                detail=(
                    "A live submission is already reserved for this confirmed cleanup preview; "
                    "manual reconciliation is required before retrying"
                ),
            )
        try:
            journal_entry = await live_service.submit_confirmed_cleanup_preview(
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
        "/v1/executions/live/paradex/cleanup/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_paradex_cleanup_for_paper_trade(
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
        live_service: Annotated[
            ParadexLiveExecutionService,
            Depends(get_paradex_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
        normalized_preview_hash = preview_hash.strip()
        if not normalized_preview_hash:
            raise HTTPException(status_code=400, detail="preview_hash must be non-empty")
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
        if cleanup_preview.preview_hash != normalized_preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Cleanup preview hash did not match the current cleanup preview",
            )
        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=paper_trade_id,
        )
        try:
            confirmation = require_confirmed_cleanup_preview(
                paper_trade_id=paper_trade_id,
                preview_hash=normalized_preview_hash,
                confirmations=confirmations,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if confirmation.entry_id is None:
            raise HTTPException(
                status_code=500,
                detail="Cleanup preview confirmation entry_id is required before live submission",
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
                raise HTTPException(status_code=409, detail=existing_entry.model_dump(mode="json"))
            raise HTTPException(
                status_code=409,
                detail=(
                    "A live submission is already reserved for this confirmed cleanup preview; "
                    "manual reconciliation is required before retrying"
                ),
            )
        try:
            journal_entry = await live_service.submit_confirmed_cleanup_preview(
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
        "/v1/executions/live/hyperliquid/cleanup/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    async def execute_hyperliquid_cleanup_for_paper_trade(
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
        live_service: Annotated[
            HyperliquidLiveExecutionService,
            Depends(get_hyperliquid_live_execution_service),
        ],
    ) -> ExecutionJournalEntry:
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
        if cleanup_preview.preview_hash != preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Cleanup preview hash did not match the current cleanup preview",
            )
        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=paper_trade_id,
        )
        try:
            confirmation = require_confirmed_cleanup_preview(
                paper_trade_id=paper_trade_id,
                preview_hash=preview_hash,
                confirmations=confirmations,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        try:
            journal_entry = await live_service.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return execution_store.append(journal_entry)

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
        execution_store.mark_live_submission_completed(
            confirmation_entry_id=confirmation.entry_id,
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
                poll_attempts=poll_attempts,
                poll_interval_seconds=poll_interval_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        cleanup_execution: ExecutionJournalEntry | None = None
        if auto_cleanup and pair_status.recommended_action == "close_open_leg":
            try:
                cleanup_preview = await cleanup_preview_service.preview_from_execution(
                    entry=primary_execution,
                    pair_status=pair_status,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc

            try:
                await _ensure_cleanup_live_ready(
                    venue=cleanup_preview.leg.venue,
                    settings=settings,
                    account_service=account_preflight_service,
                )
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
                        note="guarded pair auto-cleanup",
                    )
                )
            if cleanup_confirmation.entry_id is None:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "Cleanup preview confirmation entry_id is required before live submission"
                    ),
                )
            if not execution_store.reserve_live_submission(
                confirmation_entry_id=cleanup_confirmation.entry_id,
                preview_hash=cleanup_confirmation.preview_hash,
            ):
                existing_entry = execution_store.find_by_confirmation(
                    confirmation_entry_id=cleanup_confirmation.entry_id,
                    preview_hash=cleanup_confirmation.preview_hash,
                )
                if existing_entry is not None:
                    return GuardedPairExecutionResult(
                        paper_trade_id=paper_trade_id,
                        preview_hash=normalized_preview_hash,
                        primary_execution=primary_execution,
                        cleanup_execution=existing_entry,
                        pair_status=pair_status.model_copy(
                            update={
                                "notes": [
                                    *pair_status.notes,
                                    "Existing cleanup execution reused for this confirmation",
                                ]
                            }
                        ),
                    )
                return GuardedPairExecutionResult(
                    paper_trade_id=paper_trade_id,
                    preview_hash=normalized_preview_hash,
                    primary_execution=primary_execution,
                    cleanup_execution=None,
                    pair_status=pair_status.model_copy(
                        update={
                            "notes": [
                                *pair_status.notes,
                                (
                                    "Cleanup live submission was already reserved; "
                                    "manual reconciliation is required before retrying"
                                ),
                            ]
                        }
                    ),
                )
            try:
                cleanup_execution = await cleanup_live_router.submit_confirmed_cleanup_preview(
                    paper_trade=paper_trade,
                    confirmation=cleanup_confirmation,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            cleanup_execution = execution_store.append(cleanup_execution)
            if cleanup_execution.entry_id is None:
                raise HTTPException(
                    status_code=500,
                    detail="Execution journal append did not return an id",
                )
            execution_store.mark_live_submission_completed(
                confirmation_entry_id=cleanup_confirmation.entry_id,
                preview_hash=cleanup_confirmation.preview_hash,
                execution_entry_id=cleanup_execution.entry_id,
            )
            try:
                pair_status = await _observe_pair_status_for_execution(
                    paper_trade=paper_trade,
                    execution=cleanup_execution,
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

        return GuardedPairExecutionResult(
            paper_trade_id=paper_trade_id,
            preview_hash=normalized_preview_hash,
            primary_execution=primary_execution,
            cleanup_execution=cleanup_execution,
            pair_status=pair_status,
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
            primary_execution = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
                first_venue=first_venue,
            )
        except ValueError as exc:
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
            confirmation_entry_id=confirmation.entry_id,
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
        if auto_cleanup and pair_status.recommended_action == "close_open_leg":
            try:
                cleanup_preview = await cleanup_preview_service.preview_from_execution(
                    entry=primary_execution,
                    pair_status=pair_status,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc

            try:
                await _ensure_cleanup_live_ready(
                    venue=cleanup_preview.leg.venue,
                    settings=settings,
                    account_service=account_preflight_service,
                )
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
                        note="guarded pair close auto-cleanup",
                    )
                )
            if cleanup_confirmation.entry_id is None:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "Cleanup preview confirmation entry_id is required before live submission"
                    ),
                )
            if not execution_store.reserve_live_submission(
                confirmation_entry_id=cleanup_confirmation.entry_id,
                preview_hash=cleanup_confirmation.preview_hash,
            ):
                existing_entry = execution_store.find_by_confirmation(
                    confirmation_entry_id=cleanup_confirmation.entry_id,
                    preview_hash=cleanup_confirmation.preview_hash,
                )
                if existing_entry is not None:
                    return GuardedPairExecutionResult(
                        paper_trade_id=paper_trade_id,
                        preview_hash=normalized_preview_hash,
                        primary_execution=primary_execution,
                        cleanup_execution=existing_entry,
                        pair_status=pair_status.model_copy(
                            update={
                                "notes": [
                                    *pair_status.notes,
                                    "Existing cleanup execution reused for this confirmation",
                                ]
                            }
                        ),
                    )
                return GuardedPairExecutionResult(
                    paper_trade_id=paper_trade_id,
                    preview_hash=normalized_preview_hash,
                    primary_execution=primary_execution,
                    cleanup_execution=None,
                    pair_status=pair_status.model_copy(
                        update={
                            "notes": [
                                *pair_status.notes,
                                (
                                    "Cleanup live submission was already reserved; "
                                    "manual reconciliation is required before retrying"
                                ),
                            ]
                        }
                    ),
                )
            try:
                cleanup_execution = await cleanup_live_router.submit_confirmed_cleanup_preview(
                    paper_trade=paper_trade,
                    confirmation=cleanup_confirmation,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            cleanup_execution = execution_store.append(cleanup_execution)
            if cleanup_execution.entry_id is None:
                raise HTTPException(
                    status_code=500,
                    detail="Execution journal append did not return an id",
                )
            execution_store.mark_live_submission_completed(
                confirmation_entry_id=cleanup_confirmation.entry_id,
                preview_hash=cleanup_confirmation.preview_hash,
                execution_entry_id=cleanup_execution.entry_id,
            )
            try:
                pair_status = await _observe_pair_status_for_execution(
                    paper_trade=paper_trade,
                    execution=cleanup_execution,
                    settings=settings,
                    account_service=account_preflight_service,
                    order_state_service=order_state_service,
                    observation_store=observation_store,
                    observation_context="guarded_pair_close_cleanup_poll",
                    poll_attempts=poll_attempts,
                    poll_interval_seconds=poll_interval_seconds,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except (ConnectorError, UpstreamDataError, httpx.HTTPError) as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc

        return GuardedPairExecutionResult(
            paper_trade_id=paper_trade_id,
            preview_hash=normalized_preview_hash,
            primary_execution=primary_execution,
            cleanup_execution=cleanup_execution,
            pair_status=pair_status,
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
            return await service.scan(
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
        min_route_stability_weight: float = 0.35,
        min_route_presence_ratio: float = 0.35,
        min_route_samples: int = 2,
        include_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_symbols: Annotated[list[str] | None, Query()] = None,
        exclude_tags: Annotated[list[str] | None, Query()] = None,
        approved_only: bool = False,
        limit: int = 10,
    ) -> list[FundingUniverseCanaryCandidate]:
        try:
            selected_venues = venues or ["extended", "paradex", "hyperliquid"]
            candidates = await service.scan_canary_candidates(
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
            )
            if approved_only:
                return approval_service.filter_approved_canary_candidates(candidates)[:limit]
            return candidates
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post(
        "/v1/executions/live/canary-cycle",
        response_model=CanaryLifecycleResult,
    )
    async def execute_guarded_canary_cycle(
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
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
        cleanup_preview_service: Annotated[
            CleanupPreviewRouter,
            Depends(get_cleanup_preview_service),
        ],
        pair_close_preview_service: Annotated[
            PairClosePreviewService,
            Depends(get_pair_close_preview_service),
        ],
        cleanup_live_router: Annotated[
            CleanupLiveExecutionRouter,
            Depends(get_cleanup_live_execution_router),
        ],
        paired_service: Annotated[
            PairedLiveExecutionCoordinator,
            Depends(get_paired_live_execution_coordinator),
        ],
        pair_close_live_service: Annotated[
            PairCloseLiveExecutionCoordinator,
            Depends(get_pair_close_live_execution_coordinator),
        ],
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
        min_route_stability_weight: float = 0.35,
        min_route_presence_ratio: float = 0.35,
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
        return await _run_guarded_canary_lifecycle(
            candidate=selected,
            approval=approval,
            desired_notional=desired_notional,
            note=note,
            lifecycle_note=None,
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
        snapshot, selected, approval = _select_latest_approved_canary_snapshot(
            store=store,
            approval_service=approval_service,
            label=label,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
        )

        def _resolve_lazy_dependency(getter: object, builder: Callable[[], Any]) -> Any:
            override = request.app.dependency_overrides.get(getter)
            if override is not None:
                return override()
            return builder()

        extended_service = _resolve_lazy_dependency(
            get_extended_live_execution_service,
            lambda: get_extended_live_execution_service(settings),
        )
        hyperliquid_service = _resolve_lazy_dependency(
            get_hyperliquid_live_execution_service,
            lambda: get_hyperliquid_live_execution_service(settings),
        )
        paradex_service = _resolve_lazy_dependency(
            get_paradex_live_execution_service,
            lambda: get_paradex_live_execution_service(settings),
        )
        cleanup_live_router = _resolve_lazy_dependency(
            get_cleanup_live_execution_router,
            lambda: get_cleanup_live_execution_router(
                extended_service,
                hyperliquid_service,
                paradex_service,
            ),
        )
        paired_service = _resolve_lazy_dependency(
            get_paired_live_execution_coordinator,
            lambda: get_paired_live_execution_coordinator(
                extended_service,
                hyperliquid_service,
                paradex_service,
            ),
        )
        pair_close_live_service = _resolve_lazy_dependency(
            get_pair_close_live_execution_coordinator,
            lambda: get_pair_close_live_execution_coordinator(
                extended_service,
                hyperliquid_service,
                paradex_service,
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

        def _resolve_lazy_dependency(getter: object, builder: Callable[[], Any]) -> Any:
            override = request.app.dependency_overrides.get(getter)
            if override is not None:
                return override()
            return builder()

        extended_service = _resolve_lazy_dependency(
            get_extended_live_execution_service,
            lambda: get_extended_live_execution_service(settings),
        )
        hyperliquid_service = _resolve_lazy_dependency(
            get_hyperliquid_live_execution_service,
            lambda: get_hyperliquid_live_execution_service(settings),
        )
        paradex_service = _resolve_lazy_dependency(
            get_paradex_live_execution_service,
            lambda: get_paradex_live_execution_service(settings),
        )
        cleanup_live_router = _resolve_lazy_dependency(
            get_cleanup_live_execution_router,
            lambda: get_cleanup_live_execution_router(
                extended_service,
                hyperliquid_service,
                paradex_service,
            ),
        )
        paired_service = _resolve_lazy_dependency(
            get_paired_live_execution_coordinator,
            lambda: get_paired_live_execution_coordinator(
                extended_service,
                hyperliquid_service,
                paradex_service,
            ),
        )
        pair_close_live_service = _resolve_lazy_dependency(
            get_pair_close_live_execution_coordinator,
            lambda: get_pair_close_live_execution_coordinator(
                extended_service,
                hyperliquid_service,
                paradex_service,
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

        def _resolve_lazy_dependency(getter: object, builder: Callable[[], Any]) -> Any:
            override = request.app.dependency_overrides.get(getter)
            if override is not None:
                return override()
            return builder()

        extended_service = _resolve_lazy_dependency(
            get_extended_live_execution_service,
            lambda: get_extended_live_execution_service(settings),
        )
        hyperliquid_service = _resolve_lazy_dependency(
            get_hyperliquid_live_execution_service,
            lambda: get_hyperliquid_live_execution_service(settings),
        )
        paradex_service = _resolve_lazy_dependency(
            get_paradex_live_execution_service,
            lambda: get_paradex_live_execution_service(settings),
        )
        cleanup_live_router = _resolve_lazy_dependency(
            get_cleanup_live_execution_router,
            lambda: get_cleanup_live_execution_router(
                extended_service,
                hyperliquid_service,
                paradex_service,
            ),
        )
        paired_service = _resolve_lazy_dependency(
            get_paired_live_execution_coordinator,
            lambda: get_paired_live_execution_coordinator(
                extended_service,
                hyperliquid_service,
                paradex_service,
            ),
        )
        pair_close_live_service = _resolve_lazy_dependency(
            get_pair_close_live_execution_coordinator,
            lambda: get_pair_close_live_execution_coordinator(
                extended_service,
                hyperliquid_service,
                paradex_service,
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
            scan = await service.scan(
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
