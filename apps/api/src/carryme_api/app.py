"""FastAPI application factory for carryme."""

from __future__ import annotations

import asyncio
import logging
import math
import os
from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated

import httpx
from carryme_models import (
    AppDescriptor,
    CandidateAlertEvent,
    CleanupPreviewConfirmationEntry,
    ExecutionAlertEvent,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionObservationEntry,
    ExecutionOrderState,
    ExecutionPairClosePreview,
    ExecutionPairStatus,
    ExecutionReconciliation,
    FundingArbOpportunity,
    FundingPairTradeIntent,
    FundingUniversePortfolioPlan,
    FundingUniverseScan,
    GuardedPairExecutionResult,
    LiveSubmissionReadiness,
    OpportunityRecord,
    PairClosePreviewConfirmationEntry,
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    PaperTradeOrderPreview,
    PreviewConfirmationEntry,
    ServiceHealth,
    TradingFeeProfile,
    VenueAccountPreflight,
    VenueExecutionPreflight,
    WatchlistDocument,
)
from carryme_normalizers import list_fee_profiles
from carryme_runtime import (
    AccountPreflightConfigMap,
    AccountPreflightService,
    CleanupLiveExecutionRouter,
    CleanupPreviewRouter,
    ConnectorError,
    ExecutionAdapter,
    ExecutionOrderStateService,
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
    UpstreamDataError,
    build_account_preflight_configs,
    build_execution_pair_status,
    build_live_execution_configs,
    build_live_submission_readiness,
    build_paper_trade_execution_preflight,
    build_portfolio_plan,
    build_trade_intent,
    build_venue_execution_preflights,
    reconcile_execution,
    require_confirmed_cleanup_preview,
)
from carryme_runtime.execution_order_state import ExecutionLegOrderObserver
from carryme_storage import (
    CandidateAlertStore,
    CleanupPreviewConfirmationStore,
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    OpportunityHistoryStore,
    PairClosePreviewConfirmationStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    WatchlistStore,
)
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Response
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


def get_opportunity_service() -> OpportunityService:
    """Return the live opportunity scoring service."""

    return OpportunityService()


@lru_cache
def get_opportunity_universe_service() -> OpportunityUniverseService:
    """Return the live funding-universe discovery and ranking service."""

    return OpportunityUniverseService()


def get_history_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> OpportunityHistoryStore:
    """Return the shared opportunity history store."""

    return _history_store_for_path(settings.database_path)


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


async def _build_readiness_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    preview_hash: str,
    settings: ApiSettings,
    confirmation_store: PreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
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
    )


async def _build_venue_scoped_readiness_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    preview_hash: str,
    venue: str,
    settings: ApiSettings,
    confirmation_store: PreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
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
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
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

        try:
            readiness, confirmation = await _build_venue_scoped_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                venue="paradex",
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
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
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
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

        try:
            readiness, confirmation = await _build_venue_scoped_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                venue="extended",
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
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
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
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

        try:
            readiness, confirmation = await _build_venue_scoped_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                venue="hyperliquid",
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
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
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        account_preflight_service: Annotated[
            AccountPreflightService,
            Depends(get_account_preflight_service),
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

        try:
            readiness = await _build_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
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

        try:
            readiness = await _build_readiness_for_paper_trade(
                paper_trade=paper_trade,
                preview_hash=normalized_preview_hash,
                settings=settings,
                confirmation_store=confirmation_store,
                account_preflight_service=account_preflight_service,
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
        venues: list[str] | None = None,
        ranking: str = "quality_adjusted_roundtrip_pnl",
        target_notional: float = 5_000.0,
        min_capacity_notional: float = 0.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        limit: int = 20,
    ) -> FundingUniverseScan:
        try:
            selected_venues = venues or ["extended", "paradex", "hyperliquid"]
            return await service.scan(
                venues=selected_venues,
                ranking=ranking,  # type: ignore[arg-type]
                target_notional=target_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/opportunities/funding-universe/portfolio",
        response_model=FundingUniversePortfolioPlan,
    )
    async def funding_universe_portfolio(
        service: Annotated[OpportunityUniverseService, Depends(get_opportunity_universe_service)],
        venues: list[str] | None = None,
        ranking: str = "quality_adjusted_roundtrip_pnl",
        target_notional: float = 5_000.0,
        min_capacity_notional: float = 0.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        max_positions: int = 5,
        min_selected_notional: float = 0.0,
    ) -> FundingUniversePortfolioPlan:
        try:
            selected_venues = venues or ["extended", "paradex", "hyperliquid"]
            scan = await service.scan(
                venues=selected_venues,
                ranking=ranking,  # type: ignore[arg-type]
                target_notional=target_notional,
                min_capacity_notional=min_capacity_notional,
                min_daily_volume=min_daily_volume,
                min_open_interest=min_open_interest,
                min_roundtrip_edge=min_roundtrip_edge,
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
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    return app


app = create_app()
