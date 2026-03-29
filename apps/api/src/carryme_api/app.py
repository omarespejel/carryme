"""FastAPI application factory for carryme."""

from __future__ import annotations

from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated

import httpx
from carryme_models import (
    AppDescriptor,
    CandidateAlertEvent,
    CleanupPreviewConfirmationEntry,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionOrderState,
    ExecutionPairStatus,
    ExecutionReconciliation,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    LiveSubmissionReadiness,
    OpportunityRecord,
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
    ConnectorError,
    ExecutionAdapter,
    ExecutionOrderStateService,
    ExtendedCleanupPreviewService,
    ExtendedLiveExecutionService,
    ExtendedOrderStateObserver,
    LiveExecutionConfigMap,
    MockExecutionAdapter,
    OpportunityService,
    OrderPreviewService,
    PairedLiveExecutionCoordinator,
    ParadexLiveExecutionService,
    ParadexOrderStateObserver,
    build_execution_pair_status,
    build_live_submission_readiness,
    build_paper_trade_execution_preflight,
    build_trade_intent,
    build_venue_execution_preflights,
    reconcile_execution,
    require_confirmed_cleanup_preview,
)
from carryme_runtime.execution_order_state import ExecutionLegOrderObserver
from carryme_storage import (
    CandidateAlertStore,
    CleanupPreviewConfirmationStore,
    ExecutionJournalStore,
    OpportunityHistoryStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    WatchlistStore,
)
from fastapi import Body, Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse

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
APP_ENVIRONMENT = "development"


def get_opportunity_service() -> OpportunityService:
    """Return the live opportunity scoring service."""

    return OpportunityService()


def get_history_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> OpportunityHistoryStore:
    """Return the shared opportunity history store."""

    return OpportunityHistoryStore(settings.database_path)


def get_candidate_alert_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> CandidateAlertStore:
    """Return the shared candidate alert store."""

    return CandidateAlertStore(settings.database_path)


def get_watchlist_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> WatchlistStore:
    """Return the shared watchlist store."""

    return WatchlistStore(settings.watchlist_path)


def get_paper_trade_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> PaperTradeStore:
    """Return the shared paper trade journal store."""

    return PaperTradeStore(settings.database_path)


def get_execution_journal_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExecutionJournalStore:
    """Return the shared execution journal store."""

    return ExecutionJournalStore(settings.database_path)


def get_preview_confirmation_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> PreviewConfirmationStore:
    """Return the shared preview confirmation store."""

    return PreviewConfirmationStore(settings.database_path)


def get_cleanup_preview_confirmation_store(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> CleanupPreviewConfirmationStore:
    """Return the shared cleanup preview confirmation store."""

    return CleanupPreviewConfirmationStore(settings.database_path)


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

    return ExtendedLiveExecutionService(
        api_key=settings.extended_api_key or "",
        stark_private_key=settings.extended_stark_private_key or "",
    )


def get_extended_cleanup_preview_service(
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> ExtendedCleanupPreviewService:
    """Return the Extended cleanup-preview service."""

    return ExtendedCleanupPreviewService(api_key=settings.extended_api_key or "")


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
    return ExecutionOrderStateService(observers=observers)


def get_paired_live_execution_coordinator(
    extended_service: Annotated[
        ExtendedLiveExecutionService,
        Depends(get_extended_live_execution_service),
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


def _build_live_execution_configs(settings: ApiSettings) -> LiveExecutionConfigMap:
    """Build the current venue credential map from API settings."""

    return {
        "extended": {
            "enabled": settings.extended_live_enabled,
            "credentials": {
                "api_key": settings.extended_api_key,
                "stark_private_key": settings.extended_stark_private_key,
            },
        },
        "paradex": {
            "enabled": settings.paradex_live_enabled,
            "credentials": {
                "account_address": settings.paradex_account_address,
                "private_key": settings.paradex_private_key,
            },
        },
        "hyperliquid": {
            "enabled": settings.hyperliquid_live_enabled,
            "credentials": {
                "account_address": settings.hyperliquid_account_address,
                "api_wallet_private_key": settings.hyperliquid_api_wallet_private_key,
            },
        },
    }


def _build_account_preflight_configs(settings: ApiSettings) -> AccountPreflightConfigMap:
    """Build the authenticated-read account probe config map from API settings."""

    return {
        "extended": {
            "enabled": settings.extended_live_enabled,
            "credentials": {
                "api_key": settings.extended_api_key,
            },
        },
        "paradex": {
            "enabled": settings.paradex_live_enabled,
            "credentials": {
                "account_address": settings.paradex_account_address,
                "bearer_token": settings.paradex_bearer_token,
                "private_key": settings.paradex_private_key,
            },
        },
    }


async def _build_readiness_for_paper_trade(
    *,
    paper_trade: PaperTradeEntry,
    preview_hash: str,
    settings: ApiSettings,
    confirmation_store: PreviewConfirmationStore,
    account_preflight_service: AccountPreflightService,
) -> LiveSubmissionReadiness:
    execution_preflight = build_paper_trade_execution_preflight(
        paper_trade,
        _build_live_execution_configs(settings),
    )
    account_preflight = await account_preflight_service.probe_paper_trade(
        paper_trade,
        _build_account_preflight_configs(settings),
    )
    confirmations = confirmation_store.list_recent(
        limit=50,
        paper_trade_id=paper_trade.entry_id,
    )
    return build_live_submission_readiness(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        preview_hash=preview_hash,
        confirmations=confirmations,
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
) -> LiveSubmissionReadiness:
    execution_statuses = {
        item.venue: item
        for item in build_venue_execution_preflights(
            _build_live_execution_configs(settings)
        )
    }
    selected_execution = execution_statuses[venue]
    execution_blockers: list[str] = []
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
        venues=[selected_execution],
        blocking_reasons=execution_blockers,
    )

    account_preflight_all = await account_preflight_service.probe_paper_trade(
        paper_trade,
        _build_account_preflight_configs(settings),
    )
    account_statuses = {item.venue: item for item in account_preflight_all.venues}
    selected_account = account_statuses[venue]
    account_blockers: list[str] = []
    if not selected_account.enabled:
        account_blockers.append(f"Venue {venue} account preflight is not enabled")
    account_blockers.extend(selected_account.blocking_reasons)
    account_preflight = PaperTradeAccountPreflight(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        ready=not account_blockers,
        venues=[selected_account],
        blocking_reasons=account_blockers,
    )

    confirmations = confirmation_store.list_recent(
        limit=50,
        paper_trade_id=paper_trade.entry_id,
    )
    return build_live_submission_readiness(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        preview_hash=preview_hash,
        confirmations=confirmations,
        execution_preflight=execution_preflight,
        account_preflight=account_preflight,
    )


async def _build_cleanup_context_for_paper_trade(
    *,
    paper_trade_id: int,
    settings: ApiSettings,
    paper_store: PaperTradeStore,
    execution_store: ExecutionJournalStore,
    account_service: AccountPreflightService,
    order_state_service: ExecutionOrderStateService,
    cleanup_service: ExtendedCleanupPreviewService,
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


async def _ensure_extended_cleanup_live_ready(
    *,
    settings: ApiSettings,
    account_service: AccountPreflightService,
) -> None:
    execution_statuses = {
        item.venue: item
        for item in build_venue_execution_preflights(_build_live_execution_configs(settings))
    }
    extended_execution = execution_statuses["extended"]
    execution_blockers: list[str] = []
    if not extended_execution.enabled:
        execution_blockers.append("Venue extended live execution is not enabled")
    if extended_execution.missing_env_vars:
        execution_blockers.append(
            "Venue extended is missing required credentials: "
            + ", ".join(extended_execution.missing_env_vars)
        )
    if execution_blockers:
        raise HTTPException(
            status_code=409,
            detail=PaperTradeExecutionPreflight(
                paper_trade_id=0,
                label="extended_cleanup",
                ready=False,
                venues=[extended_execution],
                blocking_reasons=execution_blockers,
            ).model_dump(mode="json"),
        )

    account_statuses = {
        item.venue: item
        for item in await account_service.probe_venues(_build_account_preflight_configs(settings))
    }
    extended_account = account_statuses["extended"]
    account_blockers: list[str] = []
    if not extended_account.enabled:
        account_blockers.append("Venue extended account preflight is not enabled")
    account_blockers.extend(extended_account.blocking_reasons)
    if account_blockers:
        raise HTTPException(
            status_code=409,
            detail=PaperTradeAccountPreflight(
                paper_trade_id=0,
                label="extended_cleanup",
                ready=False,
                venues=[extended_account],
                blocking_reasons=account_blockers,
            ).model_dump(mode="json"),
        )


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

    if sample < limit:
        sample = limit
    records = store.list_recent(limit=sample, label=label)
    selected = _select_trade_intent_records(
        records,
        limit=limit,
        min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
        min_capacity_notional=min_capacity_notional,
        max_break_even_days_entry=max_break_even_days_entry,
    )
    return [
        build_trade_intent(
            record,
            capacity_fraction=capacity_fraction,
            max_target_notional=max_target_notional,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
            max_break_even_days_entry=max_break_even_days_entry,
        )
        for record in selected
    ]


def create_app() -> FastAPI:
    """Create the FastAPI application."""

    app = FastAPI(title="carryme", version=APP_VERSION)

    @app.get("/health", response_model=ServiceHealth)
    def health() -> ServiceHealth:
        return ServiceHealth(
            service=AppDescriptor(
                name=APP_NAME,
                version=APP_VERSION,
                environment=APP_ENVIRONMENT,
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
        return WatchlistDocument(pairs=store.load())

    @app.put("/v1/watchlist", response_model=WatchlistDocument)
    def replace_watchlist(
        document: Annotated[WatchlistDocument, Body(...)],
        store: Annotated[WatchlistStore, Depends(get_watchlist_store)],
    ) -> WatchlistDocument:
        pairs = [FundingPairSpec.model_validate(item) for item in document.pairs]
        return WatchlistDocument(pairs=store.replace(pairs))

    @app.get("/v1/history/funding-pairs", response_model=list[OpportunityRecord])
    def history(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 50,
        label: str | None = None,
    ) -> list[OpportunityRecord]:
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
        if sample < limit:
            sample = limit
        records = store.list_recent(limit=sample, label=label)
        return latest_records_by_label(records, limit=limit)

    @app.get("/v1/history/funding-pairs/ranked", response_model=list[OpportunityRecord])
    def ranked_history(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        limit: int = 20,
        sample: int = 200,
        label: str | None = None,
    ) -> list[OpportunityRecord]:
        if sample < limit:
            sample = limit
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
        if sample < limit:
            sample = limit
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
        try:
            return store.list_recent(limit=limit, label=label)
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
        return store.list_recent(limit=limit, label=label)

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
        return store.list_recent(limit=limit, label=label)

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
            ExtendedCleanupPreviewService,
            Depends(get_extended_cleanup_preview_service),
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
            ExtendedCleanupPreviewService,
            Depends(get_extended_cleanup_preview_service),
        ],
        confirmation_store: Annotated[
            CleanupPreviewConfirmationStore,
            Depends(get_cleanup_preview_confirmation_store),
        ],
        note: str | None = None,
    ) -> CleanupPreviewConfirmationEntry:
        paper_trade, execution, _, cleanup_preview = await _build_cleanup_context_for_paper_trade(
            paper_trade_id=paper_trade_id,
            settings=settings,
            paper_store=paper_store,
            execution_store=execution_store,
            account_service=account_service,
            order_state_service=order_state_service,
            cleanup_service=cleanup_service,
        )
        if cleanup_preview.preview_hash != preview_hash:
            raise HTTPException(
                status_code=409,
                detail="Preview hash did not match the current cleanup preview",
            )
        confirmation = CleanupPreviewConfirmationEntry(
            confirmed_at=datetime.now(UTC),
            paper_trade_id=paper_trade.entry_id or paper_trade_id,
            label=paper_trade.intent.label,
            preview_hash=cleanup_preview.preview_hash,
            preview=cleanup_preview,
            note=note,
        )
        return confirmation_store.append(confirmation)

    @app.get("/v1/executions/preflight/venues", response_model=list[VenueExecutionPreflight])
    def execution_preflight_venues(
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
    ) -> list[VenueExecutionPreflight]:
        return build_venue_execution_preflights(_build_live_execution_configs(settings))

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
            _build_live_execution_configs(settings),
        )

    @app.get(
        "/v1/executions/account-preflight/venues",
        response_model=list[VenueAccountPreflight],
    )
    async def execution_account_preflight_venues(
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        service: Annotated[AccountPreflightService, Depends(get_account_preflight_service)],
    ) -> list[VenueAccountPreflight]:
        return await service.probe_venues(_build_account_preflight_configs(settings))

    @app.get(
        "/v1/executions/account-preflight/from-paper-trade/{paper_trade_id}",
        response_model=PaperTradeAccountPreflight,
    )
    async def execution_account_preflight_for_paper_trade(
        paper_trade_id: int,
        settings: Annotated[ApiSettings, Depends(get_api_settings)],
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        service: Annotated[AccountPreflightService, Depends(get_account_preflight_service)],
    ) -> PaperTradeAccountPreflight:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        return await service.probe_paper_trade(
            paper_trade,
            _build_account_preflight_configs(settings),
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
    ) -> LiveSubmissionReadiness:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        return await _build_readiness_for_paper_trade(
            paper_trade=paper_trade,
            preview_hash=preview_hash,
            settings=settings,
            confirmation_store=confirmation_store,
            account_preflight_service=service,
        )

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
        except (ConnectorError, httpx.HTTPError) as exc:
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
        return store.list_recent(limit=limit, label=label, paper_trade_id=paper_trade_id)

    @app.post(
        "/v1/executions/preview-confirmations/from-paper-trade/{paper_trade_id}",
        response_model=PreviewConfirmationEntry,
    )
    async def confirm_paper_trade_preview(
        paper_trade_id: int,
        preview_hash: str,
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        confirmation_store: Annotated[
            PreviewConfirmationStore,
            Depends(get_preview_confirmation_store),
        ],
        service: Annotated[OrderPreviewService, Depends(get_order_preview_service)],
        slippage_tolerance_bps: int = 10,
        note: str | None = None,
    ) -> PreviewConfirmationEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )
        try:
            preview = await service.preview_paper_trade(
                paper_trade,
                slippage_tolerance_bps=slippage_tolerance_bps,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if preview.preview_hash != preview_hash:
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
            note=note,
        )
        return confirmation_store.append(confirmation)

    @app.post(
        "/v1/executions/mock/from-paper-trade/{paper_trade_id}",
        response_model=ExecutionJournalEntry,
    )
    def execute_saved_paper_trade(
        paper_trade_id: int,
        paper_store: Annotated[PaperTradeStore, Depends(get_paper_trade_store)],
        execution_store: Annotated[ExecutionJournalStore, Depends(get_execution_journal_store)],
        adapter: Annotated[ExecutionAdapter, Depends(get_execution_adapter)],
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

        readiness = await _build_venue_scoped_readiness_for_paper_trade(
            paper_trade=paper_trade,
            preview_hash=preview_hash,
            venue="paradex",
            settings=settings,
            confirmation_store=confirmation_store,
            account_preflight_service=account_preflight_service,
        )
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=paper_trade_id,
        )
        try:
            confirmation = next(
                item
                for item in confirmations
                if item.paper_trade_id == paper_trade_id and item.preview_hash == preview_hash
            )
        except StopIteration as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            ) from exc

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return execution_store.append(journal_entry)

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

        readiness = await _build_venue_scoped_readiness_for_paper_trade(
            paper_trade=paper_trade,
            preview_hash=preview_hash,
            venue="extended",
            settings=settings,
            confirmation_store=confirmation_store,
            account_preflight_service=account_preflight_service,
        )
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=paper_trade_id,
        )
        try:
            confirmation = next(
                item
                for item in confirmations
                if item.paper_trade_id == paper_trade_id and item.preview_hash == preview_hash
            )
        except StopIteration as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            ) from exc

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return execution_store.append(journal_entry)

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
            ExtendedCleanupPreviewService,
            Depends(get_extended_cleanup_preview_service),
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
        await _ensure_extended_cleanup_live_ready(
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
        first_venue: str,
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
    ) -> ExecutionJournalEntry:
        paper_trade = paper_store.get(paper_trade_id)
        if paper_trade is None:
            raise HTTPException(
                status_code=404,
                detail=f"Paper trade {paper_trade_id} was not found",
            )

        readiness = await _build_readiness_for_paper_trade(
            paper_trade=paper_trade,
            preview_hash=preview_hash,
            settings=settings,
            confirmation_store=confirmation_store,
            account_preflight_service=account_preflight_service,
        )
        if not readiness.ready:
            raise HTTPException(status_code=409, detail=readiness.model_dump(mode="json"))

        confirmations = confirmation_store.list_recent(
            limit=50,
            paper_trade_id=paper_trade_id,
        )
        try:
            confirmation = next(
                item
                for item in confirmations
                if item.paper_trade_id == paper_trade_id and item.preview_hash == preview_hash
            )
        except StopIteration as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No preview confirmation matched the requested paper trade and preview hash"
                ),
            ) from exc

        try:
            journal_entry = await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
                first_venue=first_venue,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return execution_store.append(journal_entry)

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(
        store: Annotated[OpportunityHistoryStore, Depends(get_history_store)],
        sample: int = 200,
        label: str | None = None,
    ) -> HTMLResponse:
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
        service: Annotated[OpportunityService, Depends(get_opportunity_service)],
    ) -> FundingArbOpportunity:
        try:
            return await service.score_pair(
                left_venue=left_venue,
                left_symbol=left_symbol,
                left_fee_profile=left_fee_profile,
                right_venue=right_venue,
                right_symbol=right_symbol,
                right_fee_profile=right_fee_profile,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (ConnectorError, httpx.HTTPError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    return app


app = create_app()
