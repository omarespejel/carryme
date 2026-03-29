"""FastAPI application factory for carryme."""

import math
import os
from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated

import httpx
from carryme_models import (
    AppDescriptor,
    CandidateAlertEvent,
    ExecutionJournalEntry,
    FundingArbOpportunity,
    FundingPairTradeIntent,
    OpportunityRecord,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    ServiceHealth,
    TradingFeeProfile,
    VenueExecutionPreflight,
    WatchlistDocument,
)
from carryme_normalizers import list_fee_profiles
from carryme_runtime import (
    ConnectorError,
    ExecutionAdapter,
    InvalidTradeCandidateError,
    LiveExecutionConfigMap,
    MockExecutionAdapter,
    OpportunityService,
    UpstreamDataError,
    build_paper_trade_execution_preflight,
    build_trade_intent,
    build_venue_execution_preflights,
)
from carryme_storage import (
    CandidateAlertStore,
    ExecutionJournalStore,
    OpportunityHistoryStore,
    PaperTradeStore,
    WatchlistStore,
)
from fastapi import Body, Depends, FastAPI, HTTPException, Response
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
DEFAULT_APP_ENVIRONMENT = "development"
APP_ENVIRONMENT_VARIABLE = "CARRYME_API_ENVIRONMENT"
MAX_HISTORY_LIMIT = 1000


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


def get_opportunity_service() -> OpportunityService:
    """Return the live opportunity scoring service."""

    return OpportunityService()


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

    return ExecutionJournalStore(settings.database_path)


def get_execution_adapter() -> ExecutionAdapter:
    """Return the default explicitly simulated execution adapter."""

    return MockExecutionAdapter()


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

    return app


app = create_app()
