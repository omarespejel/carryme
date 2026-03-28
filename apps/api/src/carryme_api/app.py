"""FastAPI application factory for carryme."""

from __future__ import annotations

from typing import Annotated

import httpx
from carryme_models import (
    AppDescriptor,
    CandidateAlertEvent,
    FundingArbOpportunity,
    FundingPairSpec,
    OpportunityRecord,
    ServiceHealth,
    TradingFeeProfile,
    WatchlistDocument,
)
from carryme_normalizers import list_fee_profiles
from carryme_runtime import ConnectorError, OpportunityService
from carryme_storage import CandidateAlertStore, OpportunityHistoryStore, WatchlistStore
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
