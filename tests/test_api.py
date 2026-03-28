import asyncio
from typing import cast

import pytest
from carryme_api.app import app, get_opportunity_service
from carryme_api.opportunities import ConnectorError, OpportunityService, SnapshotFetcher
from carryme_models import (
    CapacityEstimate,
    FundingArbOpportunity,
    MarketStats,
    NormalizedMarketSnapshot,
    TopOfBook,
)
from carryme_normalizers import normalize_market_snapshot
from fastapi.testclient import TestClient


def _snapshot(
    venue: str,
    symbol: str,
    funding_rate: float,
    bid_price: float,
    bid_size: float,
    ask_price: float,
    ask_size: float,
) -> NormalizedMarketSnapshot:
    market = MarketStats(
        venue=venue,
        symbol=symbol,
        mark_price=(bid_price + ask_price) / 2,
        funding_rate=funding_rate,
        open_interest=1_000_000,
        daily_volume=500_000,
        top_of_book=TopOfBook(
            best_bid_price=bid_price,
            best_bid_size=bid_size,
            best_ask_price=ask_price,
            best_ask_size=ask_size,
        ),
    )
    return normalize_market_snapshot(venue, market)


def test_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "service": {
            "name": "carryme-api",
            "version": "0.1.0",
            "environment": "development",
        },
        "status": "ok",
    }


def test_versioned_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_fee_profiles_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/v1/reference/fees/paradex")

    assert response.status_code == 200
    payload = response.json()
    assert {item["profile"] for item in payload} == {"retail", "pro", "pro_fastfills"}


def test_fee_profiles_endpoint_rejects_unknown_venue() -> None:
    client = TestClient(app)

    response = client.get("/v1/reference/fees/unknown")

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported venue for fee normalization: unknown"


def test_funding_pair_endpoint_uses_service_dependency() -> None:
    class StubOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    app.dependency_overrides[get_opportunity_service] = lambda: StubOpportunityService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-pair",
        params={
            "left_venue": "extended",
            "left_symbol": "STRK-USD",
            "left_fee_profile": "default",
            "right_venue": "hyperliquid",
            "right_symbol": "STRK",
            "right_fee_profile": "tier0",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["canonical_symbol"] == "STRK-USD-PERP"
    assert response.json()["capacity"]["limiting_venue"] == "hyperliquid"


def test_funding_pair_endpoint_maps_value_errors_to_bad_request() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise ValueError("Unsupported venue: nope")

    app.dependency_overrides[get_opportunity_service] = lambda: FailingOpportunityService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-pair",
        params={
            "left_venue": "nope",
            "left_symbol": "STRK-USD",
            "left_fee_profile": "default",
            "right_venue": "hyperliquid",
            "right_symbol": "STRK",
            "right_fee_profile": "tier0",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported venue: nope"


def test_funding_pair_endpoint_maps_connector_errors_to_bad_gateway() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise ConnectorError("upstream venue timeout")

    app.dependency_overrides[get_opportunity_service] = lambda: FailingOpportunityService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-pair",
        params={
            "left_venue": "extended",
            "left_symbol": "STRK-USD",
            "left_fee_profile": "default",
            "right_venue": "hyperliquid",
            "right_symbol": "STRK",
            "right_fee_profile": "tier0",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 502
    assert response.json()["detail"] == "upstream venue timeout"


def test_opportunity_service_validates_fee_profiles_before_network_calls() -> None:
    class CountingFetcher:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self, _venue: str, _symbol: str) -> NormalizedMarketSnapshot:
            self.calls += 1
            return _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)

    fetcher = CountingFetcher()
    service = OpportunityService(fetch_snapshot=cast(SnapshotFetcher, fetcher))

    with pytest.raises(ValueError, match="Unknown fee profile"):
        asyncio.run(
            service.score_pair(
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="missing",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            )
        )

    assert fetcher.calls == 0


def test_opportunity_service_fetches_snapshots_concurrently() -> None:
    class CoordinatedFetcher:
        def __init__(self) -> None:
            self.started: list[str] = []
            self.release_both = asyncio.Event()

        async def __call__(self, venue: str, symbol: str) -> NormalizedMarketSnapshot:
            self.started.append(venue)
            if len(self.started) == 2:
                self.release_both.set()
            await asyncio.wait_for(self.release_both.wait(), timeout=0.1)
            if venue == "extended":
                return _snapshot(venue, symbol, 0.0002, 0.0345, 100_000, 0.0346, 80_000)
            return _snapshot(venue, symbol, -0.00005, 0.0344, 90_000, 0.0345, 75_000)

    fetcher = CoordinatedFetcher()
    service = OpportunityService(fetch_snapshot=cast(SnapshotFetcher, fetcher))

    opportunity = asyncio.run(
        service.score_pair(
            left_venue="extended",
            left_symbol="STRK-USD",
            left_fee_profile="default",
            right_venue="hyperliquid",
            right_symbol="STRK",
            right_fee_profile="tier0",
        )
    )

    assert fetcher.started == ["extended", "hyperliquid"]
    assert opportunity.canonical_symbol == "STRK-USD-PERP"
