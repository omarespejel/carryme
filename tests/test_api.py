import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx
import pytest
from carryme_api.app import app, get_history_store, get_opportunity_service
from carryme_models import (
    CapacityEstimate,
    FundingArbOpportunity,
    FundingPairSpec,
    MarketStats,
    NormalizedMarketSnapshot,
    OpportunityRecord,
    TopOfBook,
)
from carryme_normalizers import NormalizationError, normalize_market_snapshot
from carryme_runtime import (
    ConnectorError,
    OpportunityService,
    UpstreamDataError,
    fetch_live_snapshot,
)
from carryme_runtime.opportunities import SnapshotFetcher
from carryme_storage import OpportunityHistoryStore
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


@contextmanager
def _dependency_override(
    dependency: Callable[..., object],
    provider: Callable[..., object],
) -> Iterator[None]:
    sentinel = object()
    previous = app.dependency_overrides.get(dependency, sentinel)
    app.dependency_overrides[dependency] = provider
    try:
        yield
    finally:
        if previous is sentinel:
            app.dependency_overrides.pop(dependency, None)
        else:
            app.dependency_overrides[dependency] = cast(Callable[..., object], previous)


def test_health_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARRYME_API_ENVIRONMENT", raising=False)
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


def test_versioned_health_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARRYME_API_ENVIRONMENT", raising=False)
    client = TestClient(app)

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "service": {
            "name": "carryme-api",
            "version": "0.1.0",
            "environment": "development",
        },
        "status": "ok",
    }


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


def test_history_endpoint_reads_saved_records(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
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
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/v1/history/funding-pairs")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["pair"]["label"] == "strk_extended_hyperliquid"


def test_history_endpoint_rejects_non_positive_limit(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/v1/history/funding-pairs", params={"limit": 0})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at least 1"


def test_history_endpoint_rejects_excessive_limit(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/v1/history/funding-pairs", params={"limit": 1001})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at most 1000"


def test_history_endpoint_treats_blank_label_as_unfiltered(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
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
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/v1/history/funding-pairs", params={"label": "   "})

    assert response.status_code == 200
    assert len(response.json()) == 1


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

    client = TestClient(app)
    with _dependency_override(get_opportunity_service, lambda: StubOpportunityService()):
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

    assert response.status_code == 200
    assert response.json()["canonical_symbol"] == "STRK-USD-PERP"
    assert response.json()["capacity"]["limiting_venue"] == "hyperliquid"
    assert response.headers["Cache-Control"] == "no-store"


def test_funding_pair_endpoint_maps_value_errors_to_bad_request() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise ValueError("Unsupported venue: nope")

    client = TestClient(app)
    with _dependency_override(get_opportunity_service, lambda: FailingOpportunityService()):
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

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported venue: nope"


def test_funding_pair_endpoint_maps_connector_errors_to_bad_gateway() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise ConnectorError("upstream venue timeout")

    client = TestClient(app)
    with _dependency_override(get_opportunity_service, lambda: FailingOpportunityService()):
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

    assert response.status_code == 502
    assert response.json()["detail"] == "upstream venue timeout"


def test_funding_pair_endpoint_maps_upstream_data_errors_to_bad_gateway() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise UpstreamDataError("malformed upstream payload")

    client = TestClient(app)
    with _dependency_override(get_opportunity_service, lambda: FailingOpportunityService()):
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

    assert response.status_code == 502
    assert response.json()["detail"] == "malformed upstream payload"


def test_funding_pair_endpoint_maps_httpx_errors_to_bad_gateway() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise httpx.ConnectError("connection refused")

    client = TestClient(app)
    with _dependency_override(get_opportunity_service, lambda: FailingOpportunityService()):
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

    assert response.status_code == 502
    assert "connection refused" in response.json()["detail"]


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


def test_opportunity_service_rejects_invalid_symbols_before_network_calls() -> None:
    class CountingFetcher:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self, _venue: str, _symbol: str) -> NormalizedMarketSnapshot:
            self.calls += 1
            return _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)

    fetcher = CountingFetcher()
    service = OpportunityService(fetch_snapshot=cast(SnapshotFetcher, fetcher))

    with pytest.raises(ValueError, match="Invalid Hyperliquid perp symbol"):
        asyncio.run(
            service.score_pair(
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK-USD",
                right_fee_profile="tier0",
            )
        )

    assert fetcher.calls == 0


def test_opportunity_service_rejects_mismatched_pairs_before_network_calls() -> None:
    class CountingFetcher:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self, _venue: str, _symbol: str) -> NormalizedMarketSnapshot:
            self.calls += 1
            return _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)

    fetcher = CountingFetcher()
    service = OpportunityService(fetch_snapshot=cast(SnapshotFetcher, fetcher))

    with pytest.raises(ValueError, match="Funding pairs must share the same canonical symbol"):
        asyncio.run(
            service.score_pair(
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="ETH",
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


def test_fetch_live_snapshot_rejects_upstream_symbol_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConnector:
        async def fetch_market_stats(self, _symbol: str) -> MarketStats:
            return MarketStats(
                venue="extended",
                symbol="ETH-USD",
                mark_price=1800.0,
                funding_rate=0.0002,
                open_interest=1_000_000,
                daily_volume=500_000,
            )

        async def fetch_top_of_book(self, _symbol: str) -> TopOfBook:
            return TopOfBook(
                best_bid_price=1799.0,
                best_bid_size=20.0,
                best_ask_price=1801.0,
                best_ask_size=18.0,
            )

    monkeypatch.setattr(
        "carryme_runtime.opportunities._build_connector",
        lambda _venue, _client: FakeConnector(),
    )

    with pytest.raises(UpstreamDataError, match="symbol mismatch"):
        asyncio.run(fetch_live_snapshot("extended", "STRK-USD"))


def test_fetch_live_snapshot_wraps_normalization_errors_as_upstream_data_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConnector:
        async def fetch_market_stats(self, _symbol: str) -> MarketStats:
            return MarketStats(
                venue="extended",
                symbol="STRK-USD",
                mark_price=0.03455,
                funding_rate=0.0002,
                open_interest=1_000_000,
                daily_volume=500_000,
            )

        async def fetch_top_of_book(self, _symbol: str) -> TopOfBook:
            return TopOfBook(
                best_bid_price=0.0345,
                best_bid_size=100_000,
                best_ask_price=0.0346,
                best_ask_size=80_000,
            )

    def _raise_normalization_error(_venue: str, _market: MarketStats) -> NormalizedMarketSnapshot:
        raise NormalizationError("bad payload from upstream")

    monkeypatch.setattr(
        "carryme_runtime.opportunities._build_connector",
        lambda _venue, _client: FakeConnector(),
    )
    monkeypatch.setattr(
        "carryme_runtime.opportunities.normalize_market_snapshot",
        _raise_normalization_error,
    )

    with pytest.raises(UpstreamDataError, match="could not be normalized"):
        asyncio.run(fetch_live_snapshot("extended", "STRK-USD"))


def test_fetch_live_snapshot_fetches_stats_and_book_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CoordinatedConnector:
        def __init__(self) -> None:
            self.started: list[str] = []
            self.release_both = asyncio.Event()

        async def fetch_market_stats(self, _symbol: str) -> MarketStats:
            self.started.append("stats")
            if len(self.started) == 2:
                self.release_both.set()
            await asyncio.wait_for(self.release_both.wait(), timeout=0.1)
            return MarketStats(
                venue="extended",
                symbol="STRK-USD",
                mark_price=0.03455,
                funding_rate=0.0002,
                open_interest=1_000_000,
                daily_volume=500_000,
            )

        async def fetch_top_of_book(self, _symbol: str) -> TopOfBook:
            self.started.append("book")
            if len(self.started) == 2:
                self.release_both.set()
            await asyncio.wait_for(self.release_both.wait(), timeout=0.1)
            return TopOfBook(
                best_bid_price=0.0345,
                best_bid_size=100_000,
                best_ask_price=0.0346,
                best_ask_size=80_000,
            )

    connector = CoordinatedConnector()
    monkeypatch.setattr(
        "carryme_runtime.opportunities._build_connector",
        lambda _venue, _client: connector,
    )

    snapshot = asyncio.run(fetch_live_snapshot("extended", "STRK-USD"))

    assert set(connector.started) == {"stats", "book"}
    assert snapshot.identity.canonical_symbol == "STRK-USD-PERP"
