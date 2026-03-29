import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx
import pytest
from carryme_api.app import (
    app,
    get_api_settings,
    get_candidate_alert_store,
    get_execution_adapter,
    get_execution_journal_store,
    get_history_store,
    get_opportunity_service,
    get_paper_trade_store,
    get_watchlist_store,
)
from carryme_api.config import ApiSettings
from carryme_models import (
    CandidateAlertEvent,
    CapacityEstimate,
    ExecutionJournalEntry,
    ExecutionLegResult,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    MarketStats,
    NormalizedMarketSnapshot,
    OpportunityRecord,
    PaperTradeEntry,
    PaperTradeOrderPreview,
    PreviewConfirmationEntry,
    TopOfBook,
    TradeLegIntent,
    VenueOrderPreview,
)
from carryme_normalizers import NormalizationError, normalize_market_snapshot
from carryme_runtime import (
    ConnectorError,
    OpportunityService,
    UpstreamDataError,
    fetch_live_snapshot,
)
from carryme_runtime.opportunities import SnapshotFetcher
from carryme_storage import (
    CandidateAlertStore,
    ExecutionJournalStore,
    OpportunityHistoryStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    WatchlistStore,
)
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


def test_watchlist_endpoint_reads_saved_pairs(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "watchlist.json")
    store.replace(
        [
            FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            )
        ]
    )

    client = TestClient(app)
    with _dependency_override(get_watchlist_store, lambda: store):
        response = client.get("/v1/watchlist")

    assert response.status_code == 200
    assert response.json() == {
        "pairs": [
            {
                "label": "strk_extended_hyperliquid",
                "left_venue": "extended",
                "left_symbol": "STRK-USD",
                "left_fee_profile": "default",
                "right_venue": "hyperliquid",
                "right_symbol": "STRK",
                "right_fee_profile": "tier0",
            }
        ]
    }


def test_watchlist_endpoint_returns_empty_when_file_is_missing(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "missing-watchlist.json")

    client = TestClient(app)
    with _dependency_override(get_watchlist_store, lambda: store):
        response = client.get("/v1/watchlist")

    assert response.status_code == 200
    assert response.json() == {"pairs": []}


def test_watchlist_endpoint_replaces_pairs(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "watchlist.json")

    client = TestClient(app)
    with _dependency_override(get_watchlist_store, lambda: store):
        response = client.put(
            "/v1/watchlist",
            json={
                "pairs": [
                    {
                        "label": "arb_extended_paradex",
                        "left_venue": "extended",
                        "left_symbol": "ARB-USD",
                        "left_fee_profile": "default",
                        "right_venue": "paradex",
                        "right_symbol": "ARB-USD-PERP",
                        "right_fee_profile": "pro",
                    }
                ]
            },
        )

    assert response.status_code == 200
    assert store.load()[0].label == "arb_extended_paradex"


def test_candidate_alerts_endpoint_reads_saved_events(tmp_path: Path) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")
    inserted = store.append(
        CandidateAlertEvent(
            emitted_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=2500.0,
            record=OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
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
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_candidate_alert_store, lambda: store):
        response = client.get("/v1/alerts/candidates")

    assert inserted is True
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["record"]["pair"]["label"] == "strk_extended_hyperliquid"


def test_candidate_alerts_endpoint_rejects_non_positive_limit(tmp_path: Path) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_candidate_alert_store, lambda: store):
        response = client.get("/v1/alerts/candidates", params={"limit": 0})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at least 1"


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


def test_latest_history_endpoint_deduplicates_by_label(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    for recorded_at, entry_edge in [
        (datetime(2026, 3, 29, 0, 0, tzinfo=UTC), 0.0001),
        (datetime(2026, 3, 29, 1, 0, tzinfo=UTC), 0.0002),
    ]:
        store.append(
            OpportunityRecord(
                recorded_at=recorded_at,
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
                    one_day_net_edge_after_entry=entry_edge,
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
        response = client.get("/v1/history/funding-pairs/latest", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["recorded_at"] == "2026-03-29T01:00:00Z"


def test_latest_history_endpoint_keeps_distinct_unlabeled_pairs(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        (
            datetime(2026, 3, 29, 0, 0, tzinfo=UTC),
            "extended",
            "default",
            "hyperliquid",
            "tier0",
        ),
        (
            datetime(2026, 3, 29, 1, 0, tzinfo=UTC),
            "extended",
            "maker",
            "paradex",
            "pro",
        ),
    ]
    for recorded_at, left_venue, left_fee_profile, right_venue, right_fee_profile in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=recorded_at,
                pair=FundingPairSpec(
                    label=None,
                    left_venue=left_venue,
                    left_symbol="STRK-USD",
                    left_fee_profile=left_fee_profile,
                    right_venue=right_venue,
                    right_symbol="STRK" if right_venue == "hyperliquid" else "STRK-USD-PERP",
                    right_fee_profile=right_fee_profile,
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol="STRK-USD-PERP",
                    long_venue=right_venue,
                    short_venue=left_venue,
                    long_fee_profile=right_fee_profile,
                    short_fee_profile=left_fee_profile,
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
        response = client.get("/v1/history/funding-pairs/latest", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    identities = {
        (
            item["pair"]["left_venue"],
            item["pair"]["left_fee_profile"],
            item["pair"]["right_venue"],
            item["pair"]["right_fee_profile"],
        )
        for item in payload
    }
    assert identities == {
        ("extended", "default", "hyperliquid", "tier0"),
        ("extended", "maker", "paradex", "pro"),
    }


def test_latest_history_endpoint_rejects_excessive_sample(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/history/funding-pairs/latest",
            params={"limit": 10, "sample": 1001},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "sample must be at most 1000"


def test_ranked_history_endpoint_sorts_best_entry_edge_first(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("strk_extended_hyperliquid", "STRK-USD-PERP", 0.0002),
        ("arb_extended_paradex", "ARB-USD-PERP", 0.0008),
    ]
    for label, symbol, entry_edge in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol="STRK-USD" if "STRK" in symbol else "ARB-USD",
                    left_fee_profile="default",
                    right_venue="hyperliquid" if "STRK" in symbol else "paradex",
                    right_symbol="STRK" if "STRK" in symbol else "ARB-USD-PERP",
                    right_fee_profile="tier0" if "STRK" in symbol else "pro",
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol=symbol,
                    long_venue="hyperliquid" if "STRK" in symbol else "paradex",
                    short_venue="extended",
                    long_fee_profile="tier0" if "STRK" in symbol else "pro",
                    short_fee_profile="default",
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=entry_edge - 0.0003,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    capacity=CapacityEstimate(
                        short_bid_notional=5000.0,
                        long_ask_notional=4500.0,
                        max_entry_notional=4500.0,
                        limiting_venue="paradex",
                    ),
                ),
            )
        )

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/v1/history/funding-pairs/ranked", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["pair"]["label"] == "arb_extended_paradex"
    assert payload[1]["pair"]["label"] == "strk_extended_hyperliquid"


def test_ranked_history_endpoint_penalizes_stale_thin_books(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        (
            "safe_pair",
            0.0005,
            0.0003,
            0.0001,
            0.0001,
            1_000_000.0,
            1_000_000.0,
            500_000.0,
            500_000.0,
            False,
            False,
        ),
        (
            "risky_pair",
            0.0005,
            -0.0002,
            0.01,
            0.01,
            100.0,
            100.0,
            1_000.0,
            1_000.0,
            True,
            False,
        ),
    ]
    for (
        label,
        entry_edge,
        round_trip_edge,
        short_spread,
        long_spread,
        short_oi,
        long_oi,
        short_volume,
        long_volume,
        short_stale,
        long_stale,
    ) in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
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
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=round_trip_edge,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    short_spread_rate=short_spread,
                    long_spread_rate=long_spread,
                    short_daily_volume=short_volume,
                    long_daily_volume=long_volume,
                    short_open_interest=short_oi,
                    long_open_interest=long_oi,
                    short_stale_book=short_stale,
                    long_stale_book=long_stale,
                    capacity=CapacityEstimate(
                        short_bid_notional=5000.0,
                        long_ask_notional=4500.0,
                        max_entry_notional=4500.0,
                        limiting_venue="hyperliquid",
                    ),
                ),
            )
        )

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/v1/history/funding-pairs/ranked", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["pair"]["label"] == "safe_pair"
    assert payload[1]["pair"]["label"] == "risky_pair"


def test_dashboard_renders_saved_history(tmp_path: Path) -> None:
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
        response = client.get("/dashboard")

    assert response.status_code == 200
    assert "carryme operator view" in response.text
    assert "strk_extended_hyperliquid" in response.text


def test_candidate_history_endpoint_filters_by_thresholds(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("strk_extended_hyperliquid", 0.0002, 3000.0),
        ("arb_extended_paradex", -0.0001, 6000.0),
    ]
    for label, entry_edge, capacity in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol="STRK-USD" if "strk" in label else "ARB-USD",
                    left_fee_profile="default",
                    right_venue="hyperliquid" if "strk" in label else "paradex",
                    right_symbol="STRK" if "strk" in label else "ARB-USD-PERP",
                    right_fee_profile="tier0" if "strk" in label else "pro",
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol="STRK-USD-PERP" if "strk" in label else "ARB-USD-PERP",
                    long_venue="hyperliquid" if "strk" in label else "paradex",
                    short_venue="extended",
                    long_fee_profile="tier0" if "strk" in label else "pro",
                    short_fee_profile="default",
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=entry_edge - 0.0002,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    capacity=CapacityEstimate(
                        short_bid_notional=capacity + 1000.0,
                        long_ask_notional=capacity,
                        max_entry_notional=capacity,
                        limiting_venue="paradex",
                    ),
                ),
            )
        )

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/history/funding-pairs/candidates",
            params={
                "limit": 10,
                "min_one_day_net_edge_after_entry": 0.0,
                "min_capacity_notional": 2500.0,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["pair"]["label"] == "strk_extended_hyperliquid"


def test_candidate_history_endpoint_rejects_negative_thresholds(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/history/funding-pairs/candidates",
            params={"min_capacity_notional": -1},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "min_capacity_notional must be non-negative"


def test_candidate_dashboard_renders_filtered_rows(tmp_path: Path) -> None:
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
        response = client.get(
            "/dashboard/candidates",
            params={
                "min_one_day_net_edge_after_entry": 0.0,
                "min_capacity_notional": 2500.0,
            },
        )

    assert response.status_code == 200
    assert "carryme candidate view" in response.text
    assert "strk_extended_hyperliquid" in response.text


def test_candidate_dashboard_rejects_invalid_sample(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get("/dashboard/candidates", params={"sample": 1001})

    assert response.status_code == 400
    assert response.json()["detail"] == "sample must be at most 1000"


def test_trade_intents_endpoint_builds_ranked_intents(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("strk_extended_hyperliquid", "STRK-USD-PERP", "hyperliquid", "tier0", 0.0002, 3000.0),
        ("arb_extended_paradex", "ARB-USD-PERP", "paradex", "pro", 0.0008, 4500.0),
    ]
    for label, symbol, long_venue, long_fee, entry_edge, capacity in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol="STRK-USD" if "STRK" in symbol else "ARB-USD",
                    left_fee_profile="default",
                    right_venue=long_venue,
                    right_symbol="STRK" if "STRK" in symbol else "ARB-USD-PERP",
                    right_fee_profile=long_fee,
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol=symbol,
                    long_venue=long_venue,
                    short_venue="extended",
                    long_fee_profile=long_fee,
                    short_fee_profile="default",
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=entry_edge - 0.0003,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    capacity=CapacityEstimate(
                        short_bid_notional=capacity + 500.0,
                        long_ask_notional=capacity,
                        max_entry_notional=capacity,
                        limiting_venue=long_venue,
                    ),
                ),
            )
        )

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/intents/funding-pairs",
            params={
                "limit": 10,
                "capacity_fraction": 0.25,
                "max_target_notional": 1000.0,
                "min_one_day_net_edge_after_entry": 0.0,
                "min_capacity_notional": 1000.0,
                "max_break_even_days_entry": 1.0,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    assert payload[0]["label"] == "arb_extended_paradex"
    assert payload[0]["target_notional"] == 1000.0


def test_trade_intent_endpoint_returns_not_found_when_thresholds_exclude_all(
    tmp_path: Path,
) -> None:
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
                one_day_net_edge_after_entry=-0.0001,
                one_day_net_edge_after_round_trip=-0.0004,
                break_even_days_entry=0.8,
                break_even_days_round_trip=1.6,
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
        response = client.get(
            "/v1/intents/funding-pair",
            params={
                "min_one_day_net_edge_after_entry": 0.0,
                "min_capacity_notional": 1000.0,
            },
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "No trade intent candidate matched the requested filters"


def test_trade_intents_endpoint_rejects_invalid_capacity_fraction(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/intents/funding-pairs",
            params={"capacity_fraction": 0.0},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "capacity_fraction must be within (0, 1]"


def test_trade_intents_endpoint_rejects_invalid_sample(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/intents/funding-pairs",
            params={"sample": 1001},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "sample must be at most 1000"


def test_trade_intents_endpoint_rejects_invalid_max_target_notional(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_history_store, lambda: store):
        response = client.get(
            "/v1/intents/funding-pairs",
            params={"max_target_notional": 0.0},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "max_target_notional must be greater than zero"


def test_trade_intent_endpoint_returns_not_found_when_break_even_days_are_missing(
    tmp_path: Path,
) -> None:
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
                break_even_days_entry=None,
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
        response = client.get(
            "/v1/intents/funding-pair",
            params={"max_break_even_days_entry": 1.0},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "No trade intent candidate matched the requested filters"


def test_create_paper_trade_from_intent_persists_entry(tmp_path: Path) -> None:
    history_store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    history_store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            pair=FundingPairSpec(
                label="arb_extended_paradex",
                left_venue="extended",
                left_symbol="ARB-USD",
                left_fee_profile="default",
                right_venue="paradex",
                right_symbol="ARB-USD-PERP",
                right_fee_profile="pro",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.001,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0008,
                one_day_net_edge_after_round_trip=0.0005,
                break_even_days_entry=0.5,
                break_even_days_round_trip=1.0,
                capacity=CapacityEstimate(
                    short_bid_notional=5000.0,
                    long_ask_notional=4500.0,
                    max_entry_notional=4500.0,
                    limiting_venue="paradex",
                ),
            ),
        )
    )

    client = TestClient(app)
    with (
        _dependency_override(get_history_store, lambda: history_store),
        _dependency_override(get_paper_trade_store, lambda: paper_store),
    ):
        response = client.post(
            "/v1/paper-trades/from-intent",
            params={
                "capacity_fraction": 0.25,
                "max_target_notional": 1000.0,
                "min_one_day_net_edge_after_entry": 0.0,
                "min_capacity_notional": 1000.0,
                "max_break_even_days_entry": 1.0,
                "note": "operator accepted candidate",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"]["label"] == "arb_extended_paradex"
    assert payload["note"] == "operator accepted candidate"
    assert len(paper_store.list_recent(limit=10)) == 1


def test_trade_intents_endpoint_skips_invalid_zero_capacity_records(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    common_pair = FundingPairSpec(
        label="arb_extended_paradex",
        left_venue="extended",
        left_symbol="ARB-USD",
        left_fee_profile="default",
        right_venue="paradex",
        right_symbol="ARB-USD-PERP",
        right_fee_profile="pro",
    )
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            pair=common_pair,
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.0011,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.00085,
                one_day_net_edge_after_round_trip=0.00055,
                break_even_days_entry=0.45,
                break_even_days_round_trip=0.9,
                capacity=CapacityEstimate(
                    short_bid_notional=0.0,
                    long_ask_notional=0.0,
                    max_entry_notional=0.0,
                    limiting_venue="paradex",
                ),
            ),
        )
    )
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
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
        response = client.get(
            "/v1/intents/funding-pairs",
            params={"limit": 1, "min_capacity_notional": 0.0},
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["label"] == "strk_extended_hyperliquid"


def test_create_paper_trade_from_intent_returns_not_found_for_invalid_capacity(
    tmp_path: Path,
) -> None:
    history_store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    history_store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            pair=FundingPairSpec(
                label="arb_extended_paradex",
                left_venue="extended",
                left_symbol="ARB-USD",
                left_fee_profile="default",
                right_venue="paradex",
                right_symbol="ARB-USD-PERP",
                right_fee_profile="pro",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.001,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0008,
                one_day_net_edge_after_round_trip=0.0005,
                break_even_days_entry=0.5,
                break_even_days_round_trip=1.0,
                capacity=CapacityEstimate(
                    short_bid_notional=0.0,
                    long_ask_notional=0.0,
                    max_entry_notional=0.0,
                    limiting_venue="paradex",
                ),
            ),
        )
    )

    client = TestClient(app)
    with (
        _dependency_override(get_history_store, lambda: history_store),
        _dependency_override(get_paper_trade_store, lambda: paper_store),
    ):
        response = client.post("/v1/paper-trades/from-intent")

    assert response.status_code == 404
    assert response.json()["detail"] == "No paper trade intent matched the requested filters"


def test_paper_trades_endpoint_lists_saved_entries(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store):
        response = client.get("/v1/paper-trades")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["intent"]["label"] == "arb_extended_paradex"


def test_paper_trades_endpoint_orders_newest_first(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("older_trade", datetime(2026, 3, 29, 13, 0, tzinfo=UTC)),
        ("newer_trade", datetime(2026, 3, 29, 14, 0, tzinfo=UTC)),
    ]
    for label, created_at in fixtures:
        paper_store.append(
            PaperTradeEntry(
                created_at=created_at,
                note=f"saved {label}",
                intent=FundingPairTradeIntent(
                    label=label,
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=1000.0,
                    capacity_fraction=0.25,
                    max_target_notional=1000.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                    ),
                ),
            )
        )

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store):
        response = client.get("/v1/paper-trades")

    assert response.status_code == 200
    payload = response.json()
    assert [item["intent"]["label"] for item in payload] == ["newer_trade", "older_trade"]


def test_paper_trades_endpoint_filters_by_label(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    for label in ["arb_extended_paradex", "strk_extended_hyperliquid"]:
        paper_store.append(
            PaperTradeEntry(
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note=f"saved {label}",
                intent=FundingPairTradeIntent(
                    label=label,
                    canonical_symbol="ARB-USD-PERP" if "arb" in label else "STRK-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=1000.0,
                    capacity_fraction=0.25,
                    max_target_notional=1000.0,
                    long_leg=TradeLegIntent(
                        venue="paradex" if "arb" in label else "hyperliquid",
                        symbol="ARB-USD-PERP" if "arb" in label else "STRK",
                        fee_profile="pro" if "arb" in label else "tier0",
                        side="buy",
                        target_notional=1000.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD" if "arb" in label else "STRK-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                    ),
                ),
            )
        )

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store):
        response = client.get(
            "/v1/paper-trades",
            params={"label": "strk_extended_hyperliquid"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["intent"]["label"] == "strk_extended_hyperliquid"


def test_paper_trades_endpoint_rejects_invalid_limit(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store):
        response = client.get("/v1/paper-trades", params={"limit": 0})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at least 1"


def test_mock_execution_endpoint_submits_saved_paper_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store), _dependency_override(
        get_execution_journal_store, lambda: execution_store
    ):
        response = client.post(
            f"/v1/executions/mock/from-paper-trade/{paper_trade.entry_id}"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "mock"
    assert len(execution_store.list_recent(limit=10)) == 1


def test_mock_execution_endpoint_returns_404_for_missing_paper_trade(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store), _dependency_override(
        get_execution_journal_store, lambda: execution_store
    ):
        response = client.post("/v1/executions/mock/from-paper-trade/999999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Paper trade 999999 was not found"
    assert execution_store.list_recent(limit=10) == []


def test_mock_execution_endpoint_ignores_generic_execution_adapter_override(
    tmp_path: Path,
) -> None:
    class FailingAdapter:
        def submit(self, *_: object, **__: object) -> ExecutionJournalEntry:
            raise AssertionError("generic execution adapter should not be used")

    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store), _dependency_override(
        get_execution_journal_store, lambda: execution_store
    ), _dependency_override(get_execution_adapter, lambda: FailingAdapter()):
        response = client.post(
            f"/v1/executions/mock/from-paper-trade/{paper_trade.entry_id}"
        )

    assert response.status_code == 200
    assert execution_store.list_recent(limit=10)[0].paper_trade_id == paper_trade.entry_id


def test_executions_endpoint_lists_saved_entries(tmp_path: Path) -> None:
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="mock",
            mode="mock",
            status="accepted",
            paper_trade_id=7,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=1000.0,
                    capacity_fraction=0.25,
                    max_target_notional=1000.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                    status="accepted",
                    simulated=True,
                    external_reference="mock:7:buy",
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    status="accepted",
                    simulated=True,
                    external_reference="mock:7:sell",
                ),
            ],
        )
    )

    client = TestClient(app)
    with _dependency_override(get_execution_journal_store, lambda: execution_store):
        response = client.get("/v1/executions")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["paper_trade_id"] == 7


def test_executions_endpoint_rejects_invalid_limit(tmp_path: Path) -> None:
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_execution_journal_store, lambda: execution_store):
        response = client.get("/v1/executions", params={"limit": 0})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at least 1"


def test_executions_endpoint_rejects_limit_above_history_cap(tmp_path: Path) -> None:
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    with _dependency_override(get_execution_journal_store, lambda: execution_store):
        response = client.get("/v1/executions", params={"limit": 1001})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at most 1000"


def test_live_execution_preflight_venues_endpoint_reports_missing_envs() -> None:
    client = TestClient(app)
    with _dependency_override(
        get_api_settings,
        lambda: ApiSettings(
            extended_live_enabled=True,
            extended_api_key=None,
            extended_stark_private_key=None,
            paradex_live_enabled=True,
            paradex_private_key="paradex-secret",
            hyperliquid_live_enabled=False,
            hyperliquid_account_address=None,
            hyperliquid_api_wallet_private_key=None,
        ),
    ):
        response = client.get("/v1/executions/preflight/venues")

    assert response.status_code == 200
    payload = {item["venue"]: item for item in response.json()}
    assert payload["extended"]["ready"] is False
    assert "CARRYME_API_EXTENDED_API_KEY" in payload["extended"]["missing_env_vars"]
    assert "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY" in payload["extended"]["missing_env_vars"]
    assert payload["paradex"]["ready"] is True


def test_live_execution_preflight_for_saved_paper_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store), _dependency_override(
        get_api_settings,
        lambda: ApiSettings(
            extended_live_enabled=True,
            extended_api_key="extended-key",
            extended_stark_private_key="extended-stark",
            paradex_live_enabled=False,
            paradex_private_key=None,
            hyperliquid_live_enabled=False,
            hyperliquid_account_address=None,
            hyperliquid_api_wallet_private_key=None,
        ),
    ):
        response = client.get(
            f"/v1/executions/preflight/from-paper-trade/{paper_trade.entry_id}"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["ready"] is False
    assert {item["venue"] for item in payload["venues"]} == {"extended", "paradex"}
    assert "Venue paradex live execution is not enabled" in payload["blocking_reasons"]
    extended_status = next(item for item in payload["venues"] if item["venue"] == "extended")
    assert extended_status["ready"] is True


def test_live_execution_preflight_returns_404_for_missing_paper_trade(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store), _dependency_override(
        get_api_settings,
        lambda: ApiSettings(
            extended_live_enabled=True,
            extended_api_key="extended-key",
            extended_stark_private_key="extended-stark",
            paradex_live_enabled=True,
            paradex_private_key="paradex-secret",
            hyperliquid_live_enabled=False,
            hyperliquid_account_address=None,
            hyperliquid_api_wallet_private_key=None,
        ),
    ):
        response = client.get("/v1/executions/preflight/from-paper-trade/999999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Paper trade 999999 was not found"


def test_order_preview_endpoint_returns_saved_paper_trade_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    class StubOrderPreviewService:
        async def preview_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            *,
            slippage_tolerance_bps: int = 10,
        ) -> PaperTradeOrderPreview:
            return PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=slippage_tolerance_bps,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.986984815618,
                        quantity_text="10845.98698482",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.092292,
                        worst_price_text="0.09229200",
                        order_type="limit",
                        time_in_force="ioc",
                        post_only=False,
                        reduce_only=False,
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                        auth_scheme="subkey private key",
                        payload={
                            "market": "ARB-USD-PERP",
                            "side": "BUY",
                        },
                        notes=[],
                    )
                ],
            )

    from carryme_api.app import get_order_preview_service

    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store), _dependency_override(
        get_order_preview_service,
        lambda: StubOrderPreviewService(),
    ):
        response = client.get(
            f"/v1/executions/preview/from-paper-trade/{paper_trade.entry_id}",
            params={"slippage_tolerance_bps": 12},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["slippage_tolerance_bps"] == 12
    assert payload["legs"][0]["venue"] == "paradex"
    assert payload["legs"][0]["endpoint_path_hint"] == "/v1/orders"


def test_order_preview_endpoint_returns_not_found_for_missing_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    client = TestClient(app)
    with _dependency_override(get_paper_trade_store, lambda: paper_store):
        response = client.get("/v1/executions/preview/from-paper-trade/999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Paper trade 999 was not found"


def test_preview_confirmation_endpoint_persists_matching_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )
    preview = PaperTradeOrderPreview(
        paper_trade_id=paper_trade.entry_id or 0,
        label="arb_extended_paradex",
        generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        slippage_tolerance_bps=12,
        preview_hash="preview-hash",
        legs=[
            VenueOrderPreview(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=1000.0,
                quantity=10_845.0,
                quantity_text="10845.00000000",
                reference_price=0.0922,
                reference_price_source="best_ask",
                worst_acceptable_price=0.09231064,
                worst_price_text="0.09231064",
                order_type="limit",
                time_in_force="ioc",
                http_method="POST",
                endpoint_path_hint="/v1/orders",
                required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                auth_scheme="subkey private key",
                payload={"market": "ARB-USD-PERP"},
                notes=[],
            )
        ],
    )

    class StubOrderPreviewService:
        async def preview_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            *,
            slippage_tolerance_bps: int = 10,
        ) -> PaperTradeOrderPreview:
            assert slippage_tolerance_bps == 12
            return preview

    from carryme_api.app import get_order_preview_service, get_preview_confirmation_store

    client = TestClient(app)
    with (
        _dependency_override(get_paper_trade_store, lambda: paper_store),
        _dependency_override(get_preview_confirmation_store, lambda: confirmation_store),
        _dependency_override(get_order_preview_service, lambda: StubOrderPreviewService()),
    ):
        response = client.post(
            f"/v1/executions/preview-confirmations/from-paper-trade/{paper_trade.entry_id}",
            params={
                "preview_hash": " preview-hash ",
                "slippage_tolerance_bps": 12,
                "note": "operator confirmed",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["preview_hash"] == "preview-hash"
    assert payload["note"] == "operator confirmed"
    assert len(confirmation_store.list_recent(limit=10)) == 1


def test_preview_confirmation_endpoint_rejects_hash_mismatch(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    class StubOrderPreviewService:
        async def preview_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            *,
            slippage_tolerance_bps: int = 10,
        ) -> PaperTradeOrderPreview:
            return PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=slippage_tolerance_bps,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.0,
                        quantity_text="10845.00000000",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.09231064,
                        worst_price_text="0.09231064",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                        auth_scheme="subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    )
                ],
            )

    from carryme_api.app import get_order_preview_service, get_preview_confirmation_store

    client = TestClient(app)
    with (
        _dependency_override(get_paper_trade_store, lambda: paper_store),
        _dependency_override(get_preview_confirmation_store, lambda: confirmation_store),
        _dependency_override(get_order_preview_service, lambda: StubOrderPreviewService()),
    ):
        response = client.post(
            f"/v1/executions/preview-confirmations/from-paper-trade/{paper_trade.entry_id}",
            params={"preview_hash": "wrong-hash"},
        )

    assert response.status_code == 409
    assert (
        response.json()["detail"]
        == "Preview hash did not match the current unsigned order preview"
    )


def test_preview_confirmations_endpoint_lists_saved_entries(tmp_path: Path) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=7,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.0,
                        quantity_text="10845.00000000",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.09231064,
                        worst_price_text="0.09231064",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                        auth_scheme="subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    from carryme_api.app import get_preview_confirmation_store

    client = TestClient(app)
    with _dependency_override(get_preview_confirmation_store, lambda: store):
        response = client.get("/v1/executions/preview-confirmations")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["paper_trade_id"] == 7


def test_preview_confirmations_endpoint_rejects_non_positive_limit(tmp_path: Path) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")

    from carryme_api.app import get_preview_confirmation_store

    client = TestClient(app)
    with _dependency_override(get_preview_confirmation_store, lambda: store):
        response = client.get("/v1/executions/preview-confirmations", params={"limit": 0})

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at least 1"


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
