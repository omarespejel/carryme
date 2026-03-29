from datetime import UTC, datetime
from pathlib import Path

from carryme_api.app import app, get_history_store, get_opportunity_service
from carryme_api.config import ApiSettings
from carryme_models import (
    CandidateAlertEvent,
    CapacityEstimate,
    ExecutionJournalEntry,
    ExecutionLegResult,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    OpportunityRecord,
    PaperTradeEntry,
    PaperTradeOrderPreview,
    PreviewConfirmationEntry,
    TradeLegIntent,
    VenueOrderPreview,
)
from carryme_storage import (
    CandidateAlertStore,
    ExecutionJournalStore,
    OpportunityHistoryStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    WatchlistStore,
)
from fastapi.testclient import TestClient


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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)

    response = client.get("/v1/history/funding-pairs")

    app.dependency_overrides.clear()

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

    from carryme_api.app import get_watchlist_store

    app.dependency_overrides[get_watchlist_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/watchlist")
    app.dependency_overrides.clear()

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


def test_watchlist_endpoint_replaces_pairs(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "watchlist.json")

    from carryme_api.app import get_watchlist_store

    app.dependency_overrides[get_watchlist_store] = lambda: store
    client = TestClient(app)
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
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert store.load()[0].label == "arb_extended_paradex"


def test_candidate_alerts_endpoint_reads_saved_events(tmp_path: Path) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")
    store.append(
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

    from carryme_api.app import get_candidate_alert_store

    app.dependency_overrides[get_candidate_alert_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/alerts/candidates")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["record"]["pair"]["label"] == "strk_extended_hyperliquid"


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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/history/funding-pairs/latest", params={"limit": 10})
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["recorded_at"] == "2026-03-29T01:00:00Z"


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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/history/funding-pairs/ranked", params={"limit": 10})
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["pair"]["label"] == "arb_extended_paradex"
    assert payload[1]["pair"]["label"] == "strk_extended_hyperliquid"


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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get("/dashboard")
    app.dependency_overrides.clear()

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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/history/funding-pairs/candidates",
        params={
            "limit": 10,
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 2500.0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["pair"]["label"] == "strk_extended_hyperliquid"


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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/dashboard/candidates",
        params={
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 2500.0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "carryme candidate view" in response.text
    assert "strk_extended_hyperliquid" in response.text


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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
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
    app.dependency_overrides.clear()

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

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/intents/funding-pair",
        params={
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 1000.0,
        },
    )
    app.dependency_overrides.clear()

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

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_history_store] = lambda: history_store
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
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
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"]["label"] == "arb_extended_paradex"
    assert payload["note"] == "operator accepted candidate"
    assert payload["entry_id"] is not None
    assert len(paper_store.list_recent(limit=10)) == 1


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

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
    response = client.get("/v1/paper-trades")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["intent"]["label"] == "arb_extended_paradex"


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

    from carryme_api.app import get_execution_journal_store, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    client = TestClient(app)
    response = client.post(f"/v1/executions/mock/from-paper-trade/{paper_trade.entry_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "mock"
    assert len(execution_store.list_recent(limit=10)) == 1


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

    from carryme_api.app import get_execution_journal_store

    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    client = TestClient(app)
    response = client.get("/v1/executions")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["paper_trade_id"] == 7


def test_live_execution_preflight_venues_endpoint_reports_missing_envs() -> None:
    from carryme_api.app import get_api_settings

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key=None,
        extended_stark_private_key=None,
        paradex_live_enabled=True,
        paradex_private_key="paradex-secret",
        hyperliquid_live_enabled=False,
        hyperliquid_account_address=None,
        hyperliquid_api_wallet_private_key=None,
    )
    client = TestClient(app)
    response = client.get("/v1/executions/preflight/venues")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = {item["venue"]: item for item in response.json()}
    assert payload["extended"]["ready"] is False
    assert "CARRYME_API_EXTENDED_API_KEY" in payload["extended"]["missing_env_vars"]
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

    from carryme_api.app import get_api_settings, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=False,
        paradex_private_key=None,
        hyperliquid_live_enabled=False,
        hyperliquid_account_address=None,
        hyperliquid_api_wallet_private_key=None,
    )
    client = TestClient(app)
    response = client.get(f"/v1/executions/preflight/from-paper-trade/{paper_trade.entry_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["ready"] is False
    assert {item["venue"] for item in payload["venues"]} == {"extended", "paradex"}


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

    from carryme_api.app import get_order_preview_service, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_order_preview_service] = lambda: StubOrderPreviewService()
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/preview/from-paper-trade/{paper_trade.entry_id}",
        params={"slippage_tolerance_bps": 12},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["slippage_tolerance_bps"] == 12
    assert payload["legs"][0]["venue"] == "paradex"
    assert payload["legs"][0]["endpoint_path_hint"] == "/v1/orders"


def test_order_preview_endpoint_returns_not_found_for_missing_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
    response = client.get("/v1/executions/preview/from-paper-trade/999")
    app.dependency_overrides.clear()

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

    from carryme_api.app import (
        get_order_preview_service,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_order_preview_service] = lambda: StubOrderPreviewService()
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/preview-confirmations/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "preview-hash",
            "slippage_tolerance_bps": 12,
            "note": "operator confirmed",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["preview_hash"] == "preview-hash"
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

    from carryme_api.app import (
        get_order_preview_service,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_order_preview_service] = lambda: StubOrderPreviewService()
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/preview-confirmations/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "wrong-hash"},
    )
    app.dependency_overrides.clear()

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

    app.dependency_overrides[get_preview_confirmation_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/executions/preview-confirmations")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["paper_trade_id"] == 7
