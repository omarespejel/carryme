from datetime import UTC, datetime
from pathlib import Path

from carryme_models import (
    CandidateAlertEvent,
    CapacityEstimate,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    OpportunityRecord,
    PaperTradeEntry,
    TradeLegIntent,
)
from carryme_storage import (
    CandidateAlertStore,
    OpportunityHistoryStore,
    PaperTradeStore,
    WatchlistStore,
    load_watchlist,
    save_watchlist,
)


def test_load_watchlist_from_pairs_object(tmp_path: Path) -> None:
    path = tmp_path / "watchlist.json"
    path.write_text(
        """
        {
          "pairs": [
            {
              "label": "arb_extended_paradex",
              "left_venue": "extended",
              "left_symbol": "ARB-USD",
              "left_fee_profile": "default",
              "right_venue": "paradex",
              "right_symbol": "ARB-USD-PERP",
              "right_fee_profile": "pro"
            }
          ]
        }
        """
    )

    pairs = load_watchlist(path)

    assert len(pairs) == 1
    assert pairs[0].label == "arb_extended_paradex"


def test_save_watchlist_round_trips_pairs(tmp_path: Path) -> None:
    path = tmp_path / "watchlist.json"
    pairs = [
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

    save_watchlist(path, pairs)

    reloaded = load_watchlist(path)

    assert reloaded == pairs
    assert path.read_text().strip().startswith("{")


def test_watchlist_store_replaces_pairs_atomically(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "watchlist.json")
    first = [
        FundingPairSpec(
            label="arb_extended_paradex",
            left_venue="extended",
            left_symbol="ARB-USD",
            left_fee_profile="default",
            right_venue="paradex",
            right_symbol="ARB-USD-PERP",
            right_fee_profile="pro",
        )
    ]
    second = [
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

    store.replace(first)
    replaced = store.replace(second)

    assert replaced == second
    assert store.load() == second
    assert list(tmp_path.glob("*.tmp")) == []


def test_history_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    record = OpportunityRecord(
        recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
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
            entry_cost_rate=0.00045,
            round_trip_cost_rate=0.0009,
            one_day_net_edge_after_entry=0.00055,
            one_day_net_edge_after_round_trip=0.0001,
            break_even_days_entry=0.45,
            break_even_days_round_trip=0.9,
            capacity=CapacityEstimate(
                short_bid_notional=5000.0,
                long_ask_notional=4500.0,
                max_entry_notional=4500.0,
                limiting_venue="paradex",
            ),
        ),
    )

    store.append(record)
    results = store.list_recent(limit=10)

    assert len(results) == 1
    assert results[0].pair.label == "arb_extended_paradex"
    assert results[0].opportunity.canonical_symbol == "ARB-USD-PERP"


def test_candidate_alert_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")
    event = CandidateAlertEvent(
        emitted_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=2500.0,
        record=OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
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
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00055,
                one_day_net_edge_after_round_trip=0.0001,
                break_even_days_entry=0.45,
                break_even_days_round_trip=0.9,
                capacity=CapacityEstimate(
                    short_bid_notional=5000.0,
                    long_ask_notional=4500.0,
                    max_entry_notional=4500.0,
                    limiting_venue="paradex",
                ),
            ),
        ),
    )

    store.append(event)
    results = store.list_recent(limit=10)

    assert len(results) == 1
    assert results[0].record.pair.label == "arb_extended_paradex"
    assert results[0].record.opportunity.canonical_symbol == "ARB-USD-PERP"


def test_paper_trade_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = PaperTradeStore(tmp_path / "history.sqlite3")
    entry = PaperTradeEntry(
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

    store.append(entry)
    results = store.list_recent(limit=10)

    assert len(results) == 1
    assert results[0].intent.label == "arb_extended_paradex"
    assert results[0].note == "operator accepted candidate"
