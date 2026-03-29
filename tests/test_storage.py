from datetime import UTC, datetime
from pathlib import Path

import pytest
from carryme_models import (
    CandidateAlertEvent,
    CapacityEstimate,
    FundingArbOpportunity,
    FundingPairSpec,
    OpportunityRecord,
)
from carryme_storage import (
    CandidateAlertStore,
    OpportunityHistoryStore,
    WatchlistStore,
    load_watchlist,
    save_watchlist,
)
from carryme_storage.watchlist import _parse_watchlist_payload


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


def test_parse_watchlist_payload_from_raw_array() -> None:
    pairs = _parse_watchlist_payload(
        [
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
    )

    assert len(pairs) == 1
    assert pairs[0] == FundingPairSpec(
        label="arb_extended_paradex",
        left_venue="extended",
        left_symbol="ARB-USD",
        left_fee_profile="default",
        right_venue="paradex",
        right_symbol="ARB-USD-PERP",
        right_fee_profile="pro",
    )


def test_load_watchlist_rejects_missing_pairs_key(tmp_path: Path) -> None:
    path = tmp_path / "watchlist.json"
    path.write_text('{"items": []}')

    with pytest.raises(ValueError, match="'pairs' list"):
        load_watchlist(path)


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
    assert path.read_text(encoding="utf-8").strip().startswith("{")


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


def test_history_store_normalizes_labels_on_write_and_read(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="  arb_extended_paradex  ",
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
            ),
        )
    )

    results = store.list_recent(label="arb_extended_paradex")

    assert len(results) == 1
    assert results[0].pair.label == "arb_extended_paradex"


def test_history_store_orders_ties_deterministically(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    recorded_at = datetime(2026, 3, 29, tzinfo=UTC)
    for label in ["first", "second"]:
        store.append(
            OpportunityRecord(
                recorded_at=recorded_at,
                pair=FundingPairSpec(
                    label=label,
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
                ),
            )
        )

    results = store.list_recent(limit=2)

    assert [item.pair.label for item in results] == ["second", "first"]


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_history_store_rejects_non_positive_limits(tmp_path: Path, invalid_limit: int) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="limit must be at least 1"):
        store.list_recent(limit=invalid_limit)


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

    first_insert = store.append(event)
    second_insert = store.append(event)
    results = store.list_recent(limit=10)

    assert first_insert is True
    assert second_insert is False
    assert len(results) == 1
    assert results[0].record.pair.label == "arb_extended_paradex"
    assert results[0].record.opportunity.canonical_symbol == "ARB-USD-PERP"


def test_candidate_alert_store_normalizes_labels_on_read(tmp_path: Path) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")
    store.append(
        CandidateAlertEvent(
            emitted_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=2500.0,
            record=OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                pair=FundingPairSpec(
                    label="  arb_extended_paradex  ",
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
                ),
            ),
        )
    )

    results = store.list_recent(label="arb_extended_paradex")

    assert len(results) == 1
    assert results[0].record.pair.label == "arb_extended_paradex"


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_candidate_alert_store_rejects_non_positive_limits(
    tmp_path: Path,
    invalid_limit: int,
) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="limit must be at least 1"):
        store.list_recent(limit=invalid_limit)
