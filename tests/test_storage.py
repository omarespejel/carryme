from datetime import UTC, datetime
from pathlib import Path

import pytest
from carryme_models import (
    CapacityEstimate,
    FundingArbOpportunity,
    FundingPairSpec,
    OpportunityRecord,
)
from carryme_storage import OpportunityHistoryStore, load_watchlist


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


def test_load_watchlist_rejects_missing_pairs_key(tmp_path: Path) -> None:
    path = tmp_path / "watchlist.json"
    path.write_text('{"items": []}')

    with pytest.raises(ValueError, match="'pairs' list"):
        load_watchlist(path)


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
