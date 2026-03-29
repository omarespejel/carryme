from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
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

    saved = store.append(entry)
    results = store.list_recent(limit=10)

    assert saved.entry_id is not None
    assert len(results) == 1
    assert results[0].intent.label == "arb_extended_paradex"
    assert results[0].note == "operator accepted candidate"
    assert store.get(saved.entry_id) is not None
    assert store.get(999999) is None


def test_execution_journal_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    entry = ExecutionJournalEntry(
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
                raw_payload={"venue_order_id": "paradex-7-buy"},
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
                raw_payload={"venue_order_id": "extended-7-sell"},
            ),
        ],
    )

    saved = store.append(entry)
    results = store.list_recent(limit=10)

    assert saved.entry_id is not None
    assert len(results) == 1
    assert results[0].entry_id == saved.entry_id
    assert results[0].paper_trade_id == 7
    assert results[0].legs[0].external_reference == "mock:7:buy"
    assert saved.legs[0].raw_payload == {"venue_order_id": "paradex-7-buy"}
    assert saved.legs[1].raw_payload == {"venue_order_id": "extended-7-sell"}
    assert results[0].legs[0].raw_payload == {"venue_order_id": "paradex-7-buy"}
    assert results[0].legs[1].raw_payload == {"venue_order_id": "extended-7-sell"}


def test_execution_journal_store_reserves_live_submission_once(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    assert store.reserve_live_submission(
        confirmation_entry_id=11,
        preview_hash="preview-hash",
    )
    assert not store.reserve_live_submission(
        confirmation_entry_id=11,
        preview_hash="preview-hash",
    )


def test_execution_journal_store_finds_entry_by_confirmation_entry_id(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    saved = store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paradex_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=11,
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
                    status="submitted",
                    simulated=False,
                    request_payload=["raw", "request"],
                    response_payload=["raw", "response"],
                )
            ],
        )
    )

    found = store.find_by_confirmation_entry_id(11)

    assert found is not None
    assert found.entry_id == saved.entry_id
    assert found.confirmation_entry_id == 11
    assert found.legs[0].request_payload == ["raw", "request"]
    assert found.legs[0].response_payload == ["raw", "response"]


def test_execution_journal_store_treats_blank_label_as_unfiltered(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="mock",
            mode="mock",
            submission_id="submission-blank-label",
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
                )
            ],
        )
    )

    assert len(store.list_recent(limit=10, label="   ")) == 1


def test_execution_journal_store_rejects_whitespace_only_labels(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="label must be non-empty"):
        store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                adapter="mock",
                mode="mock",
                submission_id="submission-whitespace-label",
                status="accepted",
                paper_trade_id=7,
                paper_trade=PaperTradeEntry(
                    entry_id=7,
                    created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                    note="operator accepted candidate",
                    intent=FundingPairTradeIntent(
                        label="   ",
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
                    )
                ],
            )
        )

    assert store.list_recent(limit=10) == []


def test_execution_journal_store_normalizes_executed_at_to_utc(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    saved = store.append(
        ExecutionJournalEntry(
            executed_at=datetime(
                2026,
                3,
                29,
                16,
                5,
                tzinfo=timezone(timedelta(hours=3)),
            ),
            adapter="mock",
            mode="mock",
            submission_id="submission-1",
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
                )
            ],
        )
    )

    assert saved.executed_at == datetime(2026, 3, 29, 13, 5, tzinfo=UTC)


def test_execution_journal_store_rejects_naive_executed_at(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="executed_at must be timezone-aware"):
        store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 5),
                adapter="mock",
                mode="mock",
                submission_id="submission-naive",
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
                    )
                ],
            )
        )


def test_execution_journal_store_orders_ties_deterministically(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    for paper_trade_id in [1, 2]:
        store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                adapter="mock",
                mode="mock",
                submission_id=f"submission-{paper_trade_id}",
                status="accepted",
                paper_trade_id=paper_trade_id,
                paper_trade=PaperTradeEntry(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                    note=f"operator accepted candidate {paper_trade_id}",
                    intent=FundingPairTradeIntent(
                        label=f"arb_extended_paradex_{paper_trade_id}",
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
                        external_reference=f"mock:{paper_trade_id}:buy",
                    )
                ],
            )
        )

    results = store.list_recent(limit=2)

    assert [entry.paper_trade_id for entry in results] == [2, 1]


def test_paper_trade_store_normalizes_created_at_to_utc(tmp_path: Path) -> None:
    store = PaperTradeStore(tmp_path / "history.sqlite3")
    saved = store.append(
        PaperTradeEntry(
            created_at=datetime(
                2026,
                3,
                29,
                16,
                0,
                tzinfo=timezone(timedelta(hours=3)),
            ),
            note="normalized timestamp",
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

    assert saved.created_at == datetime(2026, 3, 29, 13, 0, tzinfo=UTC)


def test_paper_trade_store_rejects_naive_created_at(tmp_path: Path) -> None:
    store = PaperTradeStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="created_at must be timezone-aware"):
        store.append(
            PaperTradeEntry(
                created_at=datetime(2026, 3, 29, 13, 0),
                note="naive timestamp",
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


def test_paper_trade_store_orders_ties_deterministically(tmp_path: Path) -> None:
    store = PaperTradeStore(tmp_path / "history.sqlite3")
    for note in ["first", "second"]:
        store.append(
            PaperTradeEntry(
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note=note,
                intent=FundingPairTradeIntent(
                    label=f"arb_extended_paradex_{note}",
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

    results = store.list_recent(limit=2)

    assert [entry.note for entry in results] == ["second", "first"]


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


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_paper_trade_store_rejects_non_positive_limits(
    tmp_path: Path,
    invalid_limit: int,
) -> None:
    store = PaperTradeStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="limit must be at least 1"):
        store.list_recent(limit=invalid_limit)


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_execution_journal_store_rejects_non_positive_limits(
    tmp_path: Path,
    invalid_limit: int,
) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="limit must be at least 1"):
        store.list_recent(limit=invalid_limit)


def test_preview_confirmation_store_appends_and_lists_recent(tmp_path: Path) -> None:
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
    store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 11, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=7,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
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
            note="operator reconfirmed",
        )
    )

    entries = store.list_recent(limit=10)

    assert len(entries) == 2
    assert entries[0].confirmed_at > entries[1].confirmed_at
    assert entries[0].paper_trade_id == 7
    assert entries[1].paper_trade_id == 7
    assert entries[1].preview.preview_hash == "preview-hash"
    assert entries[0].preview.preview_hash == "preview-hash"


def test_preview_confirmation_store_finds_latest_by_trade_and_hash(tmp_path: Path) -> None:
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
    latest = store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 11, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=7,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
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
            note="operator reconfirmed",
        )
    )
    store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash="other-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=7,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="other-hash",
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
            note="operator confirmed other hash",
        )
    )
    store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 13, tzinfo=UTC),
            paper_trade_id=8,
            label="arb_extended_paradex_other",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=8,
                label="arb_extended_paradex_other",
                generated_at=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="sell",
                        target_notional=500.0,
                        quantity=5_422.0,
                        quantity_text="5422.00000000",
                        reference_price=0.0921,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.09198948,
                        worst_price_text="0.09198948",
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
            note="other trade confirmed identical hash",
        )
    )

    found = store.find_latest_by_preview_hash(
        paper_trade_id=7,
        preview_hash=" preview-hash ",
    )

    assert found is not None
    assert found.entry_id == latest.entry_id
    assert found.preview_hash == "preview-hash"


def test_preview_confirmation_store_returns_none_for_missing_hash(tmp_path: Path) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")

    assert (
        store.find_latest_by_preview_hash(
            paper_trade_id=7,
            preview_hash="missing-hash",
        )
        is None
    )


def test_preview_confirmation_store_rejects_whitespace_only_hash_lookup(
    tmp_path: Path,
) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="preview_hash must be non-empty"):
        store.find_latest_by_preview_hash(
            paper_trade_id=7,
            preview_hash="  ",
        )


def test_preview_confirmation_store_normalizes_labels_and_hashes(tmp_path: Path) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=7,
            label="  arb_extended_paradex  ",
            preview_hash=" preview-hash ",
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

    entries = store.list_recent(limit=10, label="arb_extended_paradex")

    assert len(entries) == 1
    assert entries[0].label == "arb_extended_paradex"
    assert entries[0].preview_hash == "preview-hash"
    assert entries[0].preview.preview_hash == "preview-hash"


def test_preview_confirmation_store_rejects_preview_identifier_mismatches(
    tmp_path: Path,
) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="preview.paper_trade_id must match paper_trade_id"):
        store.append(
            PreviewConfirmationEntry(
                confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
                paper_trade_id=7,
                label="arb_extended_paradex",
                preview_hash="preview-hash",
                preview=PaperTradeOrderPreview(
                    paper_trade_id=8,
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


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_preview_confirmation_store_rejects_non_positive_limits(
    tmp_path: Path,
    invalid_limit: int,
) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="limit must be at least 1"):
        store.list_recent(limit=invalid_limit)
