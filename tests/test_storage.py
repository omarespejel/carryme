import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from carryme_models import (
    ApprovedCanarySnapshot,
    CandidateAlertEvent,
    CapacityEstimate,
    CleanupPreviewConfirmationEntry,
    ExecutionAlertEvent,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionObservationEntry,
    ExecutionOrderState,
    ExecutionPairStatus,
    ExecutionReconciliation,
    ExecutionVenueReconciliation,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    FundingUniverseVenueMarket,
    OpportunityRecord,
    PaperTradeEntry,
    PaperTradeOrderPreview,
    PreviewConfirmationEntry,
    RouteApprovalEntry,
    TradeLegIntent,
    VenueBalanceSnapshot,
    VenueOrderPreview,
)
from carryme_storage import (
    ApprovedCanaryStore,
    BalanceSnapshotStore,
    CandidateAlertStore,
    CleanupPreviewConfirmationStore,
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    OpportunityHistoryStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    RouteApprovalStore,
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


def test_execution_alert_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = ExecutionAlertStore(tmp_path / "history.sqlite3")
    event = ExecutionAlertEvent(
        emitted_at=datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
        alert_type="cleanup_needed",
        paper_trade_id=7,
        preview_hash="preview-hash",
        pair_status=ExecutionPairStatus(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            derived_state="cleanup_needed",
            recommended_action="close_open_leg",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[],
                notes=[],
            ),
            reconciliation=ExecutionReconciliation(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                status="submitted",
                recommended_action="verify_fill_status",
                matched_all_leg_symbols=False,
                venues=[],
                notes=[],
            ),
            notes=[],
        ),
    )

    first_insert = store.append(event)
    second_insert = store.append(event)
    results = store.list_recent(limit=10)

    assert first_insert is True
    assert second_insert is False
    assert len(results) == 1
    assert results[0].alert_type == "cleanup_needed"
    assert results[0].paper_trade_id == 7
    assert store.latest_for_paper_trade(7) is not None


def test_execution_alert_store_ignores_retried_older_alert(tmp_path: Path) -> None:
    store = ExecutionAlertStore(tmp_path / "history.sqlite3")
    older = ExecutionAlertEvent(
        emitted_at=datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
        alert_type="cleanup_needed",
        paper_trade_id=7,
        preview_hash="preview-hash",
        pair_status=ExecutionPairStatus(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            derived_state="cleanup_needed",
            recommended_action="close_open_leg",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[],
                notes=[],
            ),
            reconciliation=ExecutionReconciliation(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                status="submitted",
                recommended_action="verify_fill_status",
                matched_all_leg_symbols=False,
                venues=[],
                notes=[],
            ),
            notes=[],
        ),
    )
    newer = older.model_copy(
        update={
            "emitted_at": datetime(2026, 3, 29, 12, 6, tzinfo=UTC),
            "pair_status": older.pair_status.model_copy(
                update={
                    "execution_entry_id": 13,
                }
            ),
        }
    )

    assert store.append(older) is True
    assert store.append(newer) is True
    assert store.append(older) is False


def test_execution_alert_store_normalizes_offset_aware_emitted_at(tmp_path: Path) -> None:
    store = ExecutionAlertStore(tmp_path / "history.sqlite3")
    utc_event = ExecutionAlertEvent(
        emitted_at=datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
        alert_type="cleanup_needed",
        paper_trade_id=7,
        preview_hash="preview-hash",
        pair_status=ExecutionPairStatus(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            derived_state="cleanup_needed",
            recommended_action="close_open_leg",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[],
                notes=[],
            ),
            reconciliation=ExecutionReconciliation(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                status="submitted",
                recommended_action="verify_fill_status",
                matched_all_leg_symbols=False,
                venues=[],
                notes=[],
            ),
            notes=[],
        ),
    )
    offset_event = utc_event.model_copy(
        update={"emitted_at": datetime(2026, 3, 29, 15, 5, tzinfo=timezone(timedelta(hours=3)))}
    )

    assert store.append(utc_event) is True
    assert store.append(offset_event) is False
    latest = store.latest_for_paper_trade(7)

    assert latest is not None
    assert latest.emitted_at == datetime(2026, 3, 29, 12, 5, tzinfo=UTC)


def test_execution_alert_store_rejects_naive_emitted_at(tmp_path: Path) -> None:
    store = ExecutionAlertStore(tmp_path / "history.sqlite3")
    event = ExecutionAlertEvent(
        emitted_at=datetime(2026, 3, 29, 12, 5),
        alert_type="cleanup_needed",
        paper_trade_id=7,
        preview_hash="preview-hash",
        pair_status=ExecutionPairStatus(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            derived_state="cleanup_needed",
            recommended_action="close_open_leg",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[],
                notes=[],
            ),
            reconciliation=ExecutionReconciliation(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                status="submitted",
                recommended_action="verify_fill_status",
                matched_all_leg_symbols=False,
                venues=[],
                notes=[],
            ),
            notes=[],
        ),
    )

    with pytest.raises(ValueError, match="emitted_at must be timezone-aware"):
        store.append(event)


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


def test_execution_journal_store_allows_same_confirmation_id_for_different_hashes(
    tmp_path: Path,
) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    assert store.reserve_live_submission(
        confirmation_entry_id=11,
        preview_hash="preview-hash",
    )
    assert store.reserve_live_submission(
        confirmation_entry_id=11,
        preview_hash="cleanup-hash",
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


def test_execution_journal_store_finds_entry_by_confirmation_and_hash(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    preview_entry = store.append(
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
                )
            ],
        )
    )
    cleanup_entry = store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
            adapter="extended_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="cleanup-hash",
            confirmation_entry_id=11,
            paper_trade=preview_entry.paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=1000.0,
                    status="submitted",
                    simulated=False,
                )
            ],
        )
    )

    found = store.find_by_confirmation(
        confirmation_entry_id=11,
        preview_hash="cleanup-hash",
    )

    assert found is not None
    assert found.entry_id == cleanup_entry.entry_id
    assert found.preview_hash == "cleanup-hash"


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


def test_execution_journal_store_normalizes_preview_hashes_on_append(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    saved = store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="extended_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash=" cleanup-hash ",
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
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=1000.0,
                    status="submitted",
                    simulated=False,
                )
            ],
        )
    )

    assert saved.preview_hash == "cleanup-hash"
    found = store.find_by_confirmation(
        confirmation_entry_id=11,
        preview_hash="cleanup-hash",
    )
    assert found is not None
    assert found.preview_hash == "cleanup-hash"


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


def test_cleanup_preview_confirmation_store_finds_latest_by_preview_hash(tmp_path: Path) -> None:
    store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash="cleanup-hash",
            preview=ExecutionCleanupPreview(
                execution_entry_id=12,
                paper_trade_id=7,
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=500.0,
                    quantity=5422.0,
                    quantity_text="5422",
                    reference_price=0.0921,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0922,
                    worst_price_text="0.0922",
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    reduce_only=True,
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=["first"],
            ),
            note="first cleanup confirmation",
        )
    )
    latest = store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash=" cleanup-hash ",
            preview=ExecutionCleanupPreview(
                execution_entry_id=12,
                paper_trade_id=7,
                generated_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=500.0,
                    quantity=5422.0,
                    quantity_text="5422",
                    reference_price=0.0921,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0922,
                    worst_price_text="0.0922",
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    reduce_only=True,
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=["latest"],
            ),
            note="latest cleanup confirmation",
        )
    )

    found = store.find_latest_by_preview_hash(
        paper_trade_id=7,
        preview_hash=" cleanup-hash ",
    )

    assert found is not None
    assert found.entry_id == latest.entry_id
    assert found.preview_hash == "cleanup-hash"


def test_cleanup_preview_confirmation_store_backfills_legacy_whitespace_hashes(
    tmp_path: Path,
) -> None:
    store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    legacy_entry = CleanupPreviewConfirmationEntry(
        confirmed_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
        paper_trade_id=7,
        label=" arb_extended_paradex ",
        preview_hash=" cleanup-hash ",
        preview=ExecutionCleanupPreview(
            execution_entry_id=12,
            paper_trade_id=7,
            generated_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
            preview_hash=" cleanup-hash ",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=500.0,
                quantity=5422.0,
                quantity_text="5422",
                reference_price=0.0921,
                reference_price_source="best_ask",
                worst_acceptable_price=0.0922,
                worst_price_text="0.0922",
                endpoint_path_hint="/api/v1/user/order",
                auth_scheme="api key + Stark signing key",
                reduce_only=True,
                payload={"symbol": "ARB-USD", "reduce_only": True},
                notes=[],
            ),
            notes=["latest"],
        ),
        note="legacy cleanup confirmation",
    )
    with sqlite3.connect(store.database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO cleanup_preview_confirmation_entries (
                confirmed_at,
                paper_trade_id,
                label,
                preview_hash,
                entry_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                legacy_entry.confirmed_at.isoformat(),
                legacy_entry.paper_trade_id,
                legacy_entry.label,
                legacy_entry.preview_hash,
                legacy_entry.model_dump_json(),
            ),
        )
        legacy_id = cursor.lastrowid

    found = store.find_latest_by_preview_hash(
        paper_trade_id=7,
        preview_hash="cleanup-hash",
    )

    assert found is not None
    assert found.entry_id == legacy_id
    assert found.label == "arb_extended_paradex"
    assert found.preview_hash == "cleanup-hash"
    assert found.preview.preview_hash == "cleanup-hash"

    with sqlite3.connect(store.database_path) as connection:
        stored = connection.execute(
            """
            SELECT label, preview_hash, entry_json
            FROM cleanup_preview_confirmation_entries
            WHERE id = ?
            """,
            (legacy_id,),
        ).fetchone()

    assert stored is not None
    stored_label, stored_preview_hash, stored_entry_json = stored
    assert stored_label == "arb_extended_paradex"
    assert stored_preview_hash == "cleanup-hash"
    assert '"label":"arb_extended_paradex"' in stored_entry_json
    assert '"preview_hash":"cleanup-hash"' in stored_entry_json


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


def test_execution_observation_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    entry = ExecutionObservationEntry(
        observed_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
        context="guarded_pair_poll",
        execution_entry_id=12,
        paper_trade_id=7,
        preview_hash="preview-hash",
        order_state=ExecutionOrderState(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[
                ExecutionLegOrderState(
                    venue="paradex",
                    supported=True,
                    observation_source="rest_poll",
                    external_reference="order-1",
                    derived_state="unfilled",
                    order_status="CLOSED",
                )
            ],
            notes=[],
        ),
        pair_status=ExecutionPairStatus(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            derived_state="unfilled",
            recommended_action="no_action",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[],
                notes=[],
            ),
            reconciliation=ExecutionReconciliation(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                status="submitted",
                recommended_action="verify_fill_status",
                matched_all_leg_symbols=False,
                venues=[
                    ExecutionVenueReconciliation(
                        venue="extended",
                        authenticated=True,
                        ready=True,
                        position_symbols=[],
                        matched_leg_symbols=[],
                        unmatched_leg_symbols=["ARB-USD"],
                    )
                ],
                notes=[],
            ),
            notes=[],
        ),
    )

    saved = store.append(entry)
    assert entry.pair_status is not None
    pair_status = entry.pair_status
    newer = store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=13,
            paper_trade_id=7,
            preview_hash="preview-hash-newer",
            order_state=pair_status.order_state.model_copy(
                update={
                    "execution_entry_id": 13,
                    "paper_trade_id": 7,
                    "preview_hash": "preview-hash-newer",
                }
            ),
            pair_status=ExecutionPairStatus(
                execution_entry_id=13,
                paper_trade_id=7,
                preview_hash="preview-hash-newer",
                derived_state="review_required",
                recommended_action="wait_for_fill",
                order_state=pair_status.order_state.model_copy(
                    update={
                        "execution_entry_id": 13,
                        "paper_trade_id": 7,
                        "preview_hash": "preview-hash-newer",
                    }
                ),
                reconciliation=pair_status.reconciliation.model_copy(
                    update={
                        "execution_entry_id": 13,
                        "paper_trade_id": 7,
                        "preview_hash": "preview-hash-newer",
                    }
                ),
                notes=[],
            ),
        )
    )
    results = store.list_recent(limit=10)
    latest = store.latest_for_paper_trade(7)

    assert saved.entry_id is not None
    assert newer.entry_id is not None
    assert len(results) == 2
    assert results[0].entry_id == newer.entry_id
    assert results[0].context == "worker_execution_monitor"
    assert results[0].pair_status is not None
    assert results[0].pair_status.derived_state == "review_required"
    assert latest is not None
    assert latest.entry_id == newer.entry_id


def test_execution_observation_store_rejects_naive_timestamps(tmp_path: Path) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="observed_at must be timezone-aware"):
        store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 3, 29, 13, 5),
                context="worker_execution_monitor",
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                order_state=ExecutionOrderState(
                    execution_entry_id=12,
                    paper_trade_id=7,
                    preview_hash="preview-hash",
                    legs=[],
                    notes=[],
                ),
            )
        )


def test_execution_observation_store_normalizes_timestamps_to_utc(tmp_path: Path) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")

    saved = store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 15, 5, tzinfo=timezone(timedelta(hours=2))),
            context="worker_execution_monitor",
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[],
                notes=[],
            ),
        )
    )

    latest = store.latest_for_paper_trade(7)

    assert saved.observed_at == datetime(2026, 3, 29, 13, 5, tzinfo=UTC)
    assert latest is not None
    assert latest.observed_at == datetime(2026, 3, 29, 13, 5, tzinfo=UTC)


def test_execution_observation_store_filters_by_paper_trade_id(tmp_path: Path) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    first = store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
            context="trade-7",
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash-7",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash-7",
                legs=[],
                notes=[],
            ),
        )
    )
    latest = store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
            context="trade-7-latest",
            execution_entry_id=14,
            paper_trade_id=7,
            preview_hash="preview-hash-7-newer",
            order_state=ExecutionOrderState(
                execution_entry_id=14,
                paper_trade_id=7,
                preview_hash="preview-hash-7-newer",
                legs=[],
                notes=[],
            ),
        )
    )
    store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
            context="trade-8",
            execution_entry_id=13,
            paper_trade_id=8,
            preview_hash="preview-hash-8",
            order_state=ExecutionOrderState(
                execution_entry_id=13,
                paper_trade_id=8,
                preview_hash="preview-hash-8",
                legs=[],
                notes=[],
            ),
        )
    )

    filtered = store.list_recent(limit=10, paper_trade_id=7)
    latest_for_trade = store.latest_for_paper_trade(7)

    assert first.entry_id is not None
    assert latest.entry_id is not None
    assert len(filtered) == 2
    assert [entry.paper_trade_id for entry in filtered] == [7, 7]
    assert [entry.entry_id for entry in filtered] == [latest.entry_id, first.entry_id]
    assert [entry.context for entry in filtered] == ["trade-7-latest", "trade-7"]
    assert latest_for_trade is not None
    assert latest_for_trade.entry_id == latest.entry_id


def test_execution_observation_store_orders_ties_deterministically(tmp_path: Path) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    observed_at = datetime(2026, 3, 29, 13, 6, tzinfo=UTC)

    for index, context in enumerate(["first", "second"], start=12):
        store.append(
            ExecutionObservationEntry(
                observed_at=observed_at,
                context=context,
                execution_entry_id=index,
                paper_trade_id=7,
                preview_hash=f"preview-hash-{context}",
                order_state=ExecutionOrderState(
                    execution_entry_id=index,
                    paper_trade_id=7,
                    preview_hash=f"preview-hash-{context}",
                    legs=[],
                    notes=[],
                ),
            )
        )

    results = store.list_recent(limit=2)

    assert [entry.context for entry in results] == ["second", "first"]


def test_execution_observation_store_list_recent_without_limit(tmp_path: Path) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")

    for index, context in enumerate(["first", "second", "third"], start=12):
        store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 3, 29, 13, index, tzinfo=UTC),
                context=context,
                execution_entry_id=index,
                paper_trade_id=7,
                preview_hash=f"preview-hash-{context}",
                order_state=ExecutionOrderState(
                    execution_entry_id=index,
                    paper_trade_id=7,
                    preview_hash=f"preview-hash-{context}",
                    legs=[],
                    notes=[],
                ),
            )
        )

    results = store.list_recent(limit=None)

    assert [entry.context for entry in results] == ["third", "second", "first"]


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_execution_observation_store_rejects_non_positive_limits(
    tmp_path: Path,
    invalid_limit: int,
) -> None:
    store = ExecutionObservationStore(tmp_path / "history.sqlite3")

    with pytest.raises(ValueError, match="limit must be at least 1"):
        store.list_recent(limit=invalid_limit)


def test_execution_journal_store_lists_entries_for_paper_trade(tmp_path: Path) -> None:
    store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.0005,
            break_even_days_entry=0.5,
            capacity_limit_notional=4500.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
            ),
            short_leg=TradeLegIntent(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
            ),
        ),
    )
    store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
            adapter="extended_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                )
            ],
        )
    )
    store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 2, tzinfo=UTC),
            adapter="paradex_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                )
            ],
        )
    )

    results = store.list_for_paper_trade(7, limit=10)

    assert len(results) == 2
    assert results[0].adapter == "paradex_live"
    assert results[1].adapter == "extended_live"


def test_route_approval_store_upserts_and_reads_route(tmp_path: Path) -> None:
    store = RouteApprovalStore(tmp_path / "history.sqlite3")
    original = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 29, 14, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=25.0,
        note="initial canary",
    )
    updated = original.model_copy(
        update={
            "updated_at": datetime(2026, 3, 29, 14, 5, tzinfo=UTC),
            "max_live_notional": 40.0,
        }
    )

    store.upsert(original)
    store.upsert(updated)

    loaded = store.get_route(
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
    )
    results = store.list_recent(limit=10, approved=True)

    assert loaded is not None
    assert loaded.max_live_notional == 40.0
    assert len(results) == 1
    assert results[0].label == "arb_extended_paradex"


def test_approved_canary_store_appends_and_lists_recent(tmp_path: Path) -> None:
    store = ApprovedCanaryStore(tmp_path / "history.sqlite3")
    snapshot = ApprovedCanarySnapshot(
        captured_at=datetime(2026, 3, 29, 14, 10, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="ARB-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.004,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.00355,
                    one_day_net_edge_after_round_trip=0.0031,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1400.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol="ARB-USD",
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                    ),
                },
                deployable_notional=900.0,
                estimated_one_day_pnl_after_round_trip=2.79,
            ),
            suggested_canary_notional=11.0,
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 14, 9, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        ),
    )

    store.append(snapshot)
    results = store.list_recent(limit=10)
    latest = store.latest(label="arb_extended_paradex")

    assert len(results) == 1
    assert results[0].snapshot_id is not None
    assert results[0].label == "arb_extended_paradex"
    assert results[0].candidate.suggested_canary_notional == 11.0
    assert latest is not None
    assert latest.snapshot_id == results[0].snapshot_id


def test_balance_snapshot_store_appends_and_filters(tmp_path: Path) -> None:
    store = BalanceSnapshotStore(tmp_path / "history.sqlite3")
    store.append(
        VenueBalanceSnapshot(
            captured_at=datetime(2026, 3, 29, 15, 0, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            stage="pre_open",
            venue="extended",
            total_collateral=4.9,
            available_to_trade=4.9,
        )
    )
    store.append(
        VenueBalanceSnapshot(
            captured_at=datetime(2026, 3, 29, 15, 5, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            stage="post_close",
            venue="extended",
            total_collateral=4.85,
            available_to_trade=4.85,
        )
    )

    snapshots = store.list_recent(limit=10, paper_trade_id=7, venue="extended")

    assert len(snapshots) == 2
    assert snapshots[0].stage == "post_close"
    assert snapshots[1].stage == "pre_open"
