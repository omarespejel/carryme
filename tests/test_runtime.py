import asyncio
import json
from datetime import UTC, datetime
from typing import Any, cast

import carryme_runtime.account_preflight as account_preflight_runtime
import httpx
import pytest
from carryme_connectors import ParadexJwtTokenProvider, build_paradex_auth_headers
from carryme_models import (
    CapacityEstimate,
    ExecutionJournalEntry,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    LiveSubmissionReadiness,
    MarketStats,
    NormalizedMarketSnapshot,
    OpportunityRecord,
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    PaperTradeOrderPreview,
    PreviewConfirmationEntry,
    TopOfBook,
    TradeLegIntent,
    VenueAccountPreflight,
    VenueExecutionPreflight,
    VenueOrderPreview,
)
from carryme_normalizers import normalize_market_snapshot
from carryme_runtime import (
    AccountPreflightService,
    InvalidTradeCandidateError,
    MockExecutionAdapter,
    OpportunityService,
    OrderPreviewService,
    VenueAccountProbe,
    build_live_submission_readiness,
    build_paper_trade_execution_preflight,
    build_trade_intent,
    build_venue_execution_preflights,
    require_confirmed_preview,
)
from carryme_runtime.account_preflight import ExtendedAccountProbe, ParadexAccountProbe


def _snapshot(
    venue: str,
    symbol: str,
    funding_rate: float,
    bid_price: float,
    bid_size: float,
    ask_price: float,
    ask_size: float,
) -> NormalizedMarketSnapshot:
    return normalize_market_snapshot(
        venue,
        MarketStats(
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
        ),
    )


def test_opportunity_service_scores_from_fetcher() -> None:
    snapshots = {
        ("extended", "STRK-USD"): _snapshot(
            "extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000
        ),
        ("hyperliquid", "STRK"): _snapshot(
            "hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 50_000
        ),
    }

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityService(fetch_snapshot=fetch_snapshot)
        opportunity = await service.score_pair(
            left_venue="extended",
            left_symbol="STRK-USD",
            left_fee_profile="default",
            right_venue="hyperliquid",
            right_symbol="STRK",
            right_fee_profile="tier0",
        )

        assert opportunity.short_venue == "extended"
        assert opportunity.long_venue == "hyperliquid"

    asyncio.run(run())


def test_build_live_submission_readiness_requires_confirmation_and_preflights() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=7,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=5,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=5,
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
                    required_auth_env_vars=[
                        "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                        "CARRYME_API_PARADEX_PRIVATE_KEY",
                    ],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP"},
                    notes=[],
                )
            ],
        ),
    )

    readiness = build_live_submission_readiness(
        paper_trade_id=5,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        confirmations=[confirmation],
        execution_preflight=PaperTradeExecutionPreflight(
            paper_trade_id=5,
            label="arb_extended_paradex",
            ready=False,
            venues=[],
            blocking_reasons=["Venue paradex live execution is not enabled"],
        ),
        account_preflight=PaperTradeAccountPreflight(
            paper_trade_id=5,
            label="arb_extended_paradex",
            ready=False,
            venues=[],
            blocking_reasons=[
                (
                    "Venue paradex is missing required account credentials: "
                    "CARRYME_API_PARADEX_BEARER_TOKEN"
                )
            ],
        ),
    )

    assert isinstance(readiness, LiveSubmissionReadiness)
    assert readiness.confirmed_preview is True
    assert readiness.confirmation_entry_id == 7
    assert readiness.ready is False
    assert "Venue paradex live execution is not enabled" in readiness.blocking_reasons
    assert "CARRYME_API_PARADEX_BEARER_TOKEN" in str(readiness.blocking_reasons)


def test_build_trade_intent_sizes_by_capacity_fraction_and_cap() -> None:
    intent = build_trade_intent(
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
        ),
        capacity_fraction=0.5,
        max_target_notional=1000.0,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=500.0,
        max_break_even_days_entry=1.0,
    )

    assert intent.label == "strk_extended_hyperliquid"
    assert intent.target_notional == pytest.approx(1000.0)
    assert intent.long_leg.venue == "hyperliquid"
    assert intent.long_leg.side == "buy"
    assert intent.short_leg.venue == "extended"
    assert intent.short_leg.side == "sell"


def test_build_trade_intent_rejects_edge_below_threshold() -> None:
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
            gross_daily_edge=0.0005,
            entry_cost_rate=0.0003,
            round_trip_cost_rate=0.0006,
            one_day_net_edge_after_entry=-0.0001,
            one_day_net_edge_after_round_trip=-0.0004,
            break_even_days_entry=0.8,
            break_even_days_round_trip=1.6,
            capacity=CapacityEstimate(
                short_bid_notional=5000.0,
                long_ask_notional=4500.0,
                max_entry_notional=4500.0,
                limiting_venue="paradex",
            ),
        ),
    )

    with pytest.raises(InvalidTradeCandidateError, match="one-day net entry edge"):
        build_trade_intent(
            record,
            capacity_fraction=0.25,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=500.0,
        )


def test_mock_execution_adapter_builds_accepted_execution_entry() -> None:
    intent = build_trade_intent(
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
        ),
        capacity_fraction=0.5,
        max_target_notional=1000.0,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=500.0,
    )
    paper_trade = PaperTradeEntry(
        entry_id=11,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="operator accepted candidate",
        intent=intent,
    )

    entry = MockExecutionAdapter().submit(
        paper_trade,
        executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
    )

    assert isinstance(entry, ExecutionJournalEntry)
    assert entry.executed_at == datetime(2026, 3, 29, 13, 5, tzinfo=UTC)
    assert entry.adapter == "mock"
    assert entry.mode == "mock"
    assert entry.submission_id is not None
    assert entry.status == "accepted"
    assert entry.paper_trade_id == 11
    assert len(entry.legs) == 2
    assert entry.legs[0].status == "accepted"
    assert entry.legs[0].simulated is True
    assert entry.legs[0].external_reference is not None
    assert entry.legs[1].external_reference is not None
    assert entry.submission_id in entry.legs[0].external_reference
    assert entry.submission_id in entry.legs[1].external_reference


def test_mock_execution_adapter_raises_without_entry_id() -> None:
    intent = build_trade_intent(
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
        ),
        capacity_fraction=0.5,
        max_target_notional=1000.0,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=500.0,
    )
    paper_trade = PaperTradeEntry(
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="operator accepted candidate",
        intent=intent,
    )

    with pytest.raises(ValueError, match="entry_id is required"):
        MockExecutionAdapter().submit(
            paper_trade,
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        )


def test_build_venue_execution_preflights_reports_missing_credentials() -> None:
    statuses = build_venue_execution_preflights(
        {
            "extended": {
                "enabled": True,
                "credentials": {
                    "api_key": None,
                    "stark_private_key": None,
                },
            },
            "paradex": {
                "enabled": True,
                "credentials": {
                    "account_address": "0xabc",
                    "private_key": "paradex-secret",
                },
            },
            "hyperliquid": {
                "enabled": False,
                "credentials": {
                    "account_address": None,
                    "api_wallet_private_key": None,
                },
            },
        }
    )

    assert isinstance(statuses[0], VenueExecutionPreflight)
    extended = {status.venue: status for status in statuses}["extended"]
    paradex = {status.venue: status for status in statuses}["paradex"]
    assert extended.ready is False
    assert "CARRYME_API_EXTENDED_API_KEY" in extended.missing_env_vars
    assert paradex.ready is True


def test_build_paper_trade_execution_preflight_filters_to_trade_venues() -> None:
    intent = build_trade_intent(
        OpportunityRecord(
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
        ),
        capacity_fraction=0.25,
        max_target_notional=1000.0,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=1000.0,
    )
    paper_trade = PaperTradeEntry(
        entry_id=5,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=intent,
    )

    preflight = build_paper_trade_execution_preflight(
        paper_trade,
        {
            "extended": {
                "enabled": True,
                "credentials": {
                    "api_key": "extended-key",
                    "stark_private_key": "extended-stark",
                },
            },
            "paradex": {
                "enabled": False,
                "credentials": {
                    "private_key": None,
                },
            },
            "hyperliquid": {
                "enabled": False,
                "credentials": {
                    "account_address": None,
                    "api_wallet_private_key": None,
                },
            },
        },
    )

    assert isinstance(preflight, PaperTradeExecutionPreflight)
    assert preflight.paper_trade_id == 5
    assert {item.venue for item in preflight.venues} == {"extended", "paradex"}
    assert preflight.ready is False
    assert any("paradex" in reason for reason in preflight.blocking_reasons)


def test_build_paper_trade_execution_preflight_requires_persisted_entry_id() -> None:
    intent = build_trade_intent(
        OpportunityRecord(
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
        ),
        capacity_fraction=0.25,
        max_target_notional=1000.0,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=1000.0,
    )
    paper_trade = PaperTradeEntry(
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=intent,
    )

    with pytest.raises(ValueError, match="paper_trade.entry_id must be set"):
        build_paper_trade_execution_preflight(
            paper_trade,
            {
                "extended": {
                    "enabled": True,
                    "credentials": {
                        "api_key": "extended-key",
                        "stark_private_key": "extended-stark",
                    },
                },
                "paradex": {
                    "enabled": True,
                    "credentials": {
                        "private_key": "paradex-secret",
                    },
                },
                "hyperliquid": {
                    "enabled": False,
                    "credentials": {
                        "account_address": None,
                        "api_wallet_private_key": None,
                    },
                },
            },
        )


def test_build_paper_trade_execution_preflight_skips_unknown_venues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    intent = FundingPairTradeIntent(
        label="mystery_extended",
        canonical_symbol="ARB-USD-PERP",
        source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
        one_day_net_edge_after_entry=0.00055,
        break_even_days_entry=0.45,
        capacity_limit_notional=4500.0,
        target_notional=1000.0,
        capacity_fraction=0.25,
        max_target_notional=1000.0,
        long_leg=TradeLegIntent(
            venue="mysterydex",
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
    )
    paper_trade = PaperTradeEntry(
        entry_id=5,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=intent,
    )

    with caplog.at_level("WARNING"):
        preflight = build_paper_trade_execution_preflight(
            paper_trade,
            {
                "extended": {
                    "enabled": True,
                    "credentials": {
                        "api_key": "extended-key",
                        "stark_private_key": "extended-stark",
                    },
                },
                "paradex": {
                    "enabled": True,
                    "credentials": {
                        "private_key": "paradex-secret",
                    },
                },
                "hyperliquid": {
                    "enabled": False,
                    "credentials": {
                        "account_address": None,
                        "api_wallet_private_key": None,
                    },
                },
            },
        )

    assert {item.venue for item in preflight.venues} == {"extended"}
    assert "Venue mysterydex is not supported for live execution" in preflight.blocking_reasons
    assert "Skipping unsupported live execution venue mysterydex" in caplog.text


def test_order_preview_service_builds_per_venue_templates() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=5,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=build_trade_intent(
            OpportunityRecord(
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
            ),
            capacity_fraction=0.25,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=1000.0,
        ),
    )

    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended", "ARB-USD", 0.0002, 0.0919, 30_000, 0.0921, 25_000
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex", "ARB-USD-PERP", -0.0004, 0.0918, 20_000, 0.0922, 18_000
        ),
    }

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        preview = await OrderPreviewService(fetch_snapshot=fetch_snapshot).preview_paper_trade(
            paper_trade,
            slippage_tolerance_bps=10,
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        )

        assert isinstance(preview, PaperTradeOrderPreview)
        assert preview.paper_trade_id == 5
        assert preview.label == "arb_extended_paradex"
        assert preview.slippage_tolerance_bps == 10
        assert preview.preview_hash
        assert len(preview.legs) == 2

        legs = {item.venue: item for item in preview.legs}
        paradex = legs["paradex"]
        extended = legs["extended"]

        assert paradex.side == "buy"
        assert paradex.reference_price == pytest.approx(0.0922)
        assert paradex.worst_acceptable_price == pytest.approx(0.0922922)
        assert paradex.quantity == pytest.approx(1000.0 / 0.0922)
        assert paradex.time_in_force == "ioc"
        assert paradex.http_method == "POST"
        assert paradex.endpoint_path_hint == "/v1/orders"
        assert paradex.payload["market"] == "ARB-USD-PERP"
        assert paradex.payload["side"] == "BUY"

        assert extended.side == "sell"
        assert extended.reference_price == pytest.approx(0.0919)
        assert extended.worst_acceptable_price == pytest.approx(0.0918081)
        assert extended.quantity == pytest.approx(1000.0 / 0.0919)
        assert extended.time_in_force == "ioc"
        assert extended.http_method == "POST"
        assert extended.payload["symbol"] == "ARB-USD"
        assert extended.payload["side"] == "SELL"

    asyncio.run(run())


def test_build_trade_intent_rejects_invalid_capacity_fraction() -> None:
    record = OpportunityRecord(
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

    with pytest.raises(InvalidTradeCandidateError, match="capacity_fraction must be within"):
        build_trade_intent(
            record,
            capacity_fraction=0.0,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=500.0,
        )


def test_build_trade_intent_rejects_capacity_fraction_above_one() -> None:
    record = OpportunityRecord(
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

    with pytest.raises(InvalidTradeCandidateError, match="capacity_fraction must be within"):
        build_trade_intent(
            record,
            capacity_fraction=1.5,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=500.0,
        )


def test_build_trade_intent_rejects_non_positive_max_target_notional() -> None:
    record = OpportunityRecord(
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

    with pytest.raises(InvalidTradeCandidateError, match="max_target_notional"):
        build_trade_intent(
            record,
            capacity_fraction=0.25,
            max_target_notional=-100.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=500.0,
        )


def test_build_trade_intent_rejects_missing_capacity_estimate() -> None:
    record = OpportunityRecord(
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
            capacity=None,
        ),
    )

    with pytest.raises(InvalidTradeCandidateError, match="usable capacity estimate"):
        build_trade_intent(
            record,
            capacity_fraction=0.25,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=500.0,
        )


def test_build_trade_intent_rejects_break_even_days_entry_above_max() -> None:
    record = OpportunityRecord(
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
            break_even_days_entry=2.0,
            break_even_days_round_trip=3.0,
            capacity=CapacityEstimate(
                short_bid_notional=4000.0,
                long_ask_notional=3000.0,
                max_entry_notional=3000.0,
                limiting_venue="hyperliquid",
            ),
        ),
    )

    with pytest.raises(InvalidTradeCandidateError, match="break-even days exceed"):
        build_trade_intent(
            record,
            capacity_fraction=0.25,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=500.0,
            max_break_even_days_entry=1.0,
        )


def test_build_trade_intent_rejects_zero_max_entry_notional() -> None:
    record = OpportunityRecord(
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
                max_entry_notional=0.0,
                limiting_venue="hyperliquid",
            ),
        ),
    )

    with pytest.raises(InvalidTradeCandidateError, match="usable capacity estimate"):
        build_trade_intent(
            record,
            capacity_fraction=0.25,
            max_target_notional=1000.0,
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=0.0,
        )


def test_require_confirmed_preview_returns_matching_entry() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=3,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=5,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=5,
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

    matched = require_confirmed_preview(
        paper_trade_id=5,
        preview_hash="preview-hash",
        confirmations=[confirmation],
    )

    assert matched.entry_id == 3
    assert matched.preview.preview_hash == "preview-hash"

def test_account_preflight_service_filters_to_trade_venues() -> None:
    class StubProbe:
        def __init__(self, result: VenueAccountPreflight) -> None:
            self.result = result
            self.calls: list[dict[str, object]] = []

        async def probe(self, config: dict[str, object]) -> VenueAccountPreflight:
            self.calls.append(config)
            assert isinstance(config["enabled"], bool)
            return self.result

    class UnusedProbe:
        def __init__(self) -> None:
            self.calls = 0

        async def probe(self, config: dict[str, object]) -> VenueAccountPreflight:
            self.calls += 1
            raise AssertionError(f"unexpected probe for unused venue with config {config}")

    extended_probe = StubProbe(
        VenueAccountPreflight(
            venue="extended",
            enabled=True,
            authenticated=True,
            ready=True,
            credential_mode="api_key",
            account_identifier="ext-subaccount",
            available_to_trade=1500.0,
        )
    )
    paradex_probe = StubProbe(
        VenueAccountPreflight(
            venue="paradex",
            enabled=True,
            authenticated=False,
            ready=False,
            credential_mode="bearer_token",
            missing_env_vars=["CARRYME_API_PARADEX_BEARER_TOKEN"],
            blocking_reasons=[
                (
                    "Venue paradex is missing required account credentials: "
                    "CARRYME_API_PARADEX_BEARER_TOKEN"
                )
            ],
        )
    )
    unused_probe = UnusedProbe()

    service = AccountPreflightService(
        probes=cast(
            dict[str, VenueAccountProbe],
            {
                "extended": extended_probe,
                "paradex": paradex_probe,
                "hyperliquid": unused_probe,
            },
        ),
    )
    paper_trade = PaperTradeEntry(
        entry_id=9,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
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

    async def run() -> None:
        preflight = await service.probe_paper_trade(
            paper_trade,
            {
                "extended": {
                    "enabled": True,
                    "credentials": {"api_key": "extended-key"},
                },
                "paradex": {
                    "enabled": True,
                    "credentials": {
                        "account_address": "0xabc",
                        "bearer_token": None,
                    },
                },
            },
        )

        assert isinstance(preflight, PaperTradeAccountPreflight)
        assert preflight.paper_trade_id == 9
        assert preflight.ready is False
        assert {item.venue for item in preflight.venues} == {"extended", "paradex"}
        assert any("paradex" in reason for reason in preflight.blocking_reasons)
        assert extended_probe.calls == [
            {
                "enabled": True,
                "credentials": {"api_key": "extended-key"},
            }
        ]
        assert paradex_probe.calls == [
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xabc",
                    "bearer_token": None,
                },
            }
        ]
        assert unused_probe.calls == 0

    asyncio.run(run())


def test_build_paradex_auth_headers_returns_official_header_shape() -> None:
    headers = build_paradex_auth_headers(
        account_address="0x123",
        private_key="0x456",
        starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET",
        issued_at=1_700_000_000,
        expires_at=1_700_086_400,
    )

    assert headers["PARADEX-STARKNET-ACCOUNT"] == "0x123"
    assert headers["PARADEX-TIMESTAMP"] == "1700000000"
    assert headers["PARADEX-SIGNATURE-EXPIRATION"] == "1700086400"
    signature = json.loads(headers["PARADEX-STARKNET-SIGNATURE"])
    assert isinstance(signature, list)
    assert len(signature) == 2
    assert all(isinstance(item, str) and item.isdigit() for item in signature)


def test_paradex_jwt_token_provider_fetches_config_and_authenticates() -> None:
    requests: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path, dict(request.headers)))
        if request.url.path == "/v1/system/config":
            return httpx.Response(
                200,
                json={"starknet_chain_id": "PRIVATE_SN_PARACLEAR_MAINNET"},
            )
        if request.url.path == "/v1/auth":
            assert request.headers["PARADEX-STARKNET-ACCOUNT"] == "0x123"
            assert request.headers["PARADEX-TIMESTAMP"] == "1700000000"
            assert request.headers["PARADEX-SIGNATURE-EXPIRATION"] == "1700086400"
            signature = json.loads(request.headers["PARADEX-STARKNET-SIGNATURE"])
            assert len(signature) == 2
            return httpx.Response(200, json={"jwt_token": "jwt-token"})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    async def run() -> None:
        provider = ParadexJwtTokenProvider()
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(
            base_url="https://api.prod.paradex.trade",
            transport=transport,
        ) as client:
            token = await provider.issue_jwt_token(
                account_address="0x123",
                private_key="0x456",
                client=client,
                now=1_700_000_000,
            )
        assert token == "jwt-token"
        assert [item[:2] for item in requests] == [
            ("GET", "/v1/system/config"),
            ("POST", "/v1/auth"),
        ]

    asyncio.run(run())


def test_paradex_account_probe_requires_private_key_or_bearer_override() -> None:
    async def run() -> None:
        probe = ParadexAccountProbe()
        status = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xabc",
                    "private_key": None,
                    "bearer_token": None,
                },
            }
        )
        assert status.authenticated is False
        assert status.ready is False
        assert status.credential_mode == "subkey_jwt"
        assert status.missing_env_vars == ["CARRYME_API_PARADEX_PRIVATE_KEY"]

    asyncio.run(run())


def test_paradex_account_probe_uses_subkey_jwt_when_bearer_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubTokenProvider:
        async def issue_jwt_token(
            self,
            *,
            account_address: str,
            private_key: str,
            client: httpx.AsyncClient | None = None,
            now: int | None = None,
        ) -> str:
            assert account_address == "0xabc"
            assert private_key == "0x123"
            return "derived-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer derived-token"
        if request.url.path == "/v1/account":
            return httpx.Response(200, json={"account": "0xabc", "status": "ACTIVE"})
        if request.url.path == "/v1/balance":
            return httpx.Response(200, json=[])
        if request.url.path == "/v1/positions":
            return httpx.Response(200, json=[])
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        probe = ParadexAccountProbe(token_provider=StubTokenProvider())
        status = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xabc",
                    "private_key": "0x123",
                    "bearer_token": None,
                },
            }
        )
        assert status.authenticated is True
        assert status.ready is True
        assert status.credential_mode == "subkey_jwt"
        assert status.account_identifier == "0xabc"

    asyncio.run(run())


def test_account_preflight_service_blocks_unsupported_trade_venue() -> None:
    service = AccountPreflightService(
        probes=cast(
            dict[str, VenueAccountProbe],
            {
                "extended": cast(
                    VenueAccountProbe,
                    type(
                        "StubProbe",
                        (),
                        {
                            "probe": lambda self, config: asyncio.sleep(
                                0,
                                result=VenueAccountPreflight(
                                    venue="extended",
                                    enabled=True,
                                    authenticated=True,
                                    ready=True,
                                    credential_mode="api_key",
                                ),
                            )
                        },
                    )(),
                )
            },
        )
    )
    paper_trade = PaperTradeEntry(
        entry_id=11,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=FundingPairTradeIntent(
            label="arb_extended_unknown",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=4500.0,
            target_notional=1000.0,
            capacity_fraction=0.25,
            max_target_notional=1000.0,
            long_leg=TradeLegIntent(
                venue="unsupportedx",
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

    async def run() -> None:
        preflight = await service.probe_paper_trade(
            paper_trade,
            {
                "extended": {
                    "enabled": True,
                    "credentials": {"api_key": "extended-key"},
                }
            },
        )

        assert preflight.ready is False
        assert [item.venue for item in preflight.venues] == ["unsupportedx", "extended"]
        assert (
            "Venue unsupportedx account preflight is not supported"
            in preflight.blocking_reasons
        )

    asyncio.run(run())


def test_extended_account_probe_blocks_malformed_balance_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_async_client = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={"data": {"subAccountId": "ext-subaccount", "equity": "1250.5"}},
            )
        if request.url.path == "/api/v1/user/balance":
            return httpx.Response(200, json={"data": "not-a-list"})
        if request.url.path == "/api/v1/user/positions":
            return httpx.Response(200, json={"data": []})
        raise AssertionError(f"unexpected path: {request.url.path}")

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(
            *args,
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    monkeypatch.setattr(account_preflight_runtime.httpx, "AsyncClient", client_factory)

    async def run() -> None:
        status = await ExtendedAccountProbe().probe(
            {
                "enabled": True,
                "credentials": {"api_key": "extended-key"},
            }
        )

        assert status.authenticated is False
        assert status.ready is False
        assert status.blocking_reasons == [
            (
                "Extended authenticated read returned malformed payload: "
                "Extended balances payload field 'data' must be a list"
            )
        ]

    asyncio.run(run())


@pytest.mark.parametrize(
    ("paper_trade_id", "preview_hash"),
    [
        (6, "preview-hash"),
        (5, "wrong-hash"),
    ],
)
def test_require_confirmed_preview_rejects_mismatches(
    paper_trade_id: int,
    preview_hash: str,
) -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=3,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=5,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=5,
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

    with pytest.raises(ValueError, match="No preview confirmation matched"):
        require_confirmed_preview(
            paper_trade_id=paper_trade_id,
            preview_hash=preview_hash,
            confirmations=[confirmation],
        )
