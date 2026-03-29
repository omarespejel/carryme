import asyncio
import json
from datetime import UTC, datetime
from typing import Any, cast

import carryme_runtime.account_preflight as account_preflight_runtime
import httpx
import pytest
from carryme_connectors import (
    ParadexJwtTokenProvider,
    build_paradex_auth_headers,
    build_paradex_auth_request_path,
    build_signed_extended_order_payload,
    build_signed_paradex_order_payload,
)
from carryme_models import (
    CapacityEstimate,
    CleanupPreviewConfirmationEntry,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionOrderState,
    ExecutionPairStatus,
    ExecutionReconciliation,
    ExecutionVenueReconciliation,
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
    CleanupPreviewRouter,
    ExtendedCleanupPreviewService,
    ExtendedLiveExecutionService,
    MockExecutionAdapter,
    OpportunityService,
    OrderPreviewService,
    PairedLiveExecutionCoordinator,
    ParadexCleanupPreviewService,
    ParadexLiveExecutionService,
    ParadexOrderStateObserver,
    VenueAccountProbe,
    build_execution_pair_status,
    build_live_submission_readiness,
    build_paper_trade_execution_preflight,
    build_trade_intent,
    build_venue_execution_preflights,
    reconcile_execution,
    require_confirmed_cleanup_preview,
    require_confirmed_preview,
)
from carryme_runtime.account_preflight import (
    ExtendedAccountProbe,
    ParadexAccountProbe,
    _extract_balance_assets,
    _extract_position_symbols,
)


def _snapshot(
    venue: str,
    symbol: str,
    funding_rate: float,
    bid_price: float,
    bid_size: float,
    ask_price: float,
    ask_size: float,
    *,
    raw: dict[str, object] | None = None,
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
            raw=raw or {},
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

    with pytest.raises(ValueError, match="one-day net entry edge"):
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
    assert entry.adapter == "mock"
    assert entry.mode == "mock"
    assert entry.status == "accepted"
    assert entry.paper_trade_id == 11
    assert len(entry.legs) == 2
    assert entry.legs[0].status == "accepted"
    assert entry.legs[0].simulated is True


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
    assert paradex.missing_env_vars == []


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
                    "account_address": None,
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
            "extended",
            "ARB-USD",
            0.0002,
            0.0919,
            30_000,
            0.0921,
            25_000,
            raw={
                "tradingConfig": {
                    "minOrderSize": "10",
                    "minOrderSizeChange": "1",
                    "minPriceChange": "0.0001",
                    "maxLimitOrderValue": "1250000",
                }
            },
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0004,
            0.0918,
            20_000,
            0.0922,
            18_000,
            raw={
                "price_tick_size": "0.0001",
                "order_size_increment": "0.1",
                "min_notional": "10",
                "max_order_size": "12000000",
            },
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
        assert paradex.worst_acceptable_price == pytest.approx(0.0923)
        assert paradex.quantity == pytest.approx(10845.9)
        assert paradex.effective_notional == pytest.approx(999.99198)
        assert paradex.quantity_increment == pytest.approx(0.1)
        assert paradex.price_increment == pytest.approx(0.0001)
        assert paradex.time_in_force == "ioc"
        assert paradex.http_method == "POST"
        assert paradex.endpoint_path_hint == "/v1/orders"
        assert paradex.payload["market"] == "ARB-USD-PERP"
        assert paradex.payload["side"] == "BUY"
        assert paradex.payload["size"] == "10845.90000000"
        assert paradex.payload["price"] == "0.09230000"

        assert extended.side == "sell"
        assert extended.reference_price == pytest.approx(0.0919)
        assert extended.worst_acceptable_price == pytest.approx(0.0918)
        assert extended.quantity == pytest.approx(10881.0)
        assert extended.effective_notional == pytest.approx(999.9639)
        assert extended.quantity_increment == pytest.approx(1.0)
        assert extended.price_increment == pytest.approx(0.0001)
        assert extended.time_in_force == "ioc"
        assert extended.http_method == "POST"
        assert extended.payload["symbol"] == "ARB-USD"
        assert extended.payload["side"] == "SELL"
        assert extended.payload["size"] == "10881"
        assert extended.payload["price"] == "0.0918"

    asyncio.run(run())


def test_order_preview_service_rejects_preview_below_venue_minimum_notional() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=9,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=FundingPairTradeIntent(
            label="tiny_paradex_leg",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.0008,
            break_even_days_entry=0.5,
            capacity_limit_notional=4500.0,
            target_notional=5.0,
            capacity_fraction=0.25,
            max_target_notional=5.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=5.0,
            ),
            short_leg=TradeLegIntent(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=5.0,
            ),
        ),
    )

    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.0002,
            0.0919,
            30_000,
            0.0921,
            25_000,
            raw={
                "tradingConfig": {
                    "minOrderSize": "10",
                    "minOrderSizeChange": "1",
                    "minPriceChange": "0.0001",
                    "maxLimitOrderValue": "1250000",
                }
            },
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0004,
            0.0918,
            20_000,
            0.0922,
            18_000,
            raw={
                "price_tick_size": "0.0001",
                "order_size_increment": "0.1",
                "min_notional": "10",
                "max_order_size": "12000000",
            },
        ),
    }

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        with pytest.raises(ValueError, match="minimum notional"):
            await OrderPreviewService(fetch_snapshot=fetch_snapshot).preview_paper_trade(
                paper_trade,
                slippage_tolerance_bps=10,
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            )

    asyncio.run(run())


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

        async def probe(self, config: dict[str, object]) -> VenueAccountPreflight:
            assert isinstance(config["enabled"], bool)
            return self.result

    service = AccountPreflightService(
        probes=cast(
            dict[str, VenueAccountProbe],
            {
                "extended": StubProbe(
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        account_identifier="ext-subaccount",
                        available_to_trade=1500.0,
                    )
                ),
                "paradex": StubProbe(
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
                ),
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
    auth_path = build_paradex_auth_request_path(private_key="0x456")

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path, dict(request.headers)))
        if request.url.path == "/v1/system/config":
            return httpx.Response(
                200,
                json={"starknet_chain_id": "PRIVATE_SN_PARACLEAR_MAINNET"},
            )
        if request.url.path == auth_path:
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
            ("POST", auth_path),
        ]

    asyncio.run(run())


def test_build_signed_paradex_order_payload_adds_signature_fields() -> None:
    payload = build_signed_paradex_order_payload(
        account_address="0x123",
        private_key="0x456",
        starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET",
        order_payload={
            "market": "ARB-USD-PERP",
            "side": "BUY",
            "type": "LIMIT",
            "size": "10845.9",
            "price": "0.0923",
            "instruction": "IOC",
            "client_id": "carryme-pt5-paradex-buy",
        },
        signature_timestamp_ms=1_700_000_000_000,
        recv_window_ms=45_000,
    )

    assert payload["market"] == "ARB-USD-PERP"
    assert payload["side"] == "BUY"
    assert payload["signature_timestamp"] == 1_700_000_000_000
    assert payload["recv_window"] == 45_000
    assert "signature" in payload
    assert "flags" not in payload


def test_build_signed_extended_order_payload_uses_settlement_schema() -> None:
    payload = build_signed_extended_order_payload(
        api_key="extended-key",
        stark_private_key="0x7a7ff6fd3cab02ccdcd4a572563f5976f8976899b03a39773795a3c486d4986",
        account_payload={
            "data": {
                "l2Key": "0x61c5e7e8339b7d56f197f54ea91b776776690e3232313de0f2ecbd0ef76f466",
                "l2Vault": "10002",
            }
        },
        market_payload={
            "data": {
                "name": "ARB-USD",
                "assetPrecision": 0,
                "tradingConfig": {
                    "minOrderSizeChange": "1",
                    "minPriceChange": "0.0001",
                },
                "l2Config": {
                    "collateralId": "0x1",
                    "syntheticId": "0x4152422d3100000000000000000000",
                    "syntheticResolution": 10,
                    "collateralResolution": 1000000,
                },
            }
        },
        order_payload={
            "symbol": "ARB-USD",
            "side": "SELL",
            "type": "LIMIT",
            "size": "10881.00000000",
            "price": "0.09180000",
            "time_in_force": "IOC",
            "client_order_id": "carryme-pt7-extended-sell",
            "reduce_only": False,
            "post_only": False,
        },
        taker_fee_rate="0.00025",
        nonce=1473459052,
        expire_time=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
    )

    assert payload["id"] == "carryme-pt7-extended-sell"
    assert payload["market"] == "ARB-USD"
    assert payload["side"] == "SELL"
    assert payload["type"] == "LIMIT"
    assert payload["qty"] == "10881"
    assert payload["price"] == "0.0918"
    assert payload["fee"] == "0.00025"
    assert payload["timeInForce"] == "IOC"
    assert payload["nonce"] == "1473459052"
    assert payload["settlement"]["starkKey"] == (
        "0x61c5e7e8339b7d56f197f54ea91b776776690e3232313de0f2ecbd0ef76f466"
    )
    assert payload["settlement"]["collateralPosition"] == "10002"
    assert payload["settlement"]["signature"]["r"].startswith("0x")
    assert payload["settlement"]["signature"]["s"].startswith("0x")
    assert payload["debuggingAmounts"]["syntheticAmount"].startswith("-")


def test_paradex_live_execution_service_submits_confirmed_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubTokenProvider:
        async def fetch_system_config(
            self,
            client: httpx.AsyncClient | None = None,
        ) -> object:
            from carryme_connectors import ParadexSystemConfig

            return ParadexSystemConfig(starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET")

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
            return "live-jwt"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/orders"
        assert request.headers["Authorization"] == "Bearer live-jwt"
        payload = json.loads(request.content.decode("utf-8"))
        assert payload["market"] == "ARB-USD-PERP"
        assert payload["instruction"] == "IOC"
        assert "signature" in payload
        assert "signature_timestamp" in payload
        return httpx.Response(
            200,
            json={
                "id": "order-1",
                "status": "NEW",
                "client_id": payload["client_id"],
            },
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    paper_trade = PaperTradeEntry(
        entry_id=7,
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
    confirmation = PreviewConfirmationEntry(
        entry_id=3,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=7,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=7,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                    effective_notional=999.99,
                    quantity=10845.9,
                    quantity_text="10845.90000000",
                    quantity_increment=0.1,
                    minimum_order_size=0.1,
                    minimum_notional=10.0,
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.09230000",
                    price_increment=0.0001,
                    max_order_value=1_000_000.0,
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=["CARRYME_API_PARADEX_ACCOUNT_ADDRESS"],
                    auth_scheme="main account address + subkey private key",
                    payload={
                        "market": "ARB-USD-PERP",
                        "side": "BUY",
                        "type": "LIMIT",
                        "size": "10845.90000000",
                        "price": "0.09230000",
                        "instruction": "IOC",
                        "client_id": "carryme-pt7-paradex-buy",
                    },
                    notes=[],
                )
            ],
        ),
        note="operator confirmed",
    )

    async def run() -> None:
        service = ParadexLiveExecutionService(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.mode == "live"
        assert entry.status == "submitted"
        assert entry.preview_hash == "preview-hash"
        assert entry.confirmation_entry_id == 3
        assert len(entry.legs) == 1
        assert entry.legs[0].status == "submitted"
        assert entry.legs[0].simulated is False
        assert entry.legs[0].external_reference == "order-1"
        assert entry.legs[0].request_payload is not None
        assert entry.legs[0].response_payload is not None

    asyncio.run(run())


def test_extended_live_execution_service_submits_confirmed_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_request: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "accountId": 258270,
                        "status": "ACTIVE",
                        "l2Key": (
                            "0x61c5e7e8339b7d56f197f54ea91b776776690e3232313de0f2ecbd0ef76f466"
                        ),
                        "l2Vault": "10002",
                    },
                },
            )
        if request.url.path == "/api/v1/user/fees":
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "makerFee": "0",
                        "takerFee": "0.00025",
                    },
                },
            )
        if request.url.path == "/api/v1/user/order":
            payload = json.loads(request.content.decode("utf-8"))
            seen_request.update(payload)
            assert request.headers["x-api-key"] == "extended-key"
            assert payload["market"] == "ARB-USD"
            assert payload["type"] == "LIMIT"
            assert payload["side"] == "SELL"
            assert payload["timeInForce"] == "IOC"
            assert "settlement" in payload
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "id": 321,
                        "externalId": payload["id"],
                    },
                },
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    paper_trade = PaperTradeEntry(
        entry_id=8,
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
    confirmation = PreviewConfirmationEntry(
        entry_id=4,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=8,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    effective_notional=999.9639,
                    quantity=10881.0,
                    quantity_text="10881",
                    quantity_increment=1.0,
                    minimum_order_size=10.0,
                    minimum_notional=0.918,
                    reference_price=0.0919,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0918,
                    worst_price_text="0.0918",
                    price_increment=0.0001,
                    max_order_value=1_250_000.0,
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[
                        "CARRYME_API_EXTENDED_API_KEY",
                        "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                    ],
                    auth_scheme="api key + Stark signing key",
                    payload={
                        "symbol": "ARB-USD",
                        "side": "SELL",
                        "type": "LIMIT",
                        "size": "10881",
                        "price": "0.0918",
                        "time_in_force": "IOC",
                        "client_order_id": "carryme-pt8-extended-sell",
                        "reduce_only": False,
                    },
                    notes=[],
                )
            ],
        ),
        note="operator confirmed",
    )

    async def fetch_snapshot(_: str, __: str) -> NormalizedMarketSnapshot:
        return _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.0919,
            42_584.8,
            0.0922,
            42_473.0,
            raw={
                "name": "ARB-USD",
                "assetName": "ARB",
                "assetPrecision": 0,
                "collateralAssetName": "USD",
                "collateralAssetPrecision": 6,
                "active": True,
                "tradingConfig": {
                    "minOrderSize": "10",
                    "minOrderSizeChange": "1",
                    "minPriceChange": "0.0001",
                    "maxLimitOrderValue": "1250000",
                },
                "l2Config": {
                    "type": "STARKX",
                    "collateralId": "0x1",
                    "syntheticId": "0x4152422d3100000000000000000000",
                    "syntheticResolution": 10,
                    "collateralResolution": 1000000,
                },
            },
        )

    async def run() -> None:
        service = ExtendedLiveExecutionService(
            api_key="extended-key",
            stark_private_key="0x7a7ff6fd3cab02ccdcd4a572563f5976f8976899b03a39773795a3c486d4986",
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.mode == "live"
        assert entry.status == "submitted"
        assert entry.preview_hash == "preview-hash"
        assert entry.confirmation_entry_id == 4
        assert len(entry.legs) == 1
        assert entry.legs[0].status == "submitted"
        assert entry.legs[0].simulated is False
        assert entry.legs[0].external_reference == "carryme-pt8-extended-sell"
        assert entry.legs[0].request_payload is not None
        assert entry.legs[0].response_payload is not None
        assert seen_request["id"] == "carryme-pt8-extended-sell"
        assert seen_request["fee"] == "0.00025"

    asyncio.run(run())


def test_paradex_live_execution_service_submits_confirmed_cleanup_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_request: dict[str, Any] = {}

    class StubTokenProvider:
        async def fetch_system_config(
            self,
            client: httpx.AsyncClient | None = None,
        ) -> Any:
            from carryme_connectors import ParadexSystemConfig

            return ParadexSystemConfig(starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET")

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
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer jwt-token"
        assert request.url.path == "/v1/orders"
        payload = json.loads(request.content.decode("utf-8"))
        seen_request.update(payload)
        assert payload["market"] == "ARB-USD-PERP"
        assert payload["side"] == "SELL"
        assert payload["flags"] == ["REDUCE_ONLY"]
        return httpx.Response(201, json={"id": "cleanup-order-1", "status": "NEW"})

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    paper_trade = PaperTradeEntry(
        entry_id=8,
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
    confirmation = CleanupPreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash",
        preview=ExecutionCleanupPreview(
            execution_entry_id=5,
            paper_trade_id=8,
            generated_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            preview_hash="cleanup-hash",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="sell",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=123.1,
                quantity_text="123.10000000",
                quantity_increment=0.1,
                minimum_order_size=0.1,
                minimum_notional=10.0,
                reference_price=0.0892,
                reference_price_source="best_bid",
                worst_acceptable_price=0.0890,
                worst_price_text="0.08900000",
                price_increment=0.0001,
                max_order_value=1_000_000.0,
                reduce_only=True,
                endpoint_path_hint="/v1/orders",
                required_auth_env_vars=[
                    "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                    "CARRYME_API_PARADEX_PRIVATE_KEY",
                ],
                auth_scheme="main account address + subkey private key",
                payload={
                    "market": "ARB-USD-PERP",
                    "side": "SELL",
                    "type": "LIMIT",
                    "size": "123.10000000",
                    "price": "0.08900000",
                    "instruction": "IOC",
                    "client_id": "carryme-cleanup-pt8-paradex-sell",
                    "reduce_only": True,
                },
                notes=[],
            ),
            notes=[],
        ),
    )

    async def run() -> None:
        service = ParadexLiveExecutionService(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
        )
        entry = await service.submit_confirmed_cleanup_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.adapter == "paradex_cleanup_live"
        assert entry.mode == "live"
        assert entry.status == "submitted"
        assert entry.preview_hash == "cleanup-hash"
        assert entry.confirmation_entry_id == 11
        assert entry.legs[0].status == "submitted"
        assert entry.legs[0].external_reference == "cleanup-order-1"
        assert seen_request["flags"] == ["REDUCE_ONLY"]
        assert seen_request["client_id"] == "carryme-cleanup-pt8-paradex-sell"

    asyncio.run(run())


def test_paradex_live_execution_service_labels_cleanup_network_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubTokenProvider:
        async def fetch_system_config(
            self,
            client: httpx.AsyncClient | None = None,
        ) -> Any:
            from carryme_connectors import ParadexSystemConfig

            return ParadexSystemConfig(starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET")

        async def issue_jwt_token(
            self,
            *,
            account_address: str,
            private_key: str,
            client: httpx.AsyncClient | None = None,
            now: int | None = None,
        ) -> str:
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    paper_trade = PaperTradeEntry(
        entry_id=8,
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
    confirmation = CleanupPreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash",
        preview=ExecutionCleanupPreview(
            execution_entry_id=5,
            paper_trade_id=8,
            generated_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            preview_hash="cleanup-hash",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="sell",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=123.1,
                quantity_text="123.10000000",
                quantity_increment=0.1,
                minimum_order_size=0.1,
                minimum_notional=10.0,
                reference_price=0.0892,
                reference_price_source="best_bid",
                worst_acceptable_price=0.0890,
                worst_price_text="0.08900000",
                price_increment=0.0001,
                max_order_value=1_000_000.0,
                reduce_only=True,
                endpoint_path_hint="/v1/orders",
                required_auth_env_vars=[
                    "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                    "CARRYME_API_PARADEX_PRIVATE_KEY",
                ],
                auth_scheme="main account address + subkey private key",
                payload={
                    "market": "ARB-USD-PERP",
                    "side": "SELL",
                    "type": "LIMIT",
                    "size": "123.10000000",
                    "price": "0.08900000",
                    "instruction": "IOC",
                    "client_id": "carryme-cleanup-pt8-paradex-sell",
                    "reduce_only": True,
                },
                notes=[],
            ),
            notes=[],
        ),
    )

    async def run() -> None:
        service = ParadexLiveExecutionService(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
        )
        entry = await service.submit_confirmed_cleanup_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.adapter == "paradex_cleanup_live"
        assert entry.status == "rejected"

    asyncio.run(run())


def test_extended_live_execution_service_submits_confirmed_cleanup_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_request: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "accountId": 258270,
                        "status": "ACTIVE",
                        "l2Key": (
                            "0x61c5e7e8339b7d56f197f54ea91b776776690e3232313de0f2ecbd0ef76f466"
                        ),
                        "l2Vault": "10002",
                    },
                },
            )
        if request.url.path == "/api/v1/user/fees":
            return httpx.Response(
                200,
                json={"status": "OK", "data": {"makerFee": "0", "takerFee": "0.00025"}},
            )
        if request.url.path == "/api/v1/user/order":
            payload = json.loads(request.content.decode("utf-8"))
            seen_request.update(payload)
            assert payload["side"] == "BUY"
            assert payload["reduceOnly"] is True
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {"id": 654, "externalId": payload["id"]},
                },
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    paper_trade = PaperTradeEntry(
        entry_id=8,
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
    confirmation = CleanupPreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash",
        preview=ExecutionCleanupPreview(
            execution_entry_id=5,
            paper_trade_id=8,
            generated_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            preview_hash="cleanup-hash",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=10.9962,
                effective_notional=10.9962,
                quantity=123.0,
                quantity_text="123",
                quantity_increment=1.0,
                minimum_order_size=10.0,
                minimum_notional=0.918,
                reference_price=0.0894,
                reference_price_source="best_ask",
                worst_acceptable_price=0.0895,
                worst_price_text="0.0895",
                price_increment=0.0001,
                max_order_value=1250000.0,
                reduce_only=True,
                endpoint_path_hint="/api/v1/user/order",
                required_auth_env_vars=[
                    "CARRYME_API_EXTENDED_API_KEY",
                    "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                ],
                auth_scheme="api key + Stark signing key",
                payload={
                    "symbol": "ARB-USD",
                    "side": "BUY",
                    "type": "LIMIT",
                    "size": "123",
                    "price": "0.0895",
                    "time_in_force": "IOC",
                    "client_order_id": "carryme-cleanup-pt8-buy",
                    "reduce_only": True,
                },
                notes=[],
            ),
            notes=[],
        ),
    )

    async def fetch_snapshot(_: str, __: str) -> NormalizedMarketSnapshot:
        return _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.0894,
            42_584.8,
            0.0895,
            42_473.0,
            raw={
                "name": "ARB-USD",
                "assetName": "ARB",
                "assetPrecision": 0,
                "collateralAssetName": "USD",
                "collateralAssetPrecision": 6,
                "active": True,
                "tradingConfig": {
                    "minOrderSize": "10",
                    "minOrderSizeChange": "1",
                    "minPriceChange": "0.0001",
                    "maxLimitOrderValue": "1250000",
                },
                "l2Config": {
                    "type": "STARKX",
                    "collateralId": "0x1",
                    "syntheticId": "0x4152422d3100000000000000000000",
                    "syntheticResolution": 10,
                    "collateralResolution": 1000000,
                },
            },
        )

    async def run() -> None:
        service = ExtendedLiveExecutionService(
            api_key="extended-key",
            stark_private_key="0x7a7ff6fd3cab02ccdcd4a572563f5976f8976899b03a39773795a3c486d4986",
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        entry = await service.submit_confirmed_cleanup_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.adapter == "extended_cleanup_live"
        assert entry.mode == "live"
        assert entry.status == "submitted"
        assert entry.preview_hash == "cleanup-hash"
        assert entry.confirmation_entry_id == 11
        assert entry.legs[0].status == "submitted"
        assert entry.legs[0].external_reference == "carryme-cleanup-pt8-buy"
        assert seen_request["id"] == "carryme-cleanup-pt8-buy"
        assert seen_request["reduceOnly"] is True

    asyncio.run(run())


def test_extended_live_execution_service_rejects_cleanup_for_wrong_venue() -> None:
    confirmation = CleanupPreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash",
        preview=ExecutionCleanupPreview(
            execution_entry_id=5,
            paper_trade_id=8,
            generated_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            preview_hash="cleanup-hash",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="sell",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=123.1,
                quantity_text="123.10000000",
                reference_price=0.0892,
                reference_price_source="best_bid",
                worst_acceptable_price=0.0891,
                worst_price_text="0.08910000",
                reduce_only=True,
                endpoint_path_hint="/v1/orders",
                required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                auth_scheme="subkey private key",
                payload={"market": "ARB-USD-PERP", "reduce_only": True},
                notes=[],
            ),
            notes=[],
        ),
    )

    service = ExtendedLiveExecutionService(api_key="extended-key", stark_private_key="0x123")

    with pytest.raises(ValueError, match="Cleanup confirmation must target Extended venue"):
        service._select_extended_cleanup_leg(confirmation)


def test_extended_live_execution_service_rejects_non_reduce_only_cleanup() -> None:
    confirmation = CleanupPreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash",
        preview=ExecutionCleanupPreview(
            execution_entry_id=5,
            paper_trade_id=8,
            generated_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            preview_hash="cleanup-hash",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=123.0,
                quantity_text="123",
                reference_price=0.0894,
                reference_price_source="best_ask",
                worst_acceptable_price=0.0895,
                worst_price_text="0.0895",
                reduce_only=False,
                endpoint_path_hint="/api/v1/user/order",
                required_auth_env_vars=["CARRYME_API_EXTENDED_API_KEY"],
                auth_scheme="api key + Stark signing key",
                payload={"symbol": "ARB-USD", "reduce_only": False},
                notes=[],
            ),
            notes=[],
        ),
    )

    service = ExtendedLiveExecutionService(api_key="extended-key", stark_private_key="0x123")

    with pytest.raises(
        ValueError,
        match="Cleanup confirmation must be reduce-only before live execution",
    ):
        service._select_extended_cleanup_leg(confirmation)


def test_paired_live_execution_coordinator_submits_both_legs() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=11,
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
    confirmation = PreviewConfirmationEntry(
        entry_id=9,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=11,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=11,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    quantity=10881.0,
                    quantity_text="10881",
                    reference_price=0.0919,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0918,
                    worst_price_text="0.0918",
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD"},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                    quantity=10845.9,
                    quantity_text="10845.90000000",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.09230000",
                    endpoint_path_hint="/v1/orders",
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP"},
                    notes=[],
                ),
            ],
        ),
        note="operator confirmed",
    )

    class StubService:
        def __init__(self, venue: str) -> None:
            self.venue = venue

        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=executed_at or datetime.now(UTC),
                adapter=f"{self.venue}_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue=self.venue,
                        symbol=next(
                            leg.symbol
                            for leg in confirmation.preview.legs
                            if leg.venue == self.venue
                        ),
                        fee_profile="default" if self.venue == "extended" else "pro",
                        side="sell" if self.venue == "extended" else "buy",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference=f"{self.venue}-1",
                    )
                ],
            )

    async def run() -> None:
        service = PairedLiveExecutionCoordinator(
            services={
                "extended": StubService("extended"),
                "paradex": StubService("paradex"),
            }
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            first_venue="extended",
        )
        assert entry.status == "submitted"
        assert entry.adapter == "paired_live:extended_then_paradex"
        assert [leg.venue for leg in entry.legs] == ["extended", "paradex"]

    asyncio.run(run())


def test_paired_live_execution_coordinator_marks_partial_when_second_leg_fails() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=12,
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
    confirmation = PreviewConfirmationEntry(
        entry_id=10,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=12,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=12,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    quantity=10881.0,
                    quantity_text="10881",
                    reference_price=0.0919,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0918,
                    worst_price_text="0.0918",
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD"},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                    quantity=10845.9,
                    quantity_text="10845.90000000",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.09230000",
                    endpoint_path_hint="/v1/orders",
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP"},
                    notes=[],
                ),
            ],
        ),
        note="operator confirmed",
    )

    class SuccessService:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=executed_at or datetime.now(UTC),
                adapter="extended_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    )
                ],
            )

    class FailureService:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            raise httpx.HTTPError("paradex reject")

    async def run() -> None:
        service = PairedLiveExecutionCoordinator(
            services={
                "extended": SuccessService(),
                "paradex": FailureService(),
            }
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            first_venue="extended",
        )
        assert entry.status == "partial"
        assert [leg.status for leg in entry.legs] == ["submitted", "rejected"]
        assert entry.legs[1].response_payload == {"error": "paradex reject", "venue": "paradex"}

    asyncio.run(run())


def test_extended_account_probe_treats_missing_balance_rows_as_empty_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "subAccountId": "extended-subaccount",
                        "status": "OK",
                        "equity": "0",
                        "availableForTrade": "0",
                    }
                },
            )
        if request.url.path == "/api/v1/user/balance":
            return httpx.Response(404, json={"error": "NOT_FOUND"})
        if request.url.path == "/api/v1/user/positions":
            return httpx.Response(200, json={"data": []})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        probe = ExtendedAccountProbe()
        status = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "api_key": "extended-key",
                },
            }
        )
        assert status.authenticated is True
        assert status.ready is True
        assert status.balance_count == 0
        assert status.position_count == 0

    asyncio.run(run())


def test_extended_account_probe_uses_balance_payload_for_collateral(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "subAccountId": "extended-subaccount",
                        "status": "ACTIVE",
                    }
                },
            )
        if request.url.path == "/api/v1/user/balance":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "equity": "4.989999",
                        "availableForTrade": "4.989999",
                    }
                },
            )
        if request.url.path == "/api/v1/user/positions":
            return httpx.Response(200, json={"data": []})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        probe = ExtendedAccountProbe()
        status = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "api_key": "extended-key",
                },
            }
        )
        assert status.authenticated is True
        assert status.total_collateral == pytest.approx(4.989999)
        assert status.available_to_trade == pytest.approx(4.989999)

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


def test_account_preflight_extracts_balance_assets_and_position_symbols() -> None:
    balances = {
        "results": [
            {"token": "USDC", "size": "15.0"},
            {"asset": "ETH", "size": "0.1"},
            {"currency": "USDC", "size": "3.0"},
        ]
    }
    positions = {
        "positions": [
            {"market": "ARB-USD-PERP", "size": "100"},
            {"symbol": "STRK-USD", "size": "25"},
            {"ticker": "ARB-USD-PERP", "size": "5"},
        ]
    }

    assert _extract_balance_assets(balances) == ["USDC", "ETH"]
    assert _extract_position_symbols(positions) == ["ARB-USD-PERP", "STRK-USD"]


def test_reconcile_execution_marks_partial_and_missing_leg_symbols() -> None:
    entry = ExecutionJournalEntry(
        entry_id=21,
        executed_at=datetime(2026, 3, 29, 16, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="partial",
        paper_trade_id=11,
        preview_hash="preview-hash",
        confirmation_entry_id=7,
        paper_trade=PaperTradeEntry(
            entry_id=11,
            created_at=datetime(2026, 3, 29, 15, 55, tzinfo=UTC),
            note="operator approved",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 15, 50, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.4,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.01,
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
        ),
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
            ),
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
                status="rejected",
                simulated=False,
            ),
        ],
    )
    account_preflight = PaperTradeAccountPreflight(
        paper_trade_id=11,
        label="arb_extended_paradex",
        ready=True,
        venues=[
            VenueAccountPreflight(
                venue="extended",
                enabled=True,
                authenticated=True,
                ready=True,
                credential_mode="api_key",
                position_symbols=["ARB-USD"],
            ),
            VenueAccountPreflight(
                venue="paradex",
                enabled=True,
                authenticated=True,
                ready=True,
                credential_mode="subkey_jwt",
                balance_assets=["USDC"],
                position_symbols=[],
            ),
        ],
    )

    reconciliation = reconcile_execution(entry, account_preflight)

    assert reconciliation.execution_entry_id == 21
    assert reconciliation.status == "partial"
    assert reconciliation.recommended_action == "complete_or_unwind_missing_leg"
    assert reconciliation.matched_all_leg_symbols is False
    extended = next(item for item in reconciliation.venues if item.venue == "extended")
    paradex = next(item for item in reconciliation.venues if item.venue == "paradex")
    assert extended.matched_leg_symbols == ["ARB-USD"]
    assert extended.unmatched_leg_symbols == []
    assert paradex.matched_leg_symbols == []
    assert paradex.unmatched_leg_symbols == ["ARB-USD-PERP"]


def test_paradex_order_state_observer_marks_closed_ioc_as_unfilled(
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
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/orders/order-1"
        assert request.headers["Authorization"] == "Bearer jwt-token"
        return httpx.Response(
            200,
            json={
                "id": "order-1",
                "client_id": "carryme-pt1-paradex-buy",
                "market": "ARB-USD-PERP",
                "status": "CLOSED",
                "cancel_reason": "REMAINING_IOC_CANCEL",
                "avg_fill_price": "",
                "remaining_size": "122.7",
                "size": "122.7",
            },
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        observer = ParadexOrderStateObserver(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
        )
        state = await observer.observe(
            {
                "venue": "paradex",
                "external_reference": "order-1",
                "request_payload": {"client_id": "carryme-pt1-paradex-buy"},
            }
        )
        assert state.supported is True
        assert state.derived_state == "unfilled"
        assert state.order_status == "CLOSED"
        assert state.cancel_reason == "REMAINING_IOC_CANCEL"

    asyncio.run(run())


def test_paradex_order_state_observer_falls_back_to_history_for_closed_ioc(
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
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer jwt-token"
        if request.url.path == "/v1/orders/order-1":
            return httpx.Response(
                400,
                json={"error": "ORDER_ID_NOT_FOUND", "message": "could not find order id"},
            )
        assert request.url.path == "/v1/orders-history"
        assert request.url.params["market"] == "ARB-USD-PERP"
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "order-1",
                        "client_id": "carryme-pt1-paradex-buy",
                        "market": "ARB-USD-PERP",
                        "status": "CLOSED",
                        "cancel_reason": "REMAINING_IOC_CANCEL",
                        "avg_fill_price": "",
                        "remaining_size": "122.7",
                        "size": "122.7",
                    }
                ]
            },
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        observer = ParadexOrderStateObserver(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
        )
        state = await observer.observe(
            {
                "venue": "paradex",
                "external_reference": "order-1",
                "request_payload": {
                    "client_id": "carryme-pt1-paradex-buy",
                    "market": "ARB-USD-PERP",
                },
            }
        )
        assert state.supported is True
        assert state.derived_state == "unfilled"
        assert state.notes == [
            "Paradex direct order lookup missed the order; fell back to orders-history."
        ]

    asyncio.run(run())


def test_build_execution_pair_status_marks_open_unhedged_pair_for_cleanup() -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
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
        ),
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-order",
            ),
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-order",
                request_payload={
                    "client_id": "carryme-pt7-paradex-buy",
                    "market": "ARB-USD-PERP",
                },
            ),
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=12,
        paper_trade_id=7,
        preview_hash="preview-hash",
        legs=[
            ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference="ext-order",
                derived_state="unknown",
            ),
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="pdx-order",
                derived_state="unfilled",
                order_status="CLOSED",
                cancel_reason="REMAINING_IOC_CANCEL",
            ),
        ],
    )
    reconciliation = ExecutionReconciliation(
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
                position_symbols=["ARB-USD"],
                matched_leg_symbols=["ARB-USD"],
                unmatched_leg_symbols=[],
            ),
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD-PERP"],
            ),
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "cleanup_needed"
    assert status.recommended_action == "close_open_leg"
    assert any("unfilled" in note.lower() for note in status.notes)


def test_extended_cleanup_preview_service_builds_reduce_only_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
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
        ),
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-order",
            )
        ],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=12,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="cleanup_needed",
        recommended_action="close_open_leg",
        order_state=ExecutionOrderState(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[
                ExecutionLegOrderState(
                    venue="extended",
                    supported=True,
                    external_reference="ext-order",
                    derived_state="unknown",
                )
            ],
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
                    position_symbols=["ARB-USD"],
                    matched_leg_symbols=["ARB-USD"],
                    unmatched_leg_symbols=[],
                )
            ],
            notes=[],
        ),
        notes=[],
    )

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "extended"
        assert symbol == "ARB-USD"
        return _snapshot(
            "extended",
            "ARB-USD",
            0.0002,
            0.0894,
            1_000,
            0.0895,
            1_000,
            raw={
                "tradingConfig": {
                    "minOrderSize": "1",
                    "minOrderSizeChange": "1",
                    "minPriceChange": "0.0001",
                    "maxLimitOrderValue": "1000",
                }
            },
        )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/user/positions"
        assert request.headers["x-api-key"] == "api-key"
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "market": "ARB-USD",
                        "side": "SHORT",
                        "size": "123",
                    }
                ]
            },
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        service = ExtendedCleanupPreviewService(
            api_key="api-key",
            fetch_snapshot=fetch_snapshot,
        )
        preview = await service.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.reason == "close_open_leg"
        assert preview.leg.symbol == "ARB-USD"
        assert preview.leg.side == "buy"
        assert preview.leg.reduce_only is True
        assert preview.leg.quantity_text == "123"
        assert preview.leg.worst_price_text == "0.0896"

    asyncio.run(run())


def test_paradex_cleanup_preview_service_builds_reduce_only_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
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
        ),
        legs=[
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-order",
            )
        ],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=12,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="cleanup_needed",
        recommended_action="close_open_leg",
        order_state=ExecutionOrderState(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[
                ExecutionLegOrderState(
                    venue="paradex",
                    supported=True,
                    external_reference="pdx-order",
                    derived_state="unknown",
                )
            ],
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
                    venue="paradex",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB-USD-PERP"],
                    matched_leg_symbols=["ARB-USD-PERP"],
                    unmatched_leg_symbols=[],
                )
            ],
            notes=[],
        ),
        notes=[],
    )

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "paradex"
        assert symbol == "ARB-USD-PERP"
        return _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0002,
            0.0892,
            1_000,
            0.0893,
            1_000,
            raw={
                "order_size_increment": "0.1",
                "min_notional": "10",
                "price_tick_size": "0.0001",
                "max_order_size": "12000000",
            },
        )

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
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/positions"
        assert request.headers["Authorization"] == "Bearer jwt-token"
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "market": "ARB-USD-PERP",
                        "size": "123.1",
                    }
                ]
            },
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    async def run() -> None:
        service = ParadexCleanupPreviewService(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
            fetch_snapshot=fetch_snapshot,
        )
        preview = await service.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.reason == "close_open_leg"
        assert preview.leg.symbol == "ARB-USD-PERP"
        assert preview.leg.side == "sell"
        assert preview.leg.reduce_only is True
        assert preview.leg.quantity_text == "123.10000000"
        assert preview.leg.worst_price_text == "0.08910000"

    asyncio.run(run())


def test_paradex_cleanup_preview_service_logs_schema_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
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
        ),
        legs=[
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-order",
            )
        ],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=12,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="cleanup_needed",
        recommended_action="close_open_leg",
        order_state=ExecutionOrderState(
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[
                ExecutionLegOrderState(
                    venue="paradex",
                    supported=True,
                    external_reference="pdx-order",
                    derived_state="unknown",
                )
            ],
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
                    venue="paradex",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB-USD-PERP"],
                    matched_leg_symbols=["ARB-USD-PERP"],
                    unmatched_leg_symbols=[],
                )
            ],
            notes=[],
        ),
        notes=[],
    )

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return _snapshot(
            venue,
            symbol,
            -0.0002,
            0.0892,
            1_000,
            0.0893,
            1_000,
            raw={
                "order_size_increment": "0.1",
                "min_notional": "10",
                "price_tick_size": "0.0001",
                "max_order_size": "12000000",
            },
        )

    class StubTokenProvider:
        async def issue_jwt_token(
            self,
            *,
            account_address: str,
            private_key: str,
            client: httpx.AsyncClient | None = None,
            now: int | None = None,
        ) -> str:
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"rows": [{"symbol": "ARB-USD-PERP", "size": "123.1"}]},
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    caplog.set_level("WARNING")

    async def run() -> None:
        service = ParadexCleanupPreviewService(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
            fetch_snapshot=fetch_snapshot,
        )
        preview = await service.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.leg.symbol == "ARB-USD-PERP"

    asyncio.run(run())

    assert "fallback container key" in caplog.text
    assert "fallback symbol key" in caplog.text


def test_cleanup_preview_router_dispatches_to_paradex_when_paradex_leg_is_open() -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
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
        ),
        legs=[
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-order",
            )
        ],
    )
    pair_status = ExecutionPairStatus(
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
                    venue="paradex",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB-USD-PERP"],
                    matched_leg_symbols=["ARB-USD-PERP"],
                    unmatched_leg_symbols=[],
                )
            ],
            notes=[],
        ),
        notes=[],
    )

    class StubParadexService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason=pair_status.recommended_action,
                leg=VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.1,
                    quantity_text="123.10000000",
                    reference_price=0.0892,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0891,
                    worst_price_text="0.08910000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            )

    async def run() -> None:
        router = CleanupPreviewRouter(services={"paradex": StubParadexService()})
        preview = await router.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.leg.venue == "paradex"
        assert preview.leg.side == "sell"

    asyncio.run(run())


def test_extended_live_execution_service_rejects_error_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "accountId": 258270,
                        "status": "ACTIVE",
                        "l2Key": (
                            "0x61c5e7e8339b7d56f197f54ea91b776776690e3232313de0f2ecbd0ef76f466"
                        ),
                        "l2Vault": "10002",
                    },
                },
            )
        if request.url.path == "/api/v1/user/fees":
            return httpx.Response(200, json={"status": "OK", "data": {"takerFee": "0.00025"}})
        if request.url.path == "/api/v1/user/order":
            return httpx.Response(
                200,
                json={
                    "status": "error",
                    "error": "ORDER_REJECTED",
                },
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    paper_trade = PaperTradeEntry(
        entry_id=18,
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
    confirmation = PreviewConfirmationEntry(
        entry_id=5,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=18,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=18,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    effective_notional=999.9639,
                    quantity=10881.0,
                    quantity_text="10881.00000000",
                    quantity_increment=1.0,
                    minimum_order_size=10.0,
                    minimum_notional=0.918,
                    reference_price=0.0919,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0918,
                    worst_price_text="0.09180000",
                    price_increment=0.0001,
                    max_order_value=1_250_000.0,
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[
                        "CARRYME_API_EXTENDED_API_KEY",
                        "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                    ],
                    auth_scheme="api key + Stark signing key",
                    payload={
                        "symbol": "ARB-USD",
                        "side": "SELL",
                        "type": "LIMIT",
                        "size": "10881.00000000",
                        "price": "0.09180000",
                        "time_in_force": "IOC",
                        "client_order_id": "carryme-pt18-extended-sell",
                        "reduce_only": False,
                    },
                    notes=[],
                )
            ],
        ),
        note="operator confirmed",
    )

    async def fetch_snapshot(_: str, __: str) -> NormalizedMarketSnapshot:
        return _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.0919,
            42_584.8,
            0.0922,
            42_473.0,
            raw={
                "name": "ARB-USD",
                "l2Config": {
                    "collateralId": "0x1",
                    "syntheticId": "0x4152422d3100000000000000000000",
                    "syntheticResolution": 10,
                    "collateralResolution": 1000000,
                },
            },
        )

    async def run() -> None:
        service = ExtendedLiveExecutionService(
            api_key="extended-key",
            stark_private_key="0x7a7ff6fd3cab02ccdcd4a572563f5976f8976899b03a39773795a3c486d4986",
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "rejected"
        assert entry.legs[0].status == "rejected"
        assert entry.legs[0].response_payload is not None
        assert entry.legs[0].response_payload["error"] == "ORDER_REJECTED"

    asyncio.run(run())


def test_extended_live_execution_service_preserves_zero_fee_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_request: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/user/account/info":
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "accountId": 258270,
                        "status": "ACTIVE",
                        "l2Key": (
                            "0x61c5e7e8339b7d56f197f54ea91b776776690e3232313de0f2ecbd0ef76f466"
                        ),
                        "l2Vault": "10002",
                    },
                },
            )
        if request.url.path == "/api/v1/user/fees":
            return httpx.Response(200, json={"status": "OK", "data": {"takerFee": "0"}})
        if request.url.path == "/api/v1/user/order":
            payload = json.loads(request.content.decode("utf-8"))
            seen_request.update(payload)
            return httpx.Response(
                200,
                json={"status": "OK", "data": {"externalId": payload["id"]}},
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    confirmation = PreviewConfirmationEntry(
        entry_id=6,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=19,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=19,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    effective_notional=999.9639,
                    quantity=10881.0,
                    quantity_text="10881.00000000",
                    quantity_increment=1.0,
                    minimum_order_size=10.0,
                    minimum_notional=0.918,
                    reference_price=0.0919,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0918,
                    worst_price_text="0.09180000",
                    price_increment=0.0001,
                    max_order_value=1_250_000.0,
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[
                        "CARRYME_API_EXTENDED_API_KEY",
                        "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                    ],
                    auth_scheme="api key + Stark signing key",
                    payload={
                        "symbol": "ARB-USD",
                        "side": "SELL",
                        "type": "LIMIT",
                        "size": "10881.00000000",
                        "price": "0.09180000",
                        "time_in_force": "IOC",
                        "client_order_id": "carryme-pt19-extended-sell",
                        "reduce_only": False,
                    },
                    notes=[],
                )
            ],
        ),
        note="operator confirmed",
    )

    async def fetch_snapshot(_: str, __: str) -> NormalizedMarketSnapshot:
        return _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.0919,
            42_584.8,
            0.0922,
            42_473.0,
            raw={
                "name": "ARB-USD",
                "l2Config": {
                    "collateralId": "0x1",
                    "syntheticId": "0x4152422d3100000000000000000000",
                    "syntheticResolution": 10,
                    "collateralResolution": 1000000,
                },
            },
        )

    async def run() -> None:
        service = ExtendedLiveExecutionService(
            api_key="extended-key",
            stark_private_key="0x7a7ff6fd3cab02ccdcd4a572563f5976f8976899b03a39773795a3c486d4986",
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        await service.submit_confirmed_preview(
            paper_trade=PaperTradeEntry(
                entry_id=19,
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
            ),
            confirmation=confirmation,
        )
        assert seen_request["fee"] == "0"

    asyncio.run(run())


def test_extended_live_execution_service_rejects_blank_credentials() -> None:
    with pytest.raises(ValueError, match="Extended API key is required"):
        ExtendedLiveExecutionService(api_key="   ", stark_private_key="0x123")
    with pytest.raises(ValueError, match="Extended Stark private key is required"):
        ExtendedLiveExecutionService(api_key="extended-key", stark_private_key="   ")


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


def test_paradex_account_probe_blocks_mismatched_account_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_async_client = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/account":
            return httpx.Response(200, json={"account": "0xdef", "status": "ACTIVE"})
        if request.url.path == "/v1/balance":
            return httpx.Response(200, json=[])
        if request.url.path == "/v1/positions":
            return httpx.Response(200, json=[])
        raise AssertionError(f"unexpected path: {request.url.path}")

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(
            *args,
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    monkeypatch.setattr(account_preflight_runtime.httpx, "AsyncClient", client_factory)

    async def run() -> None:
        status = await ParadexAccountProbe().probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xabc",
                    "bearer_token": "paradex-bearer",
                },
            }
        )

        assert status.authenticated is False
        assert status.ready is False
        assert status.blocking_reasons == [
            (
                "Paradex authenticated read returned malformed payload: "
                "Paradex account payload did not match the configured account address"
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


@pytest.mark.parametrize(
    ("paper_trade_id", "preview_hash"),
    [
        (6, "cleanup-hash"),
        (5, "wrong-hash"),
    ],
)
def test_require_confirmed_cleanup_preview_rejects_mismatches(
    paper_trade_id: int,
    preview_hash: str,
) -> None:
    confirmation = CleanupPreviewConfirmationEntry(
        entry_id=3,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=5,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash",
        preview=ExecutionCleanupPreview(
            execution_entry_id=12,
            paper_trade_id=5,
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            preview_hash="cleanup-hash",
            reason="close_open_leg",
            leg=VenueOrderPreview(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=123.0,
                quantity_text="123",
                reference_price=0.0894,
                reference_price_source="best_ask",
                worst_acceptable_price=0.0895,
                worst_price_text="0.0895",
                reduce_only=True,
                endpoint_path_hint="/api/v1/user/order",
                required_auth_env_vars=["CARRYME_API_EXTENDED_API_KEY"],
                auth_scheme="api key + Stark signing key",
                payload={"symbol": "ARB-USD", "reduce_only": True},
                notes=[],
            ),
            notes=[],
        ),
        note="operator confirmed",
    )

    with pytest.raises(ValueError, match="No cleanup preview confirmation matched"):
        require_confirmed_cleanup_preview(
            paper_trade_id=paper_trade_id,
            preview_hash=preview_hash,
            confirmations=[confirmation],
        )
