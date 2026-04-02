import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

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
    ExecutionAccountingSummary,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegAccounting,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionObservationEntry,
    ExecutionOrderState,
    ExecutionPairClosePreview,
    ExecutionPairStatus,
    ExecutionQualitySummary,
    ExecutionReconciliation,
    ExecutionVenueReconciliation,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    FundingUniverseScan,
    FundingUniverseVenueMarket,
    LiveSubmissionReadiness,
    MarketStats,
    NormalizedMarketSnapshot,
    OpportunityRecord,
    PairClosePreviewConfirmationEntry,
    PaperTradeAccountingSummary,
    PaperTradeAccountPreflight,
    PaperTradeBalanceDelta,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    PaperTradeOrderPreview,
    PaperTradeSystemState,
    PreviewConfirmationEntry,
    RouteAccountingSummary,
    RouteApprovalEntry,
    RouteStabilitySummary,
    TopOfBook,
    TradeLegIntent,
    VenueAccountPreflight,
    VenueBalanceSnapshot,
    VenueExecutionPreflight,
    VenueOrderPreview,
    VenueSystemState,
)
from carryme_normalizers import normalize_market_snapshot
from carryme_runtime import (
    AccountPreflightService,
    BalanceAccountingService,
    CleanupLiveExecutionRouter,
    CleanupPreviewRouter,
    ExecutionAccountingService,
    ExtendedCleanupPreviewService,
    ExtendedLiveExecutionService,
    HyperliquidCleanupPreviewService,
    HyperliquidLiveExecutionService,
    HyperliquidOrderStateObserver,
    MockExecutionAdapter,
    OpportunityService,
    OpportunityUniverseService,
    OrderPreviewService,
    PairCloseLiveExecutionCoordinator,
    PairClosePreviewService,
    PairedLiveExecutionCoordinator,
    ParadexCleanupPreviewService,
    ParadexLiveExecutionService,
    ParadexOrderStateObserver,
    RouteApprovalService,
    RouteStabilityService,
    VenueAccountProbe,
    build_execution_pair_status,
    build_live_submission_readiness,
    build_paper_trade_execution_preflight,
    build_portfolio_plan,
    build_trade_intent,
    build_venue_execution_preflights,
    reconcile_execution,
    require_confirmed_cleanup_preview,
    require_confirmed_preview,
)
from carryme_runtime.account_preflight import (
    ExtendedAccountProbe,
    HyperliquidAccountProbe,
    ParadexAccountProbe,
    _extract_balance_assets,
    _extract_position_symbols,
    _row_represents_open_position,
)
from carryme_runtime.execution_quality import ExecutionQualityService
from carryme_runtime.system_state import ParadexSystemStateProbe, SystemStateService
from carryme_runtime.universe_policy import passes_symbol_policy
from carryme_storage import (
    BalanceSnapshotStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    OpportunityHistoryStore,
    RouteApprovalStore,
)
from pydantic import ValidationError


def _snapshot(
    venue: str,
    symbol: str,
    funding_rate: float,
    bid_price: float,
    bid_size: float,
    ask_price: float,
    ask_size: float,
    *,
    open_interest: float = 1_000_000,
    daily_volume: float = 500_000,
    raw: dict[str, object] | None = None,
    top_of_book: TopOfBook | None = None,
) -> NormalizedMarketSnapshot:
    return normalize_market_snapshot(
        venue,
        MarketStats(
            venue=venue,
            symbol=symbol,
            mark_price=(bid_price + ask_price) / 2,
            funding_rate=funding_rate,
            open_interest=open_interest,
            daily_volume=daily_volume,
            top_of_book=top_of_book
            or TopOfBook(
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


def test_opportunity_universe_service_ranks_by_deployable_round_trip_pnl() -> None:
    symbol_lists = {
        "extended": ["MON-USD", "STRK-USD"],
        "paradex": ["MON-USD-PERP", "STRK-USD-PERP"],
        "hyperliquid": ["STRK"],
    }
    snapshots = {
        ("extended", "MON-USD"): _snapshot(
            "extended",
            "MON-USD",
            0.000013,
            0.10,
            10_000,
            0.101,
            1_000,
            daily_volume=1_000,
            open_interest=5_000,
        ),
        ("paradex", "MON-USD-PERP"): _snapshot(
            "paradex",
            "MON-USD-PERP",
            -0.0010,
            0.10,
            1_000,
            0.101,
            500,
            daily_volume=1_500,
            open_interest=6_000,
        ),
        ("extended", "STRK-USD"): _snapshot(
            "extended",
            "STRK-USD",
            0.000013,
            0.033,
            150_000,
            0.0331,
            120_000,
            daily_volume=150_000,
            open_interest=1_000_000,
        ),
        ("paradex", "STRK-USD-PERP"): _snapshot(
            "paradex",
            "STRK-USD-PERP",
            -0.0003,
            0.033,
            250_000,
            0.0331,
            200_000,
            daily_volume=120_000,
            open_interest=800_000,
        ),
        ("hyperliquid", "STRK"): _snapshot(
            "hyperliquid",
            "STRK",
            -0.00002,
            0.033,
            80_000,
            0.0331,
            70_000,
            daily_volume=300_000,
            open_interest=2_000_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=cast(Any, list_symbols),
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        scan = await service.scan(
            venues=["extended", "paradex", "hyperliquid"],
            ranking="roundtrip_pnl",
            target_notional=5_000,
            min_capacity_notional=100,
            limit=10,
        )

        ranked_symbols = [item.opportunity.canonical_symbol for item in scan.opportunities]
        ranked_pairs = [
            (item.opportunity.long_venue, item.opportunity.short_venue)
            for item in scan.opportunities
        ]
        assert scan.overlap_count == 2
        assert len(scan.opportunities) >= 1
        assert ranked_symbols[0] == "STRK-USD-PERP"
        assert ranked_pairs[0] == ("paradex", "extended")
        assert (
            sum(
                1
                for item in scan.opportunities
                if item.opportunity.canonical_symbol == "STRK-USD-PERP"
            )
                == 1
        )
        assert scan.opportunities[0].estimated_one_day_pnl_after_round_trip is not None
        assert (scan.opportunities[0].deployable_notional or 0.0) > 1_000

    asyncio.run(run())


def test_opportunity_universe_service_filters_thin_markets_for_quality_scan() -> None:
    symbol_lists = {
        "extended": ["MON-USD", "ZEN-USD", "LIT-USD"],
        "paradex": ["MON-USD-PERP", "ZEN-USD-PERP", "LIT-USD-PERP"],
    }
    snapshots = {
        ("extended", "MON-USD"): _snapshot(
            "extended",
            "MON-USD",
            0.000013,
            0.02,
            10_000,
            0.0201,
            3_000,
            daily_volume=600,
            open_interest=25_000,
        ),
        ("paradex", "MON-USD-PERP"): _snapshot(
            "paradex",
            "MON-USD-PERP",
            -0.0011,
            0.02,
            5_000,
            0.0201,
            2_500,
            daily_volume=700,
            open_interest=26_000,
        ),
        ("extended", "ZEN-USD"): _snapshot(
            "extended",
            "ZEN-USD",
            0.000013,
            0.12,
            10_000,
            0.121,
            3_000,
            daily_volume=20_000,
            open_interest=4_000,
        ),
        ("paradex", "ZEN-USD-PERP"): _snapshot(
            "paradex",
            "ZEN-USD-PERP",
            -0.0011,
            0.12,
            5_000,
            0.121,
            2_500,
            daily_volume=25_000,
            open_interest=4_500,
        ),
        ("extended", "LIT-USD"): _snapshot(
            "extended",
            "LIT-USD",
            0.000013,
            0.83,
            2_500,
            0.831,
            1_500,
            daily_volume=120_000,
            open_interest=150_000,
        ),
        ("paradex", "LIT-USD-PERP"): _snapshot(
            "paradex",
            "LIT-USD-PERP",
            -0.0006,
            0.83,
            3_000,
            0.831,
            2_000,
            daily_volume=110_000,
            open_interest=140_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=cast(Any, list_symbols),
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="quality_adjusted_roundtrip_pnl",
            target_notional=5_000,
            min_capacity_notional=250,
            min_daily_volume=10_000,
            min_open_interest=10_000,
            min_roundtrip_edge=0.0,
            limit=10,
        )

        assert len(scan.opportunities) == 1
        assert scan.opportunities[0].opportunity.canonical_symbol == "LIT-USD-PERP"

    asyncio.run(run())


def test_opportunity_universe_service_applies_fee_profile_overrides() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.09,
            20_000,
            0.0901,
            18_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0006,
            0.09,
            18_000,
            0.0901,
            17_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            fee_profile_overrides={"paradex": "retail"},
            target_notional=5_000,
            limit=10,
        )

        assert scan.fee_profiles == {"extended": "default", "paradex": "retail"}
        assert len(scan.opportunities) == 1
        opportunity = scan.opportunities[0].opportunity
        assert opportunity.long_fee_profile == "retail"
        assert opportunity.short_fee_profile == "default"

    asyncio.run(run())


def test_funding_universe_scan_rejects_unknown_fee_profile_venues() -> None:
    with pytest.raises(
        ValidationError,
        match="fee_profiles contains venues not present in scan venues: hyperliquid",
    ):
        FundingUniverseScan(
            venues=["extended", "paradex"],
            fee_profiles={"hyperliquid": "tier0"},
            ranking="route_adjusted_quality_pnl",
            target_notional=5_000,
            overlap_count=0,
            overlaps=[],
            opportunities=[],
        )


def test_opportunity_universe_service_models_paradex_fastfills_from_visible_book_share() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.09,
            20_000,
            0.0901,
            18_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0006,
            0.09,
            18_000,
            0.0901,
            1_000,
            top_of_book=TopOfBook(
                best_bid_price=0.09,
                best_bid_size=18_000,
                best_ask_price=0.0901,
                best_ask_size=1_000,
                best_ask_api_price=0.0901,
                best_ask_api_size=900,
            ),
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            fee_profile_overrides={"paradex": "pro_fastfills"},
            target_notional=50.0,
            limit=10,
        )

        assert len(scan.opportunities) == 1
        candidate = scan.opportunities[0]
        assert candidate.opportunity.long_fee_profile == "pro_fastfills"
        assert candidate.modeled_entry_cost_rate is not None
        assert candidate.modeled_entry_cost_rate > candidate.opportunity.entry_cost_rate
        assert candidate.paradex_fastfill_share == pytest.approx(0.1802, rel=1e-3)
        assert candidate.paradex_fastfill_eligible_notional == pytest.approx(9.01)
        assert candidate.estimated_one_day_pnl_after_round_trip is not None
        naive_round_trip = 50.0 * candidate.opportunity.one_day_net_edge_after_round_trip
        assert candidate.estimated_one_day_pnl_after_round_trip < naive_round_trip

    asyncio.run(run())


def test_funding_universe_scan_rejects_empty_fee_profile_names() -> None:
    with pytest.raises(
        ValidationError,
        match="fee_profiles contains empty profile names for venues: paradex",
    ):
        FundingUniverseScan(
            venues=["extended", "paradex"],
            fee_profiles={"paradex": "   "},
            ranking="route_adjusted_quality_pnl",
            target_notional=5_000,
            overlap_count=0,
            overlaps=[],
            opportunities=[],
        )


def test_opportunity_universe_service_filters_fastfill_routes_with_modeled_edge() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.09,
            20_000,
            0.0901,
            18_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0006,
            0.09,
            18_000,
            0.0901,
            1_000,
            top_of_book=TopOfBook(
                best_bid_price=0.09,
                best_bid_size=18_000,
                best_ask_price=0.0901,
                best_ask_size=1_000,
                best_ask_api_price=0.0901,
                best_ask_api_size=900,
            ),
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
        )
        baseline_scan = await service.scan(
            venues=["extended", "paradex"],
            fee_profile_overrides={"paradex": "pro_fastfills"},
            target_notional=50.0,
            limit=10,
        )

        assert len(baseline_scan.opportunities) == 1
        candidate = baseline_scan.opportunities[0]
        assert candidate.modeled_round_trip_cost_rate is not None
        modeled_roundtrip_edge = (
            candidate.opportunity.gross_daily_edge - candidate.modeled_round_trip_cost_rate
        )
        static_roundtrip_edge = candidate.opportunity.one_day_net_edge_after_round_trip
        assert modeled_roundtrip_edge < static_roundtrip_edge

        filtered_scan = await service.scan(
            venues=["extended", "paradex"],
            fee_profile_overrides={"paradex": "pro_fastfills"},
            target_notional=50.0,
            min_roundtrip_edge=(modeled_roundtrip_edge + static_roundtrip_edge) / 2,
            limit=10,
        )

        assert filtered_scan.opportunities == []

    asyncio.run(run())


def test_opportunity_universe_service_excludes_away_from_top_interactive_liquidity() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.09,
            20_000,
            0.0901,
            18_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0006,
            0.09,
            18_000,
            0.0901,
            1_000,
            top_of_book=TopOfBook(
                best_bid_price=0.09,
                best_bid_size=18_000,
                best_ask_price=0.0901,
                best_ask_size=1_000,
                best_ask_api_price=0.0901,
                best_ask_api_size=900,
                best_ask_interactive_price=0.0902,
                best_ask_interactive_size=100,
            ),
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            fee_profile_overrides={"paradex": "pro_fastfills"},
            target_notional=50.0,
            limit=10,
        )

        assert len(scan.opportunities) == 1
        candidate = scan.opportunities[0]
        assert candidate.paradex_fastfill_share == pytest.approx(0.0)
        assert candidate.paradex_fastfill_eligible_notional == pytest.approx(0.0)

    asyncio.run(run())


def test_opportunity_universe_service_scan_canary_candidates_uses_policy_defaults() -> None:
    symbol_lists = {
        "extended": ["ARB-USD", "TRUMP-USD"],
        "paradex": ["ARB-USD-PERP", "TRUMP-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.09,
            20_000,
            0.0901,
            18_000,
            daily_volume=200_000,
            open_interest=500_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0006,
            0.09,
            18_000,
            0.0901,
            17_000,
            daily_volume=180_000,
            open_interest=450_000,
        ),
        ("extended", "TRUMP-USD"): _snapshot(
            "extended",
            "TRUMP-USD",
            0.000013,
            10.0,
            500,
            10.1,
            400,
            daily_volume=100_000,
            open_interest=300_000,
        ),
        ("paradex", "TRUMP-USD-PERP"): _snapshot(
            "paradex",
            "TRUMP-USD-PERP",
            -0.0008,
            10.0,
            400,
            10.1,
            300,
            daily_volume=90_000,
            open_interest=250_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
        )
        candidates = await service.scan_canary_candidates(
            venues=["extended", "paradex"],
            canary_max_notional=25.0,
            min_route_stability_weight=0.0,
            min_route_presence_ratio=0.0,
            min_route_samples=0,
        )

        assert len(candidates) == 1
        assert isinstance(candidates[0], FundingUniverseCanaryCandidate)
        assert candidates[0].opportunity.opportunity.canonical_symbol == "ARB-USD-PERP"
        assert candidates[0].suggested_canary_notional == pytest.approx(25.0)

    asyncio.run(run())


def test_opportunity_universe_service_retries_retryable_snapshot_errors() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.092,
            20_000,
            0.0921,
            10_000,
            daily_volume=250_000,
            open_interest=400_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0009,
            0.0919,
            18_000,
            0.0921,
            10_000,
            daily_volume=210_000,
            open_interest=350_000,
        ),
    }
    attempts: dict[tuple[str, str], int] = {}

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        key = (venue, symbol)
        attempts[key] = attempts.get(key, 0) + 1
        if key == ("paradex", "ARB-USD-PERP") and attempts[key] == 1:
            request = httpx.Request("GET", "https://api.prod.paradex.trade/v1/markets/summary")
            response = httpx.Response(429, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return snapshots[key]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            snapshot_retry_attempts=2,
            snapshot_retry_backoff_seconds=0,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="roundtrip_pnl",
            limit=10,
        )

        assert len(scan.opportunities) == 1
        assert attempts[("paradex", "ARB-USD-PERP")] == 2

    asyncio.run(run())


def test_opportunity_universe_service_does_not_retry_http_425() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    attempts: dict[tuple[str, str], int] = {}

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        key = (venue, symbol)
        attempts[key] = attempts.get(key, 0) + 1
        if key == ("paradex", "ARB-USD-PERP"):
            request = httpx.Request("GET", "https://api.prod.paradex.trade/v1/markets/summary")
            response = httpx.Response(425, request=request)
            raise httpx.HTTPStatusError("too early", request=request, response=response)
        return _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.092,
            20_000,
            0.0921,
            10_000,
            daily_volume=250_000,
            open_interest=400_000,
        )

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            snapshot_retry_attempts=3,
            snapshot_retry_backoff_seconds=0,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="roundtrip_pnl",
            limit=10,
        )

        assert scan.opportunities == []
        assert attempts[("paradex", "ARB-USD-PERP")] == 1

    asyncio.run(run())


def test_opportunity_universe_service_stops_after_retry_budget_on_http_429() -> None:
    symbol_lists = {
        "extended": ["ARB-USD"],
        "paradex": ["ARB-USD-PERP"],
    }
    attempts: dict[tuple[str, str], int] = {}

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        key = (venue, symbol)
        attempts[key] = attempts.get(key, 0) + 1
        if key == ("paradex", "ARB-USD-PERP"):
            request = httpx.Request("GET", "https://api.prod.paradex.trade/v1/markets/summary")
            response = httpx.Response(429, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.092,
            20_000,
            0.0921,
            10_000,
            daily_volume=250_000,
            open_interest=400_000,
        )

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            snapshot_retry_attempts=2,
            snapshot_retry_backoff_seconds=0,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="roundtrip_pnl",
            limit=10,
        )

        assert scan.opportunities == []
        assert attempts[("paradex", "ARB-USD-PERP")] == 2

    asyncio.run(run())


def test_opportunity_universe_service_limits_snapshot_concurrency_by_venue() -> None:
    symbol_lists = {
        "extended": ["ARB-USD", "STRK-USD"],
        "paradex": ["ARB-USD-PERP", "STRK-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended", "ARB-USD", 0.000013, 0.092, 20_000, 0.0921, 10_000
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex", "ARB-USD-PERP", -0.0009, 0.0919, 18_000, 0.0921, 10_000
        ),
        ("extended", "STRK-USD"): _snapshot(
            "extended", "STRK-USD", 0.0001, 0.034, 20_000, 0.0341, 10_000
        ),
        ("paradex", "STRK-USD-PERP"): _snapshot(
            "paradex", "STRK-USD-PERP", -0.0004, 0.0339, 18_000, 0.0341, 10_000
        ),
    }
    venue_inflight = {"paradex": 0}
    max_venue_inflight = {"paradex": 0}

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        if venue == "paradex":
            venue_inflight["paradex"] += 1
            max_venue_inflight["paradex"] = max(
                max_venue_inflight["paradex"],
                venue_inflight["paradex"],
            )
            try:
                await asyncio.sleep(0.01)
            finally:
                venue_inflight["paradex"] -= 1
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            snapshot_concurrency_by_venue={"extended": 4, "hyperliquid": 4, "paradex": 1},
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="roundtrip_pnl",
            limit=10,
        )

        assert len(scan.opportunities) == 2
        assert max_venue_inflight["paradex"] == 1

    asyncio.run(run())


def test_opportunity_universe_service_excludes_policy_tags() -> None:
    symbol_lists = {
        "extended": ["TRUMP-USD", "LIT-USD"],
        "paradex": ["TRUMP-USD-PERP", "LIT-USD-PERP"],
    }
    snapshots = {
        ("extended", "TRUMP-USD"): _snapshot(
            "extended",
            "TRUMP-USD",
            0.000013,
            8.0,
            300,
            8.02,
            200,
            daily_volume=300_000,
            open_interest=250_000,
        ),
        ("paradex", "TRUMP-USD-PERP"): _snapshot(
            "paradex",
            "TRUMP-USD-PERP",
            -0.0015,
            8.0,
            250,
            8.03,
            180,
            daily_volume=220_000,
            open_interest=190_000,
        ),
        ("extended", "LIT-USD"): _snapshot(
            "extended",
            "LIT-USD",
            0.000013,
            0.83,
            2_500,
            0.831,
            1_500,
            daily_volume=120_000,
            open_interest=150_000,
        ),
        ("paradex", "LIT-USD-PERP"): _snapshot(
            "paradex",
            "LIT-USD-PERP",
            -0.0006,
            0.83,
            3_000,
            0.831,
            2_000,
            daily_volume=110_000,
            open_interest=140_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="execution_adjusted_quality_pnl",
            exclude_tags=["meme", "political"],
            limit=10,
        )

        assert len(scan.opportunities) == 1
        assert scan.opportunities[0].opportunity.canonical_symbol == "LIT-USD-PERP"
        assert scan.opportunities[0].policy_tags == []

    asyncio.run(run())


def test_execution_quality_service_summarizes_latest_outcomes(tmp_path: Path) -> None:
    journal_store = ExecutionJournalStore(tmp_path / "quality.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "quality.sqlite3")

    def paper_trade(paper_trade_id: int, label: str) -> PaperTradeEntry:
        return PaperTradeEntry(
            entry_id=paper_trade_id,
            created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label=label,
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 11, 59, tzinfo=UTC),
                one_day_net_edge_after_entry=0.001,
                break_even_days_entry=0.2,
                capacity_limit_notional=500.0,
                target_notional=11.0,
                capacity_fraction=0.1,
                max_target_notional=100.0,
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

    journal_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=1,
            preview_hash="hash-a",
            confirmation_entry_id=1,
            paper_trade=paper_trade(1, "arb_extended_paradex_1"),
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
    journal_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 6, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=2,
            preview_hash="hash-b",
            confirmation_entry_id=2,
            paper_trade=paper_trade(2, "arb_extended_paradex_2"),
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

    base_order_state = ExecutionOrderState(
        execution_entry_id=1,
        paper_trade_id=1,
        preview_hash="hash-a",
        legs=[],
        notes=[],
    )
    base_reconciliation = ExecutionReconciliation(
        execution_entry_id=1,
        paper_trade_id=1,
        preview_hash="hash-a",
        status="submitted",
        recommended_action="observe",
        matched_all_leg_symbols=False,
        venues=[],
        notes=[],
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 12, 7, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=1,
            paper_trade_id=1,
            preview_hash="hash-a",
            order_state=base_order_state,
            pair_status=ExecutionPairStatus(
                execution_entry_id=1,
                paper_trade_id=1,
                preview_hash="hash-a",
                derived_state="cleanup_needed",
                recommended_action="close_open_leg",
                order_state=base_order_state,
                reconciliation=base_reconciliation,
                notes=[],
            ),
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 12, 8, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=2,
            paper_trade_id=2,
            preview_hash="hash-b",
            order_state=ExecutionOrderState(
                execution_entry_id=2,
                paper_trade_id=2,
                preview_hash="hash-b",
                legs=[],
                notes=[],
            ),
            pair_status=ExecutionPairStatus(
                execution_entry_id=2,
                paper_trade_id=2,
                preview_hash="hash-b",
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
                order_state=ExecutionOrderState(
                    execution_entry_id=2,
                    paper_trade_id=2,
                    preview_hash="hash-b",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=2,
                    paper_trade_id=2,
                    preview_hash="hash-b",
                    status="submitted",
                    recommended_action="observe",
                    matched_all_leg_symbols=True,
                    venues=[],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )

    summary = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
    ).build_index()[("ARB-USD-PERP", "extended", "paradex")]

    assert summary.sample_size == 2
    assert summary.latest_outcome == "hedged"
    assert summary.cleanup_needed_count == 1
    assert summary.hedged_count == 1
    assert summary.weighted_score == pytest.approx(0.6)


def test_execution_quality_service_caps_after_latest_per_trade(tmp_path: Path) -> None:
    journal_store = ExecutionJournalStore(tmp_path / "quality-window.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "quality-window.sqlite3")

    def append_trade(paper_trade_id: int) -> None:
        journal_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 12, paper_trade_id, tzinfo=UTC),
                adapter="paired_live:auto",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=f"hash-{paper_trade_id}",
                confirmation_entry_id=paper_trade_id,
                paper_trade=PaperTradeEntry(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 3, 29, 11, paper_trade_id, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label=f"arb_extended_paradex_{paper_trade_id}",
                        canonical_symbol="ARB-USD-PERP",
                        source_recorded_at=datetime(2026, 3, 29, 11, paper_trade_id, tzinfo=UTC),
                        one_day_net_edge_after_entry=0.001,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=500.0,
                        target_notional=11.0,
                        capacity_fraction=0.1,
                        max_target_notional=100.0,
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
                    )
                ],
            )
        )

    def append_observation(
        *,
        paper_trade_id: int,
        minute: int,
        outcome: Literal[
            "hedged",
            "pending",
            "unfilled",
            "closed",
            "cleanup_needed",
            "review_required",
        ],
    ) -> None:
        order_state = ExecutionOrderState(
            execution_entry_id=paper_trade_id,
            paper_trade_id=paper_trade_id,
            preview_hash=f"hash-{paper_trade_id}",
            legs=[],
            notes=[],
        )
        observation_store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 3, 29, 13, minute, tzinfo=UTC),
                context="guarded_pair_poll",
                execution_entry_id=paper_trade_id,
                paper_trade_id=paper_trade_id,
                preview_hash=f"hash-{paper_trade_id}",
                order_state=order_state,
                pair_status=ExecutionPairStatus(
                    execution_entry_id=paper_trade_id,
                    paper_trade_id=paper_trade_id,
                    preview_hash=f"hash-{paper_trade_id}",
                    derived_state=outcome,
                    recommended_action="observe",
                    order_state=order_state,
                    reconciliation=ExecutionReconciliation(
                        execution_entry_id=paper_trade_id,
                        paper_trade_id=paper_trade_id,
                        preview_hash=f"hash-{paper_trade_id}",
                        status="submitted",
                        recommended_action="observe",
                        matched_all_leg_symbols=outcome in {"hedged", "closed"},
                        venues=[],
                        notes=[],
                    ),
                    notes=[],
                ),
            )
        )

    append_trade(1)
    append_trade(2)
    append_trade(3)
    append_observation(paper_trade_id=1, minute=10, outcome="cleanup_needed")
    append_observation(paper_trade_id=1, minute=11, outcome="hedged")
    append_observation(paper_trade_id=2, minute=9, outcome="closed")
    append_observation(paper_trade_id=3, minute=8, outcome="review_required")

    summary = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
        sample_limit=2,
    ).build_index()[("ARB-USD-PERP", "extended", "paradex")]

    assert summary.sample_size == 2
    assert summary.latest_outcome == "hedged"
    assert summary.hedged_count == 1
    assert summary.closed_count == 1
    assert summary.review_required_count == 0


def test_execution_quality_service_reclassifies_stale_cleanup_review_required(
    tmp_path: Path,
) -> None:
    journal_store = ExecutionJournalStore(tmp_path / "execution-quality-reclassify.sqlite3")
    observation_store = ExecutionObservationStore(
        tmp_path / "execution-quality-reclassify.sqlite3"
    )

    journal_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 6, tzinfo=UTC),
            adapter="extended_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=1,
            preview_hash="cleanup-hash",
            confirmation_entry_id=1,
            paper_trade=PaperTradeEntry(
                entry_id=1,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="s_extended_paradex",
                    canonical_symbol="S-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 59, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.001,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=500.0,
                    target_notional=11.0,
                    capacity_fraction=0.1,
                    max_target_notional=100.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="S-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="S-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="S-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    request_payload={"reduce_only": True},
                )
            ],
        )
    )

    order_state = ExecutionOrderState(
        execution_entry_id=1,
        paper_trade_id=1,
        preview_hash="cleanup-hash",
        legs=[],
        notes=[],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=1,
        paper_trade_id=1,
        preview_hash="cleanup-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                available_to_trade=4.8,
                position_symbols=[],
            ),
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                free_collateral=14.8,
                position_symbols=[],
            ),
        ],
        notes=[],
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 12, 7, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=1,
            paper_trade_id=1,
            preview_hash="cleanup-hash",
            order_state=order_state,
            pair_status=ExecutionPairStatus(
                execution_entry_id=1,
                paper_trade_id=1,
                preview_hash="cleanup-hash",
                derived_state="review_required",
                recommended_action="manual_review_required",
                order_state=order_state,
                reconciliation=reconciliation,
                notes=[],
            ),
        )
    )

    summary = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
    ).build_index()[("S-USD-PERP", "extended", "paradex")]

    assert summary.sample_size == 1
    assert summary.latest_outcome == "closed"
    assert summary.closed_count == 1
    assert summary.review_required_count == 0
    assert summary.weighted_score == pytest.approx((2 * 0.65 + 0.9) / 3)


def test_execution_quality_service_keeps_persisted_outcome_when_latest_entry_changes(
    tmp_path: Path,
) -> None:
    journal_store = ExecutionJournalStore(tmp_path / "execution-quality-mismatch.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "execution-quality-mismatch.sqlite3")

    original_entry = journal_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 6, tzinfo=UTC),
            adapter="extended_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=1,
            preview_hash="cleanup-hash",
            confirmation_entry_id=1,
            paper_trade=PaperTradeEntry(
                entry_id=1,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="s_extended_paradex",
                    canonical_symbol="S-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 59, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.001,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=500.0,
                    target_notional=11.0,
                    capacity_fraction=0.1,
                    max_target_notional=100.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="S-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="S-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="S-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    request_payload={"reduce_only": True},
                )
            ],
        )
    )

    order_state = ExecutionOrderState(
        execution_entry_id=original_entry.entry_id,
        paper_trade_id=1,
        preview_hash="cleanup-hash",
        legs=[],
        notes=[],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=original_entry.entry_id,
        paper_trade_id=1,
        preview_hash="cleanup-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                available_to_trade=4.8,
                position_symbols=[],
            ),
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                free_collateral=14.8,
                position_symbols=[],
            ),
        ],
        notes=[],
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 12, 7, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=original_entry.entry_id,
            paper_trade_id=1,
            preview_hash="cleanup-hash",
            order_state=order_state,
            pair_status=ExecutionPairStatus(
                execution_entry_id=original_entry.entry_id,
                paper_trade_id=1,
                preview_hash="cleanup-hash",
                derived_state="review_required",
                recommended_action="manual_review_required",
                order_state=order_state,
                reconciliation=reconciliation,
                notes=[],
            ),
        )
    )

    journal_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 8, tzinfo=UTC),
            adapter="extended_live",
            mode="live",
            status="submitted",
            paper_trade_id=1,
            preview_hash="retry-hash",
            confirmation_entry_id=2,
            paper_trade=original_entry.paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="S-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    request_payload={"reduce_only": False},
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="S-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                ),
            ],
        )
    )

    summary = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
    ).build_index()[("S-USD-PERP", "extended", "paradex")]

    assert summary.sample_size == 1
    assert summary.latest_outcome == "review_required"
    assert summary.closed_count == 0
    assert summary.review_required_count == 1
    assert summary.weighted_score == pytest.approx((2 * 0.65) / 3)


def test_execution_quality_service_skips_reclassification_without_entry_identity(
    tmp_path: Path,
) -> None:
    journal_store = ExecutionJournalStore(tmp_path / "execution-quality-null-ids.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "execution-quality-null-ids.sqlite3")

    original_entry = journal_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 6, tzinfo=UTC),
            adapter="extended_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=1,
            preview_hash="cleanup-hash",
            confirmation_entry_id=1,
            paper_trade=PaperTradeEntry(
                entry_id=1,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="s_extended_paradex",
                    canonical_symbol="S-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 59, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.001,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=500.0,
                    target_notional=11.0,
                    capacity_fraction=0.1,
                    max_target_notional=100.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="S-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="S-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="S-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    request_payload={"reduce_only": True},
                )
            ],
        )
    )

    order_state = ExecutionOrderState(
        execution_entry_id=None,
        paper_trade_id=1,
        preview_hash="cleanup-hash",
        legs=[],
        notes=[],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=None,
        paper_trade_id=1,
        preview_hash="cleanup-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                available_to_trade=4.8,
                position_symbols=[],
            ),
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                free_collateral=14.8,
                position_symbols=[],
            ),
        ],
        notes=[],
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 12, 7, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=original_entry.entry_id,
            paper_trade_id=1,
            preview_hash="cleanup-hash",
            order_state=order_state,
            pair_status=ExecutionPairStatus(
                execution_entry_id=None,
                paper_trade_id=1,
                preview_hash="cleanup-hash",
                derived_state="review_required",
                recommended_action="manual_review_required",
                order_state=order_state,
                reconciliation=reconciliation,
                notes=[],
            ),
        )
    )

    summary = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
    ).build_index()[("S-USD-PERP", "extended", "paradex")]

    assert summary.sample_size == 1
    assert summary.latest_outcome == "review_required"
    assert summary.closed_count == 0
    assert summary.review_required_count == 1
    assert summary.weighted_score == pytest.approx((2 * 0.65) / 3)


def test_execution_quality_service_lists_ranked_summaries(tmp_path: Path) -> None:
    journal_store = ExecutionJournalStore(tmp_path / "execution-quality-list.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "execution-quality-list.sqlite3")

    def append_summary_sample(
        *,
        paper_trade_id: int,
        canonical_symbol: str,
        short_venue: str,
        short_symbol: str,
        long_venue: str,
        long_symbol: str,
        outcome: Literal[
            "hedged",
            "pending",
            "unfilled",
            "closed",
            "cleanup_needed",
            "review_required",
        ],
    ) -> None:
        journal_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 15, paper_trade_id, tzinfo=UTC),
                adapter="paired_live:auto",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=f"list-hash-{paper_trade_id}",
                confirmation_entry_id=paper_trade_id,
                paper_trade=PaperTradeEntry(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 3, 29, 15, paper_trade_id, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label=f"{canonical_symbol.lower()}_{short_venue}_{long_venue}",
                        canonical_symbol=canonical_symbol,
                        source_recorded_at=datetime(
                            2026, 3, 29, 15, paper_trade_id, tzinfo=UTC
                        ),
                        one_day_net_edge_after_entry=0.001,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=500.0,
                        target_notional=11.0,
                        capacity_fraction=0.1,
                        max_target_notional=100.0,
                        long_leg=TradeLegIntent(
                            venue=long_venue,
                            symbol=long_symbol,
                            fee_profile="pro",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue=short_venue,
                            symbol=short_symbol,
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                legs=[
                    ExecutionLegResult(
                        venue=long_venue,
                        symbol=long_symbol,
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                    )
                ],
            )
        )
        order_state = ExecutionOrderState(
            execution_entry_id=paper_trade_id,
            paper_trade_id=paper_trade_id,
            preview_hash=f"list-hash-{paper_trade_id}",
            legs=[],
            notes=[],
        )
        observation_store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 3, 29, 16, paper_trade_id, tzinfo=UTC),
                context="guarded_pair_poll",
                execution_entry_id=paper_trade_id,
                paper_trade_id=paper_trade_id,
                preview_hash=f"list-hash-{paper_trade_id}",
                order_state=order_state,
                pair_status=ExecutionPairStatus(
                    execution_entry_id=paper_trade_id,
                    paper_trade_id=paper_trade_id,
                    preview_hash=f"list-hash-{paper_trade_id}",
                    derived_state=outcome,
                    recommended_action="observe",
                    order_state=order_state,
                    reconciliation=ExecutionReconciliation(
                        execution_entry_id=paper_trade_id,
                        paper_trade_id=paper_trade_id,
                        preview_hash=f"list-hash-{paper_trade_id}",
                        status="submitted",
                        recommended_action="observe",
                        matched_all_leg_symbols=outcome in {"hedged", "closed"},
                        venues=[],
                        notes=[],
                    ),
                    notes=[],
                ),
            )
        )

    append_summary_sample(
        paper_trade_id=1,
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        short_symbol="ARB-USD",
        long_venue="paradex",
        long_symbol="ARB-USD-PERP",
        outcome="unfilled",
    )
    append_summary_sample(
        paper_trade_id=2,
        canonical_symbol="STRK-USD-PERP",
        short_venue="extended",
        short_symbol="STRK-USD",
        long_venue="hyperliquid",
        long_symbol="STRK",
        outcome="hedged",
    )
    append_summary_sample(
        paper_trade_id=3,
        canonical_symbol="STRK-USD-PERP",
        short_venue="extended",
        short_symbol="STRK-USD",
        long_venue="hyperliquid",
        long_symbol="STRK",
        outcome="closed",
    )
    append_summary_sample(
        paper_trade_id=4,
        canonical_symbol="STRK-USD-PERP",
        short_venue="paradex",
        short_symbol="STRK-USD-PERP",
        long_venue="hyperliquid",
        long_symbol="STRK",
        outcome="closed",
    )
    append_summary_sample(
        paper_trade_id=5,
        canonical_symbol="STRK-USD-PERP",
        short_venue="hyperliquid",
        short_symbol="STRK",
        long_venue="paradex",
        long_symbol="STRK-USD-PERP",
        outcome="closed",
    )
    append_summary_sample(
        paper_trade_id=6,
        canonical_symbol="STRK-USD-PERP",
        short_venue="paradex",
        short_symbol="STRK-USD-PERP",
        long_venue="hyperliquid",
        long_symbol="STRK",
        outcome="closed",
    )
    append_summary_sample(
        paper_trade_id=7,
        canonical_symbol="STRK-USD-PERP",
        short_venue="hyperliquid",
        short_symbol="STRK",
        long_venue="paradex",
        long_symbol="STRK-USD-PERP",
        outcome="closed",
    )

    summaries = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
    ).list_summaries(min_sample_size=2, limit=10)

    assert len(summaries) == 3
    assert [
        (summary.canonical_symbol, summary.short_venue, summary.long_venue)
        for summary in summaries
    ] == [
        ("STRK-USD-PERP", "extended", "hyperliquid"),
        ("STRK-USD-PERP", "hyperliquid", "paradex"),
        ("STRK-USD-PERP", "paradex", "hyperliquid"),
    ]


def test_opportunity_universe_service_penalizes_bad_execution_history(
    tmp_path: Path,
) -> None:
    symbol_lists = {
        "extended": ["ARB-USD", "STRK-USD"],
        "paradex": ["ARB-USD-PERP"],
        "hyperliquid": ["STRK"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.00030,
            0.0920,
            20_000,
            0.0921,
            15_000,
            daily_volume=250_000,
            open_interest=400_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.00090,
            0.0919,
            18_000,
            0.0921,
            10_000,
            daily_volume=210_000,
            open_interest=350_000,
        ),
        ("extended", "STRK-USD"): _snapshot(
            "extended",
            "STRK-USD",
            0.00012,
            0.0340,
            120_000,
            0.0341,
            110_000,
            daily_volume=320_000,
            open_interest=950_000,
        ),
        ("hyperliquid", "STRK"): _snapshot(
            "hyperliquid",
            "STRK",
            -0.00012,
            0.0339,
            140_000,
            0.0341,
            135_000,
            daily_volume=400_000,
            open_interest=1_200_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    journal_store = ExecutionJournalStore(tmp_path / "universe-quality.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "universe-quality.sqlite3")

    def append_execution(
        *,
        paper_trade_id: int,
        canonical_symbol: str,
        short_venue: str,
        short_symbol: str,
        short_fee_profile: str,
        long_venue: str,
        long_symbol: str,
        long_fee_profile: str,
        outcome: Literal[
            "hedged",
            "pending",
            "unfilled",
            "closed",
            "cleanup_needed",
            "review_required",
        ],
    ) -> None:
        journal_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, paper_trade_id, tzinfo=UTC),
                adapter="paired_live:auto",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=f"hash-{paper_trade_id}",
                confirmation_entry_id=paper_trade_id,
                paper_trade=PaperTradeEntry(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 3, 29, 12, paper_trade_id, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label=f"{canonical_symbol.lower()}_{short_venue}_{long_venue}",
                        canonical_symbol=canonical_symbol,
                        source_recorded_at=datetime(
                            2026, 3, 29, 12, paper_trade_id, tzinfo=UTC
                        ),
                        one_day_net_edge_after_entry=0.001,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=500.0,
                        target_notional=11.0,
                        capacity_fraction=0.1,
                        max_target_notional=100.0,
                        long_leg=TradeLegIntent(
                            venue=long_venue,
                            symbol=long_symbol,
                            fee_profile=long_fee_profile,
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue=short_venue,
                            symbol=short_symbol,
                            fee_profile=short_fee_profile,
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                legs=[
                    ExecutionLegResult(
                        venue=long_venue,
                        symbol=long_symbol,
                        fee_profile=long_fee_profile,
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                    )
                ],
            )
        )
        order_state = ExecutionOrderState(
            execution_entry_id=paper_trade_id,
            paper_trade_id=paper_trade_id,
            preview_hash=f"hash-{paper_trade_id}",
            legs=[],
            notes=[],
        )
        observation_store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 3, 29, 14, paper_trade_id, tzinfo=UTC),
                context="guarded_pair_poll",
                execution_entry_id=paper_trade_id,
                paper_trade_id=paper_trade_id,
                preview_hash=f"hash-{paper_trade_id}",
                order_state=order_state,
                pair_status=ExecutionPairStatus(
                    execution_entry_id=paper_trade_id,
                    paper_trade_id=paper_trade_id,
                    preview_hash=f"hash-{paper_trade_id}",
                    derived_state=outcome,
                    recommended_action="observe",
                    order_state=order_state,
                    reconciliation=ExecutionReconciliation(
                        execution_entry_id=paper_trade_id,
                        paper_trade_id=paper_trade_id,
                        preview_hash=f"hash-{paper_trade_id}",
                        status="submitted",
                        recommended_action="observe",
                        matched_all_leg_symbols=outcome in {"hedged", "closed"},
                        venues=[],
                        notes=[],
                    ),
                    notes=[],
                ),
            )
        )

    append_execution(
        paper_trade_id=1,
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        short_symbol="ARB-USD",
        short_fee_profile="default",
        long_venue="paradex",
        long_symbol="ARB-USD-PERP",
        long_fee_profile="pro",
        outcome="cleanup_needed",
    )
    append_execution(
        paper_trade_id=2,
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        short_symbol="ARB-USD",
        short_fee_profile="default",
        long_venue="paradex",
        long_symbol="ARB-USD-PERP",
        long_fee_profile="pro",
        outcome="review_required",
    )
    append_execution(
        paper_trade_id=3,
        canonical_symbol="STRK-USD-PERP",
        short_venue="extended",
        short_symbol="STRK-USD",
        short_fee_profile="default",
        long_venue="hyperliquid",
        long_symbol="STRK",
        long_fee_profile="tier0",
        outcome="hedged",
    )

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            execution_quality_service=ExecutionQualityService(
                journal_store=journal_store,
                observation_store=observation_store,
            ),
        )
        raw_scan = await service.scan(
            venues=["extended", "paradex", "hyperliquid"],
            ranking="roundtrip_pnl",
            target_notional=1_000,
            limit=10,
        )
        execution_scan = await service.scan(
            venues=["extended", "paradex", "hyperliquid"],
            ranking="execution_adjusted_quality_pnl",
            target_notional=1_000,
            limit=10,
        )

        assert raw_scan.opportunities[0].opportunity.canonical_symbol == "ARB-USD-PERP"
        assert execution_scan.opportunities[0].opportunity.canonical_symbol == "STRK-USD-PERP"
        assert execution_scan.opportunities[0].execution_quality is not None
        assert execution_scan.opportunities[1].execution_quality is not None
        assert (
            execution_scan.opportunities[0].execution_quality.weighted_score
            > execution_scan.opportunities[1].execution_quality.weighted_score
        )

    asyncio.run(run())


def test_opportunity_universe_service_uses_execution_prior_for_unknown_pairs(
    tmp_path: Path,
) -> None:
    symbol_lists = {
        "extended": ["STRK-USD"],
        "hyperliquid": ["STRK"],
    }
    snapshots = {
        ("extended", "STRK-USD"): _snapshot(
            "extended",
            "STRK-USD",
            0.00020,
            0.0340,
            120_000,
            0.0341,
            110_000,
            daily_volume=320_000,
            open_interest=950_000,
        ),
        ("hyperliquid", "STRK"): _snapshot(
            "hyperliquid",
            "STRK",
            -0.00040,
            0.0339,
            140_000,
            0.0341,
            135_000,
            daily_volume=400_000,
            open_interest=1_200_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    journal_store = ExecutionJournalStore(tmp_path / "universe-prior.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "universe-prior.sqlite3")
    quality_service = ExecutionQualityService(
        journal_store=journal_store,
        observation_store=observation_store,
    )

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            execution_quality_service=quality_service,
        )
        scan = await service.scan(
            venues=["extended", "hyperliquid"],
            ranking="execution_adjusted_quality_pnl",
            target_notional=1_000,
            limit=10,
        )

        assert len(scan.opportunities) == 1
        opportunity = scan.opportunities[0]
        assert opportunity.execution_quality is None
        assert opportunity.quality_score is not None
        assert opportunity.estimated_one_day_pnl_after_round_trip is not None
        assert opportunity.execution_adjusted_quality_score == pytest.approx(
            opportunity.quality_score * quality_service.prior_score
        )
        assert opportunity.execution_adjusted_one_day_pnl_after_round_trip == pytest.approx(
            opportunity.estimated_one_day_pnl_after_round_trip * quality_service.prior_score
        )

    asyncio.run(run())


def test_opportunity_universe_service_filters_by_min_execution_samples(
    tmp_path: Path,
) -> None:
    symbol_lists = {
        "extended": ["ARB-USD", "STRK-USD"],
        "paradex": ["ARB-USD-PERP", "STRK-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.00030,
            0.0920,
            20_000,
            0.0921,
            15_000,
            daily_volume=250_000,
            open_interest=400_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.00090,
            0.0919,
            18_000,
            0.0921,
            10_000,
            daily_volume=210_000,
            open_interest=350_000,
        ),
        ("extended", "STRK-USD"): _snapshot(
            "extended",
            "STRK-USD",
            0.00012,
            0.0340,
            120_000,
            0.0341,
            110_000,
            daily_volume=320_000,
            open_interest=950_000,
        ),
        ("paradex", "STRK-USD-PERP"): _snapshot(
            "paradex",
            "STRK-USD-PERP",
            -0.00030,
            0.0339,
            140_000,
            0.0341,
            135_000,
            daily_volume=400_000,
            open_interest=1_200_000,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    journal_store = ExecutionJournalStore(tmp_path / "execution-samples.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "execution-samples.sqlite3")

    def append_observed_trade(
        *,
        paper_trade_id: int,
        canonical_symbol: str,
        short_symbol: str,
        long_symbol: str,
    ) -> None:
        journal_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 17, paper_trade_id, tzinfo=UTC),
                adapter="paired_live:auto",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=f"sample-hash-{paper_trade_id}",
                confirmation_entry_id=paper_trade_id,
                paper_trade=PaperTradeEntry(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 3, 29, 17, paper_trade_id, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label=f"{canonical_symbol.lower()}_extended_paradex",
                        canonical_symbol=canonical_symbol,
                        source_recorded_at=datetime(
                            2026, 3, 29, 17, paper_trade_id, tzinfo=UTC
                        ),
                        one_day_net_edge_after_entry=0.001,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=500.0,
                        target_notional=11.0,
                        capacity_fraction=0.1,
                        max_target_notional=100.0,
                        long_leg=TradeLegIntent(
                            venue="paradex",
                            symbol=long_symbol,
                            fee_profile="pro",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue="extended",
                            symbol=short_symbol,
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                legs=[
                    ExecutionLegResult(
                        venue="paradex",
                        symbol=long_symbol,
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                    )
                ],
            )
        )
        order_state = ExecutionOrderState(
            execution_entry_id=paper_trade_id,
            paper_trade_id=paper_trade_id,
            preview_hash=f"sample-hash-{paper_trade_id}",
            legs=[],
            notes=[],
        )
        observation_store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 3, 29, 18, paper_trade_id, tzinfo=UTC),
                context="guarded_pair_poll",
                execution_entry_id=paper_trade_id,
                paper_trade_id=paper_trade_id,
                preview_hash=f"sample-hash-{paper_trade_id}",
                order_state=order_state,
                pair_status=ExecutionPairStatus(
                    execution_entry_id=paper_trade_id,
                    paper_trade_id=paper_trade_id,
                    preview_hash=f"sample-hash-{paper_trade_id}",
                    derived_state="hedged",
                    recommended_action="observe",
                    order_state=order_state,
                    reconciliation=ExecutionReconciliation(
                        execution_entry_id=paper_trade_id,
                        paper_trade_id=paper_trade_id,
                        preview_hash=f"sample-hash-{paper_trade_id}",
                        status="submitted",
                        recommended_action="observe",
                        matched_all_leg_symbols=True,
                        venues=[],
                        notes=[],
                    ),
                    notes=[],
                ),
            )
        )

    append_observed_trade(
        paper_trade_id=1,
        canonical_symbol="ARB-USD-PERP",
        short_symbol="ARB-USD",
        long_symbol="ARB-USD-PERP",
    )
    append_observed_trade(
        paper_trade_id=2,
        canonical_symbol="ARB-USD-PERP",
        short_symbol="ARB-USD",
        long_symbol="ARB-USD-PERP",
    )
    append_observed_trade(
        paper_trade_id=3,
        canonical_symbol="STRK-USD-PERP",
        short_symbol="STRK-USD",
        long_symbol="STRK-USD-PERP",
    )

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            execution_quality_service=ExecutionQualityService(
                journal_store=journal_store,
                observation_store=observation_store,
            ),
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="execution_adjusted_quality_pnl",
            min_execution_samples=2,
            limit=10,
        )

        assert len(scan.opportunities) == 1
        assert scan.opportunities[0].opportunity.canonical_symbol == "ARB-USD-PERP"
        assert scan.opportunities[0].execution_quality is not None
        assert scan.opportunities[0].execution_quality.sample_size == 2

    asyncio.run(run())


def test_opportunity_universe_service_rejects_invalid_ranking() -> None:
    async def list_symbols(_venue: str) -> list[str]:
        return ["STRK-USD"]

    async def fetch_snapshot(_venue: str, _symbol: str) -> NormalizedMarketSnapshot:
        raise AssertionError("fetch_snapshot should not run for invalid rankings")

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=cast(Any, list_symbols),
            fetch_snapshot=cast(Any, fetch_snapshot),
        )
        with pytest.raises(ValueError, match="Unsupported ranking: typo-ranking"):
            await service.scan(
                venues=["extended", "hyperliquid"],
                ranking="typo-ranking",  # type: ignore[arg-type]
            )

    asyncio.run(run())


def test_build_portfolio_plan_allocates_ranked_opportunities_without_duplicates() -> None:
    opportunity_high = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="paradex",
            short_venue="extended",
            long_fee_profile="pro",
            short_fee_profile="default",
            gross_daily_edge=0.004,
            entry_cost_rate=0.00045,
            round_trip_cost_rate=0.0009,
            one_day_net_edge_after_entry=0.00355,
            one_day_net_edge_after_round_trip=0.0031,
            break_even_days_entry=0.2,
            break_even_days_round_trip=0.3,
            capacity=CapacityEstimate(max_entry_notional=1_000, limiting_venue="paradex"),
        ),
        deployable_notional=1_000,
        estimated_one_day_pnl_after_entry=3.55,
        estimated_one_day_pnl_after_round_trip=3.1,
        quality_score=2.0,
    )
    opportunity_duplicate_symbol = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="hyperliquid",
            short_venue="extended",
            long_fee_profile="tier0",
            short_fee_profile="default",
            gross_daily_edge=0.003,
            entry_cost_rate=0.0007,
            round_trip_cost_rate=0.0014,
            one_day_net_edge_after_entry=0.0023,
            one_day_net_edge_after_round_trip=0.0016,
            break_even_days_entry=0.4,
            break_even_days_round_trip=0.6,
            capacity=CapacityEstimate(max_entry_notional=700, limiting_venue="hyperliquid"),
        ),
        deployable_notional=700,
        estimated_one_day_pnl_after_entry=1.61,
        estimated_one_day_pnl_after_round_trip=1.12,
        quality_score=1.0,
    )
    opportunity_second = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="STRK-USD-PERP",
            long_venue="hyperliquid",
            short_venue="extended",
            long_fee_profile="tier0",
            short_fee_profile="default",
            gross_daily_edge=0.0014,
            entry_cost_rate=0.0007,
            round_trip_cost_rate=0.0014,
            one_day_net_edge_after_entry=0.0007,
            one_day_net_edge_after_round_trip=0.0,
            break_even_days_entry=0.5,
            break_even_days_round_trip=1.0,
            capacity=CapacityEstimate(max_entry_notional=2_500, limiting_venue="extended"),
        ),
        deployable_notional=2_500,
        estimated_one_day_pnl_after_entry=1.75,
        estimated_one_day_pnl_after_round_trip=0.0,
        quality_score=0.5,
    )
    scan = FundingUniverseScan(
        venues=["extended", "paradex", "hyperliquid"],
        ranking="quality_adjusted_roundtrip_pnl",
        target_notional=5_000,
        overlap_count=2,
        overlaps=[],
        opportunities=[opportunity_high, opportunity_duplicate_symbol, opportunity_second],
    )

    plan = build_portfolio_plan(
        scan,
        target_notional=3_000,
        max_positions=3,
        min_selected_notional=100,
    )

    assert plan.allocated_notional == 3_000
    assert plan.unused_notional == 0
    assert len(plan.entries) == 2
    assert plan.entries[0].opportunity.opportunity.canonical_symbol == "ARB-USD-PERP"
    assert plan.entries[0].selected_notional == 1_000
    assert plan.entries[1].opportunity.opportunity.canonical_symbol == "STRK-USD-PERP"
    assert plan.entries[1].selected_notional == 2_000


def test_build_portfolio_plan_binding_position_cap_skips_duplicates() -> None:
    opportunity_high = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="paradex",
            short_venue="extended",
            long_fee_profile="pro",
            short_fee_profile="default",
            gross_daily_edge=0.004,
            entry_cost_rate=0.00045,
            round_trip_cost_rate=0.0009,
            one_day_net_edge_after_entry=0.00355,
            one_day_net_edge_after_round_trip=0.0031,
            break_even_days_entry=0.2,
            break_even_days_round_trip=0.3,
            capacity=CapacityEstimate(max_entry_notional=1_000, limiting_venue="paradex"),
        ),
        deployable_notional=1_000,
        estimated_one_day_pnl_after_entry=3.55,
        estimated_one_day_pnl_after_round_trip=3.1,
        quality_score=2.0,
    )
    opportunity_duplicate_symbol = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="hyperliquid",
            short_venue="extended",
            long_fee_profile="tier0",
            short_fee_profile="default",
            gross_daily_edge=0.003,
            entry_cost_rate=0.0007,
            round_trip_cost_rate=0.0014,
            one_day_net_edge_after_entry=0.0023,
            one_day_net_edge_after_round_trip=0.0016,
            break_even_days_entry=0.4,
            break_even_days_round_trip=0.6,
            capacity=CapacityEstimate(max_entry_notional=700, limiting_venue="hyperliquid"),
        ),
        deployable_notional=700,
        estimated_one_day_pnl_after_entry=1.61,
        estimated_one_day_pnl_after_round_trip=1.12,
        quality_score=1.0,
    )
    opportunity_second = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="STRK-USD-PERP",
            long_venue="hyperliquid",
            short_venue="extended",
            long_fee_profile="tier0",
            short_fee_profile="default",
            gross_daily_edge=0.0014,
            entry_cost_rate=0.0007,
            round_trip_cost_rate=0.0014,
            one_day_net_edge_after_entry=0.0007,
            one_day_net_edge_after_round_trip=0.0,
            break_even_days_entry=0.5,
            break_even_days_round_trip=1.0,
            capacity=CapacityEstimate(max_entry_notional=2_500, limiting_venue="extended"),
        ),
        deployable_notional=2_500,
        estimated_one_day_pnl_after_entry=1.75,
        estimated_one_day_pnl_after_round_trip=0.0,
        quality_score=0.5,
    )
    scan = FundingUniverseScan(
        venues=["extended", "paradex", "hyperliquid"],
        ranking="quality_adjusted_roundtrip_pnl",
        target_notional=5_000,
        overlap_count=2,
        overlaps=[],
        opportunities=[opportunity_high, opportunity_duplicate_symbol, opportunity_second],
    )

    plan = build_portfolio_plan(
        scan,
        target_notional=3_000,
        max_positions=2,
        min_selected_notional=100,
    )

    assert plan.allocated_notional == 3_000
    assert plan.unused_notional == 0
    assert len(plan.entries) == 2
    assert plan.entries[0].opportunity.opportunity.canonical_symbol == "ARB-USD-PERP"
    assert plan.entries[0].selected_notional == 1_000
    assert plan.entries[1].opportunity.opportunity.canonical_symbol == "STRK-USD-PERP"
    assert plan.entries[1].selected_notional == 2_000


def test_build_portfolio_plan_reports_execution_adjusted_round_trip_pnl() -> None:
    opportunity = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="paradex",
            short_venue="extended",
            long_fee_profile="pro",
            short_fee_profile="default",
            gross_daily_edge=0.004,
            entry_cost_rate=0.00045,
            round_trip_cost_rate=0.0009,
            one_day_net_edge_after_entry=0.00355,
            one_day_net_edge_after_round_trip=0.0031,
            break_even_days_entry=0.2,
            break_even_days_round_trip=0.3,
            capacity=CapacityEstimate(max_entry_notional=1_000, limiting_venue="paradex"),
        ),
        deployable_notional=1_000,
        estimated_one_day_pnl_after_entry=3.55,
        estimated_one_day_pnl_after_round_trip=3.1,
        quality_score=2.0,
        execution_quality=ExecutionQualitySummary(
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            sample_size=3,
            weighted_score=0.5,
            latest_outcome="unfilled",
            hedged_count=1,
            closed_count=0,
            unfilled_count=2,
            cleanup_needed_count=0,
            review_required_count=0,
            pending_count=0,
        ),
        execution_adjusted_one_day_pnl_after_round_trip=1.55,
        execution_adjusted_quality_score=1.0,
    )
    scan = FundingUniverseScan(
        venues=["extended", "paradex"],
        ranking="execution_adjusted_quality_pnl",
        target_notional=1_000,
        overlap_count=1,
        overlaps=[],
        opportunities=[opportunity],
    )

    plan = build_portfolio_plan(scan, target_notional=1_000, max_positions=1)

    assert plan.estimated_one_day_pnl_after_round_trip == pytest.approx(3.1)
    assert plan.execution_adjusted_estimated_one_day_pnl_after_round_trip == pytest.approx(1.55)
    assert (
        plan.entries[0].execution_adjusted_estimated_one_day_pnl_after_round_trip
        == pytest.approx(1.55)
    )

def test_passes_symbol_policy_accepts_single_string_inputs() -> None:
    assert passes_symbol_policy("ARB-USD-PERP", include_symbols="ARB-USD-PERP")
    assert not passes_symbol_policy("ARB-USD-PERP", exclude_symbols="ARB-USD-PERP")
    assert not passes_symbol_policy("TRUMP-USD-PERP", exclude_tags="political")


def test_build_portfolio_plan_reprices_fastfill_route_for_selected_notional() -> None:
    opportunity = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="paradex",
            short_venue="extended",
            long_fee_profile="pro_fastfills",
            short_fee_profile="default",
            gross_daily_edge=0.004,
            entry_cost_rate=0.00039,
            round_trip_cost_rate=0.00078,
            one_day_net_edge_after_entry=0.00361,
            one_day_net_edge_after_round_trip=0.00322,
            break_even_days_entry=0.2,
            break_even_days_round_trip=0.3,
            capacity=CapacityEstimate(
                short_bid_notional=1_500.0,
                long_ask_notional=900.0,
                max_entry_notional=900.0,
                limiting_venue="paradex",
            ),
        ),
        venue_markets={
            "extended": FundingUniverseVenueMarket(
                venue="extended",
                symbol="ARB-USD",
                bid_notional=1_500.0,
                ask_notional=1_400.0,
            ),
            "paradex": FundingUniverseVenueMarket(
                venue="paradex",
                symbol="ARB-USD-PERP",
                bid_notional=1_300.0,
                ask_notional=900.0,
                ask_notional_api=600.0,
                ask_notional_interactive=300.0,
            ),
        },
        deployable_notional=900.0,
        modeled_entry_cost_rate=0.00043,
        modeled_round_trip_cost_rate=0.00086,
        estimated_one_day_pnl_after_entry=3.213,
        estimated_one_day_pnl_after_round_trip=2.826,
        paradex_fastfill_share=0.333333,
        paradex_fastfill_eligible_notional=300.0,
        quality_score=1.8,
    )
    scan = FundingUniverseScan(
        venues=["extended", "paradex"],
        ranking="quality_adjusted_roundtrip_pnl",
        target_notional=900.0,
        overlap_count=1,
        overlaps=[],
        opportunities=[opportunity],
    )

    plan = build_portfolio_plan(scan, target_notional=200.0, max_positions=1)

    assert plan.estimated_one_day_pnl_after_round_trip == pytest.approx(0.644)
    assert len(plan.entries) == 1
    assert plan.entries[0].estimated_one_day_pnl_after_round_trip == pytest.approx(0.644)


def test_route_stability_service_summarizes_repeated_scan_windows(tmp_path: Path) -> None:
    history_store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    window_one = datetime(2026, 3, 29, 10, 0, tzinfo=UTC)
    window_two = datetime(2026, 3, 29, 10, 5, tzinfo=UTC)

    def append_record(
        recorded_at: datetime,
        *,
        label: str,
        canonical_symbol: str,
        left_symbol: str,
        right_symbol: str,
        long_venue: str,
        long_fee_profile: str,
        roundtrip_edge: float,
        capacity_notional: float,
    ) -> None:
        history_store.append(
            OpportunityRecord(
                recorded_at=recorded_at,
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol=left_symbol,
                    left_fee_profile="default",
                    right_venue=long_venue,
                    right_symbol=right_symbol,
                    right_fee_profile=long_fee_profile,
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol=canonical_symbol,
                    long_venue=long_venue,
                    short_venue="extended",
                    long_fee_profile=long_fee_profile,
                    short_fee_profile="default",
                    gross_daily_edge=roundtrip_edge + 0.0009,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=roundtrip_edge + 0.00045,
                    one_day_net_edge_after_round_trip=roundtrip_edge,
                    break_even_days_entry=0.25,
                    break_even_days_round_trip=0.5,
                    capacity=CapacityEstimate(
                        short_bid_notional=capacity_notional + 200.0,
                        long_ask_notional=capacity_notional,
                        max_entry_notional=capacity_notional,
                        limiting_venue=long_venue,
                    ),
                ),
            )
        )

    append_record(
        window_one,
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        left_symbol="ARB-USD",
        right_symbol="ARB-USD-PERP",
        long_venue="paradex",
        long_fee_profile="pro",
        roundtrip_edge=0.0012,
        capacity_notional=900.0,
    )
    append_record(
        window_one,
        label="strk_extended_hyperliquid",
        canonical_symbol="STRK-USD-PERP",
        left_symbol="STRK-USD",
        right_symbol="STRK",
        long_venue="hyperliquid",
        long_fee_profile="tier0",
        roundtrip_edge=0.0001,
        capacity_notional=500.0,
    )
    append_record(
        window_two,
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        left_symbol="ARB-USD",
        right_symbol="ARB-USD-PERP",
        long_venue="paradex",
        long_fee_profile="pro",
        roundtrip_edge=0.0010,
        capacity_notional=950.0,
    )
    append_record(
        window_two,
        label="lit_extended_paradex",
        canonical_symbol="LIT-USD-PERP",
        left_symbol="LIT-USD",
        right_symbol="LIT-USD-PERP",
        long_venue="paradex",
        long_fee_profile="pro",
        roundtrip_edge=-0.0002,
        capacity_notional=450.0,
    )

    service = RouteStabilityService(history_store=history_store)
    summaries = service.list_summaries(limit=10)

    assert summaries[0].canonical_symbol == "ARB-USD-PERP"
    assert summaries[0].presence_ratio == pytest.approx(1.0)
    assert summaries[0].positive_roundtrip_share == pytest.approx(1.0)
    assert summaries[0].stability_weight > summaries[1].stability_weight


def test_route_stability_service_keeps_same_second_batches_distinct(tmp_path: Path) -> None:
    history_store = OpportunityHistoryStore(tmp_path / "route_stability_same_second.sqlite3")
    window_one = datetime(2026, 3, 29, 12, 0, 0, 100_000, tzinfo=UTC)
    window_two = datetime(2026, 3, 29, 12, 0, 0, 900_000, tzinfo=UTC)

    for recorded_at, edge in ((window_one, 0.0010), (window_two, 0.0008)):
        history_store.append(
            OpportunityRecord(
                recorded_at=recorded_at,
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
                    gross_daily_edge=edge + 0.0009,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=edge + 0.00045,
                    one_day_net_edge_after_round_trip=edge,
                    break_even_days_entry=0.25,
                    break_even_days_round_trip=0.5,
                    capacity=CapacityEstimate(
                        short_bid_notional=1_100.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue="paradex",
                    ),
                ),
            )
        )

    service = RouteStabilityService(history_store=history_store, min_window_cardinality=1)
    summary = service.build_index()[("ARB-USD-PERP", "extended", "paradex", "default", "pro")]

    assert summary.window_count == 2
    assert summary.sample_size == 2


def test_opportunity_universe_service_ranks_by_route_adjusted_quality() -> None:
    symbol_lists = {
        "extended": ["ARB-USD", "WIF-USD"],
        "paradex": ["ARB-USD-PERP", "WIF-USD-PERP"],
    }
    snapshots = {
        ("extended", "ARB-USD"): _snapshot(
            "extended",
            "ARB-USD",
            0.000013,
            0.09,
            20_000,
            0.0901,
            18_000,
            daily_volume=200_000,
            open_interest=500_000,
        ),
        ("paradex", "ARB-USD-PERP"): _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0006,
            0.09,
            18_000,
            0.0901,
            17_000,
            daily_volume=180_000,
            open_interest=480_000,
        ),
        ("extended", "WIF-USD"): _snapshot(
            "extended",
            "WIF-USD",
            0.000013,
            0.18,
            8_000,
            0.1805,
            6_000,
            daily_volume=60_000,
            open_interest=90_000,
        ),
        ("paradex", "WIF-USD-PERP"): _snapshot(
            "paradex",
            "WIF-USD-PERP",
            -0.0011,
            0.18,
            7_000,
            0.1805,
            5_500,
            daily_volume=55_000,
            open_interest=80_000,
        ),
    }

    route_stability_index = {
        ("ARB-USD-PERP", "extended", "paradex", "default", "pro"): RouteStabilitySummary(
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            sample_size=6,
            window_count=5,
            presence_ratio=0.83,
            positive_roundtrip_share=1.0,
            mean_roundtrip_edge=0.0011,
            median_roundtrip_edge=0.00105,
            edge_stddev=0.00008,
            mean_capacity_notional=950.0,
            median_capacity_notional=940.0,
            capacity_stddev=40.0,
            latest_roundtrip_edge=0.0010,
            latest_recorded_at=datetime(2026, 3, 29, 11, 0, tzinfo=UTC),
            stability_weight=0.72,
            stability_score=0.000792,
        ),
        ("ARB-USD-PERP", "extended", "paradex", "vip", "retail"): RouteStabilitySummary(
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="vip",
            long_fee_profile="retail",
            sample_size=10,
            window_count=8,
            presence_ratio=0.95,
            positive_roundtrip_share=1.0,
            mean_roundtrip_edge=0.005,
            median_roundtrip_edge=0.005,
            edge_stddev=0.0001,
            mean_capacity_notional=1_500.0,
            median_capacity_notional=1_500.0,
            capacity_stddev=25.0,
            latest_roundtrip_edge=0.005,
            latest_recorded_at=datetime(2026, 3, 29, 11, 5, tzinfo=UTC),
            stability_weight=0.99,
            stability_score=0.00495,
        ),
        ("WIF-USD-PERP", "extended", "paradex", "default", "pro"): RouteStabilitySummary(
            canonical_symbol="WIF-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            sample_size=3,
            window_count=1,
            presence_ratio=0.25,
            positive_roundtrip_share=0.67,
            mean_roundtrip_edge=0.0016,
            median_roundtrip_edge=0.0017,
            edge_stddev=0.0009,
            mean_capacity_notional=400.0,
            median_capacity_notional=380.0,
            capacity_stddev=150.0,
            latest_roundtrip_edge=-0.0001,
            latest_recorded_at=datetime(2026, 3, 29, 11, 0, tzinfo=UTC),
            stability_weight=0.08,
            stability_score=0.000128,
        ),
    }

    async def list_symbols(venue: str) -> list[str]:
        return symbol_lists[venue]

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        return snapshots[(venue, symbol)]

    class StubRouteStabilityService:
        def build_index(self) -> dict[tuple[str, str, str, str, str], RouteStabilitySummary]:
            return route_stability_index

    async def run() -> None:
        service = OpportunityUniverseService(
            list_symbols=list_symbols,
            fetch_snapshot=fetch_snapshot,
            route_stability_service=cast(RouteStabilityService, StubRouteStabilityService()),
        )
        scan = await service.scan(
            venues=["extended", "paradex"],
            ranking="route_adjusted_quality_pnl",
            target_notional=5_000,
            limit=10,
        )

        assert scan.opportunities[0].opportunity.canonical_symbol == "ARB-USD-PERP"
        assert scan.opportunities[0].route_adjusted_quality_score is not None
        assert (
            scan.opportunities[0].route_adjusted_quality_score
            > cast(float, scan.opportunities[1].route_adjusted_quality_score)
        )

    asyncio.run(run())


def test_build_portfolio_plan_reports_route_adjusted_round_trip_pnl() -> None:
    opportunity = FundingUniverseOpportunity(
        opportunity=FundingArbOpportunity(
            canonical_symbol="ARB-USD-PERP",
            long_venue="paradex",
            short_venue="extended",
            long_fee_profile="pro",
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
        deployable_notional=900.0,
        estimated_one_day_pnl_after_entry=3.195,
        estimated_one_day_pnl_after_round_trip=2.79,
        quality_score=1.7,
        execution_quality=ExecutionQualitySummary(
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            sample_size=2,
            weighted_score=0.6,
            latest_outcome="hedged",
            hedged_count=1,
            closed_count=0,
            unfilled_count=1,
            cleanup_needed_count=0,
            review_required_count=0,
            pending_count=0,
        ),
        execution_adjusted_one_day_pnl_after_round_trip=1.674,
        execution_adjusted_quality_score=1.02,
        route_stability=RouteStabilitySummary(
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            sample_size=4,
            window_count=4,
            presence_ratio=0.8,
            positive_roundtrip_share=1.0,
            mean_roundtrip_edge=0.003,
            median_roundtrip_edge=0.003,
            edge_stddev=0.0002,
            mean_capacity_notional=850.0,
            median_capacity_notional=850.0,
            capacity_stddev=25.0,
            latest_roundtrip_edge=0.0031,
            latest_recorded_at=datetime(2026, 3, 29, 11, 30, tzinfo=UTC),
            stability_weight=0.5,
            stability_score=0.0015,
        ),
        stability_adjusted_one_day_pnl_after_round_trip=1.395,
        stability_adjusted_quality_score=0.85,
        route_adjusted_quality_score=0.51,
    )
    scan = FundingUniverseScan(
        venues=["extended", "paradex"],
        ranking="route_adjusted_quality_pnl",
        target_notional=1_000,
        overlap_count=1,
        overlaps=[],
        opportunities=[opportunity],
    )

    plan = build_portfolio_plan(scan, target_notional=1_000, max_positions=1)

    assert plan.stability_adjusted_estimated_one_day_pnl_after_round_trip == pytest.approx(1.395)
    assert plan.route_adjusted_estimated_one_day_pnl_after_round_trip == pytest.approx(0.837)
    assert (
        plan.entries[0].route_adjusted_estimated_one_day_pnl_after_round_trip
        == pytest.approx(0.837)
    )


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


def test_build_live_submission_readiness_blocks_zero_usable_collateral() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=8,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=12,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.09229,
                    worst_price_text="0.09229",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[
                        "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                        "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                    ],
                    auth_scheme="account address + API wallet private key",
                    payload={"coin": "ARB"},
                    notes=[],
                )
            ],
        ),
    )

    readiness = build_live_submission_readiness(
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        confirmations=[confirmation],
        execution_preflight=PaperTradeExecutionPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[],
            blocking_reasons=[],
        ),
        account_preflight=PaperTradeAccountPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[
                VenueAccountPreflight(
                    venue="hyperliquid",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_wallet",
                    total_collateral=0.0,
                    available_to_trade=0.0,
                    free_collateral=0.0,
                )
            ],
            blocking_reasons=[],
        ),
    )

    assert readiness.ready is False
    assert (
        "Venue hyperliquid has no usable collateral for the confirmed 11.00 notional preview"
        in readiness.blocking_reasons
    )


def test_build_live_submission_readiness_blocks_when_collateral_data_missing() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=9,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=12,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.09229,
                    worst_price_text="0.09229",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[
                        "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                        "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                    ],
                    auth_scheme="account address + API wallet private key",
                    payload={"coin": "ARB"},
                    notes=[],
                )
            ],
        ),
    )

    readiness = build_live_submission_readiness(
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        confirmations=[confirmation],
        execution_preflight=PaperTradeExecutionPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[],
            blocking_reasons=[],
        ),
        account_preflight=PaperTradeAccountPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[
                VenueAccountPreflight(
                    venue="hyperliquid",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_wallet",
                )
            ],
            blocking_reasons=[],
        ),
    )

    assert readiness.ready is False
    assert readiness.blocking_reasons == [
        "Venue hyperliquid is missing collateral data for the confirmed 11.00 notional preview"
    ]


def test_build_live_submission_readiness_allows_positive_collateral() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=9,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=12,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.09229,
                    worst_price_text="0.09229",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[
                        "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                        "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                    ],
                    auth_scheme="account address + API wallet private key",
                    payload={"coin": "ARB"},
                    notes=[],
                )
            ],
        ),
    )

    readiness = build_live_submission_readiness(
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        confirmations=[confirmation],
        execution_preflight=PaperTradeExecutionPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[],
            blocking_reasons=[],
        ),
        account_preflight=PaperTradeAccountPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[
                VenueAccountPreflight(
                    venue="hyperliquid",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_wallet",
                    available_to_trade=25.0,
                )
            ],
            blocking_reasons=[],
        ),
    )

    assert readiness.ready is True
    assert readiness.blocking_reasons == []


def test_build_live_submission_readiness_blocks_only_zero_collateral_venues() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=9,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=12,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.09229,
                    worst_price_text="0.09229",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[
                        "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                        "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                    ],
                    auth_scheme="account address + API wallet private key",
                    payload={"coin": "ARB"},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0921,
                    worst_price_text="0.0921",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[
                        "CARRYME_API_EXTENDED_API_KEY",
                        "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                    ],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD"},
                    notes=[],
                ),
            ],
        ),
    )

    readiness = build_live_submission_readiness(
        paper_trade_id=6,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        confirmations=[confirmation],
        execution_preflight=PaperTradeExecutionPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[],
            blocking_reasons=[],
        ),
        account_preflight=PaperTradeAccountPreflight(
            paper_trade_id=6,
            label="arb_extended_hyperliquid",
            ready=True,
            venues=[
                VenueAccountPreflight(
                    venue="hyperliquid",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_wallet",
                    available_to_trade=0.0,
                    free_collateral=0.0,
                    total_collateral=0.0,
                ),
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    free_collateral=22.0,
                ),
            ],
            blocking_reasons=[],
        ),
    )

    assert readiness.ready is False
    assert readiness.blocking_reasons == [
        "Venue hyperliquid has no usable collateral for the confirmed 11.00 notional preview"
    ]


def test_build_live_submission_readiness_blocks_degraded_system_state() -> None:
    confirmation = PreviewConfirmationEntry(
        entry_id=9,
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
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=11.0,
                    quantity=120.0,
                    quantity_text="120.00000000",
                    reference_price=0.0915,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0916,
                    worst_price_text="0.09160000",
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
        paper_trade_id=7,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        confirmations=[confirmation],
        execution_preflight=PaperTradeExecutionPreflight(
            paper_trade_id=7,
            label="arb_extended_paradex",
            ready=True,
            venues=[],
            blocking_reasons=[],
        ),
        account_preflight=PaperTradeAccountPreflight(
            paper_trade_id=7,
            label="arb_extended_paradex",
            ready=True,
            venues=[],
            blocking_reasons=[],
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=7,
            label="arb_extended_paradex",
            ready=False,
            venues=[
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=False,
                    status="maintenance",
                    blocking_reasons=["Paradex system state is maintenance"],
                )
            ],
            blocking_reasons=["Paradex system state is maintenance"],
        ),
    )

    assert readiness.ready is False
    assert readiness.system_state is not None
    assert "Paradex system state is maintenance" in readiness.blocking_reasons


def test_paradex_system_state_probe_accepts_ok_status() -> None:
    class StubConnector:
        def __init__(self, client: httpx.AsyncClient) -> None:
            _ = client

        async def fetch_system_state(self) -> dict[str, str]:
            return {"status": "ok"}

    probe = ParadexSystemStateProbe(connector_factory=cast(Any, StubConnector))

    status = asyncio.run(probe.probe({"enabled": True}))

    assert status.venue == "paradex"
    assert status.checked is True
    assert status.healthy is True
    assert status.status == "ok"
    assert status.blocking_reasons == []


def test_system_state_service_scopes_to_paper_trade_venues() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate",
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.001,
            break_even_days_entry=0.5,
            capacity_limit_notional=100.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro_fastfills",
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

    class StubProbe:
        def __init__(self, venue: str, healthy: bool) -> None:
            self.venue = venue
            self.healthy = healthy

        async def probe(self, config: dict[str, bool]) -> VenueSystemState:
            return VenueSystemState(
                venue=self.venue,
                enabled=config["enabled"],
                checked=True,
                healthy=self.healthy,
                status="ok" if self.healthy else "maintenance",
                blocking_reasons=[] if self.healthy else [f"{self.venue} unavailable"],
            )

    service = SystemStateService(
        probes=cast(
            Any,
            {
                "extended": StubProbe("extended", True),
                "paradex": StubProbe("paradex", False),
                "hyperliquid": StubProbe("hyperliquid", True),
            },
        ),
    )

    result = asyncio.run(
        service.probe_paper_trade(
            paper_trade,
            {
                "extended": {"enabled": True},
                "paradex": {"enabled": True},
                "hyperliquid": {"enabled": True},
            },
        )
    )

    assert result.paper_trade_id == 7
    assert [item.venue for item in result.venues] == ["paradex", "extended"]
    assert result.ready is False
    assert result.blocking_reasons == ["paradex unavailable"]


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


def test_order_preview_service_builds_hyperliquid_leg() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=6,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=FundingPairTradeIntent(
            label="arb_extended_hyperliquid",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.0008,
            break_even_days_entry=0.5,
            capacity_limit_notional=100.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="hyperliquid",
                symbol="ARB",
                fee_profile="tier0",
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
        ("hyperliquid", "ARB"): _snapshot(
            "hyperliquid",
            "ARB",
            -0.0004,
            0.0918,
            500,
            0.0922,
            450,
            raw={
                "szDecimals": 1,
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
        legs = {item.venue: item for item in preview.legs}
        hyperliquid = legs["hyperliquid"]

        assert hyperliquid.reference_price == pytest.approx(0.0922)
        assert hyperliquid.quantity == pytest.approx(119.3)
        assert hyperliquid.quantity_text == "119.3"
        assert hyperliquid.worst_price_text == "0.09229"
        assert hyperliquid.minimum_notional == pytest.approx(10.0)
        assert hyperliquid.endpoint_path_hint == "/exchange"
        assert hyperliquid.auth_scheme == "account address + API wallet private key"
        assert hyperliquid.payload == {
            "coin": "ARB",
            "is_buy": True,
            "sz": "119.3",
            "limit_px": "0.09229",
            "order_type": {"limit": {"tif": "Ioc"}},
            "reduce_only": False,
            "client_order_id": "carryme-pt6-hyperliquid-buy",
        }

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


def test_hyperliquid_account_probe_reports_missing_credentials() -> None:
    async def run() -> None:
        probe = HyperliquidAccountProbe()
        result = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": None,
                    "api_wallet_private_key": None,
                },
            }
        )
        assert result.venue == "hyperliquid"
        assert result.authenticated is False
        assert result.ready is False
        assert result.credential_mode == "api_wallet"
        assert set(result.missing_env_vars) == {
            "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
            "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
        }

    asyncio.run(run())


def test_hyperliquid_account_probe_reads_sdk_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubInfo:
        def user_state(self, address: str) -> dict[str, object]:
            assert address == "0xhyper"
            return {
                "marginSummary": {
                    "accountValue": "9.80",
                },
                "withdrawable": "8.15",
                "assetPositions": [
                    {"position": {"coin": "ARB", "szi": "10"}},
                    {"position": {"coin": "STRK", "szi": "-25"}},
                ],
            }

        def open_orders(self, address: str) -> list[dict[str, object]]:
            assert address == "0xhyper"
            return [{"coin": "ARB", "oid": 123}]

    monkeypatch.setattr(
        "carryme_runtime.account_preflight.build_hyperliquid_info",
        lambda *, base_url, timeout=15.0: StubInfo(),
    )

    async def run() -> None:
        probe = HyperliquidAccountProbe()
        result = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xhyper",
                    "api_wallet_private_key": "0xwallet",
                },
            }
        )
        assert result.venue == "hyperliquid"
        assert result.authenticated is True
        assert result.ready is True
        assert result.account_identifier == "0xhyper"
        assert result.total_collateral == 9.8
        assert result.available_to_trade == 8.15
        assert result.free_collateral == 8.15
        assert result.balance_assets == ["USDC"]
        assert result.position_symbols == ["ARB", "STRK"]
        assert result.position_count == 2
        assert "Observed 1 currently open Hyperliquid orders." in result.notes

    asyncio.run(run())


def test_hyperliquid_account_probe_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def slow_fetch(account_address: str) -> tuple[dict[str, object], list[dict[str, object]]]:
        assert account_address == "0xhyper"
        time.sleep(0.05)
        return {}, []

    monkeypatch.setattr(
        "carryme_runtime.account_preflight._fetch_hyperliquid_account_state",
        slow_fetch,
    )
    monkeypatch.setattr(
        "carryme_runtime.account_preflight.HYPERLIQUID_ACCOUNT_READ_TIMEOUT_SECONDS",
        0.01,
    )

    async def run() -> None:
        probe = HyperliquidAccountProbe()
        result = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xhyper",
                    "api_wallet_private_key": "0xwallet",
                },
            }
        )
        assert result.venue == "hyperliquid"
        assert result.authenticated is False
        assert result.ready is False
        assert result.blocking_reasons == ["Hyperliquid account read timed out"]
        assert any("timeout elapsed" in note for note in result.notes)

    asyncio.run(run())


def test_hyperliquid_account_probe_rejects_malformed_sdk_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubInfo:
        def user_state(self, address: str) -> dict[str, object]:
            assert address == "0xhyper"
            return {
                "marginSummary": {
                    "accountValue": "not-a-number",
                },
                "withdrawable": "8.15",
                "assetPositions": [],
            }

        def open_orders(self, address: str) -> list[dict[str, object]]:
            assert address == "0xhyper"
            return []

    monkeypatch.setattr(
        "carryme_runtime.account_preflight.build_hyperliquid_info",
        lambda *, base_url, timeout=15.0: StubInfo(),
    )

    async def run() -> None:
        probe = HyperliquidAccountProbe()
        result = await probe.probe(
            {
                "enabled": True,
                "credentials": {
                    "account_address": "0xhyper",
                    "api_wallet_private_key": "0xwallet",
                },
            }
        )
        assert result.venue == "hyperliquid"
        assert result.authenticated is False
        assert result.ready is False
        assert any("malformed payload" in reason for reason in result.blocking_reasons)
        assert any("malformed account data" in note for note in result.notes)

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


def test_build_paradex_auth_headers_are_deterministic() -> None:
    first = build_paradex_auth_headers(
        account_address="0x123",
        private_key="0x456",
        starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET",
        issued_at=1_700_000_000,
        expires_at=1_700_086_400,
    )
    second = build_paradex_auth_headers(
        account_address="0x123",
        private_key="0x456",
        starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET",
        issued_at=1_700_000_000,
        expires_at=1_700_086_400,
    )

    assert first == second


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


def test_build_paradex_auth_request_path_supports_interactive_usage() -> None:
    default_path = build_paradex_auth_request_path(private_key="0x456")
    auth_path = build_paradex_auth_request_path(
        private_key="0x456",
        token_usage="interactive",
    )

    assert auth_path.startswith("/v1/auth/")
    assert auth_path == f"{default_path}?token_usage=interactive"


def test_paradex_jwt_token_provider_supports_interactive_auth_path() -> None:
    requests: list[str] = []
    auth_path = build_paradex_auth_request_path(
        private_key="0x456",
        token_usage="interactive",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(f"{request.method} {request.url.raw_path.decode('utf-8')}")
        if request.url.path == "/v1/system/config":
            return httpx.Response(
                200,
                json={"starknet_chain_id": "PRIVATE_SN_PARACLEAR_MAINNET"},
            )
        if request.url.raw_path.decode("utf-8") == auth_path:
            return httpx.Response(200, json={"jwt_token": "interactive-jwt"})
        raise AssertionError(f"Unexpected request path: {request.url.raw_path.decode('utf-8')}")

    async def run() -> None:
        provider = ParadexJwtTokenProvider(token_usage="interactive")
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
        assert token == "interactive-jwt"
        assert requests == [
            "GET /v1/system/config",
            f"POST {auth_path}",
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


def test_build_signed_paradex_market_order_payload_omits_price() -> None:
    payload = build_signed_paradex_order_payload(
        account_address="0x123",
        private_key="0x456",
        starknet_chain_id="PRIVATE_SN_PARACLEAR_MAINNET",
        order_payload={
            "market": "WLD-USD-PERP",
            "side": "BUY",
            "type": "MARKET",
            "size": "93.5",
            "price": "0",
            "instruction": "IOC",
            "client_id": "carryme-cleanup-pt7-paradex-buy-mkt",
            "reduce_only": True,
        },
        signature_timestamp_ms=1_700_000_000_000,
        recv_window_ms=45_000,
    )

    assert payload["market"] == "WLD-USD-PERP"
    assert payload["type"] == "MARKET"
    assert payload["flags"] == ["REDUCE_ONLY"]
    assert "price" not in payload


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
    seen_requests: list[dict[str, Any]] = []

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
        assert request.headers["Authorization"] == "Bearer live-jwt"
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            seen_requests.append(payload)
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
        assert request.url.path == "/v1/orders/order-1"
        return httpx.Response(
            200,
            json={
                "id": "order-1",
                "client_id": "carryme-pt7-paradex-buy",
                "status": "CLOSED",
                "avg_fill_price": "0.0923",
                "remaining_size": "0",
                "size": "10845.9",
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
        assert len(entry.legs[0].response_payload["attempt_history"]) == 1
        assert entry.legs[0].response_payload["observed_order_state"]["derived_state"] == "filled"
        assert seen_requests[0]["client_id"] == "carryme-pt7-paradex-buy"

    asyncio.run(run())


def test_paradex_live_execution_service_uses_interactive_auth_for_retail_fee_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_auth_paths: list[str] = []
    seen_order_auth_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        raw_path = request.url.raw_path.decode("utf-8")
        if request.url.path == "/v1/system/config":
            return httpx.Response(
                200,
                json={"starknet_chain_id": "PRIVATE_SN_PARACLEAR_MAINNET"},
            )
        if raw_path.endswith("?token_usage=interactive"):
            seen_auth_paths.append(raw_path)
            return httpx.Response(200, json={"jwt_token": "interactive-jwt"})
        if request.url.path.startswith("/v1/auth/"):
            seen_auth_paths.append(raw_path)
            return httpx.Response(200, json={"jwt_token": "default-jwt"})
        if request.url.path == "/v1/orders":
            seen_order_auth_headers.append(request.headers["Authorization"])
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "id": "order-1",
                    "status": "NEW",
                    "client_id": payload["client_id"],
                },
            )
        assert request.url.path == "/v1/orders/order-1"
        return httpx.Response(
            200,
            json={
                "id": "order-1",
                "client_id": "carryme-pt7-paradex-buy",
                "status": "CLOSED",
                "avg_fill_price": "0.0923",
                "remaining_size": "0",
                "size": "10845.9",
            },
        )

    real_async_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)

    confirmation = PreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=7,
        label="arb_extended_paradex_retail",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=7,
            label="arb_extended_paradex_retail",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=12,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="retail",
                    side="buy",
                    target_notional=1000.0,
                    quantity=10845.9,
                    quantity_text="10845.90000000",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.09230000",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[
                        "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                        "CARRYME_API_PARADEX_PRIVATE_KEY",
                    ],
                    auth_scheme="main account address + subkey private key",
                    payload={
                        "market": "ARB-USD-PERP",
                        "side": "BUY",
                        "type": "LIMIT",
                        "size": "10845.9",
                        "price": "0.0923",
                        "instruction": "IOC",
                        "client_id": "carryme-pt7-paradex-buy",
                    },
                    notes=[],
                )
            ],
        ),
    )
    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex_retail",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 59, tzinfo=UTC),
            one_day_net_edge_after_entry=0.002,
            break_even_days_entry=0.5,
            capacity_limit_notional=1000.0,
            target_notional=1000.0,
            capacity_fraction=1.0,
            max_target_notional=1000.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="retail",
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
        service = ParadexLiveExecutionService(
            account_address="0xabc",
            private_key="0x123",
            adaptive_retry_attempts=1,
            adaptive_retry_poll_attempts=1,
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        )
        assert entry.legs[0].auth_usage == "interactive"

    asyncio.run(run())

    assert any("?token_usage=interactive" in path for path in seen_auth_paths)
    assert seen_order_auth_headers == ["Bearer interactive-jwt"]


def test_paradex_live_execution_service_rejects_interactive_auth_with_non_jwt_provider() -> None:
    class StubTokenProvider:
        async def fetch_system_config(
            self,
            client: httpx.AsyncClient | None = None,
        ) -> object:
            raise AssertionError("should not fetch system config when interactive auth is invalid")

        async def issue_jwt_token(
            self,
            *,
            account_address: str,
            private_key: str,
            client: httpx.AsyncClient | None = None,
            now: int | None = None,
        ) -> str:
            raise AssertionError("should not issue JWT when interactive auth is invalid")

    confirmation = PreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=7,
        label="arb_extended_paradex_retail",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=7,
            label="arb_extended_paradex_retail",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=12,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="retail",
                    side="buy",
                    target_notional=1000.0,
                    quantity=10845.9,
                    quantity_text="10845.90000000",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.09230000",
                    order_type="limit",
                    time_in_force="ioc",
                    http_method="POST",
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[
                        "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                        "CARRYME_API_PARADEX_PRIVATE_KEY",
                    ],
                    auth_scheme="main account address + subkey private key",
                    payload={
                        "market": "ARB-USD-PERP",
                        "side": "BUY",
                        "type": "LIMIT",
                        "size": "10845.9",
                        "price": "0.0923",
                        "instruction": "IOC",
                        "client_id": "carryme-pt7-paradex-buy",
                    },
                    notes=[],
                )
            ],
        ),
    )
    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex_retail",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 59, tzinfo=UTC),
            one_day_net_edge_after_entry=0.002,
            break_even_days_entry=0.5,
            capacity_limit_notional=1000.0,
            target_notional=1000.0,
            capacity_fraction=1.0,
            max_target_notional=1000.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="retail",
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
        service = ParadexLiveExecutionService(
            account_address="0xabc",
            private_key="0x123",
            token_provider=StubTokenProvider(),
        )
        with pytest.raises(
            ValueError,
            match="Interactive Paradex auth requires ParadexJwtTokenProvider",
        ):
            await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
            )

    asyncio.run(run())


def test_paradex_live_execution_service_retries_unfilled_orders_within_confirmed_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_requests: list[dict[str, Any]] = []

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

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "paradex"
        assert symbol == "ARB-USD-PERP"
        return _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0002,
            0.0920,
            20_000,
            0.0921,
            20_000,
            raw={
                "order_size_increment": "0.1",
                "min_notional": "10",
                "price_tick_size": "0.0001",
                "max_order_size": "12000000",
            },
        )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer live-jwt"
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            seen_requests.append(payload)
            order_id = f"order-{len(seen_requests)}"
            return httpx.Response(
                200,
                json={
                    "id": order_id,
                    "status": "NEW",
                    "client_id": payload["client_id"],
                },
            )
        if request.url.path == "/v1/orders/order-1":
            return httpx.Response(
                200,
                json={
                    "id": "order-1",
                    "client_id": "carryme-pt7-paradex-buy",
                    "status": "CLOSED",
                    "avg_fill_price": "",
                    "remaining_size": "10845.9",
                    "size": "10845.9",
                },
            )
        assert request.url.path == "/v1/orders/order-2"
        return httpx.Response(
            200,
            json={
                "id": "order-2",
                "client_id": "carryme-pt7-paradex-buy-r2",
                "status": "CLOSED",
                "avg_fill_price": "0.0922",
                "remaining_size": "0",
                "size": "10845.9",
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
            fetch_snapshot=cast(Any, fetch_snapshot),
            adaptive_retry_attempts=2,
            adaptive_retry_poll_attempts=1,
            adaptive_retry_poll_interval_seconds=0,
            adaptive_retry_book_slippage_bps=5,
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "submitted"
        assert entry.legs[0].external_reference == "order-2"
        assert entry.legs[0].request_payload is not None
        assert entry.legs[0].request_payload["client_id"] == "carryme-pt7-paradex-buy-r2"
        assert entry.legs[0].request_payload["price"] == "0.09230000"
        response_payload = entry.legs[0].response_payload
        assert isinstance(response_payload, dict)
        attempt_history = response_payload["attempt_history"]
        assert isinstance(attempt_history, list)
        assert len(attempt_history) == 2
        first_attempt = attempt_history[0]
        assert isinstance(first_attempt, dict)
        observed_order_state = first_attempt["observed_order_state"]
        assert isinstance(observed_order_state, dict)
        assert observed_order_state["derived_state"] == "unfilled"
        final_observed_order_state = response_payload["observed_order_state"]
        assert isinstance(final_observed_order_state, dict)
        assert final_observed_order_state["derived_state"] == "filled"
        assert [item["client_id"] for item in seen_requests] == [
            "carryme-pt7-paradex-buy",
            "carryme-pt7-paradex-buy-r2",
        ]
        assert [item["price"] for item in seen_requests] == [
            "0.09230000",
            "0.09230000",
        ]

    asyncio.run(run())


def test_paradex_live_execution_service_returns_last_attempt_when_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_requests: list[dict[str, Any]] = []

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

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "paradex"
        assert symbol == "ARB-USD-PERP"
        return _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0002,
            0.0920,
            20_000,
            0.0921,
            20_000,
            raw={
                "order_size_increment": "0.1",
                "min_notional": "10",
                "price_tick_size": "0.0001",
                "max_order_size": "12000000",
            },
        )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer live-jwt"
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            seen_requests.append(payload)
            order_id = f"order-{len(seen_requests)}"
            return httpx.Response(
                200,
                json={
                    "id": order_id,
                    "status": "NEW",
                    "client_id": payload["client_id"],
                },
            )
        assert request.url.path in {"/v1/orders/order-1", "/v1/orders/order-2"}
        return httpx.Response(
            200,
            json={
                "id": request.url.path.rsplit("/", 1)[-1],
                "client_id": (
                    "carryme-pt7-paradex-buy"
                    if request.url.path.endswith("order-1")
                    else "carryme-pt7-paradex-buy-r2"
                ),
                "status": "CLOSED",
                "avg_fill_price": "",
                "remaining_size": "10845.9",
                "size": "10845.9",
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
            fetch_snapshot=cast(Any, fetch_snapshot),
            adaptive_retry_attempts=2,
            adaptive_retry_poll_attempts=1,
            adaptive_retry_poll_interval_seconds=0,
            adaptive_retry_book_slippage_bps=5,
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "submitted"
        assert entry.legs[0].external_reference == "order-2"
        response_payload = entry.legs[0].response_payload
        assert isinstance(response_payload, dict)
        assert response_payload["observed_order_state"]["derived_state"] == "unfilled"
        attempt_history = response_payload["attempt_history"]
        assert isinstance(attempt_history, list)
        assert len(attempt_history) == 2
        assert [item["client_id"] for item in seen_requests] == [
            "carryme-pt7-paradex-buy",
            "carryme-pt7-paradex-buy-r2",
        ]

    asyncio.run(run())


def test_paradex_live_execution_service_stops_retrying_on_partial_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_requests: list[dict[str, Any]] = []

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
            return "live-jwt"

    async def fetch_snapshot(_: str, __: str) -> NormalizedMarketSnapshot:
        raise AssertionError("partial_fill should be terminal and skip retry repricing")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer live-jwt"
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            seen_requests.append(payload)
            return httpx.Response(
                200,
                json={
                    "id": "order-1",
                    "status": "NEW",
                    "client_id": payload["client_id"],
                },
            )
        assert request.url.path == "/v1/orders/order-1"
        return httpx.Response(
            200,
            json={
                "id": "order-1",
                "client_id": "carryme-pt7-paradex-buy",
                "status": "CLOSED",
                "avg_fill_price": "0.0922",
                "remaining_size": "1",
                "size": "10845.9",
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
            fetch_snapshot=cast(Any, fetch_snapshot),
            adaptive_retry_attempts=2,
            adaptive_retry_poll_attempts=1,
            adaptive_retry_poll_interval_seconds=0,
            adaptive_retry_book_slippage_bps=5,
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "submitted"
        assert entry.legs[0].external_reference == "order-1"
        response_payload = entry.legs[0].response_payload
        assert isinstance(response_payload, dict)
        assert response_payload["observed_order_state"]["derived_state"] == "partial_fill"
        assert len(response_payload["attempt_history"]) == 1
        assert [item["client_id"] for item in seen_requests] == ["carryme-pt7-paradex-buy"]

    asyncio.run(run())


def test_paradex_live_execution_service_rejects_retry_without_usable_best_ask(
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
            return "live-jwt"

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "paradex"
        assert symbol == "ARB-USD-PERP"
        return normalize_market_snapshot(
            venue,
            MarketStats(
                venue=venue,
                symbol=symbol,
                mark_price=0.09205,
                funding_rate=-0.0002,
                open_interest=1_000_000,
                daily_volume=500_000,
                top_of_book=TopOfBook(
                    best_bid_price=0.0920,
                    best_bid_size=20_000,
                    best_ask_price=None,
                    best_ask_size=20_000,
                ),
                raw={
                    "order_size_increment": "0.1",
                    "min_notional": "10",
                    "price_tick_size": "0.0001",
                    "max_order_size": "12000000",
                },
            ),
        )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer live-jwt"
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "id": "order-1",
                    "status": "NEW",
                    "client_id": payload["client_id"],
                },
            )
        assert request.url.path == "/v1/orders/order-1"
        return httpx.Response(
            200,
            json={
                "id": "order-1",
                "client_id": "carryme-pt7-paradex-buy",
                "status": "CLOSED",
                "avg_fill_price": "",
                "remaining_size": "10845.9",
                "size": "10845.9",
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
            fetch_snapshot=fetch_snapshot,
            adaptive_retry_attempts=2,
            adaptive_retry_poll_attempts=1,
            adaptive_retry_poll_interval_seconds=0,
            adaptive_retry_book_slippage_bps=5,
        )
        with pytest.raises(ValueError, match="missing a usable best_ask"):
            await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )

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
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            seen_request.update(payload)
            assert payload["market"] == "ARB-USD-PERP"
            assert payload["side"] == "SELL"
            assert payload["flags"] == ["REDUCE_ONLY"]
            return httpx.Response(201, json={"id": "cleanup-order-1", "status": "NEW"})
        assert request.url.path == "/v1/orders/cleanup-order-1"
        return httpx.Response(
            200,
            json={
                "id": "cleanup-order-1",
                "market": "ARB-USD-PERP",
                "status": "NEW",
                "remaining_size": "123.1",
                "size": "123.1",
                "client_id": "carryme-cleanup-pt8-paradex-sell",
            },
        )

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


def test_paradex_live_execution_service_uses_market_fallback_for_unfilled_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_requests: list[dict[str, Any]] = []

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

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "paradex"
        assert symbol == "ARB-USD-PERP"
        return _snapshot(
            "paradex",
            "ARB-USD-PERP",
            -0.0002,
            0.0890,
            20_000,
            0.0892,
            20_000,
            raw={
                "order_size_increment": "0.1",
                "min_notional": "10",
                "price_tick_size": "0.0001",
                "max_order_size": "12000000",
            },
        )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer jwt-token"
        if request.url.path == "/v1/orders":
            payload = json.loads(request.content.decode("utf-8"))
            seen_requests.append(payload)
            return httpx.Response(
                200,
                json={
                    "id": f"order-{len(seen_requests)}",
                    "status": "NEW",
                    "client_id": payload["client_id"],
                },
            )
        if request.url.path in {"/v1/orders/order-1", "/v1/orders/order-2"}:
            return httpx.Response(
                200,
                json={
                    "id": request.url.path.rsplit("/", 1)[-1],
                    "status": "CLOSED",
                    "avg_fill_price": "",
                    "remaining_size": "123.1",
                    "size": "123.1",
                },
            )
        assert request.url.path == "/v1/orders/order-3"
        return httpx.Response(
            200,
            json={
                "id": "order-3",
                "status": "CLOSED",
                "avg_fill_price": "0.0894",
                "remaining_size": "0",
                "size": "123.1",
            },
        )

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
            fetch_snapshot=cast(Any, fetch_snapshot),
            adaptive_retry_attempts=2,
            adaptive_retry_poll_attempts=1,
            adaptive_retry_poll_interval_seconds=0,
            adaptive_retry_book_slippage_bps=5,
        )
        entry = await service.submit_confirmed_cleanup_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        response_payload = entry.legs[0].response_payload
        assert isinstance(response_payload, dict)
        attempt_history = response_payload["attempt_history"]
        assert isinstance(attempt_history, list)
        assert len(attempt_history) == 3
        assert [item["type"] for item in seen_requests] == ["LIMIT", "LIMIT", "MARKET"]
        assert "price" not in seen_requests[2]
        assert seen_requests[2]["client_id"] == "carryme-cleanup-pt8-paradex-sell-mkt"
        assert response_payload["observed_order_state"]["derived_state"] == "filled"

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


def test_paired_live_execution_coordinator_auto_prefers_paradex_first() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=14,
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
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=14,
        label="arb_extended_paradex",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=14,
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

    seen_venues: list[str] = []

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
            seen_venues.append(self.venue)
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
        )
        assert entry.adapter == "paired_live:paradex_then_extended"
        assert seen_venues == ["paradex", "extended"]

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


def test_paired_live_execution_coordinator_supports_hyperliquid_leg() -> None:
    paper_trade = PaperTradeEntry(
        entry_id=13,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=FundingPairTradeIntent(
            label="arb_extended_hyperliquid",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00042,
            break_even_days_entry=0.63,
            capacity_limit_notional=126.83,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="hyperliquid",
                symbol="ARB",
                fee_profile="tier0",
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
    confirmation = PreviewConfirmationEntry(
        entry_id=10,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=13,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=13,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0890,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0889,
                    worst_price_text="0.0889",
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD"},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.09229,
                    worst_price_text="0.09229",
                    endpoint_path_hint="/exchange",
                    auth_scheme="account address + API wallet private key",
                    payload={"coin": "ARB"},
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
            symbol = next(
                leg.symbol for leg in confirmation.preview.legs if leg.venue == self.venue
            )
            fee_profile = "default" if self.venue == "extended" else "tier0"
            side: Literal["buy", "sell"] = "sell" if self.venue == "extended" else "buy"
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
                        symbol=symbol,
                        fee_profile=fee_profile,
                        side=side,
                        target_notional=11.0,
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
                "hyperliquid": StubService("hyperliquid"),
            }
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            first_venue="extended",
        )
        assert entry.status == "submitted"
        assert entry.adapter == "paired_live:extended_then_hyperliquid"
        assert [leg.venue for leg in entry.legs] == ["extended", "hyperliquid"]

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


def test_account_preflight_ignores_closed_or_zero_size_positions() -> None:
    positions = {
        "results": [
            {"market": "ARB-USD-PERP", "status": "CLOSED", "size": "0"},
            {"symbol": "STRK-USD", "status": "OPEN", "size": "25"},
            {"ticker": "ETH-USD-PERP", "size": "0"},
            {"market": "SOL-USD-PERP", "size": "-3"},
        ]
    }

    assert _extract_position_symbols(positions) == ["STRK-USD", "SOL-USD-PERP"]


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


def test_hyperliquid_order_state_observer_classifies_filled_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubInfo:
        def query_order_by_oid(self, address: str, oid: int) -> dict[str, object]:
            assert address == "0xhyper"
            assert oid == 777
            return {
                "status": "filled",
                "order": {
                    "coin": "ARB",
                    "origSz": "119.3",
                    "sz": "0",
                    "avgPx": "0.0923",
                },
            }

    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_info",
        lambda: StubInfo(),
    )

    async def run() -> None:
        observer = HyperliquidOrderStateObserver(
            account_address="0xhyper",
            websocket_timeout_seconds=0,
        )
        state = await observer.observe(
            {
                "venue": "hyperliquid",
                "external_reference": "777",
                "request_payload": {},
            }
        )
        assert state.supported is True
        assert state.derived_state == "filled"
        assert state.order_status == "filled"
        assert state.avg_fill_price == "0.0923"
        assert state.observation_source == "rest_poll"

    asyncio.run(run())


def test_hyperliquid_order_state_observer_prefers_websocket_order_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubWebsocketManager:
        def __init__(self) -> None:
            self.stopped = False
            self.unsubscribed = False

        def __enter__(self) -> "StubWebsocketManager":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.stop()

        def subscribe(self, subscription: dict[str, str], callback: Any) -> int:
            if subscription["type"] == "orderUpdates":
                callback(
                    {
                        "channel": "orderUpdates",
                        "data": [
                            {
                                "oid": 777,
                                "status": "filled",
                                "order": {
                                    "cloid": "0xabc",
                                    "origSz": "119.3",
                                    "sz": "0",
                                    "avgPx": "0.0923",
                                },
                            }
                        ],
                    }
                )
            return 1

        def unsubscribe(self, subscription: dict[str, str], subscription_id: int) -> bool:
            self.unsubscribed = True
            return True

        def stop(self) -> None:
            self.stopped = True

    stub_manager = StubWebsocketManager()
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_websocket_manager",
        lambda: stub_manager,
    )
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_info",
        lambda: pytest.fail("REST fallback should not be used when websocket returns a match"),
    )

    async def run() -> None:
        observer = HyperliquidOrderStateObserver(account_address="0xhyper")
        state = await observer.observe(
            {
                "venue": "hyperliquid",
                "external_reference": "777",
                "request_payload": {},
            }
        )
        assert state.supported is True
        assert state.derived_state == "filled"
        assert state.order_status == "filled"
        assert state.avg_fill_price == "0.0923"
        assert state.client_id == "0xabc"
        assert state.observation_source == "websocket_order_updates"
        assert stub_manager.unsubscribed is True
        assert stub_manager.stopped is True

    asyncio.run(run())


def test_hyperliquid_order_state_observer_falls_back_when_websocket_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubWebsocketManager:
        def __init__(self) -> None:
            self.stopped = False
            self.unsubscribed = False

        def __enter__(self) -> "StubWebsocketManager":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.stop()

        def subscribe(self, subscription: dict[str, str], callback: Any) -> int:
            return 1

        def unsubscribe(self, subscription: dict[str, str], subscription_id: int) -> bool:
            self.unsubscribed = True
            return True

        def stop(self) -> None:
            self.stopped = True

    class StubInfo:
        def query_order_by_oid(self, address: str, oid: int) -> dict[str, object]:
            assert address == "0xhyper"
            assert oid == 777
            return {
                "status": "open",
                "order": {
                    "coin": "ARB",
                    "origSz": "119.3",
                    "sz": "119.3",
                    "avgPx": None,
                },
            }

    stub_manager = StubWebsocketManager()
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_websocket_manager",
        lambda: stub_manager,
    )
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_info",
        lambda: StubInfo(),
    )

    async def run() -> None:
        observer = HyperliquidOrderStateObserver(
            account_address="0xhyper",
            websocket_timeout_seconds=0.01,
        )
        state = await observer.observe(
            {
                "venue": "hyperliquid",
                "external_reference": "777",
                "request_payload": {},
            }
        )
        assert state.supported is True
        assert state.derived_state == "open"
        assert state.observation_source == "rest_poll"
        assert state.notes == [
            (
                "Hyperliquid websocket did not yield a terminal update before timeout; "
                "fell back to REST."
            )
        ]
        assert stub_manager.unsubscribed is True
        assert stub_manager.stopped is True

    asyncio.run(run())


def test_hyperliquid_order_state_observer_prefers_vault_address_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubInfo:
        def query_order_by_oid(self, address: str, oid: int) -> dict[str, object]:
            assert address == "0xvault"
            assert oid == 777
            return {
                "status": "filled",
                "order": {
                    "coin": "ARB",
                    "origSz": "119.3",
                    "sz": "0",
                    "avgPx": "0.0923",
                },
            }

    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_info",
        lambda: StubInfo(),
    )

    async def run() -> None:
        observer = HyperliquidOrderStateObserver(
            account_address="0xhyper",
            vault_address="0xvault",
            websocket_timeout_seconds=0,
        )
        state = await observer.observe(
            {
                "venue": "hyperliquid",
                "external_reference": "777",
                "request_payload": {},
            }
        )
        assert state.supported is True
        assert state.derived_state == "filled"
        assert state.order_status == "filled"
        assert state.avg_fill_price == "0.0923"

    asyncio.run(run())


def test_hyperliquid_order_state_observer_prefers_vault_address_for_websocket_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubWebsocketManager:
        def __init__(self) -> None:
            self.stopped = False
            self.unsubscribed = False

        def __enter__(self) -> "StubWebsocketManager":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.stop()

        def subscribe(self, subscription: dict[str, str], callback: Any) -> int:
            assert subscription["user"] == "0xvault"
            if subscription["type"] == "orderUpdates":
                callback(
                    {
                        "channel": "orderUpdates",
                        "data": [
                            {
                                "oid": 777,
                                "status": "filled",
                                "order": {
                                    "cloid": "0xvault-order",
                                    "origSz": "119.3",
                                    "sz": "0",
                                    "avgPx": "0.0923",
                                },
                            }
                        ],
                    }
                )
            return 1

        def unsubscribe(self, subscription: dict[str, str], subscription_id: int) -> bool:
            assert subscription["user"] == "0xvault"
            self.unsubscribed = True
            return True

        def stop(self) -> None:
            self.stopped = True

    stub_manager = StubWebsocketManager()
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_websocket_manager",
        lambda: stub_manager,
    )
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_info",
        lambda: pytest.fail("REST fallback should not be used when websocket returns a match"),
    )

    async def run() -> None:
        observer = HyperliquidOrderStateObserver(
            account_address="0xhyper",
            vault_address="0xvault",
        )
        state = await observer.observe(
            {
                "venue": "hyperliquid",
                "external_reference": "777",
                "request_payload": {},
            }
        )
        assert state.supported is True
        assert state.derived_state == "filled"
        assert state.order_status == "filled"
        assert state.avg_fill_price == "0.0923"
        assert state.client_id == "0xvault-order"
        assert state.observation_source == "websocket_order_updates"
        assert stub_manager.unsubscribed is True
        assert stub_manager.stopped is True

    asyncio.run(run())


def test_hyperliquid_order_state_observer_handles_userfills_edge_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubWebsocketManager:
        def __init__(self) -> None:
            self.stopped = False
            self.unsubscribed = False

        def __enter__(self) -> "StubWebsocketManager":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.stop()

        def subscribe(self, subscription: dict[str, str], callback: Any) -> int:
            if subscription["type"] == "userFills":
                callback(
                    {
                        "channel": "userFills",
                        "data": {
                            "isSnapshot": False,
                            "fills": [
                                {
                                    "oid": 777,
                                    "px": "0.0923",
                                    "sz": "10.0",
                                }
                            ],
                        },
                    }
                )
            return 1

        def unsubscribe(self, subscription: dict[str, str], subscription_id: int) -> bool:
            self.unsubscribed = True
            return True

        def stop(self) -> None:
            self.stopped = True

    class StubInfo:
        def query_order_by_oid(self, address: str, oid: int) -> dict[str, object]:
            assert address == "0xhyper"
            assert oid == 777
            return {
                "status": "open",
                "order": {
                    "coin": "ARB",
                    "origSz": "119.3",
                    "sz": "109.3",
                    "avgPx": "0.0923",
                },
            }

    stub_manager = StubWebsocketManager()
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_websocket_manager",
        lambda: stub_manager,
    )
    monkeypatch.setattr(
        "carryme_runtime.execution_order_state.build_hyperliquid_info",
        lambda: StubInfo(),
    )

    async def run() -> None:
        observer = HyperliquidOrderStateObserver(
            account_address="0xhyper",
            websocket_timeout_seconds=0.01,
        )
        state = await observer.observe(
            {
                "venue": "hyperliquid",
                "external_reference": "777",
                "request_payload": {},
            }
        )
        assert state.supported is True
        assert state.derived_state == "partial_fill"
        assert state.order_status == "open"
        assert state.observation_source == "rest_poll"
        assert state.notes == [
            (
                "Observed via Hyperliquid websocket userFills; fill events may be partial, "
                "so REST fallback confirms terminal order state."
            )
        ]
        assert stub_manager.unsubscribed is True
        assert stub_manager.stopped is True

    asyncio.run(run())


def test_hyperliquid_live_execution_service_submits_confirmed_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubExchange:
        def order(
            self,
            name: str,
            is_buy: bool,
            sz: float,
            limit_px: float,
            order_type: dict[str, object],
            reduce_only: bool = False,
        ) -> dict[str, object]:
            assert name == "ARB"
            assert is_buy is True
            assert sz == pytest.approx(119.3)
            assert limit_px == pytest.approx(0.0923)
            assert order_type == {"limit": {"tif": "Ioc"}}
            assert reduce_only is False
            return {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {
                                "resting": {
                                    "oid": 777,
                                }
                            }
                        ]
                    },
                },
            }

    def stub_exchange_builder(
        *,
        private_key: str,
        account_address: str,
        vault_address: str | None = None,
    ) -> StubExchange:
        assert private_key == "0xwallet"
        assert account_address == "0xhyper"
        assert vault_address == "0xvault"
        return StubExchange()

    monkeypatch.setattr(
        "carryme_runtime.hyperliquid_live_execution.build_hyperliquid_exchange",
        stub_exchange_builder,
    )

    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=FundingPairTradeIntent(
            label="arb_extended_hyperliquid",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=100.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="hyperliquid",
                symbol="ARB",
                fee_profile="tier0",
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
    confirmation = PreviewConfirmationEntry(
        entry_id=3,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=7,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=7,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=10.99946,
                    quantity=119.3,
                    quantity_text="119.3",
                    quantity_increment=0.1,
                    minimum_order_size=0.1,
                    minimum_notional=10.0,
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.0923",
                    order_type="limit",
                    time_in_force="ioc",
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[
                        "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                        "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                    ],
                    auth_scheme="account address + API wallet private key",
                    payload={
                        "coin": "ARB",
                        "is_buy": True,
                        "sz": "119.3",
                        "limit_px": "0.0923",
                        "order_type": {"limit": {"tif": "Ioc"}},
                        "reduce_only": False,
                        "client_order_id": "carryme-pt7-hyperliquid-buy",
                    },
                    notes=[],
                )
            ],
        ),
        note="operator confirmed",
    )

    async def run() -> None:
        service = HyperliquidLiveExecutionService(
            account_address="0xhyper",
            vault_address="0xvault",
            api_wallet_private_key="0xwallet",
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "submitted"
        assert entry.adapter == "hyperliquid_live"
        assert entry.legs[0].external_reference == "777"
        assert entry.legs[0].request_payload is not None
        assert entry.legs[0].request_payload["coin"] == "ARB"

    asyncio.run(run())


def test_hyperliquid_live_execution_service_submits_confirmed_preview_without_vault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StubExchange:
        def order(
            self,
            name: str,
            is_buy: bool,
            sz: float,
            limit_px: float,
            order_type: dict[str, object],
            reduce_only: bool = False,
        ) -> dict[str, object]:
            assert name == "ARB"
            assert is_buy is True
            assert sz == pytest.approx(119.3)
            assert limit_px == pytest.approx(0.0923)
            assert order_type == {"limit": {"tif": "Ioc"}}
            assert reduce_only is False
            return {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {
                                "resting": {
                                    "oid": 778,
                                }
                            }
                        ]
                    },
                },
            }

    def stub_exchange_builder(
        *,
        private_key: str,
        account_address: str,
        vault_address: str | None = None,
    ) -> StubExchange:
        assert private_key == "0xwallet"
        assert account_address == "0xhyper"
        assert vault_address is None
        return StubExchange()

    monkeypatch.setattr(
        "carryme_runtime.hyperliquid_live_execution.build_hyperliquid_exchange",
        stub_exchange_builder,
    )

    paper_trade = PaperTradeEntry(
        entry_id=8,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="candidate accepted",
        intent=FundingPairTradeIntent(
            label="arb_extended_hyperliquid",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=100.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="hyperliquid",
                symbol="ARB",
                fee_profile="tier0",
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
    confirmation = PreviewConfirmationEntry(
        entry_id=4,
        confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_hyperliquid",
        preview_hash="preview-hash",
        preview=PaperTradeOrderPreview(
            paper_trade_id=8,
            label="arb_extended_hyperliquid",
            generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="preview-hash",
            legs=[
                VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=10.99946,
                    quantity=119.3,
                    quantity_text="119.3",
                    quantity_increment=0.1,
                    minimum_order_size=0.1,
                    minimum_notional=10.0,
                    reference_price=0.0922,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0923,
                    worst_price_text="0.0923",
                    order_type="limit",
                    time_in_force="ioc",
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[
                        "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                        "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                    ],
                    auth_scheme="account address + API wallet private key",
                    payload={
                        "coin": "ARB",
                        "is_buy": True,
                        "sz": "119.3",
                        "limit_px": "0.0923",
                        "order_type": {"limit": {"tif": "Ioc"}},
                        "reduce_only": False,
                        "client_order_id": "carryme-pt8-hyperliquid-buy",
                    },
                    notes=[],
                )
            ],
        ),
        note="operator confirmed",
    )

    async def run() -> None:
        service = HyperliquidLiveExecutionService(
            account_address="0xhyper",
            api_wallet_private_key="0xwallet",
        )
        entry = await service.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "submitted"
        assert entry.adapter == "hyperliquid_live"
        assert entry.legs[0].external_reference == "778"
        assert entry.legs[0].request_payload is not None
        assert entry.legs[0].request_payload["coin"] == "ARB"

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


def test_build_execution_pair_status_marks_reduce_only_cleanup_as_closed() -> None:
    entry = ExecutionJournalEntry(
        entry_id=22,
        executed_at=datetime(2026, 3, 29, 19, 0, tzinfo=UTC),
        adapter="paired_cleanup:paradex_then_extended",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="cleanup-hash",
        confirmation_entry_id=4,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 40, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=991.7,
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
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-close",
                request_payload={"reduce_only": True},
            ),
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-close",
                request_payload={"reduceOnly": True},
            ),
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=22,
        paper_trade_id=7,
        preview_hash="cleanup-hash",
        legs=[
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="pdx-close",
                derived_state="filled",
                order_status="CLOSED",
            ),
            ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference="ext-close",
                derived_state="unknown",
            ),
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=22,
        paper_trade_id=7,
        preview_hash="cleanup-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD-PERP"],
            ),
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD"],
            ),
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "closed"
    assert status.recommended_action == "no_action"
    assert any("no live positions remain" in note.lower() for note in status.notes)


def test_build_execution_pair_status_marks_flagged_reduce_only_cleanup_as_closed() -> None:
    entry = ExecutionJournalEntry(
        entry_id=24,
        executed_at=datetime(2026, 3, 29, 19, 2, tzinfo=UTC),
        adapter="paired_cleanup:paradex_then_extended",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="cleanup-flags-hash",
        confirmation_entry_id=6,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 40, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=991.7,
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
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-close",
                request_payload={"flags": ["REDUCE_ONLY"]},
            ),
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-close",
                request_payload={"reduceOnly": True},
            ),
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=24,
        paper_trade_id=7,
        preview_hash="cleanup-flags-hash",
        legs=[
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="pdx-close",
                derived_state="filled",
                order_status="CLOSED",
            ),
            ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference="ext-close",
                derived_state="unknown",
            ),
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=24,
        paper_trade_id=7,
        preview_hash="cleanup-flags-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD-PERP"],
            ),
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD"],
            ),
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "closed"
    assert status.recommended_action == "no_action"
    assert any("no live positions remain" in note.lower() for note in status.notes)


def test_build_execution_pair_status_does_not_mark_non_reduce_only_as_closed() -> None:
    entry = ExecutionJournalEntry(
        entry_id=23,
        executed_at=datetime(2026, 3, 29, 19, 5, tzinfo=UTC),
        adapter="paired_live:paradex_then_extended",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=5,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 40, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=991.7,
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
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-live",
                request_payload={"reduce_only": False},
            ),
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-live",
                request_payload={},
            ),
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=23,
        paper_trade_id=7,
        preview_hash="preview-hash",
        legs=[
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="pdx-live",
                derived_state="filled",
                order_status="CLOSED",
            ),
            ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference="ext-live",
                derived_state="unknown",
            ),
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=23,
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
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD-PERP"],
            ),
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["ARB-USD"],
            ),
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "review_required"
    assert status.recommended_action == "manual_review_required"
    assert any("reported a fill" in note.lower() for note in status.notes)


def test_build_execution_pair_status_marks_single_leg_cleanup_as_cleanup_needed() -> None:
    entry = ExecutionJournalEntry(
        entry_id=34,
        executed_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
        adapter="paradex_cleanup_live",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash",
        confirmation_entry_id=12,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 19, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 19, 45, tzinfo=UTC),
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
                side="sell",
                target_notional=21.0,
                status="submitted",
                simulated=False,
                external_reference="cleanup-order",
                request_payload={"reduce_only": True},
            )
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=34,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash",
        legs=[
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="cleanup-order",
                derived_state="unfilled",
            )
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=34,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=True,
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
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "cleanup_needed"
    assert status.recommended_action == "close_open_leg"


def test_build_execution_pair_status_keeps_cleanup_needed_when_opposite_route_leg_stays_open(
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=37,
        executed_at=datetime(2026, 4, 2, 16, 0, tzinfo=UTC),
        adapter="paradex_cleanup_live",
        mode="live",
        status="rejected",
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-wld",
        confirmation_entry_id=15,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 4, 2, 15, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="wld_paradex_extended",
                canonical_symbol="WLD-USD-PERP",
                source_recorded_at=datetime(2026, 4, 2, 15, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0033,
                break_even_days_entry=0.10,
                capacity_limit_notional=995.0,
                target_notional=25.0,
                capacity_fraction=1.0,
                max_target_notional=25.0,
                long_leg=TradeLegIntent(
                    venue="extended",
                    symbol="WLD-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=25.0,
                ),
                short_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="WLD-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="sell",
                    target_notional=25.0,
                ),
            ),
        ),
        legs=[
            ExecutionLegResult(
                venue="paradex",
                symbol="WLD-USD-PERP",
                fee_profile="pro_fastfills",
                side="buy",
                target_notional=25.0,
                status="rejected",
                simulated=False,
                external_reference="carryme-cleanup-pt7-paradex-buy-mkt",
                request_payload={"type": "MARKET", "flags": ["REDUCE_ONLY"]},
            )
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=37,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-wld",
        legs=[
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="carryme-cleanup-pt7-paradex-buy-mkt",
                derived_state="unknown",
            )
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=37,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-wld",
        status="rejected",
        recommended_action="no_action",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                position_symbols=["WLD-USD"],
                matched_leg_symbols=[],
                unmatched_leg_symbols=[],
            ),
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["WLD-USD-PERP"],
            ),
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "cleanup_needed"
    assert status.recommended_action == "close_open_leg"
    assert any("cleanup execution left one open leg" in note.lower() for note in status.notes)


def test_build_execution_pair_status_keeps_pending_when_same_venue_route_has_open_leg() -> None:
    entry = ExecutionJournalEntry(
        entry_id=38,
        executed_at=datetime(2026, 4, 2, 16, 5, tzinfo=UTC),
        adapter="paired_live:paradex_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=8,
        preview_hash="same-venue-preview-hash",
        confirmation_entry_id=16,
        paper_trade=PaperTradeEntry(
            entry_id=8,
            created_at=datetime(2026, 4, 2, 16, 0, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="wld_arb_paradex_pair",
                canonical_symbol="WLD-USD-PERP",
                source_recorded_at=datetime(2026, 4, 2, 15, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0021,
                break_even_days_entry=0.20,
                capacity_limit_notional=900.0,
                target_notional=25.0,
                capacity_fraction=1.0,
                max_target_notional=25.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="WLD-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=25.0,
                ),
                short_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=25.0,
                ),
            ),
        ),
        legs=[
            ExecutionLegResult(
                venue="paradex",
                symbol="WLD-USD-PERP",
                fee_profile="pro_fastfills",
                side="buy",
                target_notional=25.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-order-open",
            ),
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="sell",
                target_notional=25.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-order-unfilled",
            ),
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=38,
        paper_trade_id=8,
        preview_hash="same-venue-preview-hash",
        legs=[
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="pdx-order-open",
                derived_state="open",
            ),
            ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                external_reference="pdx-order-unfilled",
                derived_state="unfilled",
            ),
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=38,
        paper_trade_id=8,
        preview_hash="same-venue-preview-hash",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=False,
        venues=[
            ExecutionVenueReconciliation(
                venue="paradex",
                authenticated=True,
                ready=True,
                position_symbols=[],
                matched_leg_symbols=[],
                unmatched_leg_symbols=["WLD-USD-PERP", "ARB-USD-PERP"],
            )
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "pending"
    assert status.recommended_action == "wait_for_fill_or_timeout"
    assert any("still reports the order as open" in note.lower() for note in status.notes)


def test_build_execution_pair_status_keeps_cleanup_retryable_when_order_state_is_unknown() -> None:
    entry = ExecutionJournalEntry(
        entry_id=36,
        executed_at=datetime(2026, 4, 2, 12, 0, tzinfo=UTC),
        adapter="extended_cleanup_live",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-unknown",
        confirmation_entry_id=14,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 4, 2, 11, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="s_extended_paradex",
                canonical_symbol="S-USD-PERP",
                source_recorded_at=datetime(2026, 4, 2, 11, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="S-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="S-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                ),
            ),
        ),
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="S-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="cleanup-order",
                request_payload={"reduce_only": True},
            )
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=36,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-unknown",
        legs=[
            ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference="cleanup-order",
                derived_state="unknown",
            )
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=36,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-unknown",
        status="submitted",
        recommended_action="verify_fill_status",
        matched_all_leg_symbols=True,
        venues=[
            ExecutionVenueReconciliation(
                venue="extended",
                authenticated=True,
                ready=True,
                position_symbols=["S-USD"],
                matched_leg_symbols=["S-USD"],
                unmatched_leg_symbols=[],
            )
        ],
        notes=[],
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "cleanup_needed"
    assert status.recommended_action == "close_open_leg"
    assert any("cleanup" in note.lower() for note in status.notes)


def test_build_execution_pair_status_marks_single_leg_cleanup_as_closed_when_positions_are_flat(
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=35,
        executed_at=datetime(2026, 3, 29, 20, 5, tzinfo=UTC),
        adapter="extended_cleanup_live",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-flat",
        confirmation_entry_id=13,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 19, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 19, 45, tzinfo=UTC),
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
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="cleanup-order",
                request_payload={"reduceOnly": True},
            )
        ],
    )
    order_state = ExecutionOrderState(
        execution_entry_id=35,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-flat",
        legs=[
            ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference="cleanup-order",
                derived_state="unknown",
            )
        ],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=35,
        paper_trade_id=7,
        preview_hash="cleanup-preview-hash-flat",
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
    )

    status = build_execution_pair_status(entry, order_state, reconciliation)

    assert status.derived_state == "closed"
    assert status.recommended_action == "no_action"
    assert any("no live positions remaining" in note.lower() for note in status.notes)


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


def test_paradex_cleanup_preview_service_uses_live_position_direction_over_execution_side(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_cleanup:paradex_then_extended",
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
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="pdx-close-order",
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
                    external_reference="pdx-close-order",
                    derived_state="unfilled",
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
            return "jwt-token"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "market": "ARB-USD-PERP",
                        "size": "-123.1",
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
        assert preview.leg.side == "buy"

    asyncio.run(run())

def test_row_represents_open_position_checks_all_numeric_quantity_fields() -> None:
    assert _row_represents_open_position({"size": "0", "qty": "1"}) is True
    assert _row_represents_open_position({"size": "0", "qty": "0"}) is False

def test_hyperliquid_cleanup_preview_service_builds_reduce_only_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_hyperliquid",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_hyperliquid",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0011,
                break_even_days_entry=0.63,
                capacity_limit_notional=126.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
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
                venue="hyperliquid",
                symbol="ARB",
                fee_profile="tier0",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="777",
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
                    venue="hyperliquid",
                    supported=True,
                    external_reference="777",
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
                    venue="hyperliquid",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB"],
                    matched_leg_symbols=["ARB"],
                    unmatched_leg_symbols=[],
                )
            ],
            notes=[],
        ),
        notes=[],
    )

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "hyperliquid"
        assert symbol == "ARB"
        return _snapshot(
            "hyperliquid",
            "ARB",
            -0.0004,
            0.0918,
            500,
            0.0922,
            450,
            raw={"szDecimals": 1},
        )

    class StubInfo:
        def user_state(self, address: str) -> dict[str, object]:
            assert address == "0xhyper"
            return {
                "assetPositions": [
                    {
                        "position": {
                            "coin": "ARB",
                            "szi": "119.3",
                        }
                    }
                ]
            }

    monkeypatch.setattr(
        "carryme_runtime.hyperliquid_cleanup_preview.build_hyperliquid_info",
        lambda: StubInfo(),
    )

    async def run() -> None:
        service = HyperliquidCleanupPreviewService(
            account_address="0xhyper",
            fetch_snapshot=fetch_snapshot,
        )
        preview = await service.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.reason == "close_open_leg"
        assert preview.leg.symbol == "ARB"
        assert preview.leg.side == "sell"
        assert preview.leg.reduce_only is True
        assert preview.leg.quantity_text == "119.3"
        assert preview.leg.worst_price_text == "0.09171"

    asyncio.run(run())


def test_hyperliquid_cleanup_preview_service_prefers_vault_address_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 18, 0, tzinfo=UTC),
        adapter="paired_live:extended_then_hyperliquid",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 17, 59, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_hyperliquid",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 17, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
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
                venue="hyperliquid",
                symbol="ARB",
                fee_profile="tier0",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="777",
                request_payload={"client_order_id": "carryme-pt7-hyperliquid-buy"},
                response_payload={"status": "ok"},
            ),
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="rejected",
                simulated=False,
            ),
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
                    venue="hyperliquid",
                    supported=True,
                    external_reference="777",
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
                    venue="hyperliquid",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB"],
                    matched_leg_symbols=["ARB"],
                    unmatched_leg_symbols=[],
                )
            ],
            notes=[],
        ),
        notes=[],
    )

    async def fetch_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
        assert venue == "hyperliquid"
        assert symbol == "ARB"
        return _snapshot(
            "hyperliquid",
            "ARB",
            -0.0004,
            0.0918,
            500,
            0.0922,
            450,
            raw={"szDecimals": 1},
        )

    class StubInfo:
        def user_state(self, address: str) -> dict[str, object]:
            assert address == "0xvault"
            return {
                "assetPositions": [
                    {
                        "position": {
                            "coin": "ARB",
                            "szi": "119.3",
                        }
                    }
                ]
            }

    monkeypatch.setattr(
        "carryme_runtime.hyperliquid_cleanup_preview.build_hyperliquid_info",
        lambda: StubInfo(),
    )

    async def run() -> None:
        service = HyperliquidCleanupPreviewService(
            account_address="0xhyper",
            vault_address="0xvault",
            fetch_snapshot=fetch_snapshot,
        )
        preview = await service.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.reason == "close_open_leg"
        assert preview.leg.symbol == "ARB"
        assert preview.leg.side == "sell"
        assert preview.leg.reduce_only is True
        assert preview.leg.quantity_text == "119.3"
        assert preview.leg.worst_price_text == "0.09171"

    asyncio.run(run())


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
        recommended_action="complete_or_unwind_missing_leg",
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
        assert preview.reason == "complete_or_unwind_missing_leg"

    asyncio.run(run())


def test_pair_close_preview_service_builds_reduce_only_pair_close() -> None:
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
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        adapter="paired_live:paradex_then_extended",
        mode="live",
        status="submitted",
        paper_trade_id=paper_trade.entry_id,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
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
            ),
        ],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=12,
        paper_trade_id=8,
        preview_hash="preview-hash",
        derived_state="hedged",
        recommended_action="monitor_open_hedge",
        order_state=ExecutionOrderState(
            execution_entry_id=12,
            paper_trade_id=8,
            preview_hash="preview-hash",
            legs=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=12,
            paper_trade_id=8,
            preview_hash="preview-hash",
            status="submitted",
            recommended_action="monitor_open_hedge",
            matched_all_leg_symbols=True,
            venues=[
                ExecutionVenueReconciliation(
                    venue="paradex",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB-USD-PERP"],
                    matched_leg_symbols=["ARB-USD-PERP"],
                    unmatched_leg_symbols=[],
                ),
                ExecutionVenueReconciliation(
                    venue="extended",
                    authenticated=True,
                    ready=True,
                    position_symbols=["ARB-USD"],
                    matched_leg_symbols=["ARB-USD"],
                    unmatched_leg_symbols=[],
                ),
            ],
            notes=[],
        ),
        notes=[],
    )

    class StubExtendedService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            assert pair_status.recommended_action == "close_open_leg"
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                generated_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
                preview_hash="cleanup-extended",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.0,
                    quantity_text="124",
                    reference_price=0.0881,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0882,
                    worst_price_text="0.0882",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=["extended close"],
            )

    class StubParadexService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            assert pair_status.recommended_action == "close_open_leg"
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                generated_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
                preview_hash="cleanup-paradex",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.4,
                    quantity_text="124.40000000",
                    reference_price=0.0885,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0884,
                    worst_price_text="0.08840000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                notes=["paradex close"],
            )

    async def run() -> None:
        service = PairClosePreviewService(
            services={
                "extended": StubExtendedService(),
                "paradex": StubParadexService(),
            }
        )
        preview = await service.preview_from_execution(entry=entry, pair_status=pair_status)
        assert preview.reason == "close_pair"
        assert [leg.venue for leg in preview.legs] == ["paradex", "extended"]
        assert all(leg.reduce_only for leg in preview.legs)

    asyncio.run(run())


def test_pair_close_preview_service_rejects_flat_positions_on_both_venues() -> None:
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
    entry = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        adapter="paired_live:paradex_then_extended",
        mode="live",
        status="submitted",
        paper_trade_id=paper_trade.entry_id,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
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
            ),
        ],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=12,
        paper_trade_id=8,
        preview_hash="preview-hash",
        derived_state="hedged",
        recommended_action="monitor_open_hedge",
        order_state=ExecutionOrderState(
            execution_entry_id=12,
            paper_trade_id=8,
            preview_hash="preview-hash",
            legs=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=12,
            paper_trade_id=8,
            preview_hash="preview-hash",
            status="submitted",
            recommended_action="monitor_open_hedge",
            matched_all_leg_symbols=True,
            venues=[
                ExecutionVenueReconciliation(
                    venue="paradex",
                    authenticated=True,
                    ready=True,
                    position_symbols=[],
                    matched_leg_symbols=[],
                    unmatched_leg_symbols=["ARB-USD-PERP"],
                ),
                ExecutionVenueReconciliation(
                    venue="extended",
                    authenticated=True,
                    ready=True,
                    position_symbols=[],
                    matched_leg_symbols=[],
                    unmatched_leg_symbols=["ARB-USD"],
                ),
            ],
            notes=[],
        ),
        notes=[],
    )

    class StubCloseService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            raise AssertionError("close preview should not be built when both venues are flat")

    async def run() -> None:
        service = PairClosePreviewService(
            services={
                "extended": StubCloseService(),
                "paradex": StubCloseService(),
            }
        )
        with pytest.raises(
            ValueError, match="Pair close preview requires exactly two open legs across all venues"
        ):
            await service.preview_from_execution(entry=entry, pair_status=pair_status)

    asyncio.run(run())


def test_pair_close_live_execution_coordinator_submits_both_legs() -> None:
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
    confirmation = PairClosePreviewConfirmationEntry(
        entry_id=22,
        confirmed_at=datetime(2026, 3, 29, 13, 20, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="pair-close-hash",
        preview=ExecutionPairClosePreview(
            execution_entry_id=12,
            paper_trade_id=8,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="pair-close-hash",
            reason="close_pair",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.4,
                    quantity_text="124.40000000",
                    reference_price=0.0885,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0884,
                    worst_price_text="0.08840000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.0,
                    quantity_text="124",
                    reference_price=0.0881,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0882,
                    worst_price_text="0.0882",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
            ],
            notes=[],
        ),
    )

    class StubParadexService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.preview.leg.venue == "paradex"
            return ExecutionJournalEntry(
                executed_at=executed_at or datetime(2026, 3, 29, 13, 21, tzinfo=UTC),
                adapter="paradex_cleanup_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="pdx-close",
                    )
                ],
            )

    class StubExtendedService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.preview.leg.venue == "extended"
            return ExecutionJournalEntry(
                executed_at=executed_at or datetime(2026, 3, 29, 13, 21, tzinfo=UTC),
                adapter="extended_cleanup_live",
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
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="ext-close",
                    )
                ],
            )

    async def run() -> None:
        coordinator = PairCloseLiveExecutionCoordinator(
            services={
                "extended": StubExtendedService(),
                "paradex": StubParadexService(),
            }
        )
        entry = await coordinator.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.adapter == "paired_cleanup:paradex_then_extended"
        assert entry.status == "submitted"
        assert [leg.venue for leg in entry.legs] == ["paradex", "extended"]

    asyncio.run(run())


def test_pair_close_live_execution_coordinator_rejects_mismatched_confirmation_trade() -> None:
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
    confirmation = PairClosePreviewConfirmationEntry(
        entry_id=22,
        confirmed_at=datetime(2026, 3, 29, 13, 20, tzinfo=UTC),
        paper_trade_id=99,
        label="arb_extended_paradex",
        preview_hash="pair-close-hash",
        preview=ExecutionPairClosePreview(
            execution_entry_id=12,
            paper_trade_id=99,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="pair-close-hash",
            reason="close_pair",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.4,
                    quantity_text="124.40000000",
                    reference_price=0.0885,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0884,
                    worst_price_text="0.08840000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.0,
                    quantity_text="124",
                    reference_price=0.0881,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0882,
                    worst_price_text="0.0882",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
            ],
            notes=[],
        ),
    )

    async def run() -> None:
        coordinator = PairCloseLiveExecutionCoordinator(services={})
        with pytest.raises(ValueError, match="does not belong to this paper trade"):
            await coordinator.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
            )

    asyncio.run(run())


def test_pair_close_live_execution_coordinator_rejects_reordered_first_venue_override() -> None:
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
    confirmation = PairClosePreviewConfirmationEntry(
        entry_id=22,
        confirmed_at=datetime(2026, 3, 29, 13, 20, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="pair-close-hash",
        preview=ExecutionPairClosePreview(
            execution_entry_id=12,
            paper_trade_id=8,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="pair-close-hash",
            reason="close_pair",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.4,
                    quantity_text="124.40000000",
                    reference_price=0.0885,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0884,
                    worst_price_text="0.08840000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.0,
                    quantity_text="124",
                    reference_price=0.0881,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0882,
                    worst_price_text="0.0882",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
            ],
            notes=[],
        ),
    )

    async def run() -> None:
        coordinator = PairCloseLiveExecutionCoordinator(services={})
        with pytest.raises(ValueError, match="must preserve the confirmed venue order"):
            await coordinator.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
                first_venue="extended",
            )

    asyncio.run(run())


def test_pair_close_live_execution_coordinator_rejects_non_reduce_only_leg() -> None:
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
    confirmation = PairClosePreviewConfirmationEntry(
        entry_id=22,
        confirmed_at=datetime(2026, 3, 29, 13, 20, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="pair-close-hash",
        preview=ExecutionPairClosePreview(
            execution_entry_id=12,
            paper_trade_id=8,
            label="arb_extended_paradex",
            generated_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
            slippage_tolerance_bps=10,
            preview_hash="pair-close-hash",
            reason="close_pair",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.4,
                    quantity_text="124.40000000",
                    reference_price=0.0885,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0884,
                    worst_price_text="0.08840000",
                    reduce_only=False,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": False},
                    notes=[],
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=124.0,
                    quantity_text="124",
                    reference_price=0.0881,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0882,
                    worst_price_text="0.0882",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
            ],
            notes=[],
        ),
    )

    class StubParadexService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            raise AssertionError("non-reduce-only preview should be rejected before dispatch")

    class StubExtendedService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            raise AssertionError("non-reduce-only preview should be rejected before dispatch")

    async def run() -> None:
        coordinator = PairCloseLiveExecutionCoordinator(
            services={
                "extended": StubExtendedService(),
                "paradex": StubParadexService(),
            }
        )
        entry = await coordinator.submit_confirmed_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.status == "rejected"
        assert entry.legs[0].venue == "paradex"
        assert entry.legs[0].response_payload is not None
        assert "reduce-only" in str(entry.legs[0].response_payload["error"])

    asyncio.run(run())


def test_cleanup_live_execution_router_dispatches_to_preview_venue() -> None:
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
                required_auth_env_vars=[],
                auth_scheme="main account address + subkey private key",
                payload={"market": "ARB-USD-PERP", "reduce_only": True},
                notes=[],
            ),
            notes=[],
        ),
    )

    class StubParadexService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
                adapter="paradex_cleanup_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-order-2",
                    )
                ],
            )

    class StubOtherService:
        def __init__(self) -> None:
            self.called = False

        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            self.called = True
            raise AssertionError("Cleanup router dispatched to the wrong venue")

    async def run() -> None:
        other_service = StubOtherService()
        router = CleanupLiveExecutionRouter(
            services={
                "other": other_service,
                "paradex": StubParadexService(),
            }
        )
        entry = await router.submit_confirmed_cleanup_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
        )
        assert entry.adapter == "paradex_cleanup_live"
        assert entry.legs[0].venue == "paradex"
        assert other_service.called is False

    asyncio.run(run())


def test_cleanup_live_execution_router_rejects_mismatched_trade_or_non_reduce_only() -> None:
    class StubParadexService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            raise AssertionError(
                "Router should reject invalid cleanup confirmation before dispatch"
            )

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
    router = CleanupLiveExecutionRouter(services={"paradex": StubParadexService()})

    mismatched_confirmation = CleanupPreviewConfirmationEntry(
        entry_id=11,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=9,
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
                worst_acceptable_price=0.0890,
                worst_price_text="0.08900000",
                reduce_only=True,
                endpoint_path_hint="/v1/orders",
                auth_scheme="main account address + subkey private key",
                payload={"market": "ARB-USD-PERP", "reduce_only": True},
                notes=[],
            ),
            notes=[],
        ),
    )

    non_reduce_only_confirmation = CleanupPreviewConfirmationEntry(
        entry_id=12,
        confirmed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        paper_trade_id=8,
        label="arb_extended_paradex",
        preview_hash="cleanup-hash-2",
        preview=ExecutionCleanupPreview(
            execution_entry_id=5,
            paper_trade_id=8,
            generated_at=datetime(2026, 3, 29, 13, 12, tzinfo=UTC),
            preview_hash="cleanup-hash-2",
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
                worst_acceptable_price=0.0890,
                worst_price_text="0.08900000",
                reduce_only=False,
                endpoint_path_hint="/v1/orders",
                auth_scheme="main account address + subkey private key",
                payload={"market": "ARB-USD-PERP", "reduce_only": False},
                notes=[],
            ),
            notes=[],
        ),
    )

    async def run() -> None:
        with pytest.raises(ValueError, match="does not belong"):
            await router.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=mismatched_confirmation,
            )
        with pytest.raises(ValueError, match="must be reduce-only"):
            await router.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=non_reduce_only_confirmation,
            )

    asyncio.run(run())


def test_execution_accounting_service_summarizes_filled_attempt_history(tmp_path: Path) -> None:
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
    saved = store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
            adapter="paradex_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=21.48,
                    status="submitted",
                    simulated=False,
                    auth_usage="subkey_jwt",
                    response_payload={
                        "attempt_history": [
                            {
                                "observed_order_state": {
                                    "derived_state": "unfilled",
                                    "size": "241.9",
                                    "remaining_size": "241.9",
                                    "avg_fill_price": "",
                                }
                            },
                            {
                                "observed_order_state": {
                                    "derived_state": "filled",
                                    "size": "241.9",
                                    "remaining_size": "0",
                                    "avg_fill_price": "0.0881",
                                }
                            },
                        ]
                    },
                )
            ],
        )
    )

    service = ExecutionAccountingService(journal_store=store)
    summary = service.summarize_entry(saved)
    paper_trade_summary = service.latest_for_paper_trade(7)
    route_summaries = service.list_route_summaries(limit=10)

    assert isinstance(summary, ExecutionAccountingSummary)
    assert summary.filled_leg_count == 1
    assert summary.total_filled_notional == pytest.approx(241.9 * 0.0881)
    assert summary.total_estimated_fee_paid == pytest.approx((241.9 * 0.0881) * 0.0002)
    assert isinstance(summary.legs[0], ExecutionLegAccounting)
    assert summary.legs[0].derived_fill_state == "filled"
    assert paper_trade_summary is not None
    assert isinstance(paper_trade_summary, PaperTradeAccountingSummary)
    assert paper_trade_summary.total_estimated_fee_paid == pytest.approx(
        summary.total_estimated_fee_paid
    )
    assert route_summaries
    assert isinstance(route_summaries[0], RouteAccountingSummary)
    assert route_summaries[0].total_estimated_fee_paid == pytest.approx(
        summary.total_estimated_fee_paid
    )

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


def test_route_approval_service_filters_canaries_and_enforces_live_cap(
    tmp_path: Path,
) -> None:
    store = RouteApprovalStore(tmp_path / "history.sqlite3")
    service = RouteApprovalService(store=store)
    service.upsert(
        label="arb_extended_paradex",
        payload=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 14, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=12.0,
            note="canary only",
        ),
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.0015,
                entry_cost_rate=0.0004,
                round_trip_cost_rate=0.0008,
                one_day_net_edge_after_entry=0.0011,
                one_day_net_edge_after_round_trip=0.0007,
                break_even_days_entry=0.4,
                break_even_days_round_trip=0.8,
                capacity=CapacityEstimate(
                    short_bid_notional=1500.0,
                    long_ask_notional=1400.0,
                    max_entry_notional=1400.0,
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
            deployable_notional=300.0,
            estimated_one_day_pnl_after_round_trip=0.21,
        ),
        suggested_canary_notional=25.0,
    )
    filtered = service.filter_approved_canary_candidates([candidate])
    permitted_intent = FundingPairTradeIntent(
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        source_recorded_at=datetime(2026, 3, 29, 14, 0, tzinfo=UTC),
        one_day_net_edge_after_entry=0.0011,
        break_even_days_entry=0.4,
        capacity_limit_notional=1400.0,
        target_notional=11.0,
        capacity_fraction=0.25,
        max_target_notional=11.0,
        long_leg=TradeLegIntent(
            venue="paradex",
            symbol="ARB-USD-PERP",
            fee_profile="pro_fastfills",
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
    )
    blocked_intent = permitted_intent.model_copy(update={"target_notional": 15.0})

    assert len(filtered) == 1
    assert filtered[0].suggested_canary_notional == 12.0
    assert service.require_live_approval(permitted_intent).max_live_notional == 12.0
    with pytest.raises(ValueError, match="approved cap"):
        service.require_live_approval(blocked_intent)


def test_route_approval_service_builds_approved_canary_basket_plan(
    tmp_path: Path,
) -> None:
    store = RouteApprovalStore(tmp_path / "basket.sqlite3")
    service = RouteApprovalService(store=store)
    service.upsert(
        label="s_extended_paradex",
        payload=RouteApprovalEntry(
            updated_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="first basket route",
        ),
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="S-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.0025,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00205,
                one_day_net_edge_after_round_trip=0.0016,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.4,
                capacity=CapacityEstimate(
                    short_bid_notional=2000.0,
                    long_ask_notional=300.0,
                    max_entry_notional=300.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="S-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="S-USD-PERP",
                ),
            },
            deployable_notional=300.0,
            estimated_one_day_pnl_after_entry=0.615,
            estimated_one_day_pnl_after_round_trip=0.48,
            execution_adjusted_one_day_pnl_after_round_trip=0.36,
            stability_adjusted_one_day_pnl_after_round_trip=0.24,
            route_stability=RouteStabilitySummary(
                canonical_symbol="S-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_fee_profile="default",
                long_fee_profile="pro_fastfills",
                sample_size=10,
                window_count=10,
                presence_ratio=0.5,
                positive_roundtrip_share=1.0,
                mean_roundtrip_edge=0.0016,
                median_roundtrip_edge=0.0015,
                edge_stddev=0.0002,
                mean_capacity_notional=300.0,
                median_capacity_notional=300.0,
                capacity_stddev=0.0,
                latest_roundtrip_edge=0.0016,
                latest_recorded_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
                stability_weight=0.4,
                stability_score=0.00064,
            ),
        ),
        suggested_canary_notional=25.0,
    )

    plan = service.build_approved_canary_basket_plan(
        candidates=[candidate],
        venues=["extended", "paradex"],
        fee_profiles={"extended": "default", "paradex": "pro_fastfills"},
        target_notional=25.0,
    )

    assert plan.target_notional == 25.0
    assert plan.allocated_notional == 11.0
    assert plan.unused_notional == 14.0
    assert plan.fee_profiles == {"extended": "default", "paradex": "pro_fastfills"}
    assert len(plan.entries) == 1
    assert plan.entries[0].label == "s_extended_paradex"
    assert plan.entries[0].selected_notional == 11.0
    assert plan.entries[0].estimated_one_day_pnl_after_round_trip == pytest.approx(
        0.48 * (11.0 / 300.0)
    )
    assert plan.entries[
        0
    ].route_adjusted_estimated_one_day_pnl_after_round_trip == pytest.approx(
        0.48 * (11.0 / 300.0) * (0.36 / 0.48) * 0.4
    )


def test_route_approval_service_caps_approved_canary_basket_by_target_notional(
    tmp_path: Path,
) -> None:
    store = RouteApprovalStore(tmp_path / "basket-cap.sqlite3")
    service = RouteApprovalService(store=store)
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        label="s_extended_paradex",
        canonical_symbol="S-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="capped route",
    )
    second_approval = approval.model_copy(
        update={
            "label": "ondo_paradex_extended",
            "canonical_symbol": "ONDO-USD-PERP",
            "short_venue": "paradex",
            "long_venue": "extended",
            "short_fee_profile": "pro_fastfills",
            "long_fee_profile": "default",
        }
    )
    service.upsert(label=approval.label, payload=approval)
    service.upsert(label=second_approval.label, payload=second_approval)

    def _candidate(
        *,
        symbol: str,
        short_venue: str,
        long_venue: str,
        short_symbol: str,
        long_symbol: str,
    ) -> FundingUniverseCanaryCandidate:
        return FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol=symbol,
                    long_venue=long_venue,
                    short_venue=short_venue,
                    long_fee_profile="pro_fastfills"
                    if long_venue == "paradex"
                    else "default",
                    short_fee_profile="pro_fastfills"
                    if short_venue == "paradex"
                    else "default",
                    gross_daily_edge=0.002,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.00155,
                    one_day_net_edge_after_round_trip=0.0011,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.4,
                    capacity=CapacityEstimate(
                        short_bid_notional=2000.0,
                        long_ask_notional=300.0,
                        max_entry_notional=300.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    short_venue: FundingUniverseVenueMarket(
                        venue=short_venue,
                        symbol=short_symbol,
                    ),
                    long_venue: FundingUniverseVenueMarket(
                        venue=long_venue,
                        symbol=long_symbol,
                    ),
                },
                deployable_notional=300.0,
                estimated_one_day_pnl_after_entry=0.465,
                estimated_one_day_pnl_after_round_trip=0.33,
            ),
            suggested_canary_notional=11.0,
        )

    plan = service.build_approved_canary_basket_plan(
        candidates=[
            _candidate(
                symbol="S-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_symbol="S-USD",
                long_symbol="S-USD-PERP",
            ),
            _candidate(
                symbol="ONDO-USD-PERP",
                short_venue="paradex",
                long_venue="extended",
                short_symbol="ONDO-USD-PERP",
                long_symbol="ONDO-USD",
            ),
        ],
        venues=["extended", "paradex"],
        fee_profiles={"extended": "default", "paradex": "pro_fastfills"},
        target_notional=15.0,
    )

    assert plan.target_notional == 15.0
    assert plan.allocated_notional == 15.0
    assert plan.unused_notional == 0.0
    assert [entry.label for entry in plan.entries] == [
        "s_extended_paradex",
        "ondo_paradex_extended",
    ]
    assert plan.entries[0].selected_notional == 11.0
    assert plan.entries[1].selected_notional == 4.0


def test_route_approval_service_preserves_missing_adjusted_pnl_in_basket_plan(
    tmp_path: Path,
) -> None:
    store = RouteApprovalStore(tmp_path / "basket-missing-adjusted.sqlite3")
    service = RouteApprovalService(store=store)
    service.upsert(
        label="s_extended_paradex",
        payload=RouteApprovalEntry(
            updated_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="missing adjusted metrics",
        ),
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="S-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.0025,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00205,
                one_day_net_edge_after_round_trip=0.0016,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.4,
                capacity=CapacityEstimate(
                    short_bid_notional=2000.0,
                    long_ask_notional=300.0,
                    max_entry_notional=300.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="S-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="S-USD-PERP",
                ),
            },
            deployable_notional=300.0,
            estimated_one_day_pnl_after_entry=0.615,
            estimated_one_day_pnl_after_round_trip=0.48,
            execution_adjusted_one_day_pnl_after_round_trip=None,
            stability_adjusted_one_day_pnl_after_round_trip=None,
            route_stability=None,
        ),
        suggested_canary_notional=11.0,
    )

    plan = service.build_approved_canary_basket_plan(
        candidates=[candidate],
        venues=["extended", "paradex"],
        fee_profiles={"extended": "default", "paradex": "pro_fastfills"},
        target_notional=11.0,
    )

    assert plan.execution_adjusted_estimated_one_day_pnl_after_round_trip is None
    assert plan.stability_adjusted_estimated_one_day_pnl_after_round_trip is None
    assert (
        plan.entries[0].execution_adjusted_estimated_one_day_pnl_after_round_trip is None
    )
    assert (
        plan.entries[0].stability_adjusted_estimated_one_day_pnl_after_round_trip is None
    )


def test_balance_accounting_service_summarizes_snapshots(tmp_path: Path) -> None:
    store = BalanceSnapshotStore(tmp_path / "history.sqlite3")
    service = BalanceAccountingService(store=store)
    store.append(
        VenueBalanceSnapshot(
            captured_at=datetime(2026, 3, 29, 15, 0, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            stage="pre_open",
            venue="extended",
            total_collateral=4.99,
            available_to_trade=4.99,
            free_collateral=4.99,
        )
    )
    store.append(
        VenueBalanceSnapshot(
            captured_at=datetime(2026, 3, 29, 15, 0, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            stage="pre_open",
            venue="paradex",
            total_collateral=15.0,
            available_to_trade=15.0,
            free_collateral=15.0,
        )
    )
    store.append(
        VenueBalanceSnapshot(
            captured_at=datetime(2026, 3, 29, 15, 20, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            stage="post_close",
            venue="extended",
            total_collateral=4.90,
            available_to_trade=4.90,
            free_collateral=4.90,
        )
    )
    store.append(
        VenueBalanceSnapshot(
            captured_at=datetime(2026, 3, 29, 15, 20, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            stage="post_close",
            venue="paradex",
            total_collateral=14.86,
            available_to_trade=14.86,
            free_collateral=14.86,
        )
    )

    summary = service.summarize_paper_trade(7)

    assert summary is not None
    assert isinstance(summary, PaperTradeBalanceDelta)
    assert summary.snapshot_count == 4
    assert summary.venue_count == 2
    assert summary.total_collateral_delta == pytest.approx(-0.23)
    assert summary.venues[0].snapshot_count == 2
