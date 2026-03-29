import asyncio

from carryme_models import MarketStats, NormalizedMarketSnapshot, TopOfBook
from carryme_normalizers import normalize_market_snapshot
from carryme_runtime import OpportunityService


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
