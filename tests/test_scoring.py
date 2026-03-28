import pytest
from carryme_models import MarketStats, NormalizedMarketSnapshot, TopOfBook
from carryme_normalizers import get_fee_profile, normalize_market_snapshot
from carryme_scoring import rank_opportunities, score_funding_pair


def _snapshot(
    venue: str,
    symbol: str,
    funding_rate: float,
    bid_price: float,
    bid_size: float,
    ask_price: float,
    ask_size: float,
) -> NormalizedMarketSnapshot:
    market = MarketStats(
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
    )
    return normalize_market_snapshot(venue, market)


def test_score_funding_pair_picks_the_higher_funding_short() -> None:
    extended = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    hyperliquid = _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 75_000)

    opportunity = score_funding_pair(
        extended,
        hyperliquid,
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )

    assert opportunity.short_venue == "extended"
    assert opportunity.long_venue == "hyperliquid"
    assert opportunity.gross_daily_edge == pytest.approx(0.0048 - (-0.0012))


def test_score_funding_pair_computes_costs_and_break_even() -> None:
    extended = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    hyperliquid = _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 75_000)

    opportunity = score_funding_pair(
        extended,
        hyperliquid,
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )

    assert opportunity.entry_cost_rate == pytest.approx(0.00025 + 0.00045)
    assert opportunity.round_trip_cost_rate == pytest.approx((0.00025 + 0.00045) * 2)
    assert opportunity.break_even_days_entry == pytest.approx(
        opportunity.entry_cost_rate / opportunity.gross_daily_edge
    )


def test_score_funding_pair_estimates_capacity_from_relevant_book_sides() -> None:
    extended = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    hyperliquid = _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 50_000)

    opportunity = score_funding_pair(
        extended,
        hyperliquid,
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )

    assert opportunity.capacity is not None
    assert opportunity.capacity.short_bid_notional == pytest.approx(0.0345 * 100_000)
    assert opportunity.capacity.long_ask_notional == pytest.approx(0.0345 * 50_000)
    assert opportunity.capacity.max_entry_notional == pytest.approx(0.0345 * 50_000)
    assert opportunity.capacity.limiting_venue == "hyperliquid"


def test_score_funding_pair_rejects_mismatched_canonical_symbols() -> None:
    extended = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    paradex = _snapshot("paradex", "ARB-USD-PERP", -0.0002, 0.0910, 40_000, 0.0914, 30_000)

    with pytest.raises(ValueError, match="same canonical symbol"):
        score_funding_pair(
            extended,
            paradex,
            get_fee_profile("extended", "default"),
            get_fee_profile("paradex", "pro"),
        )


def test_score_funding_pair_rejects_same_venue_pairs() -> None:
    left = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    right = _snapshot("extended", "STRK-USD", -0.0001, 0.0344, 90_000, 0.0345, 75_000)

    with pytest.raises(ValueError, match="distinct venues"):
        score_funding_pair(
            left,
            right,
            get_fee_profile("extended", "default"),
            get_fee_profile("extended", "maker_share_0_5pct"),
        )


def test_score_funding_pair_rejects_fee_profile_venue_mismatch() -> None:
    extended = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    hyperliquid = _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 75_000)

    with pytest.raises(ValueError, match="Left fee profile must match the left venue"):
        score_funding_pair(
            extended,
            hyperliquid,
            get_fee_profile("hyperliquid", "tier0"),
            get_fee_profile("hyperliquid", "tier0"),
        )


def test_score_funding_pair_returns_none_capacity_when_book_data_missing() -> None:
    extended = _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000)
    hyperliquid = _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 75_000)
    hyperliquid.market.top_of_book = None

    opportunity = score_funding_pair(
        extended,
        hyperliquid,
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )

    assert opportunity.capacity is None


def test_rank_opportunities_sorts_by_round_trip_edge_then_capacity() -> None:
    better = score_funding_pair(
        _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000),
        _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 50_000),
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )
    worse = score_funding_pair(
        _snapshot("extended", "STRK-USD", 0.00002, 0.0345, 100_000, 0.0346, 80_000),
        _snapshot("hyperliquid", "STRK", -0.00001, 0.0344, 90_000, 0.0345, 50_000),
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )

    ranked = rank_opportunities([worse, better])

    assert ranked[0] == better
    assert ranked[1] == worse


def test_rank_opportunities_treats_zero_capacity_as_better_than_missing_capacity() -> None:
    zero_capacity = score_funding_pair(
        _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 0.0, 0.0346, 80_000),
        _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 50_000),
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )
    missing_capacity = score_funding_pair(
        _snapshot("extended", "STRK-USD", 0.0002, 0.0345, 100_000, 0.0346, 80_000),
        _snapshot("hyperliquid", "STRK", -0.00005, 0.0344, 90_000, 0.0345, 50_000),
        get_fee_profile("extended", "default"),
        get_fee_profile("hyperliquid", "tier0"),
    )
    missing_capacity.capacity = None

    ranked = rank_opportunities([missing_capacity, zero_capacity])

    assert ranked[0] == zero_capacity
    assert ranked[1] == missing_capacity
