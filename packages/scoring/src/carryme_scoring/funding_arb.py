"""Funding arbitrage scoring helpers."""

from __future__ import annotations

import math

from carryme_models import (
    CapacityEstimate,
    FundingArbOpportunity,
    NormalizedMarketSnapshot,
    TopOfBook,
    TradingFeeProfile,
)


def score_funding_pair(
    left: NormalizedMarketSnapshot,
    right: NormalizedMarketSnapshot,
    left_fee: TradingFeeProfile,
    right_fee: TradingFeeProfile,
) -> FundingArbOpportunity:
    """Score a two-venue funding opportunity using normalized inputs."""

    if left.identity.canonical_symbol != right.identity.canonical_symbol:
        raise ValueError("Funding pairs must share the same canonical symbol")
    if left.identity.venue == right.identity.venue:
        raise ValueError("Funding pairs must come from distinct venues")
    if left_fee.venue != left.identity.venue:
        raise ValueError("Left fee profile must match the left venue")
    if right_fee.venue != right.identity.venue:
        raise ValueError("Right fee profile must match the right venue")
    if left.funding.daily_rate is None or right.funding.daily_rate is None:
        raise ValueError("Funding pairs require daily funding rates on both sides")
    left_daily_rate = left.funding.daily_rate
    right_daily_rate = right.funding.daily_rate

    if left_daily_rate >= right_daily_rate:
        short_market = left
        long_market = right
        short_fee = left_fee
        long_fee = right_fee
    else:
        short_market = right
        long_market = left
        short_fee = right_fee
        long_fee = left_fee

    if short_market.funding.daily_rate is None or long_market.funding.daily_rate is None:
        raise ValueError("Funding pairs require daily funding rates on both sides")
    gross_daily_edge = short_market.funding.daily_rate - long_market.funding.daily_rate
    entry_cost_rate = short_fee.taker_fee_rate + long_fee.taker_fee_rate
    round_trip_cost_rate = entry_cost_rate * 2.0

    break_even_days_entry = _break_even_days(entry_cost_rate, gross_daily_edge)
    break_even_days_round_trip = _break_even_days(round_trip_cost_rate, gross_daily_edge)

    capacity = estimate_capacity(
        short_market.identity.venue,
        short_market.market.top_of_book,
        long_market.identity.venue,
        long_market.market.top_of_book,
    )

    return FundingArbOpportunity(
        canonical_symbol=left.identity.canonical_symbol,
        long_venue=long_market.identity.venue,
        short_venue=short_market.identity.venue,
        long_fee_profile=long_fee.profile,
        short_fee_profile=short_fee.profile,
        gross_daily_edge=gross_daily_edge,
        entry_cost_rate=entry_cost_rate,
        round_trip_cost_rate=round_trip_cost_rate,
        one_day_net_edge_after_entry=gross_daily_edge - entry_cost_rate,
        one_day_net_edge_after_round_trip=gross_daily_edge - round_trip_cost_rate,
        break_even_days_entry=break_even_days_entry,
        break_even_days_round_trip=break_even_days_round_trip,
        short_spread_rate=_spread_rate(short_market.market.top_of_book),
        long_spread_rate=_spread_rate(long_market.market.top_of_book),
        short_daily_volume=short_market.market.daily_volume,
        long_daily_volume=long_market.market.daily_volume,
        short_open_interest=short_market.market.open_interest,
        long_open_interest=long_market.market.open_interest,
        short_stale_book=None,
        long_stale_book=None,
        capacity=capacity,
    )


def rank_opportunities(
    opportunities: list[FundingArbOpportunity],
) -> list[FundingArbOpportunity]:
    """Sort opportunities by net edge with liquidity-quality demotions."""

    return sorted(
        opportunities,
        key=lambda item: (
            _ranking_score(item),
            _capacity_value(item),
        ),
        reverse=True,
    )


def estimate_capacity(
    short_venue: str,
    short_book: TopOfBook | None,
    long_venue: str,
    long_book: TopOfBook | None,
) -> CapacityEstimate | None:
    """Estimate the max entry notional from the relevant book sides."""

    short_bid_notional = _notional(short_book, "bid")
    long_ask_notional = _notional(long_book, "ask")

    if short_bid_notional is None or long_ask_notional is None:
        return None

    limiting_venue = short_venue if short_bid_notional <= long_ask_notional else long_venue
    return CapacityEstimate(
        short_bid_notional=short_bid_notional,
        long_ask_notional=long_ask_notional,
        max_entry_notional=min(short_bid_notional, long_ask_notional),
        limiting_venue=limiting_venue,
    )


def _notional(book: TopOfBook | None, side: str) -> float | None:
    if side not in {"bid", "ask"}:
        raise ValueError(f"Unsupported side: {side}")
    if book is None:
        return None

    if side == "bid":
        if book.best_bid_price is None or book.best_bid_size is None:
            return None
        if book.best_bid_price <= 0 or book.best_bid_size <= 0:
            return 0.0
        return book.best_bid_price * book.best_bid_size

    if book.best_ask_price is None or book.best_ask_size is None:
        return None
    if book.best_ask_price <= 0 or book.best_ask_size <= 0:
        return 0.0
    return book.best_ask_price * book.best_ask_size


def _break_even_days(cost_rate: float, gross_daily_edge: float) -> float | None:
    if gross_daily_edge <= 0:
        return None
    return cost_rate / gross_daily_edge


def _spread_rate(book: TopOfBook | None) -> float | None:
    if book is None or book.best_bid_price is None or book.best_ask_price is None:
        return None
    if book.best_bid_price <= 0 or book.best_ask_price <= 0:
        return None
    mid_price = (book.best_bid_price + book.best_ask_price) / 2.0
    if mid_price <= 0:
        return None
    return max(book.best_ask_price - book.best_bid_price, 0.0) / mid_price


def _capacity_value(item: FundingArbOpportunity) -> float:
    if item.capacity is None or item.capacity.max_entry_notional is None:
        return -1.0
    return item.capacity.max_entry_notional


def _ranking_score(item: FundingArbOpportunity) -> float:
    """Compute a composite ranking score with liquidity-quality demotions."""

    score = item.one_day_net_edge_after_round_trip
    score -= _spread_penalty(item)
    score -= _stale_penalty(item)
    score -= _missing_quality_penalty(item)
    score += _liquidity_bonus(item)
    return score


def _spread_penalty(item: FundingArbOpportunity) -> float:
    return (item.short_spread_rate or 0.0) + (item.long_spread_rate or 0.0)


def _stale_penalty(item: FundingArbOpportunity) -> float:
    penalty = 0.0
    if item.short_stale_book:
        penalty += 1.0
    if item.long_stale_book:
        penalty += 1.0
    return penalty


def _liquidity_bonus(item: FundingArbOpportunity) -> float:
    min_open_interest = _min_defined(item.short_open_interest, item.long_open_interest)
    min_daily_volume = _min_defined(item.short_daily_volume, item.long_daily_volume)

    bonus = 0.0
    if min_open_interest is not None:
        bonus += math.log10(1.0 + min_open_interest) * 1e-6
    if min_daily_volume is not None:
        bonus += math.log10(1.0 + min_daily_volume) * 1e-6
    return bonus


def _missing_quality_penalty(item: FundingArbOpportunity) -> float:
    penalty = 0.0
    if item.short_spread_rate is None:
        penalty += 0.01
    if item.long_spread_rate is None:
        penalty += 0.01
    if item.capacity is None or item.capacity.max_entry_notional is None:
        penalty += 0.01
    return penalty


def _min_defined(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return min(left, right)
