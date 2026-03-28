"""Funding arbitrage scoring helpers."""

from __future__ import annotations

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
        capacity=capacity,
    )


def rank_opportunities(
    opportunities: list[FundingArbOpportunity],
) -> list[FundingArbOpportunity]:
    """Sort opportunities by one-day net edge, then by capacity."""

    return sorted(
        opportunities,
        key=lambda item: (
            item.one_day_net_edge_after_round_trip,
            (
                item.capacity.max_entry_notional
                if item.capacity and item.capacity.max_entry_notional is not None
                else -1.0
            ),
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
    if book is None:
        return None

    if side == "bid":
        if book.best_bid_price is None or book.best_bid_size is None:
            return None
        return book.best_bid_price * book.best_bid_size

    if book.best_ask_price is None or book.best_ask_size is None:
        return None
    return book.best_ask_price * book.best_ask_size


def _break_even_days(cost_rate: float, gross_daily_edge: float) -> float | None:
    if gross_daily_edge <= 0:
        return None
    return cost_rate / gross_daily_edge
