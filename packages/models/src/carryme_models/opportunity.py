"""Opportunity-scoring models."""

from pydantic import BaseModel, Field


class CapacityEstimate(BaseModel):
    """Top-of-book capacity limits for an entry."""

    short_bid_notional: float | None = Field(default=None, ge=0)
    long_ask_notional: float | None = Field(default=None, ge=0)
    max_entry_notional: float | None = Field(default=None, ge=0)
    limiting_venue: str | None = None


class FundingArbOpportunity(BaseModel):
    """A scored funding arbitrage opportunity across two venues."""

    canonical_symbol: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_fee_profile: str = Field(min_length=1)
    short_fee_profile: str = Field(min_length=1)
    gross_daily_edge: float = Field(ge=0)
    entry_cost_rate: float = Field(ge=0)
    round_trip_cost_rate: float = Field(ge=0)
    one_day_net_edge_after_entry: float
    one_day_net_edge_after_round_trip: float
    break_even_days_entry: float | None = Field(default=None, ge=0)
    break_even_days_round_trip: float | None = Field(default=None, ge=0)
    short_spread_rate: float | None = Field(default=None, ge=0)
    long_spread_rate: float | None = Field(default=None, ge=0)
    short_daily_volume: float | None = Field(default=None, ge=0)
    long_daily_volume: float | None = Field(default=None, ge=0)
    short_open_interest: float | None = Field(default=None, ge=0)
    long_open_interest: float | None = Field(default=None, ge=0)
    short_stale_book: bool | None = None
    long_stale_book: bool | None = None
    capacity: CapacityEstimate | None = None
