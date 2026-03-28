"""Opportunity-scoring models."""

from pydantic import BaseModel, Field


class CapacityEstimate(BaseModel):
    """Top-of-book capacity limits for an entry."""

    short_bid_notional: float | None = None
    long_ask_notional: float | None = None
    max_entry_notional: float | None = None
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
    capacity: CapacityEstimate | None = None
