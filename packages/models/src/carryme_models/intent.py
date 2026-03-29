"""Trade intent models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class TradeLegIntent(BaseModel):
    """A single venue-side action for opening a paired funding trade."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    fee_profile: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    target_notional: float = Field(gt=0)


class FundingPairTradeIntent(BaseModel):
    """A dry-run, risk-gated paired trade recommendation."""

    label: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    source_recorded_at: datetime
    one_day_net_edge_after_entry: float
    break_even_days_entry: float | None = Field(default=None, ge=0)
    capacity_limit_notional: float = Field(gt=0)
    target_notional: float = Field(gt=0)
    capacity_fraction: float = Field(gt=0, le=1)
    max_target_notional: float = Field(gt=0)
    long_leg: TradeLegIntent
    short_leg: TradeLegIntent
