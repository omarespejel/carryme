"""Normalization models shared across venue-specific packages."""

from typing import Literal

from pydantic import BaseModel, Field

from carryme_models.market import MarketStats

AccrualStyle = Literal["continuous", "scheduled"]
ContractType = Literal["perpetual"]


class MarketIdentity(BaseModel):
    """Canonical identity for a perp market across venues."""

    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    base_asset: str = Field(min_length=1)
    quote_asset: str = Field(min_length=1)
    contract_type: ContractType = "perpetual"
    canonical_symbol: str = Field(min_length=1)


class FundingRateNormalization(BaseModel):
    """Normalized funding view for a venue-specific quoted rate."""

    venue: str = Field(min_length=1)
    raw_rate: float | None = None
    quoted_interval_hours: float = Field(gt=0)
    payment_interval_hours: float | None = Field(default=None, gt=0)
    formula_interval_hours: float | None = Field(default=None, gt=0)
    accrual_style: AccrualStyle
    hourly_rate: float | None = None
    daily_rate: float | None = None
    notes: list[str] = Field(default_factory=list)


class TradingFeeProfile(BaseModel):
    """A concrete maker/taker fee profile for a venue."""

    venue: str = Field(min_length=1)
    profile: str = Field(min_length=1)
    maker_fee_rate: float
    taker_fee_rate: float
    notes: list[str] = Field(default_factory=list)


class NormalizedMarketSnapshot(BaseModel):
    """Market stats joined with canonical identity and funding metadata."""

    identity: MarketIdentity
    market: MarketStats
    funding: FundingRateNormalization

