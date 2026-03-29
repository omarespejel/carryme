"""Normalization models shared across venue-specific packages."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field

from carryme_models.market import MarketStats

AccrualStyle = Literal["continuous", "scheduled"]
ContractType = Literal["perpetual"]
FeeChannel = Literal["api"]
FeeModifierType = Literal[
    "none",
    "rolling_volume_usd",
    "maker_share_fraction",
    "fastfills_discount_fraction",
]
ModifierValueUnit = Literal["none", "usd_notional", "fraction"]
FeeRateUnit = Literal["fraction_of_notional"]
ResolutionStatus = Literal["resolved", "unresolved", "disputed"]
SettlementTiming = Literal["fixed_utc_windows", "rolling", "continuous"]
NonEmptyStr = Annotated[str, Field(min_length=1)]


class MarketIdentity(BaseModel):
    """Canonical identity for a perp market across venues."""

    venue: NonEmptyStr
    venue_symbol: NonEmptyStr
    base_asset: NonEmptyStr
    quote_asset: NonEmptyStr
    contract_type: ContractType = "perpetual"
    canonical_symbol: NonEmptyStr
    aliases: list[NonEmptyStr] = Field(default_factory=list)
    resolution_status: ResolutionStatus = "resolved"
    resolution_note: str | None = None


class FundingRateNormalization(BaseModel):
    """Normalized funding view for a venue-specific quoted rate."""

    venue: NonEmptyStr
    raw_rate: float | None = None
    quoted_interval_hours: float = Field(gt=0)
    payment_interval_hours: float | None = Field(default=None, gt=0)
    formula_interval_hours: float | None = Field(default=None, gt=0)
    accrual_style: AccrualStyle
    settlement_timing: SettlementTiming
    settlement_interval_hours: float | None = Field(default=None, gt=0)
    hourly_rate: float | None = None
    daily_rate: float | None = None
    notes: list[str] = Field(default_factory=list)


class TradingFeeProfile(BaseModel):
    """A concrete maker/taker fee profile for a venue."""

    venue: NonEmptyStr
    profile: NonEmptyStr
    channel: FeeChannel = "api"
    modifier_type: FeeModifierType = "none"
    modifier_value: float | None = None
    modifier_value_unit: ModifierValueUnit = "none"
    maker_fee_rate: float
    taker_fee_rate: float
    fee_rate_unit: FeeRateUnit = "fraction_of_notional"
    source: NonEmptyStr
    as_of: AwareDatetime
    notes: list[str] = Field(default_factory=list)


class NormalizedMarketSnapshot(BaseModel):
    """Market stats joined with canonical identity and funding metadata."""

    identity: MarketIdentity
    market: MarketStats
    funding: FundingRateNormalization
