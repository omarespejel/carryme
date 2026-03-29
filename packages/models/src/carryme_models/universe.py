"""Funding-universe discovery and ranking models."""

from pydantic import BaseModel, Field

from carryme_models.opportunity import FundingArbOpportunity


class FundingUniverseOverlap(BaseModel):
    """A canonical perp that exists on at least two venues."""

    canonical_symbol: str = Field(min_length=1)
    venues: list[str] = Field(default_factory=list)
    venue_symbols: dict[str, str] = Field(default_factory=dict)


class FundingUniverseVenueMarket(BaseModel):
    """Venue-specific market context attached to a universe opportunity."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    mark_price: float | None = None
    daily_funding_rate: float | None = None
    open_interest: float | None = None
    daily_volume: float | None = None
    bid_notional: float | None = None
    ask_notional: float | None = None


class FundingUniverseOpportunity(BaseModel):
    """A scored funding opportunity enriched with universe-level context."""

    opportunity: FundingArbOpportunity
    venue_markets: dict[str, FundingUniverseVenueMarket] = Field(default_factory=dict)
    min_daily_volume: float | None = None
    min_open_interest: float | None = None
    target_notional: float | None = Field(default=None, ge=0)
    deployable_notional: float | None = Field(default=None, ge=0)
    estimated_one_day_pnl_after_entry: float | None = None
    estimated_one_day_pnl_after_round_trip: float | None = None
    quality_score: float | None = None


class FundingUniverseScan(BaseModel):
    """A ranked scan across all overlapping perp markets for the selected venues."""

    venues: list[str] = Field(default_factory=list)
    ranking: str = Field(min_length=1)
    target_notional: float = Field(ge=0)
    overlap_count: int = Field(ge=0)
    overlaps: list[FundingUniverseOverlap] = Field(default_factory=list)
    opportunities: list[FundingUniverseOpportunity] = Field(default_factory=list)


class FundingUniversePortfolioEntry(BaseModel):
    """A selected allocation within a funding-universe portfolio plan."""

    opportunity: FundingUniverseOpportunity
    selected_notional: float = Field(ge=0)
    estimated_one_day_pnl_after_entry: float
    estimated_one_day_pnl_after_round_trip: float


class FundingUniversePortfolioPlan(BaseModel):
    """A greedy allocation plan built from a ranked funding-universe scan."""

    ranking: str = Field(min_length=1)
    target_notional: float = Field(ge=0)
    allocated_notional: float = Field(ge=0)
    unused_notional: float = Field(ge=0)
    estimated_one_day_pnl_after_entry: float
    estimated_one_day_pnl_after_round_trip: float
    entries: list[FundingUniversePortfolioEntry] = Field(default_factory=list)
