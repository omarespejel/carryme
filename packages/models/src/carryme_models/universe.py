"""Funding-universe discovery and ranking models."""

import math

from pydantic import BaseModel, Field, model_validator

from carryme_models.opportunity import FundingArbOpportunity


class FundingUniverseOverlap(BaseModel):
    """A canonical perp that exists on at least two venues."""

    canonical_symbol: str = Field(min_length=1)
    venues: list[str] = Field(default_factory=list, min_length=2)
    venue_symbols: dict[str, str] = Field(default_factory=dict, min_length=2)


class FundingUniverseVenueMarket(BaseModel):
    """Venue-specific market context attached to a universe opportunity."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    mark_price: float | None = Field(default=None, ge=0)
    daily_funding_rate: float | None = None
    open_interest: float | None = Field(default=None, ge=0)
    daily_volume: float | None = Field(default=None, ge=0)
    bid_notional: float | None = Field(default=None, ge=0)
    ask_notional: float | None = Field(default=None, ge=0)


class FundingUniverseOpportunity(BaseModel):
    """A scored funding opportunity enriched with universe-level context."""

    opportunity: FundingArbOpportunity
    venue_markets: dict[str, FundingUniverseVenueMarket] = Field(default_factory=dict)
    min_daily_volume: float | None = Field(default=None, ge=0)
    min_open_interest: float | None = Field(default=None, ge=0)
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

    @model_validator(mode="after")
    def _validate_totals(self) -> "FundingUniversePortfolioPlan":
        if not math.isclose(
            self.target_notional,
            self.allocated_notional + self.unused_notional,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError(
                "Portfolio plan target_notional must equal allocated_notional + unused_notional"
            )
        return self
