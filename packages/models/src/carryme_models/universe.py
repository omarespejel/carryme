"""Funding-universe discovery and ranking models."""

import math
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from carryme_models.approval import RouteApprovalEntry, RouteApprovalUpsert
from carryme_models.history import FundingPairSpec
from carryme_models.opportunity import FundingArbOpportunity

SUPPORTED_UNIVERSE_VENUES: tuple[str, ...] = ("extended", "paradex", "hyperliquid")


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
    bid_notional_api: float | None = Field(default=None, ge=0)
    ask_notional_api: float | None = Field(default=None, ge=0)
    bid_notional_interactive: float | None = Field(default=None, ge=0)
    ask_notional_interactive: float | None = Field(default=None, ge=0)


class ExecutionQualitySummary(BaseModel):
    """Smoothed execution-quality summary for one symbol and venue direction."""

    canonical_symbol: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    sample_size: int = Field(ge=0)
    weighted_score: float = Field(ge=0)
    latest_outcome: Literal[
        "hedged",
        "pending",
        "unfilled",
        "closed",
        "cleanup_needed",
        "review_required",
    ] | None = None
    hedged_count: int = Field(default=0, ge=0)
    closed_count: int = Field(default=0, ge=0)
    unfilled_count: int = Field(default=0, ge=0)
    cleanup_needed_count: int = Field(default=0, ge=0)
    review_required_count: int = Field(default=0, ge=0)
    pending_count: int = Field(default=0, ge=0)


class RouteStabilitySummary(BaseModel):
    """Repeated-scan stability summary for one routed venue direction."""

    canonical_symbol: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    short_fee_profile: str = Field(min_length=1)
    long_fee_profile: str = Field(min_length=1)
    sample_size: int = Field(ge=0)
    window_count: int = Field(ge=0)
    presence_ratio: float = Field(ge=0, le=1)
    positive_roundtrip_share: float = Field(ge=0, le=1)
    mean_roundtrip_edge: float
    median_roundtrip_edge: float
    edge_stddev: float = Field(ge=0)
    mean_capacity_notional: float | None = Field(default=None, ge=0)
    median_capacity_notional: float | None = Field(default=None, ge=0)
    capacity_stddev: float | None = Field(default=None, ge=0)
    latest_roundtrip_edge: float | None = None
    latest_recorded_at: datetime | None = None
    stability_weight: float = Field(ge=0, le=1)
    stability_score: float


class FundingUniverseOpportunity(BaseModel):
    """A scored funding opportunity enriched with universe-level context."""

    opportunity: FundingArbOpportunity
    policy_tags: list[str] = Field(default_factory=list)
    venue_markets: dict[str, FundingUniverseVenueMarket] = Field(default_factory=dict)
    min_daily_volume: float | None = Field(default=None, ge=0)
    min_open_interest: float | None = Field(default=None, ge=0)
    target_notional: float | None = Field(default=None, ge=0)
    deployable_notional: float | None = Field(default=None, ge=0)
    modeled_entry_cost_rate: float | None = Field(default=None, ge=0)
    modeled_round_trip_cost_rate: float | None = Field(default=None, ge=0)
    estimated_one_day_pnl_after_entry: float | None = None
    estimated_one_day_pnl_after_round_trip: float | None = None
    paradex_fastfill_share: float | None = Field(default=None, ge=0, le=1)
    paradex_fastfill_eligible_notional: float | None = Field(default=None, ge=0)
    quality_score: float | None = None
    execution_quality: ExecutionQualitySummary | None = None
    route_stability: RouteStabilitySummary | None = None
    execution_adjusted_one_day_pnl_after_round_trip: float | None = None
    execution_adjusted_quality_score: float | None = None
    stability_adjusted_one_day_pnl_after_round_trip: float | None = None
    stability_adjusted_quality_score: float | None = None
    route_adjusted_quality_score: float | None = None


class FundingUniverseScan(BaseModel):
    """A ranked scan across all overlapping perp markets for the selected venues."""

    venues: list[str] = Field(default_factory=list)
    fee_profiles: dict[str, str] = Field(default_factory=dict)
    ranking: str = Field(min_length=1)
    target_notional: float = Field(ge=0)
    overlap_count: int = Field(ge=0)
    overlaps: list[FundingUniverseOverlap] = Field(default_factory=list)
    opportunities: list[FundingUniverseOpportunity] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_fee_profiles(self) -> "FundingUniverseScan":
        normalized_venues = {venue.strip().lower() for venue in self.venues if venue.strip()}
        unknown = sorted(
            venue for venue in self.fee_profiles if venue.strip().lower() not in normalized_venues
        )
        if unknown:
            joined = ", ".join(unknown)
            raise ValueError(f"fee_profiles contains venues not present in scan venues: {joined}")
        empty = sorted(venue for venue, profile in self.fee_profiles.items() if not profile.strip())
        if empty:
            joined = ", ".join(empty)
            raise ValueError(f"fee_profiles contains empty profile names for venues: {joined}")
        return self


class FundingUniverseCanaryCandidate(BaseModel):
    """A route approved for a tiny controlled live-money canary."""

    opportunity: FundingUniverseOpportunity
    suggested_canary_notional: float = Field(ge=0)


class FundingUniverseCanaryApprovalProposal(BaseModel):
    """A live canary route that is promising but not live-approved yet."""

    candidate_rank: int = Field(ge=1)
    label: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    short_fee_profile: str = Field(min_length=1)
    long_fee_profile: str = Field(min_length=1)
    approval_status: Literal["missing", "disabled"]
    suggested_max_live_notional: float = Field(gt=0)
    pair: FundingPairSpec
    approval_payload: RouteApprovalUpsert
    existing_approval: RouteApprovalEntry | None = None
    candidate: FundingUniverseCanaryCandidate


class FundingUniverseCanaryApprovalProposalSummary(BaseModel):
    """A lightweight operator-facing summary of one approval proposal."""

    generated_at: datetime = Field(
        description="UTC timestamp when this proposal summary was generated."
    )
    candidate_rank: int = Field(ge=1)
    label: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    short_fee_profile: str = Field(min_length=1)
    long_fee_profile: str = Field(min_length=1)
    approval_status: Literal["missing", "disabled"]
    suggested_canary_notional: float = Field(gt=0)
    suggested_max_live_notional: float = Field(gt=0)
    deployable_notional: float | None = Field(default=None, ge=0)
    estimated_one_day_pnl_after_round_trip: float | None = None
    route_adjusted_quality_score: float | None = None
    pair: FundingPairSpec
    approval_payload: RouteApprovalUpsert
    existing_approval: RouteApprovalEntry | None = None


class ApprovedCanaryBasketEntry(BaseModel):
    """One currently launchable approved canary route."""

    label: str = Field(min_length=1)
    approval: RouteApprovalEntry
    candidate: FundingUniverseCanaryCandidate
    selected_notional: float = Field(ge=0)
    estimated_one_day_pnl_after_entry: float
    estimated_one_day_pnl_after_round_trip: float
    execution_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    stability_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    route_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None


class ApprovedCanaryBasketPlan(BaseModel):
    """A ranked basket of live-approved canary routes."""

    venues: list[str] = Field(default_factory=list)
    fee_profiles: dict[str, str] = Field(default_factory=dict)
    target_notional: float = Field(ge=0)
    allocated_notional: float = Field(ge=0)
    unused_notional: float = Field(ge=0)
    estimated_one_day_pnl_after_entry: float
    estimated_one_day_pnl_after_round_trip: float
    execution_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    stability_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    route_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    entries: list[ApprovedCanaryBasketEntry] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_totals(self) -> "ApprovedCanaryBasketPlan":
        if not math.isclose(
            self.target_notional,
            self.allocated_notional + self.unused_notional,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError(
                "Approved canary basket target_notional must equal "
                "allocated_notional + unused_notional"
            )
        return self


class FundingUniversePortfolioEntry(BaseModel):
    """A selected allocation within a funding-universe portfolio plan."""

    opportunity: FundingUniverseOpportunity
    selected_notional: float = Field(ge=0)
    estimated_one_day_pnl_after_entry: float
    estimated_one_day_pnl_after_round_trip: float
    execution_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    stability_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    route_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None


class FundingUniversePortfolioPlan(BaseModel):
    """A greedy allocation plan built from a ranked funding-universe scan."""

    ranking: str = Field(min_length=1)
    target_notional: float = Field(ge=0)
    allocated_notional: float = Field(ge=0)
    unused_notional: float = Field(ge=0)
    estimated_one_day_pnl_after_entry: float
    estimated_one_day_pnl_after_round_trip: float
    execution_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    stability_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
    route_adjusted_estimated_one_day_pnl_after_round_trip: float | None = None
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
