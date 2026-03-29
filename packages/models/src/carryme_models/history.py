"""History, watchlist, and alert models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from carryme_models.execution import ExecutionPairStatus
from carryme_models.opportunity import FundingArbOpportunity


class FundingPairSpec(BaseModel):
    """A configured funding pair to fetch and score repeatedly."""

    label: str | None = None
    left_venue: str = Field(min_length=1)
    left_symbol: str = Field(min_length=1)
    left_fee_profile: str = Field(min_length=1)
    right_venue: str = Field(min_length=1)
    right_symbol: str = Field(min_length=1)
    right_fee_profile: str = Field(min_length=1)


class OpportunityRecord(BaseModel):
    """A persisted scored opportunity at a point in time."""

    recorded_at: datetime
    pair: FundingPairSpec
    opportunity: FundingArbOpportunity


class WatchlistDocument(BaseModel):
    """A persisted funding-pair watchlist document."""

    pairs: list[FundingPairSpec] = Field(default_factory=list)


class CandidateAlertEvent(BaseModel):
    """A candidate event emitted when a saved record clears worker thresholds."""

    emitted_at: datetime
    alert_type: Literal["candidate_threshold_match"] = "candidate_threshold_match"
    min_one_day_net_edge_after_entry: float | None = None
    min_capacity_notional: float | None = None
    record: OpportunityRecord


class ExecutionAlertEvent(BaseModel):
    """An execution-monitor alert emitted when a live pair needs operator attention."""

    emitted_at: datetime
    alert_type: Literal["cleanup_needed", "review_required"]
    paper_trade_id: int
    preview_hash: str | None = None
    pair_status: ExecutionPairStatus
