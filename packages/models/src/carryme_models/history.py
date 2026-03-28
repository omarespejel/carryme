"""History and watchlist models."""

from datetime import datetime

from pydantic import BaseModel, Field

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

