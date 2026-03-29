"""Route approval models."""

from datetime import datetime

from pydantic import BaseModel, Field


class RouteApprovalUpsert(BaseModel):
    """Operator-controlled approval settings for one live route."""

    canonical_symbol: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    short_fee_profile: str = Field(min_length=1)
    long_fee_profile: str = Field(min_length=1)
    approved: bool = True
    max_live_notional: float = Field(gt=0)
    note: str | None = None


class RouteApprovalEntry(RouteApprovalUpsert):
    """Persisted route approval entry."""

    label: str = Field(min_length=1)
    updated_at: datetime
