"""Approved canary snapshot models."""

from datetime import datetime

from pydantic import BaseModel, Field

from carryme_models.approval import RouteApprovalEntry
from carryme_models.universe import FundingUniverseCanaryCandidate


class ApprovedCanarySnapshot(BaseModel):
    """Persisted snapshot of a route that is both live-eligible and operator-approved."""

    snapshot_id: int | None = None
    captured_at: datetime
    label: str = Field(min_length=1)
    candidate: FundingUniverseCanaryCandidate
    approval: RouteApprovalEntry
