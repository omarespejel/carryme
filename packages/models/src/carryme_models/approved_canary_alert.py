"""Approved-canary alert models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from carryme_models.approved_canary import ApprovedCanarySnapshot


class ApprovedCanaryAlertEvent(BaseModel):
    """Alert emitted when approved-canary availability changes materially."""

    emitted_at: datetime
    alert_type: Literal[
        "approved_canary_available",
        "approved_canary_changed",
        "approved_canary_stale",
    ]
    max_snapshot_age_seconds: int
    current_snapshot: ApprovedCanarySnapshot | None = None
    previous_snapshot: ApprovedCanarySnapshot | None = None

