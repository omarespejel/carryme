"""Stable launch-ready canary alert models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from carryme_models.launch_ready_canary import LaunchReadyCanaryStability


class StableLaunchReadyAlertEvent(BaseModel):
    """Alert emitted when stable launch-ready availability changes materially."""

    emitted_at: datetime
    alert_type: Literal[
        "stable_launch_ready_available",
        "stable_launch_ready_changed",
        "stable_launch_ready_stale",
    ]
    max_snapshot_age_seconds: int
    min_snapshot_count: int
    min_stable_seconds: float
    current_stability: LaunchReadyCanaryStability | None = None
    previous_stability: LaunchReadyCanaryStability | None = None
