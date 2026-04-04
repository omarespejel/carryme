"""Stable canary launch record models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class StableCanaryLaunchRecord(BaseModel):
    """Persisted record of a stable cached canary launch attempt."""

    launch_id: int | None = None
    launched_at: datetime
    status: Literal["launched", "shadowed"]
    label: str = Field(min_length=1)
    launch_ready_snapshot_id: int
    approved_snapshot_id: int
    paper_trade_id: int
    final_pair_state: str = Field(min_length=1)
    detail: str | None = None
