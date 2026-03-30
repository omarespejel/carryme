"""Launch-ready canary snapshot models."""

from datetime import datetime

from pydantic import BaseModel, Field

from carryme_models.approved_canary import ApprovedCanarySnapshot
from carryme_models.system_state import PaperTradeSystemState


class LaunchReadyCanarySnapshot(BaseModel):
    """Persisted snapshot of a fresh approved canary with healthy venue state."""

    launch_ready_snapshot_id: int | None = None
    captured_at: datetime
    label: str = Field(min_length=1)
    max_snapshot_age_seconds: int
    approved_snapshot: ApprovedCanarySnapshot
    system_state: PaperTradeSystemState
