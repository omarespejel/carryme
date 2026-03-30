"""System-state alert models for worker notifications."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from carryme_models.system_state import VenueSystemState


class SystemStateAlertEvent(BaseModel):
    """Alert emitted when a venue system-state transitions materially."""

    emitted_at: datetime
    venue: str = Field(min_length=1)
    alert_type: Literal[
        "venue_degraded",
        "venue_recovered",
        "venue_status_changed",
    ]
    current_state: VenueSystemState
    previous_state: VenueSystemState | None = None
