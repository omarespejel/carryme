"""System-state readiness models for live execution gating."""

from pydantic import BaseModel, Field


class VenueSystemState(BaseModel):
    """Observed system-state status for one venue."""

    venue: str = Field(min_length=1)
    enabled: bool
    checked: bool
    healthy: bool
    status: str | None = None
    blocking_reasons: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class PaperTradeSystemState(BaseModel):
    """System-state status scoped to the venues touched by one paper trade."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    ready: bool
    venues: list[VenueSystemState] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)
