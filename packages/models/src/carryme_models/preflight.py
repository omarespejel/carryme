"""Live execution preflight models."""

from pydantic import BaseModel, Field


class CredentialRequirementStatus(BaseModel):
    """One required live credential and whether it is currently present."""

    key: str = Field(min_length=1)
    env_var: str = Field(min_length=1)
    description: str = Field(min_length=1)
    secret: bool = True
    present: bool


class VenueExecutionPreflight(BaseModel):
    """Live-readiness status for one venue."""

    venue: str = Field(min_length=1)
    enabled: bool
    live_supported: bool = True
    ready: bool
    missing_env_vars: list[str] = Field(default_factory=list)
    requirements: list[CredentialRequirementStatus] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class PaperTradeExecutionPreflight(BaseModel):
    """Live-readiness status for executing one saved paper trade."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    ready: bool
    venues: list[VenueExecutionPreflight] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)
