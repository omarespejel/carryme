"""Unified live submission and production automation readiness models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from carryme_models.account_preflight import PaperTradeAccountPreflight
from carryme_models.preflight import PaperTradeExecutionPreflight
from carryme_models.stable_canary_launch import StableCanaryLaunchRecord
from carryme_models.system_state import PaperTradeSystemState


class LiveSubmissionReadiness(BaseModel):
    """Combined safety gate for a saved paper trade and one preview hash."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    preview_hash: str = Field(min_length=1)
    confirmation_entry_id: int | None = None
    confirmed_preview: bool
    ready: bool
    execution_preflight: PaperTradeExecutionPreflight
    account_preflight: PaperTradeAccountPreflight
    system_state: PaperTradeSystemState | None = None
    blocking_reasons: list[str] = Field(default_factory=list)


class AutomationSnapshotSummary(BaseModel):
    """Bounded snapshot identity for production automation status views."""

    snapshot_id: int | None = None
    label: str = Field(min_length=1)
    captured_at: datetime
    age_seconds: float = Field(ge=0)


class ProductionAutomationReadiness(BaseModel):
    """Read-only summary of whether unattended canary launch can proceed."""

    checked_at: datetime
    ready: bool
    status: Literal["ready", "blocked"]
    blocking_reasons: list[str] = Field(default_factory=list)
    approved_snapshot: AutomationSnapshotSummary | None = None
    launch_ready_snapshot: AutomationSnapshotSummary | None = None
    stable_launch_ready: bool
    stable_launch_consecutive_snapshots: int | None = None
    stable_launch_stable_seconds: float | None = None
    latest_stable_launch: StableCanaryLaunchRecord | None = None
    active_live_execution_count: int = Field(ge=0)
    max_active_live_executions: int = Field(ge=0)
