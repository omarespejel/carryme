"""Canary lifecycle result models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from carryme_models.approval import RouteApprovalEntry
from carryme_models.balance_accounting import PaperTradeBalanceDelta, VenueBalanceSnapshot
from carryme_models.confirmation import PairClosePreviewConfirmationEntry, PreviewConfirmationEntry
from carryme_models.execution import ExecutionPairStatus, GuardedPairExecutionResult
from carryme_models.intent import PaperTradeEntry
from carryme_models.universe import ApprovedCanaryBasketPlan, FundingUniverseCanaryCandidate


class CanaryLifecycleResult(BaseModel):
    """End-to-end operator-facing result for one guarded canary cycle."""

    candidate: FundingUniverseCanaryCandidate
    approval: RouteApprovalEntry
    paper_trade: PaperTradeEntry
    open_confirmation: PreviewConfirmationEntry
    open_execution: GuardedPairExecutionResult
    close_confirmation: PairClosePreviewConfirmationEntry | None = None
    close_execution: GuardedPairExecutionResult | None = None
    pre_open_snapshots: list[VenueBalanceSnapshot] = Field(default_factory=list)
    post_open_snapshots: list[VenueBalanceSnapshot] = Field(default_factory=list)
    post_close_snapshots: list[VenueBalanceSnapshot] = Field(default_factory=list)
    balance_delta: PaperTradeBalanceDelta | None = None
    final_pair_status: ExecutionPairStatus
    notes: list[str] = Field(default_factory=list)


class CanaryBasketRouteOutcome(BaseModel):
    """One route outcome within a sequential approved-canary basket launch."""

    label: str = Field(min_length=1)
    selected_notional: float = Field(ge=0)
    status: Literal["completed", "failed"]
    paper_trade_id: int | None = None
    result: CanaryLifecycleResult | None = None
    error: str | None = None


class CanaryBasketBalanceDelta(BaseModel):
    """Aggregated balance delta across all successful routes in one basket run."""

    route_count: int = Field(ge=0)
    successful_route_count: int = Field(ge=0)
    total_collateral_delta: float | None = None
    total_available_to_trade_delta: float | None = None
    total_free_collateral_delta: float | None = None
    routes: list[PaperTradeBalanceDelta] = Field(default_factory=list)


class CanaryBasketLaunchResult(BaseModel):
    """Persisted result for one shared-cap approved-canary basket launch."""

    basket_id: int | None = None
    launched_at: datetime
    status: Literal["completed", "completed_with_failures"]
    basket_plan: ApprovedCanaryBasketPlan
    route_count: int = Field(ge=0)
    successful_route_count: int = Field(ge=0)
    failed_route_count: int = Field(ge=0)
    routes: list[CanaryBasketRouteOutcome] = Field(default_factory=list)
    balance_delta: CanaryBasketBalanceDelta | None = None
    notes: list[str] = Field(default_factory=list)
