"""Canary lifecycle result models."""

from pydantic import BaseModel, Field

from carryme_models.approval import RouteApprovalEntry
from carryme_models.balance_accounting import PaperTradeBalanceDelta, VenueBalanceSnapshot
from carryme_models.confirmation import PairClosePreviewConfirmationEntry, PreviewConfirmationEntry
from carryme_models.execution import ExecutionPairStatus, GuardedPairExecutionResult
from carryme_models.intent import PaperTradeEntry
from carryme_models.universe import FundingUniverseCanaryCandidate


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
