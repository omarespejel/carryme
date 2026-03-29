"""Execution journal models."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from carryme_models.intent import PaperTradeEntry


class ExecutionLegResult(BaseModel):
    """A simulated or real execution result for one leg."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    fee_profile: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    target_notional: float = Field(gt=0)
    status: Literal["accepted", "rejected", "submitted"]
    simulated: bool = True
    external_reference: str | None = None
    request_payload: Any | None = None
    response_payload: Any | None = None
    signature_timestamp_ms: int | None = None
    raw_payload: Any | None = None


class ExecutionJournalEntry(BaseModel):
    """An append-only execution journal entry derived from a paper trade."""

    entry_id: int | None = None
    executed_at: datetime
    adapter: str = Field(min_length=1)
    mode: Literal["mock", "live"]
    submission_id: str | None = None
    status: Literal["accepted", "rejected", "submitted", "partial"]
    paper_trade_id: int | None = None
    preview_hash: str | None = None
    confirmation_entry_id: int | None = None
    paper_trade: PaperTradeEntry
    legs: list[ExecutionLegResult] = Field(min_length=1)


class ExecutionVenueReconciliation(BaseModel):
    """Observed account-state summary for one venue after an execution attempt."""

    venue: str = Field(min_length=1)
    authenticated: bool
    ready: bool
    account_identifier: str | None = None
    total_collateral: float | None = None
    available_to_trade: float | None = None
    free_collateral: float | None = None
    balance_assets: list[str] = Field(default_factory=list)
    position_symbols: list[str] = Field(default_factory=list)
    matched_leg_symbols: list[str] = Field(default_factory=list)
    unmatched_leg_symbols: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)


class ExecutionReconciliation(BaseModel):
    """Combined execution-journal and live account-state reconciliation."""

    execution_entry_id: int | None = None
    paper_trade_id: int | None = None
    preview_hash: str | None = None
    status: Literal["accepted", "rejected", "submitted", "partial"]
    recommended_action: str = Field(min_length=1)
    matched_all_leg_symbols: bool
    venues: list[ExecutionVenueReconciliation] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


ObservationSource = Literal[
    "websocket_order_updates",
    "websocket_user_fills",
    "websocket_error",
    "rest_poll",
    "observer_error",
]


class ExecutionLegOrderState(BaseModel):
    """Observed live order state for one execution leg."""

    venue: str = Field(min_length=1)
    supported: bool
    observation_source: ObservationSource | None = None
    external_reference: str | None = None
    client_id: str | None = None
    derived_state: Literal["open", "filled", "partial_fill", "unfilled", "unknown", "unsupported"]
    order_status: str | None = None
    cancel_reason: str | None = None
    avg_fill_price: str | None = None
    remaining_size: str | None = None
    size: str | None = None
    notes: list[str] = Field(default_factory=list)
    raw_response: dict[str, Any] | None = None


class ExecutionOrderState(BaseModel):
    """Observed venue order-state view for a journaled execution."""

    execution_entry_id: int | None = None
    paper_trade_id: int | None = None
    preview_hash: str | None = None
    legs: list[ExecutionLegOrderState] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ExecutionObservationEntry(BaseModel):
    """Append-only observation snapshot captured while monitoring a live execution."""

    entry_id: int | None = None
    observed_at: datetime
    context: str = Field(min_length=1)
    execution_entry_id: int | None = None
    paper_trade_id: int | None = None
    preview_hash: str | None = None
    order_state: ExecutionOrderState
    pair_status: "ExecutionPairStatus | None" = None


class ExecutionPairStatus(BaseModel):
    """Combined pair-level execution status derived from orders and positions."""

    execution_entry_id: int | None = None
    paper_trade_id: int | None = None
    preview_hash: str | None = None
    derived_state: Literal[
        "hedged",
        "pending",
        "unfilled",
        "closed",
        "cleanup_needed",
        "review_required",
    ]
    recommended_action: str = Field(min_length=1)
    order_state: ExecutionOrderState
    reconciliation: ExecutionReconciliation
    notes: list[str] = Field(default_factory=list)


class GuardedPairExecutionResult(BaseModel):
    """Result of a guarded pair attempt with optional automatic cleanup."""

    paper_trade_id: int
    preview_hash: str = Field(min_length=1)
    primary_execution: ExecutionJournalEntry
    cleanup_execution: ExecutionJournalEntry | None = None
    pair_status: ExecutionPairStatus


ExecutionObservationEntry.model_rebuild()
