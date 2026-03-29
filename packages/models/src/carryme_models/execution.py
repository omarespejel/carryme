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
    status: Literal["accepted", "rejected", "submitted"]
    paper_trade_id: int | None = None
    preview_hash: str | None = None
    confirmation_entry_id: int | None = None
    paper_trade: PaperTradeEntry
    legs: list[ExecutionLegResult] = Field(min_length=1)
