"""Unified live submission readiness models."""

from pydantic import BaseModel, Field

from carryme_models.account_preflight import PaperTradeAccountPreflight
from carryme_models.preflight import PaperTradeExecutionPreflight


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
    blocking_reasons: list[str] = Field(default_factory=list)
