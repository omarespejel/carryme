"""Execution adapter helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import uuid4

from carryme_models import ExecutionJournalEntry, ExecutionLegResult, PaperTradeEntry


class ExecutionAdapter(Protocol):
    """Interface for submitting a saved paper trade to an execution backend."""

    def submit(
        self,
        paper_trade: PaperTradeEntry,
        *,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry: ...


class MockExecutionAdapter:
    """Deterministic simulated execution adapter used before live venue auth exists."""

    adapter_name = "mock"
    mode: Literal["mock"] = "mock"

    def submit(
        self,
        paper_trade: PaperTradeEntry,
        *,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        timestamp = executed_at or datetime.now(UTC)
        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before execution submission")
        submission_id = uuid4().hex

        return ExecutionJournalEntry(
            executed_at=timestamp,
            adapter=self.adapter_name,
            mode=self.mode,
            submission_id=submission_id,
            status="accepted",
            paper_trade_id=paper_trade.entry_id,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue=paper_trade.intent.long_leg.venue,
                    symbol=paper_trade.intent.long_leg.symbol,
                    fee_profile=paper_trade.intent.long_leg.fee_profile,
                    side=paper_trade.intent.long_leg.side,
                    target_notional=paper_trade.intent.long_leg.target_notional,
                    status="accepted",
                    simulated=True,
                    external_reference=(
                        f"{self.adapter_name}:{paper_trade.entry_id}:"
                        f"{paper_trade.intent.long_leg.venue}:"
                        f"{paper_trade.intent.long_leg.symbol}:"
                        f"{paper_trade.intent.long_leg.side}:"
                        f"{submission_id}"
                    ),
                ),
                ExecutionLegResult(
                    venue=paper_trade.intent.short_leg.venue,
                    symbol=paper_trade.intent.short_leg.symbol,
                    fee_profile=paper_trade.intent.short_leg.fee_profile,
                    side=paper_trade.intent.short_leg.side,
                    target_notional=paper_trade.intent.short_leg.target_notional,
                    status="accepted",
                    simulated=True,
                    external_reference=(
                        f"{self.adapter_name}:{paper_trade.entry_id}:"
                        f"{paper_trade.intent.short_leg.venue}:"
                        f"{paper_trade.intent.short_leg.symbol}:"
                        f"{paper_trade.intent.short_leg.side}:"
                        f"{submission_id}"
                    ),
                ),
            ],
        )
