"""Venue-routed cleanup live execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from carryme_models import CleanupPreviewConfirmationEntry, ExecutionJournalEntry, PaperTradeEntry


class CleanupLiveExecutionService(Protocol):
    """Submit one confirmed cleanup preview for a specific venue."""

    async def submit_confirmed_cleanup_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: CleanupPreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry: ...


@dataclass(frozen=True)
class CleanupLiveExecutionRouter:
    """Dispatch cleanup live execution to the venue targeted by the cleanup preview."""

    services: dict[str, CleanupLiveExecutionService]

    async def submit_confirmed_cleanup_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: CleanupPreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        venue = confirmation.preview.leg.venue
        service = self.services.get(venue)
        if service is None:
            raise ValueError(f"No cleanup live execution service is registered for venue {venue!r}")
        return await service.submit_confirmed_cleanup_preview(
            paper_trade=paper_trade,
            confirmation=confirmation,
            executed_at=executed_at,
        )
