"""Venue-routed cleanup preview generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from carryme_models import ExecutionCleanupPreview, ExecutionJournalEntry, ExecutionPairStatus

_CLEANUP_RECOMMENDED_ACTIONS = {
    "close_open_leg",
    "complete_or_unwind_missing_leg",
}


class VenueCleanupPreviewService(Protocol):
    """Build one reduce-only cleanup preview for a specific venue."""

    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
        slippage_tolerance_bps: int = 10,
    ) -> ExecutionCleanupPreview: ...


@dataclass(frozen=True)
class CleanupPreviewRouter:
    """Dispatch cleanup-preview generation to the venue that is still exposed."""

    services: dict[str, VenueCleanupPreviewService]

    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
        slippage_tolerance_bps: int = 10,
    ) -> ExecutionCleanupPreview:
        target_venue = select_cleanup_preview_venue(entry, pair_status)
        service = self.services.get(target_venue)
        if service is None:
            raise ValueError(f"No cleanup preview service is registered for venue {target_venue!r}")
        return await service.preview_from_execution(
            entry=entry,
            pair_status=pair_status,
            slippage_tolerance_bps=slippage_tolerance_bps,
        )


def select_cleanup_preview_venue(
    entry: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
) -> str:
    """Return the single venue that still has an open leg requiring cleanup."""

    if pair_status.recommended_action not in _CLEANUP_RECOMMENDED_ACTIONS:
        raise ValueError("Cleanup preview is only available when pair status recommends it")

    position_symbols_by_venue = {
        venue.venue: set(venue.position_symbols) for venue in pair_status.reconciliation.venues
    }
    candidate_venues: list[str] = []
    for leg in entry.legs:
        if leg.symbol not in position_symbols_by_venue.get(leg.venue, set()):
            continue
        if leg.venue not in candidate_venues:
            candidate_venues.append(leg.venue)

    if len(candidate_venues) != 1:
        raise ValueError("Cleanup preview requires exactly one open leg across all venues")
    return candidate_venues[0]


def cleanup_action_requires_unwind(pair_status: ExecutionPairStatus) -> bool:
    """Return whether a pair-status action should trigger one-leg cleanup."""

    return pair_status.recommended_action in _CLEANUP_RECOMMENDED_ACTIONS
