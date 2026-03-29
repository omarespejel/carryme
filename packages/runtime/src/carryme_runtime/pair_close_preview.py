"""Pair-close preview generation for live hedged executions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from carryme_models import (
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionPairClosePreview,
    ExecutionPairStatus,
)

from carryme_runtime.paired_live_execution import PairedLiveExecutionCoordinator


class VenuePairClosePreviewService(Protocol):
    """Build one venue-scoped reduce-only close preview for a hedged pair."""

    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
        slippage_tolerance_bps: int = 10,
    ) -> ExecutionCleanupPreview: ...


@dataclass(frozen=True)
class PairClosePreviewService:
    """Build a multi-leg reduce-only close preview for a live hedged pair."""

    services: dict[str, VenuePairClosePreviewService]

    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
        slippage_tolerance_bps: int = 10,
        generated_at: datetime | None = None,
    ) -> ExecutionPairClosePreview:
        if pair_status.derived_state != "hedged":
            raise ValueError("Pair close preview is only available for a live hedged pair")
        if pair_status.recommended_action != "monitor_open_hedge":
            raise ValueError("Pair close preview requires a hedged pair ready for monitoring")

        venues = select_pair_close_preview_venues(entry, pair_status)
        preview_status = pair_status.model_copy(update={"recommended_action": "close_open_leg"})

        cleanup_previews: list[ExecutionCleanupPreview] = []
        for venue in venues:
            service = self.services.get(venue)
            if service is None:
                raise ValueError(
                    f"No pair-close preview service is registered for venue {venue!r}"
                )
            cleanup_previews.append(
                await service.preview_from_execution(
                    entry=entry,
                    pair_status=preview_status,
                    slippage_tolerance_bps=slippage_tolerance_bps,
                )
            )

        timestamp = generated_at or datetime.now(UTC)
        legs = [preview.leg for preview in cleanup_previews]
        preview_hash = _pair_close_hash(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            legs=legs,
        )
        notes = ["Pair close preview was derived from current live positions on both venues."]
        for preview in cleanup_previews:
            notes.extend(preview.notes)

        return ExecutionPairClosePreview(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            label=entry.paper_trade.intent.label,
            generated_at=timestamp,
            slippage_tolerance_bps=slippage_tolerance_bps,
            preview_hash=preview_hash,
            reason="close_pair",
            legs=legs,
            notes=notes,
        )


def select_pair_close_preview_venues(
    entry: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
) -> list[str]:
    """Return the venues that currently hold the live hedged pair positions."""

    position_symbols_by_venue = {
        venue.venue: set(venue.position_symbols) for venue in pair_status.reconciliation.venues
    }
    candidate_venues: list[str] = []
    for leg in entry.legs:
        if leg.symbol not in position_symbols_by_venue.get(leg.venue, set()):
            continue
        if leg.venue not in candidate_venues:
            candidate_venues.append(leg.venue)

    if len(candidate_venues) != 2:
        raise ValueError("Pair close preview requires exactly two open legs across all venues")

    first_venue = PairedLiveExecutionCoordinator._resolve_first_venue(
        requested_first_venue="auto",
        preview_venues=candidate_venues,
    )
    second_venue = next(venue for venue in candidate_venues if venue != first_venue)
    return [first_venue, second_venue]


def _pair_close_hash(
    *,
    execution_entry_id: int | None,
    paper_trade_id: int | None,
    legs: Sequence[Any],
) -> str:
    encoded = json.dumps(
        {
            "execution_entry_id": execution_entry_id,
            "paper_trade_id": paper_trade_id,
            "legs": [
                leg.model_dump(mode="json") if hasattr(leg, "model_dump") else leg for leg in legs
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
