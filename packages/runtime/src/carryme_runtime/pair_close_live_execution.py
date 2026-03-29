"""Paired close execution coordinator for live hedged pairs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

import httpx
from carryme_connectors import ConnectorError
from carryme_models import (
    CleanupPreviewConfirmationEntry,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegResult,
    PairClosePreviewConfirmationEntry,
    PaperTradeEntry,
)

from carryme_runtime.paired_live_execution import PairedLiveExecutionCoordinator


class VenuePairCloseLiveExecutionService(Protocol):
    """Submit one confirmed reduce-only close preview to a specific venue."""

    async def submit_confirmed_cleanup_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: CleanupPreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry: ...


@dataclass(frozen=True)
class PairCloseLiveExecutionCoordinator:
    """Submit a confirmed pair-close preview sequentially and journal the combined result."""

    services: dict[str, VenuePairCloseLiveExecutionService]

    async def submit_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: PairClosePreviewConfirmationEntry,
        first_venue: str = "auto",
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before pair close execution")
        if confirmation.entry_id is None:
            raise ValueError("Pair-close confirmation entry_id is required before live execution")

        preview_venues = [leg.venue.strip().lower() for leg in confirmation.preview.legs]
        if len(preview_venues) != 2:
            raise ValueError("Pair close execution requires exactly two preview legs")
        normalized_first = PairedLiveExecutionCoordinator._resolve_first_venue(
            requested_first_venue=first_venue,
            preview_venues=preview_venues,
        )
        if normalized_first not in preview_venues:
            raise ValueError(
                f"Requested first venue {first_venue!r} is not present in the close preview"
            )
        second_venue = next(venue for venue in preview_venues if venue != normalized_first)
        timestamp = executed_at or datetime.now(UTC)

        legs: list[ExecutionLegResult] = []
        first_result = await self._submit_one(
            venue=normalized_first,
            paper_trade=paper_trade,
            confirmation=confirmation,
            executed_at=timestamp,
        )
        legs.extend(first_result.legs)
        if first_result.status != "submitted":
            return ExecutionJournalEntry(
                executed_at=timestamp,
                adapter=f"paired_cleanup:{normalized_first}_then_{second_venue}",
                mode="live",
                status="rejected",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=legs,
            )

        second_result = await self._submit_one(
            venue=second_venue,
            paper_trade=paper_trade,
            confirmation=confirmation,
            executed_at=timestamp,
        )
        legs.extend(second_result.legs)

        status: Literal["submitted", "partial"] = (
            "submitted" if second_result.status == "submitted" else "partial"
        )
        return ExecutionJournalEntry(
            executed_at=timestamp,
            adapter=f"paired_cleanup:{normalized_first}_then_{second_venue}",
            mode="live",
            status=status,
            paper_trade_id=paper_trade.entry_id,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            paper_trade=paper_trade,
            legs=legs,
        )

    async def _submit_one(
        self,
        *,
        venue: str,
        paper_trade: PaperTradeEntry,
        confirmation: PairClosePreviewConfirmationEntry,
        executed_at: datetime,
    ) -> ExecutionJournalEntry:
        service = self.services.get(venue)
        if service is None:
            raise ValueError(f"No pair-close live execution service registered for venue {venue!r}")
        try:
            return await service.submit_confirmed_cleanup_preview(
                paper_trade=paper_trade,
                confirmation=_build_cleanup_confirmation(confirmation, venue),
                executed_at=executed_at,
            )
        except (ValueError, ConnectorError, httpx.HTTPError) as exc:
            leg = next(item for item in confirmation.preview.legs if item.venue == venue)
            return ExecutionJournalEntry(
                executed_at=executed_at,
                adapter=f"{venue}_cleanup_live",
                mode="live",
                status="rejected",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue=leg.venue,
                        symbol=leg.symbol,
                        fee_profile=leg.fee_profile,
                        side=leg.side,
                        target_notional=leg.target_notional,
                        status="rejected",
                        simulated=False,
                        request_payload=leg.payload,
                        response_payload={"error": str(exc), "venue": venue},
                    )
                ],
            )


def _build_cleanup_confirmation(
    confirmation: PairClosePreviewConfirmationEntry,
    venue: str,
) -> CleanupPreviewConfirmationEntry:
    leg = next(item for item in confirmation.preview.legs if item.venue == venue)
    preview_hash = f"{confirmation.preview_hash}:{venue}"
    cleanup_preview = ExecutionCleanupPreview(
        execution_entry_id=confirmation.preview.execution_entry_id,
        paper_trade_id=confirmation.preview.paper_trade_id,
        generated_at=confirmation.preview.generated_at,
        preview_hash=preview_hash,
        reason=confirmation.preview.reason,
        leg=leg,
        notes=confirmation.preview.notes,
    )
    return CleanupPreviewConfirmationEntry(
        entry_id=confirmation.entry_id,
        confirmed_at=confirmation.confirmed_at,
        paper_trade_id=confirmation.paper_trade_id,
        label=confirmation.label,
        preview_hash=preview_hash,
        preview=cleanup_preview,
        note=confirmation.note,
    )
