"""Paired manual live execution coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

import httpx
from carryme_connectors import ConnectorError
from carryme_models import (
    ExecutionJournalEntry,
    ExecutionLegResult,
    PaperTradeEntry,
    PreviewConfirmationEntry,
)


class SingleVenueLiveExecutionService(Protocol):
    """Protocol implemented by one-venue live execution services."""

    async def submit_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: PreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry: ...


@dataclass(frozen=True)
class PairedLiveExecutionCoordinator:
    """Submit a confirmed two-leg preview sequentially and journal the combined result."""

    services: dict[str, SingleVenueLiveExecutionService]

    async def submit_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: PreviewConfirmationEntry,
        first_venue: str = "auto",
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before live execution")
        if confirmation.entry_id is None:
            raise ValueError("Preview confirmation entry_id is required before live execution")

        preview_venues = [leg.venue.strip().lower() for leg in confirmation.preview.legs]
        if not all(preview_venues):
            raise ValueError("Paired execution requires non-empty preview leg venues")
        normalized_first = self._resolve_first_venue(
            requested_first_venue=first_venue,
            preview_venues=preview_venues,
        )
        if normalized_first not in preview_venues:
            raise ValueError(
                f"Requested first venue {first_venue!r} is not present in the confirmed preview"
            )
        if len(preview_venues) != 2 or len(set(preview_venues)) != 2:
            raise ValueError("Paired execution requires exactly two distinct preview legs")

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
                adapter=f"paired_live:{normalized_first}_then_{second_venue}",
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
            adapter=f"paired_live:{normalized_first}_then_{second_venue}",
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
        confirmation: PreviewConfirmationEntry,
        executed_at: datetime,
    ) -> ExecutionJournalEntry:
        service = self.services.get(venue)
        if service is None:
            raise ValueError(f"No live execution service registered for venue {venue!r}")
        try:
            return await service.submit_confirmed_preview(
                paper_trade=paper_trade,
                confirmation=confirmation,
                executed_at=executed_at,
            )
        except (ValueError, ConnectorError, httpx.HTTPError) as exc:
            leg = next(
                item for item in confirmation.preview.legs if item.venue.strip().lower() == venue
            )
            return ExecutionJournalEntry(
                executed_at=executed_at,
                adapter=f"{venue}_live",
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
                        response_payload={"error": str(exc), "venue": venue},
                    )
                ],
            )

    @staticmethod
    def _resolve_first_venue(
        *,
        requested_first_venue: str,
        preview_venues: list[str],
    ) -> str:
        normalized = requested_first_venue.strip().lower()
        if normalized and normalized != "auto":
            return normalized

        venue_priority = {
            # Prefer the venue with the weaker observed fill behavior first so the paired
            # coordinator avoids opening the easier hedge leg before the harder leg is live.
            "paradex": 0,
            "extended": 1,
            "hyperliquid": 2,
        }
        return min(
            preview_venues,
            key=lambda venue: (venue_priority.get(venue, 100), preview_venues.index(venue)),
        )
