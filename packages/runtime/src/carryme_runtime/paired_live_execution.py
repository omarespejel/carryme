"""Paired manual live execution coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import httpx
from carryme_connectors import ConnectorError
from carryme_models import (
    ExecutionJournalEntry,
    ExecutionLegResult,
    PaperTradeEntry,
    PreviewConfirmationEntry,
)

ObservedFillState = Literal["filled", "partial_fill", "unfilled", "open", "unknown"]


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
        first_fill_state = self._submission_observed_fill_state(first_result)
        if first_fill_state != "filled":
            guarded_status: Literal["rejected", "partial"] = (
                "partial" if first_fill_state == "partial_fill" else "rejected"
            )
            return ExecutionJournalEntry(
                executed_at=timestamp,
                adapter=f"paired_live:{normalized_first}_then_{second_venue}",
                mode="live",
                status=guarded_status,
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

    @staticmethod
    def _submission_observed_fill_state(entry: ExecutionJournalEntry) -> ObservedFillState:
        states = [
            PairedLiveExecutionCoordinator._leg_observed_fill_state(leg)
            for leg in entry.legs
        ]
        concrete_states = [state for state in states if state != "unknown"]
        if not concrete_states:
            return "unknown"
        if any(state == "partial_fill" for state in concrete_states):
            return "partial_fill"
        if any(state == "open" for state in concrete_states):
            return "open"
        has_filled = any(state == "filled" for state in concrete_states)
        has_unfilled = any(state == "unfilled" for state in concrete_states)
        if has_filled and has_unfilled:
            return "partial_fill"
        if has_unfilled:
            return "unfilled"
        if has_filled:
            return "filled"
        return "unknown"

    @staticmethod
    def _leg_observed_fill_state(leg: ExecutionLegResult) -> ObservedFillState:
        payload = leg.response_payload if isinstance(leg.response_payload, dict) else {}
        observed_state = _observed_fill_state(payload.get("observed_order_state"))
        if observed_state != "unknown":
            return observed_state

        attempt_history = payload.get("attempt_history")
        if not isinstance(attempt_history, list):
            return "unknown"
        for attempt in reversed(attempt_history):
            if not isinstance(attempt, dict):
                continue
            observed_state = _observed_fill_state(attempt.get("observed_order_state"))
            if observed_state != "unknown":
                return observed_state
        return "unknown"


def _observed_fill_state(value: Any) -> ObservedFillState:
    if not isinstance(value, dict):
        return "unknown"
    derived_state = value.get("derived_state")
    if derived_state == "filled":
        return "filled"
    if derived_state == "partial_fill":
        return "partial_fill"
    if derived_state == "unfilled":
        return "unfilled"
    if derived_state == "open":
        return "open"
    return "unknown"
