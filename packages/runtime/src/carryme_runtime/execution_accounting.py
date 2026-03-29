"""Derived execution accounting from journaled live submissions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from carryme_connectors.base import parse_float
from carryme_models import (
    ExecutionAccountingSummary,
    ExecutionJournalEntry,
    ExecutionLegAccounting,
    PaperTradeAccountingSummary,
    RouteAccountingSummary,
)
from carryme_normalizers import get_fee_profile
from carryme_storage import ExecutionJournalStore


@dataclass
class ExecutionAccountingService:
    """Build derived accounting summaries from append-only execution history."""

    journal_store: ExecutionJournalStore
    sample_limit: int = 500

    def summarize_entry(self, entry: ExecutionJournalEntry) -> ExecutionAccountingSummary:
        """Return derived accounting for one journal entry."""

        leg_summaries = [self._summarize_leg(leg) for leg in entry.legs]
        total_filled_notional = sum(item.filled_notional or 0.0 for item in leg_summaries)
        total_estimated_fee_paid = sum(item.estimated_fee_paid or 0.0 for item in leg_summaries)
        filled_leg_count = sum(
            1 for item in leg_summaries if item.derived_fill_state in {"filled", "partial_fill"}
        )
        return ExecutionAccountingSummary(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            label=entry.paper_trade.intent.label,
            canonical_symbol=entry.paper_trade.intent.canonical_symbol,
            adapter=entry.adapter,
            mode=entry.mode,
            status=entry.status,
            executed_at=entry.executed_at,
            total_leg_count=len(leg_summaries),
            filled_leg_count=filled_leg_count,
            total_filled_notional=total_filled_notional,
            total_estimated_fee_paid=total_estimated_fee_paid,
            legs=leg_summaries,
        )

    def latest_for_paper_trade(self, paper_trade_id: int) -> PaperTradeAccountingSummary | None:
        """Return aggregated accounting for one paper trade."""

        entries = self.journal_store.list_for_paper_trade(paper_trade_id, limit=self.sample_limit)
        if not entries:
            return None
        return self._summarize_paper_trade(entries)

    def list_route_summaries(
        self,
        *,
        canonical_symbol: str | None = None,
        label: str | None = None,
        limit: int = 50,
    ) -> list[RouteAccountingSummary]:
        """Aggregate execution accounting by route label."""

        entries = self.journal_store.list_recent(limit=self.sample_limit)
        buckets: dict[tuple[str, str, str, str, str, str], list[ExecutionJournalEntry]] = {}
        for entry in entries:
            intent = entry.paper_trade.intent
            if (
                canonical_symbol
                and intent.canonical_symbol.upper() != canonical_symbol.strip().upper()
            ):
                continue
            if label and intent.label != label:
                continue
            key = (
                intent.label,
                intent.canonical_symbol,
                intent.short_leg.venue,
                intent.long_leg.venue,
                intent.short_leg.fee_profile,
                intent.long_leg.fee_profile,
            )
            buckets.setdefault(key, []).append(entry)

        summaries: list[RouteAccountingSummary] = []
        for key, grouped_entries in buckets.items():
            entry_summaries = [self.summarize_entry(item) for item in grouped_entries]
            paper_trade_ids = {
                item.paper_trade_id
                for item in grouped_entries
                if item.paper_trade_id is not None
            }
            latest_executed_at = max(item.executed_at for item in grouped_entries)
            summaries.append(
                RouteAccountingSummary(
                    label=key[0],
                    canonical_symbol=key[1],
                    short_venue=key[2],
                    long_venue=key[3],
                    short_fee_profile=key[4],
                    long_fee_profile=key[5],
                    execution_count=len(grouped_entries),
                    paper_trade_count=len(paper_trade_ids),
                    latest_executed_at=latest_executed_at,
                    total_filled_notional=sum(
                        item.total_filled_notional for item in entry_summaries
                    ),
                    total_estimated_fee_paid=sum(
                        item.total_estimated_fee_paid for item in entry_summaries
                    ),
                )
            )

        ranked = sorted(
            summaries,
            key=lambda item: (item.total_estimated_fee_paid, item.execution_count, item.label),
            reverse=True,
        )
        if limit > 0:
            return ranked[:limit]
        return ranked

    def _summarize_paper_trade(
        self,
        entries: list[ExecutionJournalEntry],
    ) -> PaperTradeAccountingSummary:
        entry_summaries = [self.summarize_entry(item) for item in entries]
        latest_executed_at = max((item.executed_at for item in entries), default=None)
        first_intent = entries[0].paper_trade.intent
        return PaperTradeAccountingSummary(
            paper_trade_id=entries[0].paper_trade_id or 0,
            label=first_intent.label,
            canonical_symbol=first_intent.canonical_symbol,
            execution_count=len(entries),
            latest_executed_at=latest_executed_at,
            total_filled_notional=sum(
                item.total_filled_notional for item in entry_summaries
            ),
            total_estimated_fee_paid=sum(
                item.total_estimated_fee_paid for item in entry_summaries
            ),
            entries=entry_summaries,
        )

    def _summarize_leg(self, leg: Any) -> ExecutionLegAccounting:
        payload = leg.response_payload if isinstance(leg.response_payload, dict) else {}
        fills = self._extract_fill_events(payload)
        filled_size = sum(item["filled_size"] for item in fills) if fills else None
        filled_notional = sum(item["filled_notional"] for item in fills) if fills else None
        avg_fill_price = None
        if fills and filled_size and filled_size > 0 and filled_notional is not None:
            avg_fill_price = filled_notional / filled_size
        fee_rate = None
        fee_paid = None
        notes: list[str] = []
        try:
            fee_rate = get_fee_profile(leg.venue, leg.fee_profile).taker_fee_rate
        except ValueError:
            notes.append("No known fee profile available for this leg.")
        if fee_rate is not None and filled_notional is not None:
            fee_paid = filled_notional * fee_rate
        return ExecutionLegAccounting(
            venue=leg.venue,
            symbol=leg.symbol,
            fee_profile=leg.fee_profile,
            side=leg.side,
            status=leg.status,
            auth_usage=leg.auth_usage,
            derived_fill_state=self._derive_fill_state(fills),
            filled_size=filled_size,
            avg_fill_price=avg_fill_price,
            filled_notional=filled_notional,
            estimated_fee_rate=fee_rate,
            estimated_fee_paid=fee_paid,
            notes=notes,
        )

    def _extract_fill_events(self, payload: dict[str, Any]) -> list[dict[str, float]]:
        attempt_history = payload.get("attempt_history")
        if isinstance(attempt_history, list):
            events = [
                event
                for item in attempt_history
                for event in self._fill_events_from_attempt(item)
            ]
            if events:
                return events
        observed = payload.get("observed_order_state")
        if isinstance(observed, dict):
            event = self._fill_event_from_state(observed)
            return [event] if event is not None else []
        return []

    def _fill_events_from_attempt(self, attempt: Any) -> list[dict[str, float]]:
        if not isinstance(attempt, dict):
            return []
        observed = attempt.get("observed_order_state")
        if not isinstance(observed, dict):
            return []
        event = self._fill_event_from_state(observed)
        return [event] if event is not None else []

    def _fill_event_from_state(self, state: dict[str, Any]) -> dict[str, float] | None:
        derived_state = state.get("derived_state")
        if derived_state not in {"filled", "partial_fill"}:
            return None
        size = parse_float(state.get("size"))
        remaining_size = parse_float(state.get("remaining_size"))
        avg_fill_price = parse_float(state.get("avg_fill_price"))
        if size is None:
            return None
        if derived_state == "filled":
            filled_size = size
        else:
            if remaining_size is None:
                return None
            filled_size = max(size - remaining_size, 0.0)
        if filled_size <= 0:
            return None
        if avg_fill_price is None:
            return None
        return {
            "filled_size": filled_size,
            "filled_notional": filled_size * avg_fill_price,
        }

    def _derive_fill_state(
        self,
        fills: list[dict[str, float]],
    ) -> Literal["filled", "partial_fill", "unfilled", "unknown"]:
        if not fills:
            return "unfilled"
        return "filled"
