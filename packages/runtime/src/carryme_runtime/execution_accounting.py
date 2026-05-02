"""Derived execution accounting from journaled live submissions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from carryme_connectors.base import parse_float
from carryme_models import (
    ExecutionAccountingSummary,
    ExecutionJournalEntry,
    ExecutionLegAccounting,
    ExecutionObservationEntry,
    PaperTradeAccountingSummary,
    RouteAccountingSummary,
)
from carryme_normalizers import get_fee_profile
from carryme_storage import ExecutionJournalStore, ExecutionObservationStore


@dataclass
class ExecutionAccountingService:
    """Build derived accounting summaries from append-only execution history."""

    journal_store: ExecutionJournalStore
    observation_store: ExecutionObservationStore | None = None
    sample_limit: int = 500

    def summarize_entry(
        self,
        entry: ExecutionJournalEntry,
        *,
        observation: ExecutionObservationEntry | None = None,
    ) -> ExecutionAccountingSummary:
        """Return derived accounting for one journal entry."""

        leg_summaries = [self._summarize_leg(leg) for leg in entry.legs]
        effective_observation = self._observation_for_entry(entry, observation)
        if effective_observation is not None:
            leg_summaries = self._apply_observation_inference(
                entry,
                leg_summaries,
                effective_observation,
            )
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
        observations = self._observations_for_entries(entries)
        entry_summaries = [
            self.summarize_entry(item, observation=observations.get(item.entry_id))
            for item in entries
        ]
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

    def _observation_for_entry(
        self,
        entry: ExecutionJournalEntry,
        observation: ExecutionObservationEntry | None,
    ) -> ExecutionObservationEntry | None:
        if observation is not None and self._observation_matches_entry(observation, entry):
            return observation
        if self.observation_store is None or entry.paper_trade_id is None:
            return None
        latest = self.observation_store.latest_for_paper_trade(entry.paper_trade_id)
        if latest is None or not self._observation_matches_entry(latest, entry):
            return None
        return latest

    def _observations_for_entries(
        self,
        entries: list[ExecutionJournalEntry],
    ) -> dict[int | None, ExecutionObservationEntry]:
        if self.observation_store is None:
            return {}
        observations: dict[int | None, ExecutionObservationEntry] = {}
        paper_trade_ids = {
            entry.paper_trade_id
            for entry in entries
            if entry.paper_trade_id is not None
        }
        latest_by_paper_trade = {
            paper_trade_id: self.observation_store.latest_for_paper_trade(paper_trade_id)
            for paper_trade_id in paper_trade_ids
        }
        for entry in entries:
            if entry.paper_trade_id is None:
                continue
            latest = latest_by_paper_trade.get(entry.paper_trade_id)
            if latest is None or not self._observation_matches_entry(latest, entry):
                continue
            observations[entry.entry_id] = latest
        return observations

    def _observation_matches_entry(
        self,
        observation: ExecutionObservationEntry,
        entry: ExecutionJournalEntry,
    ) -> bool:
        if observation.execution_entry_id is not None and entry.entry_id is not None:
            return observation.execution_entry_id == entry.entry_id
        if observation.preview_hash is not None and entry.preview_hash is not None:
            return observation.preview_hash == entry.preview_hash
        return False

    def _apply_observation_inference(
        self,
        entry: ExecutionJournalEntry,
        leg_summaries: list[ExecutionLegAccounting],
        observation: ExecutionObservationEntry,
    ) -> list[ExecutionLegAccounting]:
        pair_status = observation.pair_status
        if pair_status is None:
            return leg_summaries
        matched_symbols_by_venue = {
            venue.venue: set(venue.matched_leg_symbols)
            for venue in pair_status.reconciliation.venues
        }
        order_leg_states = {
            leg.venue: leg
            for leg in observation.order_state.legs
        }
        enriched: list[ExecutionLegAccounting] = []
        for journal_leg, summary in zip(entry.legs, leg_summaries, strict=True):
            order_leg = order_leg_states.get(journal_leg.venue)
            exact_fill = (
                self._fill_event_from_state(order_leg.model_dump(mode="json"))
                if order_leg is not None
                else None
            )
            notes = list(summary.notes)
            if order_leg is not None:
                notes.extend(order_leg.notes)
            if exact_fill is not None:
                filled_size = exact_fill["filled_size"]
                filled_notional = exact_fill["filled_notional"]
                avg_fill_price = filled_notional / filled_size if filled_size > 0 else None
                enriched.append(
                    summary.model_copy(
                        update={
                            "derived_fill_state": "filled",
                            "filled_size": filled_size,
                            "filled_notional": filled_notional,
                            "avg_fill_price": avg_fill_price,
                            "estimated_fee_paid": (
                                summary.estimated_fee_rate * filled_notional
                                if summary.estimated_fee_rate is not None
                                else None
                            ),
                            "notes": notes,
                        }
                    )
                )
                continue
            matched_symbols = matched_symbols_by_venue.get(journal_leg.venue, set())
            if (
                journal_leg.symbol in matched_symbols
                and summary.derived_fill_state in {"unfilled", "unknown"}
                and pair_status.derived_state == "hedged"
            ):
                inferred_notional = journal_leg.target_notional
                notes.append(
                    "Filled notional was inferred from live hedge reconciliation "
                    "because venue order history was unavailable."
                )
                enriched.append(
                    summary.model_copy(
                        update={
                            "derived_fill_state": "filled",
                            "filled_notional": inferred_notional,
                            "estimated_fee_paid": (
                                summary.estimated_fee_rate * inferred_notional
                                if summary.estimated_fee_rate is not None
                                else None
                            ),
                            "notes": notes,
                        }
                    )
                )
                continue
            if notes != summary.notes:
                enriched.append(summary.model_copy(update={"notes": notes}))
            else:
                enriched.append(summary)
        return enriched

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
        response_events = self._extract_hyperliquid_submission_fill_events(payload)
        if response_events:
            return response_events
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

    def _extract_hyperliquid_submission_fill_events(
        self,
        payload: dict[str, Any],
    ) -> list[dict[str, float]]:
        response = payload.get("response")
        if not isinstance(response, dict):
            return []
        data = response.get("data")
        if not isinstance(data, dict):
            return []
        statuses = data.get("statuses")
        if not isinstance(statuses, list):
            return []

        events: list[dict[str, float]] = []
        for status in statuses:
            if not isinstance(status, dict):
                continue
            filled = status.get("filled")
            if not isinstance(filled, dict):
                continue
            filled_size = parse_float(filled.get("totalSz"))
            if filled_size is None:
                filled_size = parse_float(filled.get("sz"))
            avg_fill_price = parse_float(filled.get("avgPx"))
            if filled_size is None or filled_size <= 0:
                continue
            if avg_fill_price is None:
                continue
            events.append(
                {
                    "filled_size": filled_size,
                    "filled_notional": filled_size * avg_fill_price,
                }
            )
        return events

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
