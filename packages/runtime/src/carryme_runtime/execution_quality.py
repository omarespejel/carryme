"""Execution-quality summaries derived from live observation history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from carryme_models import (
    ExecutionJournalEntry,
    ExecutionObservationEntry,
    ExecutionQualitySummary,
)
from carryme_storage import ExecutionJournalStore, ExecutionObservationStore

from carryme_runtime.execution_pair_status import build_execution_pair_status

_OUTCOME_WEIGHTS: dict[str, float] = {
    "hedged": 1.0,
    "closed": 0.9,
    "pending": 0.5,
    "unfilled": 0.35,
    "cleanup_needed": 0.1,
    "review_required": 0.0,
}

_Outcome = Literal[
    "hedged",
    "pending",
    "unfilled",
    "closed",
    "cleanup_needed",
    "review_required",
]


@dataclass
class _ExecutionQualityBucket:
    sample_size: int = 0
    weighted_sum: float = 0.0
    latest_outcome: _Outcome | None = None
    latest_observed_at: datetime | None = None
    hedged_count: int = 0
    closed_count: int = 0
    unfilled_count: int = 0
    cleanup_needed_count: int = 0
    review_required_count: int = 0
    pending_count: int = 0

    def add_outcome(self, outcome: _Outcome, *, observed_at: datetime) -> None:
        self.sample_size += 1
        self.weighted_sum += _OUTCOME_WEIGHTS.get(outcome, 0.0)
        if self.latest_observed_at is None or observed_at >= self.latest_observed_at:
            self.latest_observed_at = observed_at
            self.latest_outcome = outcome
        if outcome == "hedged":
            self.hedged_count += 1
        elif outcome == "closed":
            self.closed_count += 1
        elif outcome == "unfilled":
            self.unfilled_count += 1
        elif outcome == "cleanup_needed":
            self.cleanup_needed_count += 1
        elif outcome == "review_required":
            self.review_required_count += 1
        elif outcome == "pending":
            self.pending_count += 1


@dataclass
class ExecutionQualityService:
    """Summarize observed execution quality for ranking and filtering."""

    journal_store: ExecutionJournalStore
    observation_store: ExecutionObservationStore
    prior_weight: float = 2.0
    prior_score: float = 0.65
    sample_limit: int = 500

    def build_index(self) -> dict[tuple[str, str, str], ExecutionQualitySummary]:
        """Aggregate latest observed pair outcomes by symbol and venue direction."""

        observations = self.observation_store.list_recent(limit=None)
        latest_by_paper_trade: dict[int, ExecutionObservationEntry] = {}
        for observation in observations:
            if observation.paper_trade_id is None or observation.pair_status is None:
                continue
            existing = latest_by_paper_trade.get(observation.paper_trade_id)
            if existing is None or _observation_sort_key(observation) > _observation_sort_key(
                existing
            ):
                latest_by_paper_trade[observation.paper_trade_id] = observation

        unique_observations = sorted(
            latest_by_paper_trade.values(),
            key=_observation_sort_key,
            reverse=True,
        )
        if self.sample_limit > 0:
            unique_observations = unique_observations[: self.sample_limit]

        buckets: dict[tuple[str, str, str], _ExecutionQualityBucket] = {}

        for observation in unique_observations:
            paper_trade_id = observation.paper_trade_id
            if paper_trade_id is None:
                continue
            entry = self.journal_store.latest_for_paper_trade(paper_trade_id)
            if entry is None:
                continue
            pair_status = observation.pair_status
            if pair_status is None:
                continue
            intent = entry.paper_trade.intent
            key = (
                intent.canonical_symbol,
                intent.short_leg.venue,
                intent.long_leg.venue,
            )
            outcome = _effective_outcome(entry=entry, observation=observation)
            buckets.setdefault(key, _ExecutionQualityBucket()).add_outcome(
                outcome,
                observed_at=observation.observed_at,
            )

        result: dict[tuple[str, str, str], ExecutionQualitySummary] = {}
        for key, bucket in buckets.items():
            sample_size = bucket.sample_size
            weighted_score = (
                self.prior_weight * self.prior_score + bucket.weighted_sum
            ) / (self.prior_weight + sample_size)
            result[key] = ExecutionQualitySummary(
                canonical_symbol=key[0],
                short_venue=key[1],
                long_venue=key[2],
                sample_size=sample_size,
                weighted_score=weighted_score,
                latest_outcome=bucket.latest_outcome,
                hedged_count=bucket.hedged_count,
                closed_count=bucket.closed_count,
                unfilled_count=bucket.unfilled_count,
                cleanup_needed_count=bucket.cleanup_needed_count,
                review_required_count=bucket.review_required_count,
                pending_count=bucket.pending_count,
            )
        return result

    def list_summaries(
        self,
        *,
        canonical_symbol: str | None = None,
        short_venue: str | None = None,
        long_venue: str | None = None,
        min_sample_size: int = 0,
        limit: int = 50,
    ) -> list[ExecutionQualitySummary]:
        """Return ranked execution-quality summaries with optional filters."""

        normalized_symbol = canonical_symbol.strip().upper() if canonical_symbol else None
        normalized_short_venue = short_venue.strip().lower() if short_venue else None
        normalized_long_venue = long_venue.strip().lower() if long_venue else None

        summaries: list[ExecutionQualitySummary] = []
        for summary in self.build_index().values():
            if normalized_symbol and summary.canonical_symbol.upper() != normalized_symbol:
                continue
            if normalized_short_venue and summary.short_venue.lower() != normalized_short_venue:
                continue
            if normalized_long_venue and summary.long_venue.lower() != normalized_long_venue:
                continue
            if summary.sample_size < min_sample_size:
                continue
            summaries.append(summary)

        ranked = sorted(
            summaries,
            key=lambda item: (
                -item.weighted_score,
                -item.sample_size,
                item.canonical_symbol,
                item.short_venue,
                item.long_venue,
            ),
        )
        if limit > 0:
            return ranked[:limit]
        return ranked


def _observation_sort_key(observation: ExecutionObservationEntry) -> tuple[datetime, int]:
    return observation.observed_at, observation.entry_id or 0


def _effective_outcome(
    *,
    entry: ExecutionJournalEntry,
    observation: ExecutionObservationEntry,
) -> _Outcome:
    pair_status = observation.pair_status
    if pair_status is None:
        raise ValueError("observation must include pair_status")
    if pair_status.derived_state != "review_required":
        return pair_status.derived_state
    recomputed = build_execution_pair_status(
        entry,
        pair_status.order_state,
        pair_status.reconciliation,
    )
    return recomputed.derived_state
