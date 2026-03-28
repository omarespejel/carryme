"""Polling helpers for the carryme worker."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from carryme_models import FundingArbOpportunity, OpportunityRecord
from carryme_runtime import OpportunityService
from carryme_storage import OpportunityHistoryStore, load_watchlist

from carryme_worker.config import WorkerSettings


class PairScorer(Protocol):
    """Interface for scoring configured funding pairs."""

    async def score_pair(
        self,
        *,
        left_venue: str,
        left_symbol: str,
        left_fee_profile: str,
        right_venue: str,
        right_symbol: str,
        right_fee_profile: str,
    ) -> FundingArbOpportunity: ...


@dataclass
class PollCycleSummary:
    """Summary emitted after a watchlist poll cycle."""

    watched_pairs: int
    saved_records: int
    database_path: str


@dataclass
class PollLoopSummary:
    """Summary emitted after a multi-iteration worker loop."""

    attempts: int
    successful_cycles: int
    failures: int
    saved_records: int
    database_path: str


async def poll_watchlist_once(
    settings: WorkerSettings,
    *,
    scorer: PairScorer | None = None,
    store: OpportunityHistoryStore | None = None,
    now: datetime | None = None,
) -> PollCycleSummary:
    """Score the configured watchlist once and persist results."""

    pairs = load_watchlist(settings.watchlist_path)
    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    timestamp = now or datetime.now(UTC)

    saved_records = 0
    for pair in pairs:
        opportunity = await runtime.score_pair(
            left_venue=pair.left_venue,
            left_symbol=pair.left_symbol,
            left_fee_profile=pair.left_fee_profile,
            right_venue=pair.right_venue,
            right_symbol=pair.right_symbol,
            right_fee_profile=pair.right_fee_profile,
        )
        history_store.append(
            OpportunityRecord(
                recorded_at=timestamp,
                pair=pair,
                opportunity=opportunity,
            )
        )
        saved_records += 1

    return PollCycleSummary(
        watched_pairs=len(pairs),
        saved_records=saved_records,
        database_path=settings.database_path,
    )


async def run_polling_loop(
    settings: WorkerSettings,
    *,
    iterations: int,
    scorer: PairScorer | None = None,
    store: OpportunityHistoryStore | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
) -> PollLoopSummary:
    """Run the worker for a fixed number of scheduled iterations."""

    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")

    successful_cycles = 0
    failures = 0
    saved_records = 0

    for attempt in range(1, iterations + 1):
        loop_logger.info("starting poll cycle %s of %s", attempt, iterations)
        try:
            summary = await poll_watchlist_once(
                settings,
                scorer=runtime,
                store=history_store,
            )
            successful_cycles += 1
            saved_records += summary.saved_records
            loop_logger.info(
                "completed poll cycle %s of %s with %s saved records",
                attempt,
                iterations,
                summary.saved_records,
            )
            if attempt < iterations:
                await sleep(settings.poll_interval_seconds)
        except Exception:
            failures += 1
            backoff_seconds = min(
                settings.max_backoff_seconds,
                settings.poll_interval_seconds * (2 ** (failures - 1)),
            )
            loop_logger.exception(
                "poll cycle %s of %s failed; backing off for %s seconds",
                attempt,
                iterations,
                backoff_seconds,
            )
            if attempt < iterations:
                await sleep(backoff_seconds)

    return PollLoopSummary(
        attempts=iterations,
        successful_cycles=successful_cycles,
        failures=failures,
        saved_records=saved_records,
        database_path=settings.database_path,
    )
