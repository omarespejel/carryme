"""Polling helpers for the carryme worker."""

from __future__ import annotations

import asyncio
import logging
import signal
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

import httpx
from carryme_models import CandidateAlertEvent, FundingArbOpportunity, OpportunityRecord
from carryme_runtime import ConnectorError, OpportunityService, filter_candidate_records
from carryme_storage import CandidateAlertStore, OpportunityHistoryStore, load_watchlist

from carryme_worker.config import WorkerSettings

logger = logging.getLogger(__name__)


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


class CandidateAlertSink(Protocol):
    """Append-only sink for emitted candidate alert events."""

    def append(self, event: CandidateAlertEvent) -> bool: ...


@dataclass
class PollCycleSummary:
    """Summary emitted after a watchlist poll cycle."""

    watched_pairs: int
    saved_records: int
    failed_records: int
    database_path: str
    records: list[OpportunityRecord] = field(default_factory=list, repr=False)


@dataclass
class PollLoopSummary:
    """Summary emitted after a multi-iteration worker loop."""

    attempts: int
    successful_cycles: int
    failures: int
    saved_records: int
    database_path: str
    alert_events: int = 0


@dataclass
class CandidateRecordSummary:
    """Summary emitted after applying candidate thresholds to a cycle."""

    total_records: int
    candidate_records: int


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
    failed_records = 0
    records: list[OpportunityRecord] = []
    for pair in pairs:
        try:
            async with asyncio.timeout(settings.score_timeout_seconds):
                opportunity = await runtime.score_pair(
                    left_venue=pair.left_venue,
                    left_symbol=pair.left_symbol,
                    left_fee_profile=pair.left_fee_profile,
                    right_venue=pair.right_venue,
                    right_symbol=pair.right_symbol,
                    right_fee_profile=pair.right_fee_profile,
                )
            record = OpportunityRecord(
                recorded_at=timestamp,
                pair=pair,
                opportunity=opportunity,
            )
            history_store.append(record)
        except (ConnectorError, httpx.HTTPError, ValueError, TimeoutError, sqlite3.Error) as exc:
            logger.warning(
                "Failed to score or persist pair %s/%s ↔ %s/%s: %s",
                pair.left_venue,
                pair.left_symbol,
                pair.right_venue,
                pair.right_symbol,
                exc,
                exc_info=True,
            )
            failed_records += 1
            continue
        records.append(record)
        saved_records += 1

    return PollCycleSummary(
        watched_pairs=len(pairs),
        saved_records=saved_records,
        failed_records=failed_records,
        database_path=settings.database_path,
        records=records,
    )


def summarize_candidates(
    records: list[OpportunityRecord],
    *,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
) -> CandidateRecordSummary:
    """Count candidate records that satisfy the worker thresholds."""

    candidate_records = len(
        filter_candidate_records(
            records,
            min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
            min_capacity_notional=min_capacity_notional,
        )
    )

    return CandidateRecordSummary(
        total_records=len(records),
        candidate_records=candidate_records,
    )


def emit_candidate_alerts(
    records: list[OpportunityRecord],
    *,
    sink: CandidateAlertSink,
    emitted_at: datetime,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
) -> int:
    """Emit one append-only candidate event per selected record."""

    inserted = 0
    for record in records:
        inserted += int(
            sink.append(
                CandidateAlertEvent(
                    emitted_at=emitted_at,
                    min_one_day_net_edge_after_entry=min_one_day_net_edge_after_entry,
                    min_capacity_notional=min_capacity_notional,
                    record=record,
                )
            )
        )
    return inserted


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

    if iterations < 1:
        raise ValueError("iterations must be at least 1")

    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")

    successful_cycles = 0
    failures = 0
    consecutive_failures = 0
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
            consecutive_failures = 0
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
            consecutive_failures += 1
            backoff_seconds = min(
                settings.max_backoff_seconds,
                settings.poll_interval_seconds * (2 ** (consecutive_failures - 1)),
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


async def _sleep_or_stop(
    seconds: float,
    *,
    sleep: Callable[[float], Awaitable[None]],
    stop_event: asyncio.Event | None,
) -> None:
    """Sleep until the next cycle unless a stop signal arrives first."""

    if stop_event is None:
        await sleep(seconds)
        return
    if stop_event.is_set():
        return

    async def _await_sleep() -> None:
        await sleep(seconds)

    async def _await_stop() -> None:
        await stop_event.wait()

    sleep_task: asyncio.Task[None] = asyncio.create_task(_await_sleep())
    stop_task: asyncio.Task[None] = asyncio.create_task(_await_stop())
    done, pending = await asyncio.wait(
        {sleep_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await asyncio.gather(*done, return_exceptions=True)


async def run_supervised_polling_loop(
    settings: WorkerSettings,
    *,
    scorer: PairScorer | None = None,
    store: OpportunityHistoryStore | None = None,
    alert_sink: CandidateAlertSink | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> PollLoopSummary:
    """Run the worker until stopped by signal or max-iteration limit."""

    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    runtime = scorer or OpportunityService()
    history_store = store or OpportunityHistoryStore(settings.database_path)
    candidate_alert_sink = alert_sink or CandidateAlertStore(settings.database_path)
    loop_logger = logger or logging.getLogger("carryme.worker")
    supervised_stop_event = stop_event or asyncio.Event()
    attempts = 0
    successful_cycles = 0
    failures = 0
    consecutive_failures = 0
    saved_records = 0
    alert_events = 0

    while not supervised_stop_event.is_set():
        attempts += 1
        loop_logger.info("starting supervised poll cycle %s", attempts)
        try:
            summary = await poll_watchlist_once(
                settings,
                scorer=runtime,
                store=history_store,
            )
            successful_cycles += 1
            consecutive_failures = 0
            saved_records += summary.saved_records

            candidate_summary = CandidateRecordSummary(
                total_records=0,
                candidate_records=0,
            )
            inserted_alerts = 0
            if summary.records:
                candidate_records = filter_candidate_records(
                    summary.records,
                    min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
                    min_capacity_notional=settings.min_candidate_capacity_notional,
                )
                candidate_summary = summarize_candidates(
                    summary.records,
                    min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
                    min_capacity_notional=settings.min_candidate_capacity_notional,
                )
                if candidate_records:
                    try:
                        inserted_alerts = emit_candidate_alerts(
                            candidate_records,
                            sink=candidate_alert_sink,
                            emitted_at=summary.records[0].recorded_at,
                            min_one_day_net_edge_after_entry=settings.min_candidate_entry_edge,
                            min_capacity_notional=settings.min_candidate_capacity_notional,
                        )
                    except Exception:
                        loop_logger.exception(
                            "failed to persist candidate alerts for supervised poll cycle %s",
                            attempts,
                        )
            alert_events += inserted_alerts
            loop_logger.info(
                (
                    "completed supervised poll cycle %s with %s saved records, "
                    "%s candidates, and %s alerts"
                ),
                attempts,
                summary.saved_records,
                candidate_summary.candidate_records,
                inserted_alerts,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            await _sleep_or_stop(
                settings.poll_interval_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )
        except Exception:
            failures += 1
            consecutive_failures += 1
            backoff_seconds = min(
                settings.max_backoff_seconds,
                settings.poll_interval_seconds * (2 ** (consecutive_failures - 1)),
            )
            loop_logger.exception(
                "supervised poll cycle %s failed; backing off for %s seconds",
                attempts,
                backoff_seconds,
            )
            if max_iterations is not None and attempts >= max_iterations:
                break
            await _sleep_or_stop(
                backoff_seconds,
                sleep=sleep,
                stop_event=supervised_stop_event,
            )

    return PollLoopSummary(
        attempts=attempts,
        successful_cycles=successful_cycles,
        failures=failures,
        saved_records=saved_records,
        database_path=settings.database_path,
        alert_events=alert_events,
    )


def install_signal_handlers(
    stop_event: asyncio.Event,
    *,
    signals_to_handle: tuple[str, ...],
    logger: logging.Logger | None = None,
) -> None:
    """Register signal handlers that request a supervised stop."""

    loop_logger = logger or logging.getLogger("carryme.worker")
    event_loop = asyncio.get_running_loop()
    for signal_name in signals_to_handle:
        sig = getattr(signal, signal_name)
        event_loop.add_signal_handler(
            sig,
            _request_stop,
            stop_event,
            loop_logger,
            signal_name,
        )


def _request_stop(stop_event: asyncio.Event, logger: logging.Logger, signal_name: str) -> None:
    logger.info("received %s, requesting supervised worker shutdown", signal_name)
    stop_event.set()
