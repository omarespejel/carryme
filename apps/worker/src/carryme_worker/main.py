"""CLI entrypoint and runtime helpers for the carryme worker."""

import argparse
import asyncio
import logging

from carryme_models import AppDescriptor, ServiceHealth

from carryme_worker.config import WorkerSettings
from carryme_worker.poller import (
    CandidateRecordSummary,
    PollCycleSummary,
    PollLoopSummary,
    install_signal_handlers,
    poll_watchlist_once,
    run_polling_loop,
    run_supervised_polling_loop,
)

APP_NAME = "carryme-worker"
APP_VERSION = "0.1.0"


def build_health_payload(settings: WorkerSettings) -> ServiceHealth:
    """Build a deterministic worker health payload."""

    return ServiceHealth(
        service=AppDescriptor(
            name=APP_NAME,
            version=APP_VERSION,
            environment=settings.environment,
        )
    )


def build_cycle_payload(summary: PollCycleSummary) -> dict[str, int | str]:
    """Build a deterministic summary payload for one poll cycle."""

    return {
        "watched_pairs": summary.watched_pairs,
        "saved_records": summary.saved_records,
        "database_path": summary.database_path,
    }


def build_loop_payload(summary: PollLoopSummary) -> dict[str, int | str]:
    """Build a deterministic summary payload for a worker loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "saved_records": summary.saved_records,
        "alert_events": summary.alert_events,
        "database_path": summary.database_path,
    }


def build_candidate_payload(summary: CandidateRecordSummary) -> dict[str, int]:
    """Build a deterministic summary payload for candidate counts."""

    return {
        "total_records": summary.total_records,
        "candidate_records": summary.candidate_records,
    }


def main() -> None:
    """Run one poll cycle or print worker health."""

    parser = argparse.ArgumentParser(prog="carryme-worker")
    parser.add_argument("--once", action="store_true", help="Poll the configured watchlist once")
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Run the worker loop for a fixed number of iterations",
    )
    parser.add_argument(
        "--supervise",
        action="store_true",
        help="Run the signal-aware supervised worker loop",
    )
    args = parser.parse_args()

    settings = WorkerSettings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    if args.once:
        cycle_summary = asyncio.run(poll_watchlist_once(settings))
        print(build_cycle_payload(cycle_summary))
        return
    if args.iterations is not None:
        loop_summary = asyncio.run(run_polling_loop(settings, iterations=args.iterations))
        print(build_loop_payload(loop_summary))
        return
    if args.supervise:
        async def run_supervised() -> PollLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_polling_loop(settings, stop_event=stop_event)

        supervised_summary = asyncio.run(run_supervised())
        print(build_loop_payload(supervised_summary))
        return

    payload = build_health_payload(settings)
    print(payload.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
