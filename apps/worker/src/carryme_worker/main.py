"""CLI entrypoint and runtime helpers for the carryme worker."""

import argparse
import asyncio
import json
import logging

from carryme_models import AppDescriptor, ServiceHealth

from carryme_worker.config import WorkerSettings
from carryme_worker.poller import (
    CandidateRecordSummary,
    ExecutionObservationLoopSummary,
    ExecutionObservationSummary,
    PollCycleSummary,
    PollLoopSummary,
    install_signal_handlers,
    observe_live_executions_once,
    poll_watchlist_once,
    run_polling_loop,
    run_supervised_execution_observation_loop,
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
        "failed_records": summary.failed_records,
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


def build_execution_observation_payload(
    summary: ExecutionObservationSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for execution observation runs."""

    return {
        "scanned_executions": summary.scanned_executions,
        "observed_executions": summary.observed_executions,
        "saved_observations": summary.saved_observations,
        "saved_alerts": summary.saved_alerts,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_execution_observation_loop_payload(
    summary: ExecutionObservationLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for an execution monitor loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "scanned_executions": summary.scanned_executions,
        "observed_executions": summary.observed_executions,
        "saved_observations": summary.saved_observations,
        "saved_alerts": summary.saved_alerts,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def main() -> None:
    """Run one poll cycle or print worker health."""

    parser = argparse.ArgumentParser(prog="carryme-worker")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Poll the configured watchlist once")
    mode.add_argument(
        "--observe-executions-once",
        action="store_true",
        help="Observe recent live executions once and persist snapshots",
    )
    mode.add_argument(
        "--observe-executions-supervise",
        action="store_true",
        help="Run the signal-aware supervised execution monitor loop",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Run loop modes for a fixed number of iterations",
    )
    mode.add_argument(
        "--supervise",
        action="store_true",
        help="Run the signal-aware supervised worker loop",
    )
    args = parser.parse_args()

    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be at least 1")
    if args.iterations is not None and (args.once or args.observe_executions_once):
        parser.error("--iterations is only supported with the looped worker modes")

    settings = WorkerSettings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    if args.once:
        summary = asyncio.run(poll_watchlist_once(settings))
        print(json.dumps(build_cycle_payload(summary), indent=2))
        return
    if args.observe_executions_once:
        observation_summary = asyncio.run(observe_live_executions_once(settings))
        print(json.dumps(build_execution_observation_payload(observation_summary), indent=2))
        return
    if args.observe_executions_supervise:

        async def run_execution_supervised() -> ExecutionObservationLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_execution_observation_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_observation_summary = asyncio.run(run_execution_supervised())
        print(
            json.dumps(
                build_execution_observation_loop_payload(supervised_observation_summary),
                indent=2,
            )
        )
        return
    if args.supervise:

        async def run_supervised() -> PollLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_polling_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_summary = asyncio.run(run_supervised())
        print(json.dumps(build_loop_payload(supervised_summary), indent=2))
        return
    if args.iterations is not None:
        loop_summary = asyncio.run(run_polling_loop(settings, iterations=args.iterations))
        print(json.dumps(build_loop_payload(loop_summary), indent=2))
        return

    payload = build_health_payload(settings)
    print(payload.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
