"""CLI entrypoint and runtime helpers for the carryme worker."""

import argparse
import asyncio
import json
import logging

from carryme_models import AppDescriptor, ServiceHealth

from carryme_worker.config import WorkerSettings
from carryme_worker.poller import (
    ApprovedCanaryScanLoopSummary,
    ApprovedCanaryScanSummary,
    CandidateRecordSummary,
    ExecutionObservationLoopSummary,
    ExecutionObservationSummary,
    LaunchReadyCanaryCacheLoopSummary,
    LaunchReadyCanaryCacheSummary,
    PollCycleSummary,
    PollLoopSummary,
    ProductionSupervisorCycleSummary,
    ProductionSupervisorLoopSummary,
    StableCanaryLaunchLoopSummary,
    StableCanaryLaunchSummary,
    SystemStateObservationLoopSummary,
    SystemStateObservationSummary,
    UniverseScanLoopSummary,
    UniverseScanSummary,
    cache_launch_ready_canaries_once,
    install_signal_handlers,
    launch_latest_stable_canary_once,
    observe_live_executions_once,
    observe_system_state_once,
    poll_watchlist_once,
    run_polling_loop,
    run_production_supervisor_cycle_once,
    run_supervised_approved_canary_scan_loop,
    run_supervised_execution_observation_loop,
    run_supervised_launch_ready_canary_cache_loop,
    run_supervised_polling_loop,
    run_supervised_production_supervisor_loop,
    run_supervised_stable_canary_launch_loop,
    run_supervised_system_state_observation_loop,
    run_supervised_universe_scan_loop,
    scan_approved_canary_once,
    scan_funding_universe_once,
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


def build_universe_scan_payload(summary: UniverseScanSummary) -> dict[str, int | str]:
    """Build a deterministic summary payload for one universe scan."""

    return {
        "overlap_count": summary.overlap_count,
        "scanned_opportunities": summary.scanned_opportunities,
        "saved_records": summary.saved_records,
        "alert_events": summary.alert_events,
        "database_path": summary.database_path,
    }


def build_universe_scan_loop_payload(
    summary: UniverseScanLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for a supervised universe-scan loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "overlap_count": summary.overlap_count,
        "scanned_opportunities": summary.scanned_opportunities,
        "saved_records": summary.saved_records,
        "alert_events": summary.alert_events,
        "database_path": summary.database_path,
    }


def build_approved_canary_scan_payload(
    summary: ApprovedCanaryScanSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for one approved-canary scan."""

    return {
        "scanned_candidates": summary.scanned_candidates,
        "approved_candidates": summary.approved_candidates,
        "saved_snapshots": summary.saved_snapshots,
        "alert_events": summary.alert_events,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_approved_canary_scan_loop_payload(
    summary: ApprovedCanaryScanLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for a supervised approved-canary scan loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "scanned_candidates": summary.scanned_candidates,
        "approved_candidates": summary.approved_candidates,
        "saved_snapshots": summary.saved_snapshots,
        "alert_events": summary.alert_events,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_launch_ready_canary_cache_payload(
    summary: LaunchReadyCanaryCacheSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for one launch-ready cache run."""

    return {
        "scanned_snapshots": summary.scanned_snapshots,
        "launch_ready_candidates": summary.launch_ready_candidates,
        "saved_snapshots": summary.saved_snapshots,
        "alert_events": summary.alert_events,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_launch_ready_canary_cache_loop_payload(
    summary: LaunchReadyCanaryCacheLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for the launch-ready cache loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "scanned_snapshots": summary.scanned_snapshots,
        "launch_ready_candidates": summary.launch_ready_candidates,
        "saved_snapshots": summary.saved_snapshots,
        "alert_events": summary.alert_events,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_stable_canary_launch_payload(
    summary: StableCanaryLaunchSummary,
) -> dict[str, object]:
    """Build a deterministic summary payload for one stable-canary launch attempt."""

    return {
        "status": summary.status,
        "label": summary.label,
        "launch_ready_snapshot_id": summary.launch_ready_snapshot_id,
        "approved_snapshot_id": summary.approved_snapshot_id,
        "paper_trade_id": summary.paper_trade_id,
        "final_pair_state": summary.final_pair_state,
        "detail": summary.detail,
        "database_path": summary.database_path,
    }


def build_stable_canary_launch_loop_payload(
    summary: StableCanaryLaunchLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for the stable-canary launch loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "launched": summary.launched,
        "skipped": summary.skipped,
        "database_path": summary.database_path,
    }


def build_production_supervisor_cycle_payload(
    summary: ProductionSupervisorCycleSummary,
) -> dict[str, int | str | None]:
    """Build a deterministic payload for one production supervisor cycle."""

    return {
        "checked_venues": summary.checked_venues,
        "degraded_venues": summary.degraded_venues,
        "scanned_candidates": summary.scanned_candidates,
        "approved_candidates": summary.approved_candidates,
        "saved_approved_snapshots": summary.saved_approved_snapshots,
        "scanned_launch_ready_snapshots": summary.scanned_launch_ready_snapshots,
        "launch_ready_candidates": summary.launch_ready_candidates,
        "saved_launch_ready_snapshots": summary.saved_launch_ready_snapshots,
        "launch_status": summary.launch_status,
        "paper_trade_id": summary.paper_trade_id,
        "final_pair_state": summary.final_pair_state,
        "observed_executions": summary.observed_executions,
        "saved_execution_observations": summary.saved_execution_observations,
        "execution_alerts": summary.execution_alerts,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_production_supervisor_loop_payload(
    summary: ProductionSupervisorLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic payload for the production supervisor loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "launched": summary.launched,
        "skipped": summary.skipped,
        "observed_executions": summary.observed_executions,
        "execution_alerts": summary.execution_alerts,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_system_state_observation_payload(
    summary: SystemStateObservationSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for one system-state observation run."""

    return {
        "checked_venues": summary.checked_venues,
        "degraded_venues": summary.degraded_venues,
        "saved_alerts": summary.saved_alerts,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
    }


def build_system_state_observation_loop_payload(
    summary: SystemStateObservationLoopSummary,
) -> dict[str, int | str]:
    """Build a deterministic summary payload for a system-state monitor loop."""

    return {
        "attempts": summary.attempts,
        "successful_cycles": summary.successful_cycles,
        "failures": summary.failures,
        "checked_venues": summary.checked_venues,
        "degraded_venues": summary.degraded_venues,
        "saved_alerts": summary.saved_alerts,
        "sent_notifications": summary.sent_notifications,
        "database_path": summary.database_path,
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
        "--scan-universe-once",
        action="store_true",
        help="Scan the configured live funding universe once",
    )
    mode.add_argument(
        "--scan-universe-supervise",
        action="store_true",
        help="Run the signal-aware supervised funding-universe scan loop",
    )
    mode.add_argument(
        "--scan-approved-canary-once",
        action="store_true",
        help="Scan and persist currently approved canary candidates once",
    )
    mode.add_argument(
        "--scan-approved-canary-supervise",
        action="store_true",
        help="Run the signal-aware supervised approved-canary scan loop",
    )
    mode.add_argument(
        "--cache-launch-ready-canary-once",
        action="store_true",
        help="Cache fresh launch-ready canary snapshots once",
    )
    mode.add_argument(
        "--cache-launch-ready-canary-supervise",
        action="store_true",
        help="Run the signal-aware supervised launch-ready canary cache loop",
    )
    mode.add_argument(
        "--launch-latest-stable-canary-once",
        action="store_true",
        help="Launch the latest stable cached canary once behind all live gates",
    )
    mode.add_argument(
        "--launch-latest-stable-canary-supervise",
        action="store_true",
        help="Run the signal-aware supervised stable-canary launch loop",
    )
    mode.add_argument(
        "--run-production-supervisor-once",
        action="store_true",
        help="Run one end-to-end production supervisor cycle",
    )
    mode.add_argument(
        "--run-production-supervisor-supervise",
        action="store_true",
        help="Run the signal-aware end-to-end production supervisor loop",
    )
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
        "--observe-system-state-once",
        action="store_true",
        help="Observe venue system-state once and emit transition alerts",
    )
    parser.add_argument(
        "--observe-system-state-supervise",
        action="store_true",
        help="Run the signal-aware supervised system-state monitor loop",
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
    mode_flags = (
        args.once,
        args.scan_universe_once,
        args.scan_universe_supervise,
        args.scan_approved_canary_once,
        args.scan_approved_canary_supervise,
        args.cache_launch_ready_canary_once,
        args.cache_launch_ready_canary_supervise,
        args.launch_latest_stable_canary_once,
        args.launch_latest_stable_canary_supervise,
        args.run_production_supervisor_once,
        args.run_production_supervisor_supervise,
        args.observe_executions_once,
        args.observe_executions_supervise,
        args.observe_system_state_once,
        args.observe_system_state_supervise,
        args.supervise,
    )
    if sum(bool(flag) for flag in mode_flags) > 1:
        parser.error("choose only one worker mode flag")
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be at least 1")
    if args.iterations is not None and (
        args.once
        or args.scan_universe_once
        or args.scan_approved_canary_once
        or args.cache_launch_ready_canary_once
        or args.launch_latest_stable_canary_once
        or args.observe_executions_once
        or args.observe_system_state_once
        or args.run_production_supervisor_once
    ):
        parser.error("--iterations is only supported with the looped worker modes")

    settings = WorkerSettings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    if args.once:
        summary = asyncio.run(poll_watchlist_once(settings))
        print(json.dumps(build_cycle_payload(summary), indent=2))
        return
    if args.scan_universe_once:
        universe_summary = asyncio.run(scan_funding_universe_once(settings))
        print(json.dumps(build_universe_scan_payload(universe_summary), indent=2))
        return
    if args.scan_universe_supervise:

        async def run_supervised_universe() -> UniverseScanLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_universe_scan_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_universe_summary = asyncio.run(run_supervised_universe())
        print(json.dumps(build_universe_scan_loop_payload(supervised_universe_summary), indent=2))
        return
    if args.scan_approved_canary_once:
        approved_canary_summary = asyncio.run(scan_approved_canary_once(settings))
        print(build_approved_canary_scan_payload(approved_canary_summary))
        return
    if args.scan_approved_canary_supervise:

        async def run_supervised_approved_canary() -> ApprovedCanaryScanLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_approved_canary_scan_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_approved_canary_summary = asyncio.run(run_supervised_approved_canary())
        print(build_approved_canary_scan_loop_payload(supervised_approved_canary_summary))
        return
    if args.cache_launch_ready_canary_once:
        launch_ready_summary = asyncio.run(cache_launch_ready_canaries_once(settings))
        print(build_launch_ready_canary_cache_payload(launch_ready_summary))
        return
    if args.cache_launch_ready_canary_supervise:

        async def run_launch_ready_supervised() -> LaunchReadyCanaryCacheLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_launch_ready_canary_cache_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_launch_ready_summary = asyncio.run(run_launch_ready_supervised())
        print(build_launch_ready_canary_cache_loop_payload(supervised_launch_ready_summary))
        return
    if args.launch_latest_stable_canary_once:
        launch_summary = asyncio.run(launch_latest_stable_canary_once(settings))
        print(build_stable_canary_launch_payload(launch_summary))
        return
    if args.launch_latest_stable_canary_supervise:
        async def run_stable_canary_launch_supervised() -> StableCanaryLaunchLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_stable_canary_launch_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_stable_launch_summary = asyncio.run(
            run_stable_canary_launch_supervised()
        )
        print(build_stable_canary_launch_loop_payload(supervised_stable_launch_summary))
        return
    if args.run_production_supervisor_once:
        supervisor_summary = asyncio.run(run_production_supervisor_cycle_once(settings))
        print(build_production_supervisor_cycle_payload(supervisor_summary))
        return
    if args.run_production_supervisor_supervise:
        async def run_production_supervisor() -> ProductionSupervisorLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_production_supervisor_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervisor_loop_summary = asyncio.run(run_production_supervisor())
        print(build_production_supervisor_loop_payload(supervisor_loop_summary))
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
    if args.observe_system_state_once:
        system_state_summary = asyncio.run(observe_system_state_once(settings))
        print(json.dumps(build_system_state_observation_payload(system_state_summary), indent=2))
        return
    if args.observe_system_state_supervise:

        async def run_system_state_supervised() -> SystemStateObservationLoopSummary:
            stop_event = asyncio.Event()
            install_signal_handlers(
                stop_event,
                signals_to_handle=settings.stop_signals,
            )
            return await run_supervised_system_state_observation_loop(
                settings,
                stop_event=stop_event,
                max_iterations=args.iterations,
            )

        supervised_system_state_summary = asyncio.run(run_system_state_supervised())
        print(
            json.dumps(
                build_system_state_observation_loop_payload(supervised_system_state_summary),
                indent=2,
            )
        )
        return
    if args.iterations is not None:
        loop_summary = asyncio.run(run_polling_loop(settings, iterations=args.iterations))
        print(json.dumps(build_loop_payload(loop_summary), indent=2))
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
