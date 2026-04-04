import asyncio
import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import httpx
import pytest
from carryme_api.config import ApiSettings
from carryme_connectors import ConnectorError
from carryme_models import (
    ApprovedCanaryAlertEvent,
    ApprovedCanarySnapshot,
    CandidateAlertEvent,
    CapacityEstimate,
    ExecutionAlertEvent,
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionObservationEntry,
    ExecutionOrderState,
    ExecutionPairStatus,
    ExecutionReconciliation,
    ExecutionVenueReconciliation,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    FundingUniverseScan,
    FundingUniverseVenueMarket,
    LaunchReadyCanarySnapshot,
    LaunchReadyCanaryStability,
    OpportunityRecord,
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    PaperTradeSystemState,
    RouteApprovalEntry,
    StableCanaryLaunchRecord,
    StableLaunchReadyAlertEvent,
    SystemStateAlertEvent,
    TradeLegIntent,
    VenueAccountPreflight,
    VenueSystemState,
)
from carryme_runtime import (
    AccountPreflightService,
    ExecutionOrderStateService,
    RouteApprovalService,
    SystemStateService,
)
from carryme_storage import (
    ApprovedCanaryAlertStore,
    ApprovedCanaryStore,
    BalanceSnapshotStore,
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    LaunchReadyCanaryStore,
    OpportunityHistoryStore,
    RouteApprovalStore,
    StableCanaryLaunchStore,
    StableLaunchReadyAlertStore,
    SystemStateAlertStore,
)
from carryme_storage.db import DatabaseConnection, redact_database_url
from carryme_worker.config import WorkerSettings
from carryme_worker.main import (
    build_approved_canary_scan_loop_payload,
    build_approved_canary_scan_payload,
    build_candidate_payload,
    build_cycle_payload,
    build_execution_observation_loop_payload,
    build_execution_observation_payload,
    build_health_payload,
    build_launch_ready_canary_cache_loop_payload,
    build_launch_ready_canary_cache_payload,
    build_loop_payload,
    build_production_supervisor_cycle_payload,
    build_production_supervisor_loop_payload,
    build_readiness_payload,
    build_stable_canary_launch_loop_payload,
    build_stable_canary_launch_payload,
    build_system_state_observation_loop_payload,
    build_system_state_observation_payload,
    build_universe_scan_loop_payload,
    build_universe_scan_payload,
)
from carryme_worker.main import (
    main as worker_main,
)
from carryme_worker.notifications import (
    ApprovedCanaryAlertNotifier,
    ExecutionAlertNotifier,
    StableLaunchReadyAlertNotifier,
    SystemStateAlertNotifier,
)
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
    _build_open_hedge_auto_close_reason,
    _build_order_state_observers,
    _execution_requires_continued_monitoring,
    _maybe_auto_close_open_hedged_execution,
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
    summarize_candidates,
)
from fastapi import HTTPException
from pydantic import ValidationError


def _clear_worker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARRYME_WORKER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_LOG_LEVEL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_MAX_BACKOFF_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_SCORE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_MIN_CANDIDATE_ENTRY_EDGE", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_MIN_CANDIDATE_CAPACITY_NOTIONAL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_VENUES", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_RANKING", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_TARGET_NOTIONAL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_MIN_CAPACITY_NOTIONAL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_MIN_DAILY_VOLUME", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_MIN_OPEN_INTEREST", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_MIN_ROUNDTRIP_EDGE", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_MIN_EXECUTION_QUALITY_SCORE", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_MIN_EXECUTION_SAMPLES", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_LIMIT", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_UNIVERSE_SCAN_SNAPSHOT_BATCH_SIZE", raising=False)
    monkeypatch.delenv(
        "CARRYME_WORKER_UNIVERSE_SCAN_EXTENDED_SNAPSHOT_CONCURRENCY",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_UNIVERSE_SCAN_PARADEX_SNAPSHOT_CONCURRENCY",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_UNIVERSE_SCAN_HYPERLIQUID_SNAPSHOT_CONCURRENCY",
        raising=False,
    )
    monkeypatch.delenv("CARRYME_WORKER_EXTENDED_STARK_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_EXECUTION_OBSERVATION_MAX_AGE_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_EXECUTION_AUTO_PAIR_CLOSE_ENABLED", raising=False)
    monkeypatch.delenv(
        "CARRYME_WORKER_EXECUTION_AUTO_PAIR_CLOSE_SHADOW_MODE",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_EXECUTION_AUTO_PAIR_CLOSE_MIN_ENTRY_EDGE_RETENTION_RATIO",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_EXECUTION_AUTO_PAIR_CLOSE_MAX_ROUND_TRIP_BREAK_EVEN_HOLD_WINDOWS",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_EXECUTION_AUTO_PAIR_CLOSE_MAX_HOLD_WINDOWS",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_EXECUTION_AUTO_PAIR_CLOSE_TIMEOUT_SECONDS",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_LAUNCH_READY_MIN_EDGE_RETENTION_RATIO",
        raising=False,
    )
    monkeypatch.delenv("CARRYME_WORKER_STABLE_CANARY_LAUNCH_SHADOW_MODE", raising=False)
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_LAUNCH_READY_MAX_ENTRY_BREAK_EVEN_FUNDING_WINDOWS",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_LAUNCH_READY_MAX_ROUND_TRIP_BREAK_EVEN_FUNDING_WINDOWS",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_CANARY_LAUNCH_MAX_ACTIVE_LIVE_EXECUTIONS",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_CANARY_LAUNCH_ACTIVE_EXECUTION_LIMIT",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_CANARY_LAUNCH_MAX_TOTAL_LIVE_NOTIONAL",
        raising=False,
    )
    monkeypatch.delenv(
        "CARRYME_WORKER_STABLE_CANARY_LAUNCH_MAX_LIVE_NOTIONAL_PER_VENUE",
        raising=False,
    )
    monkeypatch.delenv("CARRYME_WORKER_PARADEX_RECV_WINDOW_MS", raising=False)
    monkeypatch.delenv("CARRYME_API_EXTENDED_STARK_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("CARRYME_API_PARADEX_RECV_WINDOW_MS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_STOP_SIGNALS", raising=False)


def test_worker_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_worker_env(monkeypatch)
    settings = WorkerSettings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.poll_interval_seconds == 30
    assert settings.max_backoff_seconds == 300
    assert settings.universe_scan_interval_seconds == 60
    assert settings.universe_scan_max_backoff_seconds == 300
    assert settings.universe_scan_snapshot_batch_size == 6
    assert settings.universe_scan_extended_snapshot_concurrency == 2
    assert settings.universe_scan_paradex_snapshot_concurrency == 2
    assert settings.universe_scan_hyperliquid_snapshot_concurrency == 2
    assert settings.execution_observation_interval_seconds == 10
    assert settings.execution_observation_max_backoff_seconds == 60
    assert settings.stable_canary_launch_max_active_live_executions == 0
    assert settings.stable_canary_launch_active_execution_limit == 20
    assert settings.stable_canary_launch_max_total_live_notional is None
    assert settings.stable_canary_launch_max_live_notional_per_venue is None
    assert settings.execution_auto_pair_close_enabled is False
    assert settings.execution_auto_pair_close_shadow_mode is False
    assert settings.execution_auto_pair_close_max_snapshot_age_seconds == 300
    assert settings.execution_auto_pair_close_live_revalidation_timeout_seconds == 20.0
    assert settings.execution_auto_pair_close_min_entry_edge_retention_ratio == 0.35
    assert settings.execution_auto_pair_close_max_round_trip_break_even_hold_windows == 2.5
    assert settings.execution_auto_pair_close_max_hold_windows == 2.0
    assert settings.execution_auto_pair_close_timeout_seconds == 30.0
    assert settings.stable_canary_launch_shadow_mode is False
    assert settings.stable_launch_ready_min_edge_retention_ratio == 0.7
    assert settings.stable_launch_ready_max_entry_break_even_funding_windows == 6.0
    assert settings.stable_launch_ready_max_round_trip_break_even_funding_windows == 12.0
    assert settings.score_timeout_seconds == 30.0
    assert settings.universe_scan_timeout_seconds == 30.0
    assert settings.min_candidate_entry_edge == 0.0
    assert settings.min_candidate_capacity_notional == 0.0
    assert settings.universe_scan_venues == ("extended", "paradex", "hyperliquid")
    assert settings.universe_scan_ranking == "route_adjusted_quality_pnl"
    assert settings.universe_scan_target_notional == 5_000.0
    assert settings.universe_scan_min_capacity_notional == 250.0
    assert settings.universe_scan_min_daily_volume == 10_000.0
    assert settings.universe_scan_min_open_interest == 50_000.0
    assert settings.universe_scan_min_roundtrip_edge == 0.0
    assert settings.universe_scan_min_execution_quality_score == 0.0
    assert settings.universe_scan_min_route_stability_weight == 0.0
    assert settings.universe_scan_min_route_presence_ratio == 0.0
    assert settings.universe_scan_min_route_samples == 0
    assert settings.universe_scan_min_execution_samples == 0
    assert settings.universe_scan_limit == 10
    assert settings.approved_canary_scan_min_execution_quality_score == 0.45
    assert settings.approved_canary_scan_min_route_stability_weight == 0.0
    assert settings.approved_canary_scan_min_route_presence_ratio == 0.0
    assert settings.approved_canary_scan_min_route_samples == 0
    assert settings.approved_canary_exact_scan_limit == 25
    assert settings.paradex_recv_window_ms == 300000
    assert settings.execution_observation_max_age_seconds == 1800
    assert settings.stop_signals == ("SIGINT", "SIGTERM")
    assert settings.database_path == "data/carryme.sqlite3"
    assert Path(settings.watchlist_path).is_file()
    assert settings.watchlist_path.endswith("config/watchlists/default.json")


def test_worker_rejects_unknown_universe_fee_profile(tmp_path: Path) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match="Unknown fee profile"):
        WorkerSettings(
            watchlist_path=str(watchlist),
            universe_scan_paradex_fee_profile="definitely-not-real",
        )


def test_worker_rejects_missing_watchlist_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"

    with pytest.raises(ValidationError, match="watchlist_path"):
        WorkerSettings(watchlist_path=str(missing))


def test_worker_rejects_negative_candidate_thresholds(tmp_path: Path) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match="min_candidate_entry_edge"):
        WorkerSettings(
            watchlist_path=str(watchlist),
            min_candidate_entry_edge=-0.01,
        )


def test_worker_rejects_non_positive_execution_observation_limit(tmp_path: Path) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match="execution_observation_limit"):
        WorkerSettings(
            watchlist_path=str(watchlist),
            execution_observation_limit=0,
        )


def test_worker_rejects_non_positive_execution_observation_interval(tmp_path: Path) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match="execution_observation_interval_seconds"):
        WorkerSettings(
            watchlist_path=str(watchlist),
            execution_observation_interval_seconds=0,
        )


@pytest.mark.parametrize("value", (0, -1))
def test_worker_rejects_non_positive_universe_scan_snapshot_batch_size(
    tmp_path: Path,
    value: int,
) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match="universe_scan_snapshot_batch_size"):
        WorkerSettings(
            watchlist_path=str(watchlist),
            universe_scan_snapshot_batch_size=value,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("universe_scan_extended_snapshot_concurrency", 0),
        ("universe_scan_extended_snapshot_concurrency", -1),
        ("universe_scan_paradex_snapshot_concurrency", 0),
        ("universe_scan_paradex_snapshot_concurrency", -1),
        ("universe_scan_hyperliquid_snapshot_concurrency", 0),
        ("universe_scan_hyperliquid_snapshot_concurrency", -1),
    ),
)
def test_worker_rejects_non_positive_snapshot_concurrency(
    tmp_path: Path,
    field_name: str,
    value: int,
) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match=field_name):
        WorkerSettings(
            watchlist_path=str(watchlist),
            **cast(Any, {field_name: value}),
        )


def test_worker_rejects_non_positive_execution_observation_max_age(tmp_path: Path) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(ValidationError, match="execution_observation_max_age_seconds"):
        WorkerSettings(
            watchlist_path=str(watchlist),
            execution_observation_max_age_seconds=0,
        )


def test_worker_rejects_enabled_live_mode_without_required_credentials(tmp_path: Path) -> None:
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text('{"pairs": []}')

    with pytest.raises(
        ValidationError,
        match="extended_api_key",
    ):
        WorkerSettings(
            watchlist_path=str(watchlist),
            extended_live_enabled=True,
        )


def test_worker_health_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_worker_env(monkeypatch)
    payload = build_health_payload(WorkerSettings(environment="test"))

    assert payload.model_dump() == {
        "service": {
            "name": "carryme-worker",
            "version": "0.1.0",
            "environment": "test",
        },
        "status": "ok",
    }


def test_worker_database_target_redacts_urls() -> None:
    settings = WorkerSettings(
        database_path="postgresql+psycopg://user:secret@db.example.com/carryme"
    )

    assert settings.database_target == redact_database_url(settings.database_path)
    assert settings.database_target == "postgresql+psycopg://***@db.example.com/carryme"


def test_worker_readiness_payload(tmp_path: Path) -> None:
    payload = build_readiness_payload(
        WorkerSettings(environment="test", database_path=str(tmp_path / "ready.sqlite3"))
    )

    assert payload.model_dump() == {
        "service": {
            "name": "carryme-worker",
            "version": "0.1.0",
            "environment": "test",
        },
        "status": "ready",
        "database": {
            "target": str(tmp_path / "ready.sqlite3"),
            "ready": True,
        },
    }


def test_worker_readiness_payload_returns_degraded_when_database_ping_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from carryme_worker import main as worker_main_module

    def fail_ping(self: object, timeout_seconds: float) -> None:
        assert timeout_seconds > 0
        raise TimeoutError("database unavailable")

    monkeypatch.setattr(worker_main_module.Database, "ping_with_timeout", fail_ping)

    payload = build_readiness_payload(
        WorkerSettings(
            environment="test",
            database_path="postgresql+psycopg://user:secret@db.example.com/carryme",
        )
    )

    assert payload.model_dump() == {
        "service": {
            "name": "carryme-worker",
            "version": "0.1.0",
            "environment": "test",
        },
        "status": "degraded",
        "database": {
            "target": "postgresql+psycopg://***@db.example.com/carryme",
            "ready": False,
        },
    }


def test_worker_cycle_payload() -> None:
    payload = build_cycle_payload(
        PollCycleSummary(
            watched_pairs=2,
            saved_records=2,
            failed_records=0,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "watched_pairs": 2,
        "saved_records": 2,
        "failed_records": 0,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_loop_payload() -> None:
    payload = build_loop_payload(
        PollLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            saved_records=2,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "saved_records": 2,
        "alert_events": 0,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_candidate_payload() -> None:
    payload = build_candidate_payload(CandidateRecordSummary(total_records=3, candidate_records=1))

    assert payload == {
        "total_records": 3,
        "candidate_records": 1,
    }


def test_worker_universe_scan_payload() -> None:
    payload = build_universe_scan_payload(
        UniverseScanSummary(
            overlap_count=94,
            scanned_opportunities=10,
            saved_records=10,
            alert_events=2,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "overlap_count": 94,
        "scanned_opportunities": 10,
        "saved_records": 10,
        "alert_events": 2,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_universe_scan_loop_payload() -> None:
    payload = build_universe_scan_loop_payload(
        UniverseScanLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            overlap_count=50,
            scanned_opportunities=12,
            saved_records=12,
            alert_events=4,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "overlap_count": 50,
        "scanned_opportunities": 12,
        "saved_records": 12,
        "alert_events": 4,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_approved_canary_scan_payload() -> None:
    payload = build_approved_canary_scan_payload(
        ApprovedCanaryScanSummary(
            scanned_candidates=4,
            approved_candidates=2,
            saved_snapshots=2,
            alert_events=1,
            sent_notifications=1,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "scanned_candidates": 4,
        "approved_candidates": 2,
        "saved_snapshots": 2,
        "alert_events": 1,
        "sent_notifications": 1,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_approved_canary_scan_loop_payload() -> None:
    payload = build_approved_canary_scan_loop_payload(
        ApprovedCanaryScanLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            scanned_candidates=9,
            approved_candidates=4,
            saved_snapshots=4,
            alert_events=2,
            sent_notifications=2,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "scanned_candidates": 9,
        "approved_candidates": 4,
        "saved_snapshots": 4,
        "alert_events": 2,
        "sent_notifications": 2,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_launch_ready_canary_cache_payload() -> None:
    payload = build_launch_ready_canary_cache_payload(
        LaunchReadyCanaryCacheSummary(
            scanned_snapshots=3,
            launch_ready_candidates=2,
            saved_snapshots=2,
            alert_events=1,
            sent_notifications=1,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "scanned_snapshots": 3,
        "launch_ready_candidates": 2,
        "saved_snapshots": 2,
        "alert_events": 1,
        "sent_notifications": 1,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_launch_ready_canary_cache_loop_payload() -> None:
    payload = build_launch_ready_canary_cache_loop_payload(
        LaunchReadyCanaryCacheLoopSummary(
            attempts=4,
            successful_cycles=3,
            failures=1,
            scanned_snapshots=9,
            launch_ready_candidates=5,
            saved_snapshots=5,
            alert_events=2,
            sent_notifications=2,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 4,
        "successful_cycles": 3,
        "failures": 1,
        "scanned_snapshots": 9,
        "launch_ready_candidates": 5,
        "saved_snapshots": 5,
        "alert_events": 2,
        "sent_notifications": 2,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_stable_canary_launch_payload() -> None:
    payload = build_stable_canary_launch_payload(
        StableCanaryLaunchSummary(
            status="launched",
            label="arb_extended_paradex",
            launch_ready_snapshot_id=7,
            approved_snapshot_id=5,
            paper_trade_id=11,
            final_pair_state="closed",
            detail=None,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "status": "launched",
        "label": "arb_extended_paradex",
        "launch_ready_snapshot_id": 7,
        "approved_snapshot_id": 5,
        "paper_trade_id": 11,
        "final_pair_state": "closed",
        "detail": None,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_stable_canary_launch_loop_payload() -> None:
    payload = build_stable_canary_launch_loop_payload(
        StableCanaryLaunchLoopSummary(
            attempts=4,
            successful_cycles=4,
            failures=0,
            launched=1,
            skipped=3,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 4,
        "successful_cycles": 4,
        "failures": 0,
        "launched": 1,
        "skipped": 3,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_production_supervisor_cycle_payload() -> None:
    payload = build_production_supervisor_cycle_payload(
        ProductionSupervisorCycleSummary(
            checked_venues=3,
            degraded_venues=1,
            scanned_candidates=5,
            approved_candidates=2,
            saved_approved_snapshots=2,
            scanned_launch_ready_snapshots=2,
            launch_ready_candidates=1,
            saved_launch_ready_snapshots=1,
            launch_status="launched",
            paper_trade_id=17,
            final_pair_state="hedged",
            observed_executions=1,
            saved_execution_observations=1,
            execution_alerts=0,
            sent_notifications=4,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "checked_venues": 3,
        "degraded_venues": 1,
        "scanned_candidates": 5,
        "approved_candidates": 2,
        "saved_approved_snapshots": 2,
        "scanned_launch_ready_snapshots": 2,
        "launch_ready_candidates": 1,
        "saved_launch_ready_snapshots": 1,
        "launch_status": "launched",
        "paper_trade_id": 17,
        "final_pair_state": "hedged",
        "observed_executions": 1,
        "saved_execution_observations": 1,
        "execution_alerts": 0,
        "sent_notifications": 4,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_production_supervisor_loop_payload() -> None:
    payload = build_production_supervisor_loop_payload(
        ProductionSupervisorLoopSummary(
            attempts=4,
            successful_cycles=3,
            failures=1,
            launched=1,
            skipped=2,
            observed_executions=3,
            execution_alerts=1,
            sent_notifications=5,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 4,
        "successful_cycles": 3,
        "failures": 1,
        "launched": 1,
        "skipped": 2,
        "observed_executions": 3,
        "execution_alerts": 1,
        "sent_notifications": 5,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_system_state_observation_payload() -> None:
    payload = build_system_state_observation_payload(
        SystemStateObservationSummary(
            checked_venues=1,
            degraded_venues=1,
            saved_alerts=1,
            sent_notifications=1,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "checked_venues": 1,
        "degraded_venues": 1,
        "saved_alerts": 1,
        "sent_notifications": 1,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_system_state_observation_loop_payload() -> None:
    payload = build_system_state_observation_loop_payload(
        SystemStateObservationLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            checked_venues=4,
            degraded_venues=2,
            saved_alerts=2,
            sent_notifications=2,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "checked_venues": 4,
        "degraded_venues": 2,
        "saved_alerts": 2,
        "sent_notifications": 2,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_execution_observation_payload() -> None:
    payload = build_execution_observation_payload(
        ExecutionObservationSummary(
            scanned_executions=2,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=1,
            sent_notifications=1,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "scanned_executions": 2,
        "observed_executions": 1,
        "saved_observations": 1,
        "saved_alerts": 1,
        "sent_notifications": 1,
        "database_path": "tmp/history.sqlite3",
    }


def test_worker_execution_observation_loop_payload() -> None:
    payload = build_execution_observation_loop_payload(
        ExecutionObservationLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            scanned_executions=5,
            observed_executions=4,
            saved_observations=4,
            saved_alerts=1,
            sent_notifications=1,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "scanned_executions": 5,
        "observed_executions": 4,
        "saved_observations": 4,
        "saved_alerts": 1,
        "sent_notifications": 1,
        "database_path": "tmp/history.sqlite3",
    }


def test_poll_watchlist_once_saves_history(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class StubScorer:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    store = OpportunityHistoryStore(settings.database_path)

    summary = asyncio.run(
        poll_watchlist_once(
            settings,
            scorer=StubScorer(),
            store=store,
            now=datetime(2026, 3, 29, tzinfo=UTC),
        )
    )

    history = store.list_recent()

    assert summary.watched_pairs == 1
    assert summary.saved_records == 1
    assert summary.failed_records == 0
    assert len(history) == 1
    assert history[0].pair.label == "strk_extended_hyperliquid"


def test_poll_watchlist_once_continues_after_pair_failures(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "bad_pair",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "missing",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            },
            {
              "label": "good_pair",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class StubScorer:
        async def score_pair(self, **kwargs: str) -> FundingArbOpportunity:
            if kwargs["left_fee_profile"] == "missing":
                raise ValueError("Unknown fee profile 'missing' for venue 'extended'")
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    store = OpportunityHistoryStore(settings.database_path)

    with caplog.at_level("WARNING"):
        summary = asyncio.run(
            poll_watchlist_once(
                settings,
                scorer=StubScorer(),
                store=store,
                now=datetime(2026, 3, 29, tzinfo=UTC),
            )
        )

    history = store.list_recent()

    assert summary.watched_pairs == 2
    assert summary.saved_records == 1
    assert summary.failed_records == 1
    assert len(history) == 1
    assert history[0].pair.label == "good_pair"
    assert "Failed to score or persist pair extended/STRK-USD" in caplog.text


def test_poll_watchlist_once_continues_after_transport_errors(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "bad_pair",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            },
            {
              "label": "good_pair",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class StubScorer:
        def __init__(self) -> None:
            self.calls = 0

        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            self.calls += 1
            if self.calls == 1:
                raise httpx.ConnectError("connection refused")
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    store = OpportunityHistoryStore(settings.database_path)

    with caplog.at_level("WARNING"):
        summary = asyncio.run(
            poll_watchlist_once(
                settings,
                scorer=StubScorer(),
                store=store,
                now=datetime(2026, 3, 29, tzinfo=UTC),
            )
        )

    history = store.list_recent()

    assert summary.watched_pairs == 2
    assert summary.saved_records == 1
    assert summary.failed_records == 1
    assert len(history) == 1
    assert history[0].pair.label == "good_pair"
    assert "connection refused" in caplog.text


def test_poll_watchlist_once_times_out_slow_pairs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "slow_pair",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class SlowScorer:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            await asyncio.sleep(0.05)
            raise AssertionError("timeout should have fired first")

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        score_timeout_seconds=0.001,
    )
    store = OpportunityHistoryStore(settings.database_path)

    with caplog.at_level("WARNING"):
        summary = asyncio.run(
            poll_watchlist_once(
                settings,
                scorer=SlowScorer(),
                store=store,
                now=datetime(2026, 3, 29, tzinfo=UTC),
            )
        )

    assert summary.watched_pairs == 1
    assert summary.saved_records == 0
    assert summary.failed_records == 1
    assert "Failed to score or persist pair extended/STRK-USD" in caplog.text


def test_poll_watchlist_once_handles_empty_watchlist(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    store = OpportunityHistoryStore(settings.database_path)

    summary = asyncio.run(
        poll_watchlist_once(
            settings,
            store=store,
            now=datetime(2026, 3, 29, tzinfo=UTC),
        )
    )

    assert summary.watched_pairs == 0
    assert summary.saved_records == 0
    assert summary.failed_records == 0
    assert store.list_recent() == []


def test_scan_funding_universe_once_saves_ranked_history_and_alerts(tmp_path: Path) -> None:
    class StubUniverseScanner:
        async def scan(self, **_: object) -> FundingUniverseScan:
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                ranking="execution_adjusted_quality_pnl",
                target_notional=5000.0,
                overlap_count=1,
                overlaps=[],
                opportunities=[
                    FundingUniverseOpportunity(
                        opportunity=FundingArbOpportunity(
                            canonical_symbol="ARB-USD-PERP",
                            long_venue="paradex",
                            short_venue="extended",
                            long_fee_profile="pro",
                            short_fee_profile="default",
                            gross_daily_edge=0.004,
                            entry_cost_rate=0.00045,
                            round_trip_cost_rate=0.0009,
                            one_day_net_edge_after_entry=0.00355,
                            one_day_net_edge_after_round_trip=0.0031,
                            break_even_days_entry=0.2,
                            break_even_days_round_trip=0.3,
                            capacity=CapacityEstimate(
                                short_bid_notional=1400.0,
                                long_ask_notional=900.0,
                                max_entry_notional=900.0,
                                limiting_venue="paradex",
                            ),
                        ),
                        venue_markets={
                            "extended": FundingUniverseVenueMarket(
                                venue="extended",
                                symbol="ARB-USD",
                                mark_price=0.091,
                                daily_funding_rate=0.000312,
                                open_interest=200_000,
                                daily_volume=150_000,
                                bid_notional=1400.0,
                                ask_notional=1600.0,
                            ),
                            "paradex": FundingUniverseVenueMarket(
                                venue="paradex",
                                symbol="ARB-USD-PERP",
                                mark_price=0.0911,
                                daily_funding_rate=-0.0037,
                                open_interest=180_000,
                                daily_volume=140_000,
                                bid_notional=1200.0,
                                ask_notional=900.0,
                            ),
                        },
                        min_daily_volume=140_000.0,
                        min_open_interest=180_000.0,
                        target_notional=5000.0,
                        deployable_notional=900.0,
                        estimated_one_day_pnl_after_entry=3.195,
                        estimated_one_day_pnl_after_round_trip=2.79,
                        quality_score=1.7,
                    )
                ],
            )

    events: list[CandidateAlertEvent] = []

    class StubCandidateAlertSink:
        def append(self, event: CandidateAlertEvent) -> bool:
            events.append(event)
            return True

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        min_candidate_entry_edge=0.001,
        min_candidate_capacity_notional=500.0,
    )
    store = OpportunityHistoryStore(settings.database_path)

    summary = asyncio.run(
        scan_funding_universe_once(
            settings,
            scanner=StubUniverseScanner(),
            store=store,
            alert_sink=StubCandidateAlertSink(),
            now=datetime(2026, 3, 29, 19, 0, tzinfo=UTC),
        )
    )

    history = store.list_recent(limit=10)
    assert summary.overlap_count == 1
    assert summary.scanned_opportunities == 1
    assert summary.saved_records == 1
    assert summary.alert_events == 1
    assert len(history) == 1
    assert history[0].pair.label == "arb_extended_paradex"
    assert len(events) == 1


def test_scan_funding_universe_once_passes_route_stability_filters(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class StubUniverseScanner:
        async def scan(self, **kwargs: object) -> FundingUniverseScan:
            captured.update(kwargs)
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                ranking=cast(str, kwargs["ranking"]),
                target_notional=cast(float, kwargs["target_notional"]),
                overlap_count=0,
                overlaps=[],
                opportunities=[],
            )

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        universe_scan_min_route_stability_weight=0.25,
        universe_scan_min_route_presence_ratio=0.5,
        universe_scan_min_route_samples=4,
    )

    summary = asyncio.run(
        scan_funding_universe_once(
            settings,
            scanner=StubUniverseScanner(),
            store=OpportunityHistoryStore(settings.database_path),
        )
    )

    assert summary.saved_records == 0
    assert captured["ranking"] == "route_adjusted_quality_pnl"
    assert captured["min_route_stability_weight"] == 0.25
    assert captured["min_route_presence_ratio"] == 0.5
    assert captured["min_route_samples"] == 4


def test_scan_funding_universe_once_passes_fee_profile_overrides(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class StubUniverseScanner:
        async def scan(self, **kwargs: object) -> FundingUniverseScan:
            captured.update(kwargs)
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                fee_profiles=cast(dict[str, str], kwargs["fee_profile_overrides"] or {}),
                ranking=cast(str, kwargs["ranking"]),
                target_notional=cast(float, kwargs["target_notional"]),
                overlap_count=0,
                overlaps=[],
                opportunities=[],
            )

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        universe_scan_extended_fee_profile="default",
        universe_scan_paradex_fee_profile="retail",
    )

    asyncio.run(
        scan_funding_universe_once(
            settings,
            scanner=StubUniverseScanner(),
            store=OpportunityHistoryStore(settings.database_path),
        )
    )

    assert captured["fee_profile_overrides"] == {
        "extended": "default",
        "paradex": "retail",
    }


def test_scan_approved_canary_once_saves_operator_approved_snapshots(tmp_path: Path) -> None:
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="ARB-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=25.0,
    )

    class StubUniverseScanner:
        async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
            return [candidate]

    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)
    alert_store = ApprovedCanaryAlertStore(settings.database_path)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, StubUniverseScanner()),
            store=snapshot_store,
            alert_sink=alert_store,
            now=datetime(2026, 3, 29, 20, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10)
    alerts = alert_store.list_recent(limit=10, label="arb_extended_paradex")

    assert summary.scanned_candidates == 1
    assert summary.approved_candidates == 1
    assert summary.saved_snapshots == 1
    assert summary.alert_events == 1
    assert len(snapshots) == 1
    assert snapshots[0].label == "arb_extended_paradex"
    assert snapshots[0].candidate.suggested_canary_notional == 11.0
    assert snapshots[0].approval.max_live_notional == 11.0
    assert len(alerts) == 1
    assert alerts[0].alert_type == "approved_canary_available"


def test_scan_approved_canary_once_uses_exact_approved_fee_profiles(
    tmp_path: Path,
) -> None:
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="S-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="S-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="S-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=25.0,
    )
    calls: list[dict[str, object]] = []

    class StubUniverseScanner:
        async def scan_canary_candidates(
            self,
            **kwargs: object,
        ) -> list[FundingUniverseCanaryCandidate]:
            calls.append(dict(kwargs))
            fee_profiles = cast(dict[str, str] | None, kwargs["fee_profile_overrides"])
            include_symbols = cast(list[str] | None, kwargs["include_symbols"])
            if fee_profiles == {"extended": "default", "paradex": "pro"}:
                assert include_symbols == ["S-USD-PERP"]
                return [candidate]
            return []

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_paradex_fee_profile="pro_fastfills",
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=11.0,
            note="production canary",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, StubUniverseScanner()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10, label="s_extended_paradex")

    assert summary.scanned_candidates == 1
    assert summary.approved_candidates == 1
    assert summary.saved_snapshots == 1
    assert len(snapshots) == 1
    assert snapshots[0].approval.long_fee_profile == "pro"
    assert snapshots[0].candidate.opportunity.opportunity.long_fee_profile == "pro"
    assert len(calls) == 1
    assert calls[0]["fee_profile_overrides"] == {"extended": "default", "paradex": "pro"}
    assert calls[0]["include_symbols"] == ["S-USD-PERP"]
    assert calls[0]["venues"] == ["extended", "paradex"]


def test_scan_approved_canary_once_skips_timed_out_exact_scans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        universe_scan_timeout_seconds=0.001,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="slow_extended_paradex",
            canonical_symbol="SLOW-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=11.0,
            note="slow exact route",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="fast_extended_paradex",
            canonical_symbol="FAST-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="fast exact route",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    async def fake_scan_exact_canary_candidate_for_approval(
        **kwargs: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        approval = cast(RouteApprovalEntry, kwargs["approval"])
        if approval.label == "slow_extended_paradex":
            await asyncio.sleep(0.01)
            return None, 0
        return (
            FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="FAST-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="FAST-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="FAST-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            1,
        )

    monkeypatch.setattr(
        "carryme_worker.poller.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )

    with caplog.at_level(logging.WARNING):
        summary = asyncio.run(
            scan_approved_canary_once(
                settings,
                scanner=cast(Any, object()),
                approval_service=RouteApprovalService(store=approval_store),
                store=snapshot_store,
                alert_sink=ApprovedCanaryAlertStore(settings.database_path),
                now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
            )
        )

    snapshots = snapshot_store.list_recent(limit=10)

    assert summary.scanned_candidates == 1
    assert summary.approved_candidates == 1
    assert summary.saved_snapshots == 1
    assert len(snapshots) == 1
    assert snapshots[0].label == "fast_extended_paradex"
    assert "approved canary exact scan timed out for label=slow_extended_paradex" in caplog.text


def test_scan_approved_canary_once_preserves_exact_approval_when_labels_repeat(
    tmp_path: Path,
) -> None:
    def _candidate(
        long_fee_profile: str,
        *,
        route_adjusted_quality_score: float,
    ) -> FundingUniverseCanaryCandidate:
        return FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="S-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile=long_fee_profile,
                    short_fee_profile="default",
                    gross_daily_edge=0.004,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.00355,
                    one_day_net_edge_after_round_trip=0.0031,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1400.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol="S-USD",
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="S-USD-PERP",
                    ),
                },
                deployable_notional=900.0,
                estimated_one_day_pnl_after_round_trip=2.79,
                route_adjusted_quality_score=route_adjusted_quality_score,
            ),
            suggested_canary_notional=25.0,
        )

    class StubUniverseScanner:
        async def scan_canary_candidates(
            self,
            **kwargs: object,
        ) -> list[FundingUniverseCanaryCandidate]:
            fee_profiles = cast(dict[str, str] | None, kwargs["fee_profile_overrides"])
            if fee_profiles == {"extended": "default", "paradex": "pro"}:
                return [_candidate("pro", route_adjusted_quality_score=0.4)]
            if fee_profiles == {"extended": "default", "paradex": "pro_fastfills"}:
                return [_candidate("pro_fastfills", route_adjusted_quality_score=0.9)]
            return []

    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=11.0,
            note="tier a",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="tier b",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, StubUniverseScanner()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10, label="s_extended_paradex")

    assert summary.scanned_candidates == 2
    assert summary.approved_candidates == 1
    assert summary.saved_snapshots == 1
    assert [snapshot.approval.long_fee_profile for snapshot in snapshots] == ["pro_fastfills"]
    assert [
        snapshot.candidate.opportunity.opportunity.long_fee_profile for snapshot in snapshots
    ] == ["pro_fastfills"]


def test_scan_approved_canary_once_limit_counts_labels_not_duplicate_approvals(
    tmp_path: Path,
) -> None:
    def _candidate(
        canonical_symbol: str,
        label_side: tuple[str, str],
        fee_profiles: tuple[str, str],
        *,
        route_adjusted_quality_score: float,
    ) -> FundingUniverseCanaryCandidate:
        short_venue, long_venue = label_side
        short_fee_profile, long_fee_profile = fee_profiles
        venue_markets = {
            short_venue: FundingUniverseVenueMarket(
                venue=short_venue,
                symbol=f"{canonical_symbol}-SHORT",
            ),
            long_venue: FundingUniverseVenueMarket(
                venue=long_venue,
                symbol=f"{canonical_symbol}-LONG",
            ),
        }
        return FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol=canonical_symbol,
                    long_venue=long_venue,
                    short_venue=short_venue,
                    long_fee_profile=long_fee_profile,
                    short_fee_profile=short_fee_profile,
                    gross_daily_edge=0.004,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.00355,
                    one_day_net_edge_after_round_trip=0.0031,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1400.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue=long_venue,
                    ),
                ),
                venue_markets=venue_markets,
                deployable_notional=900.0,
                estimated_one_day_pnl_after_round_trip=2.79,
                route_adjusted_quality_score=route_adjusted_quality_score,
            ),
            suggested_canary_notional=25.0,
        )

    class StubUniverseScanner:
        async def scan_canary_candidates(
            self,
            **kwargs: object,
        ) -> list[FundingUniverseCanaryCandidate]:
            include_symbols = cast(list[str] | None, kwargs["include_symbols"])
            fee_profiles = cast(dict[str, str] | None, kwargs["fee_profile_overrides"])
            if include_symbols == ["S-USD-PERP"] and fee_profiles == {
                "extended": "default",
                "paradex": "pro",
            }:
                return [
                    _candidate(
                        "S-USD-PERP",
                        ("extended", "paradex"),
                        ("default", "pro"),
                        route_adjusted_quality_score=0.4,
                    )
                ]
            if include_symbols == ["S-USD-PERP"] and fee_profiles == {
                "extended": "default",
                "paradex": "pro_fastfills",
            }:
                return [
                    _candidate(
                        "S-USD-PERP",
                        ("extended", "paradex"),
                        ("default", "pro_fastfills"),
                        route_adjusted_quality_score=0.9,
                    )
                ]
            if include_symbols == ["JUP-USD-PERP"] and fee_profiles == {
                "paradex": "pro_fastfills",
                "extended": "default",
            }:
                return [
                    _candidate(
                        "JUP-USD-PERP",
                        ("paradex", "extended"),
                        ("pro_fastfills", "default"),
                        route_adjusted_quality_score=0.7,
                    )
                ]
            return []

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_limit=2,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 2, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="s fastfills",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=11.0,
            note="s pro",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="jup_paradex_extended",
            canonical_symbol="JUP-USD-PERP",
            short_venue="paradex",
            long_venue="extended",
            short_fee_profile="pro_fastfills",
            long_fee_profile="default",
            approved=True,
            max_live_notional=11.0,
            note="jup fastfills",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, StubUniverseScanner()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10)
    labels = {snapshot.label for snapshot in snapshots}

    assert summary.scanned_candidates == 3
    assert summary.approved_candidates == 2
    assert summary.saved_snapshots == 2
    assert labels == {"s_extended_paradex", "jup_paradex_extended"}


def test_scan_approved_canary_once_decouples_exact_scan_depth_from_label_budget(
    tmp_path: Path,
) -> None:
    def _candidate(long_fee_profile: str) -> FundingUniverseCanaryCandidate:
        return FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="S-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile=long_fee_profile,
                    short_fee_profile="default",
                    gross_daily_edge=0.004,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.00355,
                    one_day_net_edge_after_round_trip=0.0031,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1400.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol="S-USD",
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="S-USD-PERP",
                    ),
                },
                deployable_notional=900.0,
                estimated_one_day_pnl_after_round_trip=2.79,
                route_adjusted_quality_score=(
                    0.9 if long_fee_profile == "pro_fastfills" else 0.4
                ),
            ),
            suggested_canary_notional=25.0,
        )

    class StubUniverseScanner:
        async def scan_canary_candidates(
            self,
            **kwargs: object,
        ) -> list[FundingUniverseCanaryCandidate]:
            limit = cast(int, kwargs["limit"])
            return [_candidate("pro"), _candidate("pro_fastfills")][:limit]

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_limit=1,
        approved_canary_exact_scan_limit=2,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=11.0,
            note="tier a",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="tier b",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, StubUniverseScanner()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10, label="s_extended_paradex")

    assert summary.scanned_candidates == 4
    assert summary.approved_candidates == 1
    assert summary.saved_snapshots == 1
    assert [snapshot.approval.long_fee_profile for snapshot in snapshots] == ["pro_fastfills"]


def test_scan_approved_canary_once_scans_labels_in_parallel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_limit=2,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="alpha_extended_paradex",
            canonical_symbol="ALPHA-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="alpha",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="beta_extended_paradex",
            canonical_symbol="BETA-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="beta",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)
    started_labels: set[str] = set()
    both_started = asyncio.Event()

    def _candidate(symbol: str) -> FundingUniverseCanaryCandidate:
        return FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol=symbol,
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.004,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.00355,
                    one_day_net_edge_after_round_trip=0.0031,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1400.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol=f"{symbol}-SHORT",
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol=f"{symbol}-LONG",
                    ),
                },
                deployable_notional=900.0,
                estimated_one_day_pnl_after_round_trip=2.79,
            ),
            suggested_canary_notional=11.0,
        )

    async def fake_scan_exact_canary_candidate_for_approval(
        **kwargs: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        approval = cast(RouteApprovalEntry, kwargs["approval"])
        started_labels.add(approval.label)
        if len(started_labels) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=5.0)
        return (_candidate(approval.canonical_symbol), 1)

    monkeypatch.setattr(
        "carryme_worker.poller.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, object()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10)

    assert started_labels == {"alpha_extended_paradex", "beta_extended_paradex"}
    assert summary.scanned_candidates == 2
    assert summary.saved_snapshots == 2
    assert {snapshot.label for snapshot in snapshots} == {
        "alpha_extended_paradex",
        "beta_extended_paradex",
    }


def test_scan_approved_canary_once_continues_on_recoverable_exact_scan_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="broken_extended_paradex",
            canonical_symbol="BROKEN-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=11.0,
            note="broken exact route",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="healthy_extended_paradex",
            canonical_symbol="HEALTHY-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="healthy exact route",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    async def fake_scan_exact_canary_candidate_for_approval(
        **kwargs: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        approval = cast(RouteApprovalEntry, kwargs["approval"])
        if approval.label == "broken_extended_paradex":
            raise ConnectorError("extended temporarily unavailable")
        return (
            FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="HEALTHY-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="HEALTHY-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="HEALTHY-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            1,
        )

    monkeypatch.setattr(
        "carryme_worker.poller.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )

    with caplog.at_level(logging.WARNING):
        summary = asyncio.run(
            scan_approved_canary_once(
                settings,
                scanner=cast(Any, object()),
                approval_service=RouteApprovalService(store=approval_store),
                store=snapshot_store,
                alert_sink=ApprovedCanaryAlertStore(settings.database_path),
                now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
            )
        )

    snapshots = snapshot_store.list_recent(limit=10)

    assert summary.scanned_candidates == 1
    assert summary.approved_candidates == 1
    assert summary.saved_snapshots == 1
    assert len(snapshots) == 1
    assert snapshots[0].label == "healthy_extended_paradex"
    expected_warning = (
        "approved canary exact scan failed for label=broken_extended_paradex: "
        "extended temporarily unavailable"
    )
    assert expected_warning in caplog.text


def test_scan_approved_canary_once_does_not_emit_stale_alert_when_label_scan_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_alert_max_snapshot_age_seconds=60,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
        label="broken_extended_paradex",
        canonical_symbol="BROKEN-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="broken exact route",
    )
    approval_store.upsert(approval)
    snapshot_store = ApprovedCanaryStore(settings.database_path)
    snapshot_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 4, 2, 9, 0, tzinfo=UTC),
            label=approval.label,
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="BROKEN-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="BROKEN-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="BROKEN-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            approval=approval,
        )
    )
    alert_store = ApprovedCanaryAlertStore(settings.database_path)

    async def fail_scan_exact_canary_candidate_for_approval(
        **_: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        raise ConnectorError("extended temporarily unavailable")

    monkeypatch.setattr(
        "carryme_worker.poller.scan_exact_canary_candidate_for_approval",
        fail_scan_exact_canary_candidate_for_approval,
    )

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, object()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=alert_store,
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    assert summary.saved_snapshots == 0
    assert summary.alert_events == 0
    assert alert_store.list_recent(limit=10, label=approval.label) == []


def test_scan_approved_canary_once_uses_relaxed_worker_defaults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    async def fake_scan_exact_canary_candidate_for_approval(
        **kwargs: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        assert kwargs["min_execution_quality_score"] == 0.45
        assert kwargs["min_route_stability_weight"] == 0.0
        assert kwargs["min_route_presence_ratio"] == 0.0
        assert kwargs["min_route_samples"] == 0
        approval = cast(RouteApprovalEntry, kwargs["approval"])
        return (
            FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol=approval.canonical_symbol,
                        long_venue=approval.long_venue,
                        short_venue=approval.short_venue,
                        long_fee_profile=approval.long_fee_profile,
                        short_fee_profile=approval.short_fee_profile,
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue=approval.long_venue,
                        ),
                    ),
                    venue_markets={
                        approval.short_venue: FundingUniverseVenueMarket(
                            venue=approval.short_venue,
                            symbol="JUP-USD",
                        ),
                        approval.long_venue: FundingUniverseVenueMarket(
                            venue=approval.long_venue,
                            symbol="JUP-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=25.0,
            ),
            1,
        )

    monkeypatch.setattr(
        "carryme_worker.poller.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, object()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 4, 10, 5, tzinfo=UTC),
        )
    )

    snapshots = snapshot_store.list_recent(limit=10)
    assert summary.saved_snapshots == 1
    assert len(snapshots) == 1
    assert snapshots[0].label == "jup_extended_paradex"


def test_scan_approved_canary_once_emits_stale_alert_for_missing_route(tmp_path: Path) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_alert_max_snapshot_age_seconds=300,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)
    snapshot_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="ARB-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="ARB-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            approval=approval_store.list_recent(limit=1, label="arb_extended_paradex")[0],
        )
    )

    class EmptyUniverseScanner:
        async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
            return []

    alert_store = ApprovedCanaryAlertStore(settings.database_path)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, EmptyUniverseScanner()),
            store=snapshot_store,
            alert_sink=alert_store,
            now=datetime(2026, 3, 29, 20, 6, tzinfo=UTC),
        )
    )

    alerts = alert_store.list_recent(limit=10, label="arb_extended_paradex")

    assert summary.scanned_candidates == 0
    assert summary.approved_candidates == 0
    assert summary.saved_snapshots == 0
    assert summary.alert_events == 1
    assert len(alerts) == 1
    assert alerts[0].alert_type == "approved_canary_stale"


def test_scan_approved_canary_once_does_not_alert_for_budget_skipped_labels(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_limit=1,
        approved_canary_alert_max_snapshot_age_seconds=60,
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 1, tzinfo=UTC),
            label="scanned_extended_paradex",
            canonical_symbol="SCAN-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="scanned label",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
            label="skipped_extended_paradex",
            canonical_symbol="SKIP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="skipped label",
        )
    )
    snapshot_store = ApprovedCanaryStore(settings.database_path)
    alert_store = ApprovedCanaryAlertStore(settings.database_path)
    stale_at = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    for label, symbol in (
        ("scanned_extended_paradex", "SCAN-USD-PERP"),
        ("skipped_extended_paradex", "SKIP-USD-PERP"),
    ):
        approval = approval_store.list_recent(limit=1, label=label)[0]
        snapshot_store.append(
            ApprovedCanarySnapshot(
                captured_at=stale_at,
                label=label,
                candidate=FundingUniverseCanaryCandidate(
                    opportunity=FundingUniverseOpportunity(
                        opportunity=FundingArbOpportunity(
                            canonical_symbol=symbol,
                            long_venue="paradex",
                            short_venue="extended",
                            long_fee_profile="pro_fastfills",
                            short_fee_profile="default",
                            gross_daily_edge=0.004,
                            entry_cost_rate=0.00045,
                            round_trip_cost_rate=0.0009,
                            one_day_net_edge_after_entry=0.00355,
                            one_day_net_edge_after_round_trip=0.0031,
                            break_even_days_entry=0.2,
                            break_even_days_round_trip=0.3,
                            capacity=CapacityEstimate(
                                short_bid_notional=1400.0,
                                long_ask_notional=900.0,
                                max_entry_notional=900.0,
                                limiting_venue="paradex",
                            ),
                        ),
                        venue_markets={
                            "extended": FundingUniverseVenueMarket(
                                venue="extended",
                                symbol=f"{symbol}-SHORT",
                            ),
                            "paradex": FundingUniverseVenueMarket(
                                venue="paradex",
                                symbol=f"{symbol}-LONG",
                            ),
                        },
                        deployable_notional=900.0,
                        estimated_one_day_pnl_after_round_trip=2.79,
                    ),
                    suggested_canary_notional=11.0,
                ),
                approval=approval,
            )
        )

    class EmptyUniverseScanner:
        async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
            return []

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, EmptyUniverseScanner()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=alert_store,
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    alerts = alert_store.list_recent(limit=10)

    assert summary.alert_events == 1
    assert len(alerts) == 1
    assert alerts[0].previous_snapshot is not None
    assert alerts[0].previous_snapshot.label == "scanned_extended_paradex"


def test_scan_approved_canary_once_uses_approval_label_when_snapshot_label_derivation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
        label="alpha_extended_paradex",
        canonical_symbol="ALPHA-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    approval_store.upsert(approval)
    snapshot_store = ApprovedCanaryStore(settings.database_path)

    async def fake_scan_exact_canary_candidate_for_approval(
        **_: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        return (
            FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="ALPHA-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="ALPHA-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="ALPHA-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            1,
        )

    def fail_build_pair_spec_from_universe_opportunity(_: object) -> object:
        raise AttributeError("bad label")

    monkeypatch.setattr(
        "carryme_worker.poller.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )
    monkeypatch.setattr(
        "carryme_worker.poller.build_pair_spec_from_universe_opportunity",
        fail_build_pair_spec_from_universe_opportunity,
    )

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, object()),
            approval_service=RouteApprovalService(store=approval_store),
            store=snapshot_store,
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            now=datetime(2026, 4, 2, 10, 5, tzinfo=UTC),
        )
    )

    latest_snapshot = snapshot_store.latest(label=approval.label)

    assert summary.saved_snapshots == 1
    assert latest_snapshot is not None
    assert latest_snapshot.label == approval.label


def test_scan_approved_canary_once_notifies_approved_canary_alerts(tmp_path: Path) -> None:
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="ARB-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=25.0,
    )

    class StubUniverseScanner:
        async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
            return [candidate]

    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        )
    )
    notified: list[str] = []

    class StubNotifier:
        async def notify(self, event: ApprovedCanaryAlertEvent) -> None:
            notified.append(event.alert_type)

    summary = asyncio.run(
        scan_approved_canary_once(
            settings,
            scanner=cast(Any, StubUniverseScanner()),
            store=ApprovedCanaryStore(settings.database_path),
            alert_sink=ApprovedCanaryAlertStore(settings.database_path),
            alert_notifier=StubNotifier(),
            now=datetime(2026, 3, 29, 20, 5, tzinfo=UTC),
        )
    )

    assert summary.alert_events == 1
    assert summary.sent_notifications == 1
    assert notified == ["approved_canary_available"]


def test_cache_launch_ready_canaries_once_saves_fresh_ready_snapshots(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        launch_ready_canary_max_snapshot_age_seconds=300,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_bearer_token="token",
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval = approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        )
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 5, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="ARB-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="ARB-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=25.0,
            ),
            approval=approval,
        )
    )

    class StubSystemStateService:
        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            _ = configs
            return [
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="hyperliquid",
                    enabled=False,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
            ]

    launch_ready_store = LaunchReadyCanaryStore(settings.database_path)
    summary = asyncio.run(
        cache_launch_ready_canaries_once(
            settings,
            approved_store=approved_store,
            launch_ready_store=launch_ready_store,
            system_state_service=cast(Any, StubSystemStateService()),
            now=datetime(2026, 3, 29, 20, 6, tzinfo=UTC),
        )
    )

    snapshots = launch_ready_store.list_recent(limit=10, label="arb_extended_paradex")

    assert summary.scanned_snapshots == 1
    assert summary.launch_ready_candidates == 1
    assert summary.saved_snapshots == 1
    assert summary.alert_events == 0
    assert len(snapshots) == 1
    assert snapshots[0].approved_snapshot.approval.max_live_notional == 11.0
    assert snapshots[0].approved_snapshot.candidate.suggested_canary_notional == 11.0
    assert snapshots[0].system_state.ready is True


def test_cache_launch_ready_canaries_once_skips_decayed_approved_snapshot_chain(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        launch_ready_canary_max_snapshot_age_seconds=300,
        stable_launch_ready_min_edge_retention_ratio=0.7,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_bearer_token="token",
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval = approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="near_extended_paradex",
            canonical_symbol="NEAR-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        )
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="near_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="NEAR-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.0021,
                        entry_cost_rate=0.0004,
                        round_trip_cost_rate=0.0008,
                        one_day_net_edge_after_entry=0.0017,
                        one_day_net_edge_after_round_trip=0.0013,
                        break_even_days_entry=0.19,
                        break_even_days_round_trip=0.38,
                        capacity=CapacityEstimate(
                            short_bid_notional=500.0,
                            long_ask_notional=400.0,
                            max_entry_notional=400.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="NEAR-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="NEAR-USD-PERP",
                        ),
                    },
                    deployable_notional=400.0,
                    estimated_one_day_pnl_after_round_trip=0.52,
                ),
                suggested_canary_notional=25.0,
            ),
            approval=approval,
        )
    )
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 2, tzinfo=UTC),
            label="near_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="NEAR-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.0014,
                        entry_cost_rate=0.0004,
                        round_trip_cost_rate=0.0008,
                        one_day_net_edge_after_entry=0.0006,
                        one_day_net_edge_after_round_trip=0.0003,
                        break_even_days_entry=0.19,
                        break_even_days_round_trip=0.38,
                        capacity=CapacityEstimate(
                            short_bid_notional=500.0,
                            long_ask_notional=400.0,
                            max_entry_notional=400.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="NEAR-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="NEAR-USD-PERP",
                        ),
                    },
                    deployable_notional=400.0,
                    estimated_one_day_pnl_after_round_trip=0.12,
                ),
                suggested_canary_notional=25.0,
            ),
            approval=approval,
        )
    )

    class StubSystemStateService:
        def __init__(self) -> None:
            self.calls = 0

        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            self.calls += 1
            _ = configs
            return [
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ]

    system_state_service = StubSystemStateService()
    launch_ready_store = LaunchReadyCanaryStore(settings.database_path)
    summary = asyncio.run(
        cache_launch_ready_canaries_once(
            settings,
            approved_store=approved_store,
            launch_ready_store=launch_ready_store,
            system_state_service=cast(Any, system_state_service),
            now=datetime(2026, 3, 29, 20, 2, 30, tzinfo=UTC),
        )
    )

    assert summary.scanned_snapshots == 1
    assert summary.launch_ready_candidates == 0
    assert summary.saved_snapshots == 0
    assert system_state_service.calls == 0
    assert launch_ready_store.list_recent(limit=10, label="near_extended_paradex") == []


def test_cache_launch_ready_canaries_once_skips_long_break_even_routes(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        launch_ready_canary_max_snapshot_age_seconds=300,
        stable_launch_ready_max_entry_break_even_funding_windows=6.0,
        stable_launch_ready_max_round_trip_break_even_funding_windows=12.0,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_bearer_token="token",
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval = approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        )
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="JUP-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.0014,
                        entry_cost_rate=0.0004,
                        round_trip_cost_rate=0.0008,
                        one_day_net_edge_after_entry=0.0010,
                        one_day_net_edge_after_round_trip=0.0007,
                        break_even_days_entry=0.35,
                        break_even_days_round_trip=0.7,
                        capacity=CapacityEstimate(
                            short_bid_notional=700.0,
                            long_ask_notional=600.0,
                            max_entry_notional=600.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="JUP-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="JUP-USD-PERP",
                        ),
                    },
                    deployable_notional=600.0,
                    estimated_one_day_pnl_after_round_trip=0.42,
                ),
                suggested_canary_notional=25.0,
            ),
            approval=approval,
        )
    )

    class StubSystemStateService:
        def __init__(self) -> None:
            self.calls = 0

        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            self.calls += 1
            _ = configs
            return [
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ]

    system_state_service = StubSystemStateService()
    launch_ready_store = LaunchReadyCanaryStore(settings.database_path)
    summary = asyncio.run(
        cache_launch_ready_canaries_once(
            settings,
            approved_store=approved_store,
            launch_ready_store=launch_ready_store,
            system_state_service=cast(Any, system_state_service),
            now=datetime(2026, 3, 29, 20, 0, 30, tzinfo=UTC),
        )
    )

    assert summary.scanned_snapshots == 1
    assert summary.launch_ready_candidates == 0
    assert summary.saved_snapshots == 0
    assert system_state_service.calls == 0
    assert launch_ready_store.list_recent(limit=10, label="jup_extended_paradex") == []


def test_run_supervised_launch_ready_canary_cache_loop_honors_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        launch_ready_canary_interval_seconds=4,
    )
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def fake_cache_launch_ready_canaries_once(
        settings_arg: WorkerSettings,
        *,
        approved_store: object | None = None,
        launch_ready_store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        approval_service: object | None = None,
        system_state_service: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> LaunchReadyCanaryCacheSummary:
        assert settings_arg is settings
        _ = approved_store
        _ = launch_ready_store
        _ = alert_sink
        _ = alert_notifier
        _ = approval_service
        _ = system_state_service
        _ = logger
        _ = now
        return LaunchReadyCanaryCacheSummary(
            scanned_snapshots=2,
            launch_ready_candidates=1,
            saved_snapshots=1,
            alert_events=1,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.cache_launch_ready_canaries_once",
            fake_cache_launch_ready_canaries_once,
        )
        summary = asyncio.run(
            run_supervised_launch_ready_canary_cache_loop(
                settings,
                approved_store=ApprovedCanaryStore(settings.database_path),
                launch_ready_store=LaunchReadyCanaryStore(settings.database_path),
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.scanned_snapshots == 4
    assert summary.launch_ready_candidates == 2
    assert summary.saved_snapshots == 2
    assert summary.alert_events == 2
    assert summary.sent_notifications == 2
    assert sleeps == [4]


def test_cache_launch_ready_canaries_once_emits_stable_launch_ready_available_alert(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        launch_ready_canary_max_snapshot_age_seconds=300,
        stable_launch_ready_min_snapshot_count=2,
        stable_launch_ready_min_stable_seconds=30.0,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_bearer_token="token",
    )
    approval_store = RouteApprovalStore(settings.database_path)
    approval = approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        )
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="ARB-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="ARB-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            approval=approval,
        )
    )
    approved_snapshot = approved_store.latest(label="arb_extended_paradex")
    assert approved_snapshot is not None
    launch_ready_store = LaunchReadyCanaryStore(settings.database_path)
    launch_ready_store.append(
        LaunchReadyCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            max_snapshot_age_seconds=300,
            approved_snapshot=approved_snapshot,
            system_state=PaperTradeSystemState(
                paper_trade_id=0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                        status=None,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            ),
        )
    )

    class StubSystemStateService:
        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            _ = configs
            return [
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ]

    class StubStableNotifier:
        def __init__(self) -> None:
            self.events: list[str] = []

        async def notify(self, event: StableLaunchReadyAlertEvent) -> None:
            self.events.append(event.alert_type)

    alert_store = StableLaunchReadyAlertStore(settings.database_path)
    notifier = StubStableNotifier()
    summary = asyncio.run(
        cache_launch_ready_canaries_once(
            settings,
            approved_store=approved_store,
            launch_ready_store=launch_ready_store,
            alert_sink=alert_store,
            alert_notifier=notifier,
            system_state_service=cast(Any, StubSystemStateService()),
            now=datetime(2026, 3, 29, 20, 1, tzinfo=UTC),
        )
    )

    alerts = alert_store.list_recent(limit=10, label="arb_extended_paradex")

    assert summary.alert_events == 1
    assert summary.sent_notifications == 1
    assert len(alerts) == 1
    assert alerts[0].alert_type == "stable_launch_ready_available"
    assert alerts[0].current_stability is not None
    assert alerts[0].current_stability.snapshot.label == "arb_extended_paradex"
    assert notifier.events == ["stable_launch_ready_available"]


def test_launch_latest_stable_canary_once_skips_when_no_stable_snapshot(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: (_ for _ in ()).throw(
                HTTPException(status_code=404, detail="No launch-ready canary snapshot found")
            ),
        )
        summary = asyncio.run(launch_latest_stable_canary_once(settings))

    assert summary.status == "skipped"
    assert summary.detail == "No launch-ready canary snapshot found"
    assert summary.paper_trade_id is None


def test_launch_latest_stable_canary_once_skips_when_active_live_hedge_exists(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=31,
            preview_hash="blocking-live-preview",
            confirmation_entry_id=41,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=31,
                created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                label="near_extended_paradex",
                canonical_symbol="NEAR-USD-PERP",
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=31,
            preview_hash=execution.preview_hash,
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=31,
                preview_hash=execution.preview_hash,
                legs=[],
                notes=[],
            ),
            pair_status=_build_auto_close_pair_status(
                execution=execution,
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
            ),
        )
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("active live hedges must block before launch selection")
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.detail == (
        "Active live executions still require monitoring before unattended launch "
        "(max_allowed=0, current=1): "
        "paper_trade_id=31 label=near_extended_paradex "
        "state=hedged/monitor_open_hedge"
    )


def test_launch_latest_stable_canary_once_skips_when_recent_live_trade_is_unobserved(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    execution_store = ExecutionJournalStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=32,
            preview_hash="unobserved-live-preview",
            confirmation_entry_id=42,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=32,
                created_at=datetime(2026, 4, 4, 9, 58, tzinfo=UTC),
                label="jup_extended_paradex",
                canonical_symbol="JUP-USD-PERP",
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("unobserved live submissions must block before launch selection")
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.detail == (
        "Active live executions still require monitoring before unattended launch "
        "(max_allowed=0, current=1): "
        "paper_trade_id=32 label=jup_extended_paradex "
        "state=pending_initial_monitoring"
    )


def test_launch_latest_stable_canary_once_counts_beyond_active_execution_scan_limit(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_max_active_live_executions=1,
        stable_canary_launch_active_execution_limit=1,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)

    for paper_trade_id, label, preview_hash in (
        (41, "near_extended_paradex", "blocking-live-preview-1"),
        (42, "jup_extended_paradex", "blocking-live-preview-2"),
    ):
        execution = execution_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
                adapter="paired_live:extended_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=preview_hash,
                confirmation_entry_id=paper_trade_id + 100,
                paper_trade=_build_auto_close_paper_trade(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                    label=label,
                ),
                legs=[_build_auto_close_execution_leg()],
            )
        )
        observation_store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
                context="worker_execution_monitor",
                execution_entry_id=execution.entry_id,
                paper_trade_id=paper_trade_id,
                preview_hash=execution.preview_hash,
                order_state=ExecutionOrderState(
                    execution_entry_id=execution.entry_id,
                    paper_trade_id=paper_trade_id,
                    preview_hash=execution.preview_hash,
                    legs=[],
                    notes=[],
                ),
                pair_status=_build_auto_close_pair_status(
                    execution=execution,
                    derived_state="hedged",
                    recommended_action="monitor_open_hedge",
                ),
            )
        )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("blocking live executions must short-circuit launch selection")
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert "max_allowed=1, current=2" in cast(str, summary.detail)


def test_launch_latest_stable_canary_once_skips_when_total_live_notional_budget_exceeded(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_max_active_live_executions=1,
        stable_canary_launch_max_total_live_notional=40.0,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=51,
            preview_hash="active-live-preview",
            confirmation_entry_id=151,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=51,
                created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                label="near_extended_paradex",
                canonical_symbol="NEAR-USD-PERP",
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=51,
            preview_hash=execution.preview_hash,
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=51,
                preview_hash=execution.preview_hash,
                legs=[],
                notes=[],
            ),
            pair_status=_build_auto_close_pair_status(
                execution=execution,
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
            ),
        )
    )

    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=20.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(venue="extended", symbol="ARB-USD"),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=20.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=19,
        captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=18,
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})
    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )
    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("risk budget must block before lifecycle launch")
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                api_settings=api_settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.detail == (
        "Stable launch total live notional budget exceeded: "
        "active=25.00 + proposed=20.00 > max=40.00"
    )


def test_launch_latest_stable_canary_once_skips_when_risk_budget_scan_truncates(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_max_active_live_executions=5,
        stable_canary_launch_max_total_live_notional=100.0,
        stable_canary_launch_active_execution_limit=1,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)

    for paper_trade_id, label, preview_hash in (
        (61, "near_extended_paradex", "budget-preview-1"),
        (62, "jup_extended_paradex", "budget-preview-2"),
    ):
        execution = execution_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
                adapter="paired_live:extended_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=preview_hash,
                confirmation_entry_id=paper_trade_id + 100,
                paper_trade=_build_auto_close_paper_trade(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                    label=label,
                ),
                legs=[_build_auto_close_execution_leg()],
            )
        )
        observation_store.append(
            ExecutionObservationEntry(
                observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
                context="worker_execution_monitor",
                execution_entry_id=execution.entry_id,
                paper_trade_id=paper_trade_id,
                preview_hash=execution.preview_hash,
                order_state=ExecutionOrderState(
                    execution_entry_id=execution.entry_id,
                    paper_trade_id=paper_trade_id,
                    preview_hash=execution.preview_hash,
                    legs=[],
                    notes=[],
                ),
                pair_status=_build_auto_close_pair_status(
                    execution=execution,
                    derived_state="hedged",
                    recommended_action="monitor_open_hedge",
                ),
            )
        )

    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=20.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(venue="extended", symbol="ARB-USD"),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=20.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=23,
        captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=22,
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})
    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("truncated budget scan must block before lifecycle launch")
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                api_settings=api_settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.detail == (
        "Stable launch risk-budget check truncated by "
        "stable_canary_launch_active_execution_limit; increase limit"
    )


def test_launch_latest_stable_canary_once_allows_total_live_notional_budget_at_exact_threshold(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_max_active_live_executions=5,
        stable_canary_launch_max_total_live_notional=45.0,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=63,
            preview_hash="active-live-preview-exact-total",
            confirmation_entry_id=163,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=63,
                created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=63,
            preview_hash=execution.preview_hash,
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=63,
                preview_hash=execution.preview_hash,
                legs=[],
                notes=[],
            ),
            pair_status=_build_auto_close_pair_status(
                execution=execution,
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
            ),
        )
    )

    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=20.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(venue="extended", symbol="ARB-USD"),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=20.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=21,
        captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=20,
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})
    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )

    class StubLifecycleResult:
        def __init__(self) -> None:
            self.paper_trade = PaperTradeEntry(
                entry_id=17,
                created_at=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00355,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=900.0,
                    target_notional=20.0,
                    capacity_fraction=20.0 / 900.0,
                    max_target_notional=20.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=20.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=20.0,
                    ),
                ),
            )
            self.execution = ExecutionJournalEntry(
                entry_id=171,
                executed_at=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
                adapter="paired_live:extended_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=17,
                preview_hash="launch-preview-exact-total",
                confirmation_entry_id=271,
                paper_trade=self.paper_trade,
                legs=[_build_auto_close_execution_leg()],
            )
            self.final_pair_status = _build_auto_close_pair_status(
                execution=self.execution,
                derived_state="closed",
                recommended_action="no_action",
            )
            self.observation = ExecutionObservationEntry(
                observed_at=datetime(2026, 4, 4, 10, 3, tzinfo=UTC),
                context="worker_execution_monitor",
                execution_entry_id=171,
                paper_trade_id=17,
                preview_hash="launch-preview-exact-total",
                order_state=ExecutionOrderState(
                    execution_entry_id=171,
                    paper_trade_id=17,
                    preview_hash="launch-preview-exact-total",
                    legs=[],
                    notes=[],
                ),
            )

    async def run_stub_lifecycle(**_: object) -> StubLifecycleResult:
        return StubLifecycleResult()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            run_stub_lifecycle,
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                api_settings=api_settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "launched"


def test_launch_latest_stable_canary_once_skips_when_venue_live_notional_budget_exceeded(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_max_active_live_executions=1,
        stable_canary_launch_max_live_notional_per_venue=30.0,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=52,
            preview_hash="active-live-preview",
            confirmation_entry_id=152,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=52,
                created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                label="near_extended_paradex",
                canonical_symbol="NEAR-USD-PERP",
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=52,
            preview_hash=execution.preview_hash,
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=52,
                preview_hash=execution.preview_hash,
                legs=[],
                notes=[],
            ),
            pair_status=_build_auto_close_pair_status(
                execution=execution,
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
            ),
        )
    )

    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=20.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(venue="extended", symbol="ARB-USD"),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=20.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=20,
        captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=19,
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})
    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("venue budget must block before lifecycle launch")
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                api_settings=api_settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.detail == (
        "Stable launch venue live notional budget exceeded: "
        "venue=extended active=25.00 + proposed=20.00 > max=30.00"
    )


def test_launch_latest_stable_canary_once_allows_venue_live_notional_budget_at_exact_threshold(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_max_active_live_executions=5,
        stable_canary_launch_max_live_notional_per_venue=45.0,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=64,
            preview_hash="active-live-preview-exact-venue",
            confirmation_entry_id=164,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=64,
                created_at=datetime(2026, 4, 4, 9, 55, tzinfo=UTC),
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=64,
            preview_hash=execution.preview_hash,
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=64,
                preview_hash=execution.preview_hash,
                legs=[],
                notes=[],
            ),
            pair_status=_build_auto_close_pair_status(
                execution=execution,
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
            ),
        )
    )

    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=20.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(venue="extended", symbol="ARB-USD"),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=20.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=22,
        captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=21,
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})
    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )

    class StubLifecycleResult:
        def __init__(self) -> None:
            self.paper_trade = PaperTradeEntry(
                entry_id=18,
                created_at=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00355,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=900.0,
                    target_notional=20.0,
                    capacity_fraction=20.0 / 900.0,
                    max_target_notional=20.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=20.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=20.0,
                    ),
                ),
            )
            self.execution = ExecutionJournalEntry(
                entry_id=181,
                executed_at=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
                adapter="paired_live:extended_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=18,
                preview_hash="launch-preview-exact-venue",
                confirmation_entry_id=281,
                paper_trade=self.paper_trade,
                legs=[_build_auto_close_execution_leg()],
            )
            self.final_pair_status = _build_auto_close_pair_status(
                execution=self.execution,
                derived_state="closed",
                recommended_action="no_action",
            )
            self.observation = ExecutionObservationEntry(
                observed_at=datetime(2026, 4, 4, 10, 3, tzinfo=UTC),
                context="worker_execution_monitor",
                execution_entry_id=181,
                paper_trade_id=18,
                preview_hash="launch-preview-exact-venue",
                order_state=ExecutionOrderState(
                    execution_entry_id=181,
                    paper_trade_id=18,
                    preview_hash="launch-preview-exact-venue",
                    legs=[],
                    notes=[],
                ),
            )

    async def run_stub_lifecycle(**_: object) -> StubLifecycleResult:
        return StubLifecycleResult()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            run_stub_lifecycle,
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                api_settings=api_settings,
                now=datetime(2026, 4, 4, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "launched"


def test_launch_latest_stable_canary_once_skips_stale_latest_approved_snapshot(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval_store = RouteApprovalStore(settings.database_path)
    approval = approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        )
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    older_snapshot = approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 20, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="JUP-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.0016,
                        entry_cost_rate=0.0004,
                        round_trip_cost_rate=0.0008,
                        one_day_net_edge_after_entry=0.0012,
                        one_day_net_edge_after_round_trip=0.0009,
                        break_even_days_entry=0.15,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=700.0,
                            long_ask_notional=600.0,
                            max_entry_notional=600.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="JUP-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="JUP-USD-PERP",
                        ),
                    },
                    deployable_notional=600.0,
                    estimated_one_day_pnl_after_round_trip=0.54,
                ),
                suggested_canary_notional=25.0,
            ),
            approval=approval,
        )
    )
    latest_snapshot = approved_store.append(
        older_snapshot.model_copy(
            update={
                "snapshot_id": None,
                "captured_at": datetime(2026, 3, 29, 20, 1, tzinfo=UTC),
            }
        )
    )
    launch_ready_snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 29, 20, 0, 30, tzinfo=UTC),
        label="jup_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=older_snapshot,
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="jup_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=launch_ready_snapshot,
        consecutive_snapshots=2,
        stable_seconds=30.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (
                launch_ready_snapshot,
                older_snapshot.candidate,
                older_snapshot.approval,
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                approved_store=approved_store,
                now=latest_snapshot.captured_at,
            )
        )

    assert summary.status == "skipped"
    assert summary.launch_ready_snapshot_id == 9
    assert summary.approved_snapshot_id == older_snapshot.snapshot_id
    assert (
        summary.detail
        == "Launch-ready canary snapshot is stale relative to the latest approved snapshot"
    )


def test_launch_latest_stable_canary_once_returns_launched_summary(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="ARB-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=8,
            captured_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})

    class StubLifecycleResult:
        def __init__(self) -> None:
            self.paper_trade = PaperTradeEntry(
                entry_id=17,
                created_at=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00355,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=900.0,
                    target_notional=11.0,
                    capacity_fraction=11.0 / 900.0,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
                note="worker launch",
            )
            self.final_pair_status = ExecutionPairStatus(
                execution_entry_id=31,
                paper_trade_id=17,
                preview_hash="preview",
                derived_state="closed",
                recommended_action="no_action",
                order_state=ExecutionOrderState(
                    execution_entry_id=31,
                    paper_trade_id=17,
                    preview_hash="preview",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=31,
                    paper_trade_id=17,
                    preview_hash="preview",
                    status="accepted",
                    recommended_action="no_action",
                    matched_all_leg_symbols=True,
                    venues=[
                        ExecutionVenueReconciliation(
                            venue="extended",
                            authenticated=True,
                            ready=True,
                            matched_leg_symbols=["ARB-USD"],
                            unmatched_leg_symbols=[],
                        ),
                        ExecutionVenueReconciliation(
                            venue="paradex",
                            authenticated=True,
                            ready=True,
                            matched_leg_symbols=["ARB-USD-PERP"],
                            unmatched_leg_symbols=[],
                        ),
                    ],
                    notes=[],
                ),
                notes=[],
            )

    api_settings = ApiSettings(
        database_path=settings.database_path,
        watchlist_path=settings.watchlist_path,
        environment=settings.environment,
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
    )

    async def run_stub_lifecycle(**_: object) -> StubLifecycleResult:
        return StubLifecycleResult()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            run_stub_lifecycle,
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                api_settings=api_settings,
                now=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "launched"
    assert summary.label == "arb_extended_paradex"
    assert summary.launch_ready_snapshot_id == 9
    assert summary.approved_snapshot_id == snapshot.approved_snapshot.snapshot_id
    assert summary.paper_trade_id == 17
    assert summary.final_pair_state == "closed"
    launch_records = StableCanaryLaunchStore(settings.database_path).list_recent(limit=10)
    assert len(launch_records) == 1
    assert launch_records[0].paper_trade_id == 17
    assert launch_records[0].launch_ready_snapshot_id == 9


def test_launch_latest_stable_canary_once_uses_worker_settings_when_api_settings_omitted(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-secret",
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
        paradex_recv_window_ms=1234,
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0x789",
        hyperliquid_vault_address="0xabc",
        hyperliquid_api_wallet_private_key="0xdef",
    )
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="ARB-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=8,
            captured_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})

    class StubLifecycleResult:
        def __init__(self) -> None:
            self.paper_trade = PaperTradeEntry(
                entry_id=17,
                created_at=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00355,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=900.0,
                    target_notional=11.0,
                    capacity_fraction=11.0 / 900.0,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
                note="worker launch",
            )
            self.final_pair_status = ExecutionPairStatus(
                execution_entry_id=31,
                paper_trade_id=17,
                preview_hash="preview",
                derived_state="closed",
                recommended_action="no_action",
                order_state=ExecutionOrderState(
                    execution_entry_id=31,
                    paper_trade_id=17,
                    preview_hash="preview",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=31,
                    paper_trade_id=17,
                    preview_hash="preview",
                    status="accepted",
                    recommended_action="no_action",
                    matched_all_leg_symbols=True,
                    venues=[
                        ExecutionVenueReconciliation(
                            venue="extended",
                            authenticated=True,
                            ready=True,
                            matched_leg_symbols=["ARB-USD"],
                            unmatched_leg_symbols=[],
                        ),
                        ExecutionVenueReconciliation(
                            venue="paradex",
                            authenticated=True,
                            ready=True,
                            matched_leg_symbols=["ARB-USD-PERP"],
                            unmatched_leg_symbols=[],
                        ),
                    ],
                    notes=[],
                ),
                notes=[],
            )

    async def run_stub_lifecycle(**kwargs: object) -> StubLifecycleResult:
        runtime_settings = cast(ApiSettings, kwargs["settings"])
        assert runtime_settings.extended_live_enabled is True
        assert runtime_settings.extended_api_key == "extended-key"
        assert runtime_settings.extended_stark_private_key == "extended-secret"
        assert runtime_settings.paradex_live_enabled is True
        assert runtime_settings.paradex_account_address == "0x123"
        assert runtime_settings.paradex_private_key == "0x456"
        assert runtime_settings.paradex_recv_window_ms == 1234
        assert runtime_settings.hyperliquid_live_enabled is True
        assert runtime_settings.hyperliquid_account_address == "0x789"
        assert runtime_settings.hyperliquid_vault_address == "0xabc"
        assert runtime_settings.hyperliquid_api_wallet_private_key == "0xdef"
        return StubLifecycleResult()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            run_stub_lifecycle,
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                now=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "launched"
    assert summary.paper_trade_id == 17


def test_launch_latest_stable_canary_once_skips_when_snapshot_already_launched(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    launch_store = StableCanaryLaunchStore(settings.database_path)
    launch_store.append(
        StableCanaryLaunchRecord(
            launched_at=datetime(2026, 3, 30, 10, 3, tzinfo=UTC),
            status="launched",
            label="arb_extended_paradex",
            launch_ready_snapshot_id=9,
            approved_snapshot_id=8,
            paper_trade_id=17,
            final_pair_state="closed",
        )
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=8,
            captured_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="ARB-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.004,
                        entry_cost_rate=0.00045,
                        round_trip_cost_rate=0.0009,
                        one_day_net_edge_after_entry=0.00355,
                        one_day_net_edge_after_round_trip=0.0031,
                        break_even_days_entry=0.2,
                        break_even_days_round_trip=0.3,
                        capacity=CapacityEstimate(
                            short_bid_notional=1400.0,
                            long_ask_notional=900.0,
                            max_entry_notional=900.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="ARB-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                        ),
                    },
                    deployable_notional=900.0,
                    estimated_one_day_pnl_after_round_trip=2.79,
                ),
                suggested_canary_notional=11.0,
            ),
            approval=RouteApprovalEntry(
                updated_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_fee_profile="default",
                long_fee_profile="pro_fastfills",
                approved=True,
                max_live_notional=11.0,
                note="approved canary",
            ),
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (
                snapshot,
                snapshot.approved_snapshot.candidate,
                snapshot.approved_snapshot.approval,
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                launch_store=launch_store,
                now=datetime(2026, 3, 30, 10, 4, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.paper_trade_id == 17
    assert "already launched" in cast(str, summary.detail)


def test_launch_latest_stable_canary_once_ignores_unselected_live_credentials(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        paradex_live_enabled=True,
        paradex_account_address="0x123",
        paradex_private_key="0x456",
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0x789",
        hyperliquid_api_wallet_private_key="0xdef",
    )
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
        label="arb_hyperliquid_paradex",
        canonical_symbol="ETH-USD-PERP",
        short_venue="hyperliquid",
        long_venue="paradex",
        short_fee_profile="vip",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ETH-USD-PERP",
                long_venue="paradex",
                short_venue="hyperliquid",
                long_fee_profile="pro_fastfills",
                short_fee_profile="vip",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "hyperliquid": FundingUniverseVenueMarket(
                    venue="hyperliquid",
                    symbol="ETH",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ETH-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_hyperliquid_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=8,
            captured_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
            label="arb_hyperliquid_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_hyperliquid_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="hyperliquid",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    persisted_snapshot = approved_store.append(snapshot.approved_snapshot)
    snapshot = snapshot.model_copy(update={"approved_snapshot": persisted_snapshot})
    stability = stability.model_copy(update={"snapshot": snapshot})

    class StubLifecycleResult:
        def __init__(self) -> None:
            self.paper_trade = PaperTradeEntry(
                entry_id=17,
                created_at=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
                intent=FundingPairTradeIntent(
                    label="arb_hyperliquid_paradex",
                    canonical_symbol="ETH-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00355,
                    break_even_days_entry=0.2,
                    capacity_limit_notional=900.0,
                    target_notional=11.0,
                    capacity_fraction=11.0 / 900.0,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ETH-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="hyperliquid",
                        symbol="ETH",
                        fee_profile="vip",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
                note="worker launch",
            )
            self.final_pair_status = ExecutionPairStatus(
                execution_entry_id=31,
                paper_trade_id=17,
                preview_hash="preview",
                derived_state="closed",
                recommended_action="no_action",
                order_state=ExecutionOrderState(
                    execution_entry_id=31,
                    paper_trade_id=17,
                    preview_hash="preview",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=31,
                    paper_trade_id=17,
                    preview_hash="preview",
                    status="accepted",
                    recommended_action="no_action",
                    matched_all_leg_symbols=True,
                    venues=[
                        ExecutionVenueReconciliation(
                            venue="hyperliquid",
                            authenticated=True,
                            ready=True,
                            matched_leg_symbols=["ETH"],
                            unmatched_leg_symbols=[],
                        ),
                        ExecutionVenueReconciliation(
                            venue="paradex",
                            authenticated=True,
                            ready=True,
                            matched_leg_symbols=["ETH-USD-PERP"],
                            unmatched_leg_symbols=[],
                        ),
                    ],
                    notes=[],
                ),
                notes=[],
            )

    async def run_stub_lifecycle(**kwargs: object) -> StubLifecycleResult:
        cleanup_preview_service = cast(Any, kwargs["cleanup_preview_service"])
        pair_close_preview_service = cast(Any, kwargs["pair_close_preview_service"])
        cleanup_live_router = cast(Any, kwargs["cleanup_live_router"])
        paired_service = cast(Any, kwargs["paired_service"])
        pair_close_live_service = cast(Any, kwargs["pair_close_live_service"])

        assert set(cleanup_preview_service.services) == {"hyperliquid", "paradex"}
        assert set(pair_close_preview_service.services) == {"hyperliquid", "paradex"}
        assert set(cleanup_live_router.services) == {"hyperliquid", "paradex"}
        assert set(paired_service.services) == {"hyperliquid", "paradex"}
        assert set(pair_close_live_service.services) == {"hyperliquid", "paradex"}
        return StubLifecycleResult()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            run_stub_lifecycle,
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                now=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
            )
        )

    assert summary.status == "launched"
    assert summary.paper_trade_id == 17


def _build_auto_close_paper_trade(
    *,
    entry_id: int,
    created_at: datetime,
    label: str = "jup_extended_paradex",
    canonical_symbol: str = "JUP-USD-PERP",
    entry_edge: float = 0.0012,
) -> PaperTradeEntry:
    return PaperTradeEntry(
        entry_id=entry_id,
        created_at=created_at,
        intent=FundingPairTradeIntent(
            label=label,
            canonical_symbol=canonical_symbol,
            source_recorded_at=created_at,
            one_day_net_edge_after_entry=entry_edge,
            break_even_days_entry=0.2,
            capacity_limit_notional=300.0,
            target_notional=25.0,
            capacity_fraction=25.0 / 300.0,
            max_target_notional=25.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol=f"{canonical_symbol}",
                fee_profile="pro_fastfills",
                side="buy",
                target_notional=25.0,
            ),
            short_leg=TradeLegIntent(
                venue="extended",
                symbol=canonical_symbol.replace("-PERP", ""),
                fee_profile="default",
                side="sell",
                target_notional=25.0,
            ),
        ),
    )


def _build_auto_close_snapshot(
    *,
    captured_at: datetime,
    label: str = "jup_extended_paradex",
    canonical_symbol: str = "JUP-USD-PERP",
    entry_edge: float = 0.0010,
    round_trip_edge: float = 0.0008,
    break_even_days_round_trip: float | None = 0.4,
) -> ApprovedCanarySnapshot:
    return ApprovedCanarySnapshot(
        captured_at=captured_at,
        label=label,
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol=canonical_symbol,
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.0014,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=round_trip_edge,
                    break_even_days_entry=0.1,
                    break_even_days_round_trip=break_even_days_round_trip,
                    capacity=CapacityEstimate(
                        short_bid_notional=500.0,
                        long_ask_notional=400.0,
                        max_entry_notional=400.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol=canonical_symbol.replace("-PERP", ""),
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol=canonical_symbol,
                    ),
                },
                deployable_notional=400.0,
                estimated_one_day_pnl_after_round_trip=0.3,
            ),
            suggested_canary_notional=25.0,
        ),
        approval=RouteApprovalEntry(
            updated_at=captured_at,
            label=label,
            canonical_symbol=canonical_symbol,
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        ),
    )


def _build_auto_close_pair_status(
    *,
    execution: ExecutionJournalEntry,
    derived_state: Literal[
        "hedged",
        "pending",
        "unfilled",
        "closed",
        "cleanup_needed",
        "review_required",
    ] = "hedged",
    recommended_action: str = "monitor_open_hedge",
) -> ExecutionPairStatus:
    return ExecutionPairStatus(
        execution_entry_id=execution.entry_id or 1,
        paper_trade_id=execution.paper_trade_id,
        preview_hash=execution.preview_hash,
        derived_state=derived_state,
        recommended_action=recommended_action,
        order_state=ExecutionOrderState(
            execution_entry_id=execution.entry_id or 1,
            paper_trade_id=execution.paper_trade_id,
            preview_hash=execution.preview_hash,
            legs=[],
            notes=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=execution.entry_id or 1,
            paper_trade_id=execution.paper_trade_id,
            preview_hash=execution.preview_hash,
            status="accepted",
            recommended_action="no_action",
            matched_all_leg_symbols=True,
            venues=[],
            notes=[],
        ),
        notes=[],
    )


def _build_auto_close_execution_leg() -> ExecutionLegResult:
    return ExecutionLegResult(
        venue="extended",
        symbol="JUP-USD",
        fee_profile="default",
        side="sell",
        target_notional=25.0,
        status="submitted",
        simulated=False,
        external_reference="ext-order-auto-close",
    )


def test_launch_latest_stable_canary_once_shadow_mode_skips_live_submission(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_shadow_mode=True,
    )
    launch_store = StableCanaryLaunchStore(settings.database_path)
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="ARB-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    approved_snapshot = approved_store.append(
        ApprovedCanarySnapshot(
            snapshot_id=8,
            captured_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=candidate,
            approval=approval,
        )
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=approved_snapshot,
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )

    async def fail_if_called(**_: object) -> object:
        raise AssertionError("shadow mode should not submit live executions")

    with pytest.MonkeyPatch.context() as monkeypatch:
        caplog.set_level(logging.INFO, logger="carryme.worker")
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._run_guarded_canary_lifecycle",
            fail_if_called,
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                launch_store=launch_store,
                approved_store=approved_store,
                now=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
            )
        )

    launch_records = launch_store.list_recent(limit=10)
    assert summary.status == "skipped"
    assert summary.launch_ready_snapshot_id == 9
    assert summary.detail == "Shadow mode: would launch stable canary from launch-ready snapshot 9"
    assert "shadow launch for label=arb_extended_paradex" in caplog.text
    assert len(launch_records) == 1
    assert launch_records[0].status == "shadowed"
    assert launch_records[0].launch_ready_snapshot_id == 9
    assert launch_records[0].paper_trade_id == 0

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (snapshot, candidate, approval),
        )
        repeat_summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                launch_store=launch_store,
                approved_store=approved_store,
                now=datetime(2026, 3, 30, 10, 3, tzinfo=UTC),
            )
        )

    assert repeat_summary.status == "skipped"
    assert (
        repeat_summary.detail
        == "Launch-ready canary snapshot already evaluated in shadow mode by worker"
    )
    assert len(launch_store.list_recent(limit=10)) == 1


def test_launch_latest_stable_canary_once_shadow_gate_block_logs_revalidation_skip(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_shadow_mode=True,
        stable_launch_ready_min_edge_retention_ratio=0.9,
    )
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    older_snapshot = ApprovedCanarySnapshot(
        snapshot_id=7,
        captured_at=datetime(2026, 3, 30, 10, 0, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="ARB-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.0045,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.004,
                    one_day_net_edge_after_round_trip=0.0036,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1400.0,
                        long_ask_notional=900.0,
                        max_entry_notional=900.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol="ARB-USD",
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                    ),
                },
                deployable_notional=900.0,
                estimated_one_day_pnl_after_round_trip=3.24,
            ),
            suggested_canary_notional=11.0,
        ),
        approval=approval,
    )
    latest_snapshot = ApprovedCanarySnapshot(
        snapshot_id=8,
        captured_at=datetime(2026, 3, 30, 10, 1, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="ARB-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.0015,
                    entry_cost_rate=0.00045,
                    round_trip_cost_rate=0.0009,
                    one_day_net_edge_after_entry=0.0012,
                    one_day_net_edge_after_round_trip=0.001,
                    break_even_days_entry=0.2,
                    break_even_days_round_trip=0.3,
                    capacity=CapacityEstimate(
                        short_bid_notional=1200.0,
                        long_ask_notional=850.0,
                        max_entry_notional=850.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(
                        venue="extended",
                        symbol="ARB-USD",
                    ),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                    ),
                },
                deployable_notional=850.0,
                estimated_one_day_pnl_after_round_trip=0.85,
            ),
            suggested_canary_notional=11.0,
        ),
        approval=approval,
    )
    approved_store = ApprovedCanaryStore(settings.database_path)
    older_snapshot = approved_store.append(older_snapshot)
    latest_snapshot = approved_store.append(latest_snapshot)
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=9,
        captured_at=datetime(2026, 3, 30, 10, 2, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=latest_snapshot,
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    stability = LaunchReadyCanaryStability(
        snapshot=snapshot,
        consecutive_snapshots=3,
        stable_seconds=45.0,
        min_snapshot_count=2,
        min_stable_seconds=30.0,
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        caplog.set_level(logging.INFO, logger="carryme.worker")
        monkeypatch.setattr(
            "carryme_worker.poller._build_launch_ready_canary_stability",
            lambda **_: stability,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._select_latest_launch_ready_canary_snapshot",
            lambda **_: (
                snapshot,
                latest_snapshot.candidate,
                latest_snapshot.approval,
            ),
        )
        summary = asyncio.run(
            launch_latest_stable_canary_once(
                settings,
                approved_store=approved_store,
                now=datetime(2026, 3, 30, 10, 3, tzinfo=UTC),
            )
        )

    assert summary.status == "skipped"
    assert summary.detail == (
        "Latest approved snapshot no longer satisfies automated launch gates: "
        "entry edge retention 0.30 below minimum 0.90"
    )
    assert (
        "shadow launch gate blocked label=arb_extended_paradex because entry edge retention "
        "0.30 below minimum 0.90" in caplog.text
    )


def test_build_open_hedge_auto_close_reason_flags_entry_edge_decay() -> None:
    settings = WorkerSettings(
        execution_auto_pair_close_enabled=True,
        execution_auto_pair_close_min_entry_edge_retention_ratio=0.35,
    )
    paper_trade = PaperTradeEntry(
        entry_id=11,
        created_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
        intent=FundingPairTradeIntent(
            label="near_extended_paradex",
            canonical_symbol="NEAR-USD-PERP",
            source_recorded_at=datetime(2026, 4, 4, 8, 59, tzinfo=UTC),
            one_day_net_edge_after_entry=0.0016,
            break_even_days_entry=0.2,
            capacity_limit_notional=300.0,
            target_notional=25.0,
            capacity_fraction=25.0 / 300.0,
            max_target_notional=25.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="NEAR-USD-PERP",
                fee_profile="pro_fastfills",
                side="buy",
                target_notional=25.0,
            ),
            short_leg=TradeLegIntent(
                venue="extended",
                symbol="NEAR-USD",
                fee_profile="default",
                side="sell",
                target_notional=25.0,
            ),
        ),
    )
    snapshot = ApprovedCanarySnapshot(
        captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        label="near_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="NEAR-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.0010,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=0.0004,
                    one_day_net_edge_after_round_trip=0.0002,
                    break_even_days_entry=0.3,
                    break_even_days_round_trip=0.6,
                    capacity=CapacityEstimate(
                        short_bid_notional=500.0,
                        long_ask_notional=400.0,
                        max_entry_notional=400.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(venue="extended", symbol="NEAR-USD"),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="NEAR-USD-PERP",
                    ),
                },
                deployable_notional=400.0,
                estimated_one_day_pnl_after_round_trip=0.08,
            ),
            suggested_canary_notional=25.0,
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 4, 4, 8, 58, tzinfo=UTC),
            label="near_extended_paradex",
            canonical_symbol="NEAR-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        ),
    )

    reason = _build_open_hedge_auto_close_reason(
        paper_trade=paper_trade,
        opened_at=datetime(2026, 4, 4, 8, 0, tzinfo=UTC),
        snapshot=snapshot,
        settings=settings,
        now=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
    )

    assert reason is not None
    assert reason.startswith("entry edge retention")


def test_build_open_hedge_auto_close_reason_flags_max_hold_windows() -> None:
    settings = WorkerSettings(
        execution_auto_pair_close_enabled=True,
        execution_auto_pair_close_max_hold_windows=2.0,
        execution_auto_pair_close_min_entry_edge_retention_ratio=0.2,
        execution_auto_pair_close_max_round_trip_break_even_hold_windows=4.0,
    )
    paper_trade = PaperTradeEntry(
        entry_id=12,
        created_at=datetime(2026, 4, 3, 0, 0, tzinfo=UTC),
        intent=FundingPairTradeIntent(
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            source_recorded_at=datetime(2026, 4, 2, 23, 59, tzinfo=UTC),
            one_day_net_edge_after_entry=0.0012,
            break_even_days_entry=0.2,
            capacity_limit_notional=300.0,
            target_notional=25.0,
            capacity_fraction=25.0 / 300.0,
            max_target_notional=25.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="JUP-USD-PERP",
                fee_profile="pro_fastfills",
                side="buy",
                target_notional=25.0,
            ),
            short_leg=TradeLegIntent(
                venue="extended",
                symbol="JUP-USD",
                fee_profile="default",
                side="sell",
                target_notional=25.0,
            ),
        ),
    )
    snapshot = ApprovedCanarySnapshot(
        captured_at=datetime(2026, 4, 4, 0, 30, tzinfo=UTC),
        label="jup_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="JUP-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.0014,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=0.0010,
                    one_day_net_edge_after_round_trip=0.0008,
                    break_even_days_entry=0.1,
                    break_even_days_round_trip=0.4,
                    capacity=CapacityEstimate(
                        short_bid_notional=500.0,
                        long_ask_notional=400.0,
                        max_entry_notional=400.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(venue="extended", symbol="JUP-USD"),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="JUP-USD-PERP",
                    ),
                },
                deployable_notional=400.0,
                estimated_one_day_pnl_after_round_trip=0.3,
            ),
            suggested_canary_notional=25.0,
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 4, 3, 23, 59, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        ),
    )

    reason = _build_open_hedge_auto_close_reason(
        paper_trade=paper_trade,
        opened_at=datetime(2026, 4, 3, 0, 0, tzinfo=UTC),
        snapshot=snapshot,
        settings=settings,
        now=datetime(2026, 4, 4, 17, 0, tzinfo=UTC),
    )

    assert reason is not None
    assert reason.startswith("hold age windows")


def test_build_open_hedge_auto_close_reason_flags_non_positive_round_trip_edge() -> None:
    settings = WorkerSettings(execution_auto_pair_close_enabled=True)
    paper_trade = _build_auto_close_paper_trade(
        entry_id=13,
        created_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
    )
    snapshot = _build_auto_close_snapshot(
        captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        round_trip_edge=0.0,
    )

    reason = _build_open_hedge_auto_close_reason(
        paper_trade=paper_trade,
        opened_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
        snapshot=snapshot,
        settings=settings,
        now=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
    )

    assert reason is not None
    assert reason.startswith("latest approved round-trip edge is non-positive")


def test_build_open_hedge_auto_close_reason_flags_round_trip_break_even_hold_windows() -> None:
    settings = WorkerSettings(
        execution_auto_pair_close_enabled=True,
        execution_auto_pair_close_max_hold_windows=10.0,
        execution_auto_pair_close_max_round_trip_break_even_hold_windows=1.0,
    )
    paper_trade = _build_auto_close_paper_trade(
        entry_id=14,
        created_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
    )
    snapshot = _build_auto_close_snapshot(
        captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
        break_even_days_round_trip=0.7,
    )

    reason = _build_open_hedge_auto_close_reason(
        paper_trade=paper_trade,
        opened_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
        snapshot=snapshot,
        settings=settings,
        now=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
    )

    assert reason is not None
    assert reason.startswith("round-trip break-even hold windows")




def test_build_open_hedge_auto_close_reason_uses_execution_open_time_for_hold_age() -> None:
    settings = WorkerSettings(
        execution_auto_pair_close_enabled=True,
        execution_auto_pair_close_max_hold_windows=2.0,
        execution_auto_pair_close_min_entry_edge_retention_ratio=0.2,
        execution_auto_pair_close_max_round_trip_break_even_hold_windows=10.0,
    )
    paper_trade = PaperTradeEntry(
        entry_id=12,
        created_at=datetime(2026, 4, 3, 0, 0, tzinfo=UTC),
        intent=FundingPairTradeIntent(
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            source_recorded_at=datetime(2026, 4, 2, 23, 59, tzinfo=UTC),
            one_day_net_edge_after_entry=0.0012,
            break_even_days_entry=0.2,
            capacity_limit_notional=300.0,
            target_notional=25.0,
            capacity_fraction=25.0 / 300.0,
            max_target_notional=25.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="JUP-USD-PERP",
                fee_profile="pro_fastfills",
                side="buy",
                target_notional=25.0,
            ),
            short_leg=TradeLegIntent(
                venue="extended",
                symbol="JUP-USD",
                fee_profile="default",
                side="sell",
                target_notional=25.0,
            ),
        ),
    )
    snapshot = ApprovedCanarySnapshot(
        captured_at=datetime(2026, 4, 4, 0, 30, tzinfo=UTC),
        label="jup_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
            opportunity=FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="JUP-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro_fastfills",
                    short_fee_profile="default",
                    gross_daily_edge=0.0014,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=0.0010,
                    one_day_net_edge_after_round_trip=0.0008,
                    break_even_days_entry=0.1,
                    break_even_days_round_trip=0.4,
                    capacity=CapacityEstimate(
                        short_bid_notional=500.0,
                        long_ask_notional=400.0,
                        max_entry_notional=400.0,
                        limiting_venue="paradex",
                    ),
                ),
                venue_markets={
                    "extended": FundingUniverseVenueMarket(venue="extended", symbol="JUP-USD"),
                    "paradex": FundingUniverseVenueMarket(
                        venue="paradex",
                        symbol="JUP-USD-PERP",
                    ),
                },
                deployable_notional=400.0,
                estimated_one_day_pnl_after_round_trip=0.3,
            ),
            suggested_canary_notional=25.0,
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 4, 3, 23, 59, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        ),
    )

    reason = _build_open_hedge_auto_close_reason(
        paper_trade=paper_trade,
        opened_at=datetime(2026, 4, 3, 1, 30, tzinfo=UTC),
        snapshot=snapshot,
        settings=settings,
        now=datetime(2026, 4, 3, 17, 0, tzinfo=UTC),
    )

    assert reason is None

def test_maybe_auto_close_open_hedged_execution_shadow_mode_logs_without_closing(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_shadow_mode=True,
    )
    approval_store = ApprovedCanaryStore(settings.database_path)
    approved_snapshot = approval_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            label="near_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
                opportunity=FundingUniverseOpportunity(
                    opportunity=FundingArbOpportunity(
                        canonical_symbol="NEAR-USD-PERP",
                        long_venue="paradex",
                        short_venue="extended",
                        long_fee_profile="pro_fastfills",
                        short_fee_profile="default",
                        gross_daily_edge=0.0003,
                        entry_cost_rate=0.0003,
                        round_trip_cost_rate=0.0006,
                        one_day_net_edge_after_entry=0.0002,
                        one_day_net_edge_after_round_trip=-0.0001,
                        break_even_days_entry=0.4,
                        break_even_days_round_trip=0.8,
                        capacity=CapacityEstimate(
                            short_bid_notional=500.0,
                            long_ask_notional=400.0,
                            max_entry_notional=400.0,
                            limiting_venue="paradex",
                        ),
                    ),
                    venue_markets={
                        "extended": FundingUniverseVenueMarket(
                            venue="extended",
                            symbol="NEAR-USD",
                        ),
                        "paradex": FundingUniverseVenueMarket(
                            venue="paradex",
                            symbol="NEAR-USD-PERP",
                        ),
                    },
                    deployable_notional=400.0,
                    estimated_one_day_pnl_after_round_trip=-0.04,
                ),
                suggested_canary_notional=25.0,
            ),
            approval=RouteApprovalEntry(
                updated_at=datetime(2026, 4, 4, 9, 59, tzinfo=UTC),
                label="near_extended_paradex",
                canonical_symbol="NEAR-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_fee_profile="default",
                long_fee_profile="pro_fastfills",
                approved=True,
                max_live_notional=25.0,
                note="approved canary",
            ),
        )
    )
    execution = ExecutionJournalEntry(
        executed_at=datetime(2026, 4, 4, 9, 30, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=11,
        preview_hash="preview-hash",
        confirmation_entry_id=9,
        paper_trade=PaperTradeEntry(
            entry_id=11,
            created_at=datetime(2026, 4, 4, 9, 26, tzinfo=UTC),
            note="open hedge",
            intent=FundingPairTradeIntent(
                label="near_extended_paradex",
                canonical_symbol="NEAR-USD-PERP",
                source_recorded_at=datetime(2026, 4, 4, 9, 25, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0015,
                break_even_days_entry=0.2,
                capacity_limit_notional=315.0,
                target_notional=25.0,
                capacity_fraction=25.0 / 315.0,
                max_target_notional=25.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="NEAR-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=25.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="NEAR-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=25.0,
                ),
            ),
        ),
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="NEAR-USD",
                fee_profile="default",
                side="sell",
                target_notional=25.0,
                status="submitted",
                simulated=False,
                external_reference="ext-1",
            )
        ],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=1,
        paper_trade_id=11,
        preview_hash="preview-hash",
        derived_state="hedged",
        recommended_action="monitor_open_hedge",
        order_state=ExecutionOrderState(
            execution_entry_id=1,
            paper_trade_id=11,
            preview_hash="preview-hash",
            legs=[],
            notes=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=1,
            paper_trade_id=11,
            preview_hash="preview-hash",
            status="accepted",
            recommended_action="no_action",
            matched_all_leg_symbols=True,
            venues=[],
            notes=[],
        ),
        notes=[],
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        async def unexpected_live_revalidation(**_: object) -> tuple[object, int]:
            raise AssertionError("fresh approved snapshots must not trigger live revalidation")

        monkeypatch.setattr(
            "carryme_worker.poller.scan_live_route_candidate_for_approval",
            unexpected_live_revalidation,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("shadow mode should not build a close context")
            ),
        )
        caplog.set_level(logging.INFO)
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=pair_status,
                approved_store=approval_store,
                execution_store=ExecutionJournalStore(settings.database_path),
                observation_store=ExecutionObservationStore(settings.database_path),
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                logger=logging.getLogger("carryme.worker"),
                now=approved_snapshot.captured_at,
            )
        )

    assert result is None
    assert "shadow auto-close for paper_trade_id=11" in caplog.text
    assert "source=approved_snapshot" in caplog.text


def test_maybe_auto_close_open_hedged_execution_shadow_mode_revalidates_stale_snapshot(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_shadow_mode=True,
        execution_auto_pair_close_max_snapshot_age_seconds=300,
    )
    approval_store = ApprovedCanaryStore(settings.database_path)
    approval_store.append(
        _build_auto_close_snapshot(
            captured_at=datetime(2026, 4, 4, 8, 0, tzinfo=UTC),
        )
    )
    route_approval_service = RouteApprovalService(store=RouteApprovalStore(settings.database_path))
    route_approval_service.store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        )
    )
    execution = ExecutionJournalEntry(
        executed_at=datetime(2026, 4, 4, 9, 30, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=18,
        preview_hash="stale-preview-hash",
        confirmation_entry_id=10,
        paper_trade=_build_auto_close_paper_trade(
            entry_id=18,
            created_at=datetime(2026, 4, 4, 9, 26, tzinfo=UTC),
        ),
        legs=[_build_auto_close_execution_leg()],
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        async def fake_live_revalidation(
            **_: object,
        ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
            return (
                _build_auto_close_snapshot(
                    captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
                    round_trip_edge=-0.0001,
                ).candidate,
                1,
            )

        monkeypatch.setattr(
            "carryme_worker.poller.scan_live_route_candidate_for_approval",
            fake_live_revalidation,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("shadow mode should not build a close context")
            ),
        )
        caplog.set_level(logging.INFO)
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=_build_auto_close_pair_status(execution=execution),
                approved_store=approval_store,
                execution_store=ExecutionJournalStore(settings.database_path),
                observation_store=ExecutionObservationStore(settings.database_path),
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                approval_service=route_approval_service,
                scanner=cast(Any, object()),
                logger=logging.getLogger("carryme.worker"),
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert result is None
    assert "shadow auto-close for paper_trade_id=18" in caplog.text
    assert "source=live_revalidation" in caplog.text


def test_maybe_auto_close_open_hedged_execution_skips_when_stale_snapshot_cannot_revalidate(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=True,
        execution_auto_pair_close_max_snapshot_age_seconds=300,
    )
    approval_store = ApprovedCanaryStore(settings.database_path)
    approval_store.append(
        _build_auto_close_snapshot(
            captured_at=datetime(2026, 4, 4, 8, 0, tzinfo=UTC),
        )
    )
    route_approval_service = RouteApprovalService(store=RouteApprovalStore(settings.database_path))
    route_approval_service.store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
            label="jup_extended_paradex",
            canonical_symbol="JUP-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="approved canary",
        )
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=19,
            preview_hash="missing-route-preview",
            confirmation_entry_id=22,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=19,
                created_at=datetime(2026, 4, 4, 7, 0, tzinfo=UTC),
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        async def missing_live_revalidation(
            **_: object,
        ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
            return None, 0

        monkeypatch.setattr(
            "carryme_worker.poller.scan_live_route_candidate_for_approval",
            missing_live_revalidation,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("failed revalidation must not build pair-close context")
            ),
        )
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=_build_auto_close_pair_status(execution=execution),
                approved_store=approval_store,
                execution_store=execution_store,
                observation_store=observation_store,
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                approval_service=route_approval_service,
                scanner=cast(Any, object()),
                logger=logging.getLogger("test"),
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert result is None


def test_maybe_auto_close_open_hedged_execution_revalidates_without_active_approval(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_shadow_mode=True,
        execution_auto_pair_close_max_snapshot_age_seconds=300,
    )
    approval_store = ApprovedCanaryStore(settings.database_path)
    approval_store.append(
        _build_auto_close_snapshot(
            captured_at=datetime(2026, 4, 4, 8, 0, tzinfo=UTC),
        )
    )
    execution = ExecutionJournalEntry(
        executed_at=datetime(2026, 4, 4, 9, 30, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=20,
        preview_hash="synthetic-approval-preview",
        confirmation_entry_id=23,
        paper_trade=_build_auto_close_paper_trade(
            entry_id=20,
            created_at=datetime(2026, 4, 4, 9, 26, tzinfo=UTC),
        ),
        legs=[_build_auto_close_execution_leg()],
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        async def fake_live_revalidation(
            **_: object,
        ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
            return (
                _build_auto_close_snapshot(
                    captured_at=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
                    round_trip_edge=-0.0001,
                ).candidate,
                1,
            )

        monkeypatch.setattr(
            "carryme_worker.poller.scan_live_route_candidate_for_approval",
            fake_live_revalidation,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            lambda **_: (_ for _ in ()).throw(
                AssertionError("shadow mode should not build a close context")
            ),
        )
        caplog.set_level(logging.INFO)
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=_build_auto_close_pair_status(execution=execution),
                approved_store=approval_store,
                execution_store=ExecutionJournalStore(settings.database_path),
                observation_store=ExecutionObservationStore(settings.database_path),
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                approval_service=RouteApprovalService(
                    store=RouteApprovalStore(settings.database_path)
                ),
                scanner=cast(Any, object()),
                logger=logging.getLogger("carryme.worker"),
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert result is None
    assert "shadow auto-close for paper_trade_id=20" in caplog.text
    assert "source=live_revalidation" in caplog.text


def test_run_supervised_stable_canary_launch_loop_honors_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_interval_seconds=6,
    )
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    summaries = iter(
        [
            StableCanaryLaunchSummary(
                status="launched",
                label="arb_extended_paradex",
                launch_ready_snapshot_id=9,
                approved_snapshot_id=8,
                paper_trade_id=17,
                final_pair_state="hedged",
                database_path=settings.database_path,
            ),
            StableCanaryLaunchSummary(
                status="skipped",
                detail="No launch-ready canary snapshot found",
                database_path=settings.database_path,
            ),
        ]
    )

    async def fake_launch_latest_stable_canary_once(
        settings_arg: WorkerSettings,
        *,
        api_settings: object | None = None,
        launch_store: object | None = None,
        now: datetime | None = None,
    ) -> StableCanaryLaunchSummary:
        assert settings_arg is settings
        _ = api_settings
        _ = launch_store
        _ = now
        return next(summaries)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.launch_latest_stable_canary_once",
            fake_launch_latest_stable_canary_once,
        )
        summary = asyncio.run(
            run_supervised_stable_canary_launch_loop(
                settings,
                launch_store=StableCanaryLaunchStore(settings.database_path),
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.launched == 1
    assert summary.skipped == 1
    assert sleeps == [6]


def test_run_production_supervisor_cycle_once_orders_stages(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))
    calls: list[str] = []
    expected_now = datetime(2026, 3, 30, 11, 0, tzinfo=UTC)
    forwarded_now: list[datetime | None] = []

    class StubSystemNotifier:
        async def notify(self, event: SystemStateAlertEvent) -> None:
            _ = event

    class StubApprovedNotifier:
        async def notify(self, event: ApprovedCanaryAlertEvent) -> None:
            _ = event

    class StubStableNotifier:
        async def notify(self, event: StableLaunchReadyAlertEvent) -> None:
            _ = event

    class StubExecutionNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            _ = event
            return 1

    system_notifier: SystemStateAlertNotifier = StubSystemNotifier()
    approved_notifier: ApprovedCanaryAlertNotifier = StubApprovedNotifier()
    stable_notifier: StableLaunchReadyAlertNotifier = StubStableNotifier()
    execution_notifier: ExecutionAlertNotifier = StubExecutionNotifier()

    async def fake_observe_system_state_once(
        settings_arg: WorkerSettings,
        *,
        service: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> SystemStateObservationSummary:
        assert settings_arg is settings
        _ = service
        _ = alert_sink
        assert alert_notifier is system_notifier
        _ = logger
        forwarded_now.append(now)
        calls.append("system")
        return SystemStateObservationSummary(
            checked_venues=3,
            degraded_venues=0,
            saved_alerts=0,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    async def fake_scan_approved_canary_once(
        settings_arg: WorkerSettings,
        *,
        scanner: object | None = None,
        approval_service: object | None = None,
        store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ApprovedCanaryScanSummary:
        assert settings_arg is settings
        _ = scanner
        _ = approval_service
        _ = store
        _ = alert_sink
        assert alert_notifier is approved_notifier
        _ = logger
        forwarded_now.append(now)
        calls.append("approved")
        return ApprovedCanaryScanSummary(
            scanned_candidates=4,
            approved_candidates=1,
            saved_snapshots=1,
            alert_events=0,
            sent_notifications=2,
            database_path=settings.database_path,
        )

    async def fake_cache_launch_ready_canaries_once(
        settings_arg: WorkerSettings,
        *,
        approved_store: object | None = None,
        launch_ready_store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        approval_service: object | None = None,
        system_state_service: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> LaunchReadyCanaryCacheSummary:
        assert settings_arg is settings
        _ = approved_store
        _ = launch_ready_store
        _ = alert_sink
        assert alert_notifier is stable_notifier
        _ = approval_service
        _ = system_state_service
        _ = logger
        forwarded_now.append(now)
        calls.append("launch_ready")
        return LaunchReadyCanaryCacheSummary(
            scanned_snapshots=1,
            launch_ready_candidates=1,
            saved_snapshots=1,
            alert_events=0,
            sent_notifications=3,
            database_path=settings.database_path,
        )

    async def fake_launch_latest_stable_canary_once(
        settings_arg: WorkerSettings,
        *,
        api_settings: object | None = None,
        launch_store: object | None = None,
        now: datetime | None = None,
    ) -> StableCanaryLaunchSummary:
        assert settings_arg is settings
        _ = api_settings
        _ = launch_store
        forwarded_now.append(now)
        calls.append("launch")
        return StableCanaryLaunchSummary(
            status="launched",
            label="arb_extended_paradex",
            launch_ready_snapshot_id=9,
            approved_snapshot_id=8,
            paper_trade_id=17,
            final_pair_state="hedged",
            database_path=settings.database_path,
        )

    async def fake_observe_live_executions_once(
        settings_arg: WorkerSettings,
        *,
        execution_store: object | None = None,
        observation_store: object | None = None,
        approved_store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        _ = execution_store
        _ = observation_store
        _ = approved_store
        _ = alert_sink
        assert alert_notifier is execution_notifier
        _ = account_service
        _ = order_state_service
        _ = logger
        forwarded_now.append(now)
        calls.append("observe")
        return ExecutionObservationSummary(
            scanned_executions=1,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=0,
            sent_notifications=4,
            database_path=settings.database_path,
        )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.observe_system_state_once",
            fake_observe_system_state_once,
        )
        monkeypatch.setattr(
            "carryme_worker.poller.scan_approved_canary_once",
            fake_scan_approved_canary_once,
        )
        monkeypatch.setattr(
            "carryme_worker.poller.cache_launch_ready_canaries_once",
            fake_cache_launch_ready_canaries_once,
        )
        monkeypatch.setattr(
            "carryme_worker.poller.launch_latest_stable_canary_once",
            fake_launch_latest_stable_canary_once,
        )
        monkeypatch.setattr(
            "carryme_worker.poller.observe_live_executions_once",
            fake_observe_live_executions_once,
        )
        summary = asyncio.run(
            run_production_supervisor_cycle_once(
                settings,
                system_state_alert_notifier=system_notifier,
                approved_canary_alert_notifier=approved_notifier,
                stable_launch_ready_alert_notifier=stable_notifier,
                execution_alert_notifier=execution_notifier,
                now=expected_now,
            )
        )

    assert calls == ["system", "approved", "launch_ready", "launch", "observe"]
    assert forwarded_now == [
        expected_now,
        expected_now,
        expected_now,
        expected_now,
        expected_now,
    ]
    assert summary.launch_status == "launched"
    assert summary.paper_trade_id == 17
    assert summary.observed_executions == 1
    assert summary.sent_notifications == 10


def test_run_supervised_production_supervisor_loop_honors_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_interval_seconds=7,
    )
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    summaries = iter(
        [
            ProductionSupervisorCycleSummary(
                checked_venues=3,
                degraded_venues=0,
                scanned_candidates=4,
                approved_candidates=1,
                saved_approved_snapshots=1,
                scanned_launch_ready_snapshots=1,
                launch_ready_candidates=1,
                saved_launch_ready_snapshots=1,
                launch_status="launched",
                paper_trade_id=17,
                final_pair_state="hedged",
                observed_executions=1,
                saved_execution_observations=1,
                execution_alerts=0,
                sent_notifications=4,
                database_path=settings.database_path,
            ),
            ProductionSupervisorCycleSummary(
                checked_venues=3,
                degraded_venues=0,
                scanned_candidates=0,
                approved_candidates=0,
                saved_approved_snapshots=0,
                scanned_launch_ready_snapshots=0,
                launch_ready_candidates=0,
                saved_launch_ready_snapshots=0,
                launch_status="skipped",
                paper_trade_id=None,
                final_pair_state=None,
                observed_executions=0,
                saved_execution_observations=0,
                execution_alerts=0,
                sent_notifications=0,
                database_path=settings.database_path,
            ),
        ]
    )

    async def fake_run_production_supervisor_cycle_once(
        settings_arg: WorkerSettings,
        *,
        system_state_alert_notifier: object | None = None,
        approved_canary_alert_notifier: object | None = None,
        stable_launch_ready_alert_notifier: object | None = None,
        execution_alert_notifier: object | None = None,
        now: datetime | None = None,
        logger: object | None = None,
    ) -> ProductionSupervisorCycleSummary:
        assert settings_arg is settings
        _ = system_state_alert_notifier
        _ = approved_canary_alert_notifier
        _ = stable_launch_ready_alert_notifier
        _ = execution_alert_notifier
        _ = now
        _ = logger
        return next(summaries)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.run_production_supervisor_cycle_once",
            fake_run_production_supervisor_cycle_once,
        )
        summary = asyncio.run(
            run_supervised_production_supervisor_loop(
                settings,
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.launched == 1
    assert summary.skipped == 1
    assert summary.observed_executions == 1
    assert summary.execution_alerts == 0
    assert summary.sent_notifications == 4
    assert sleeps == [7]


def test_run_supervised_production_supervisor_loop_rejects_non_positive_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
    )

    with pytest.raises(ValueError, match="max_iterations must be at least 1"):
        asyncio.run(
            run_supervised_production_supervisor_loop(
                settings,
                max_iterations=0,
            )
        )


def test_run_supervised_production_supervisor_loop_applies_backoff(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        stable_canary_launch_interval_seconds=3,
        stable_canary_launch_max_backoff_seconds=8,
    )
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    outcomes: list[ProductionSupervisorCycleSummary | Exception] = [
        RuntimeError("temporary supervisor failure"),
        RuntimeError("temporary supervisor failure #2"),
        ProductionSupervisorCycleSummary(
            checked_venues=3,
            degraded_venues=0,
            scanned_candidates=4,
            approved_candidates=1,
            saved_approved_snapshots=1,
            scanned_launch_ready_snapshots=1,
            launch_ready_candidates=1,
            saved_launch_ready_snapshots=1,
            launch_status="launched",
            paper_trade_id=17,
            final_pair_state="hedged",
            observed_executions=1,
            saved_execution_observations=1,
            execution_alerts=0,
            sent_notifications=5,
            database_path=settings.database_path,
        ),
    ]

    async def fake_run_production_supervisor_cycle_once(
        settings_arg: WorkerSettings,
        *,
        system_state_alert_notifier: object | None = None,
        approved_canary_alert_notifier: object | None = None,
        stable_launch_ready_alert_notifier: object | None = None,
        execution_alert_notifier: object | None = None,
        logger: object | None = None,
    ) -> ProductionSupervisorCycleSummary:
        assert settings_arg is settings
        _ = system_state_alert_notifier
        _ = approved_canary_alert_notifier
        _ = stable_launch_ready_alert_notifier
        _ = execution_alert_notifier
        _ = logger
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.run_production_supervisor_cycle_once",
            fake_run_production_supervisor_cycle_once,
        )
        summary = asyncio.run(
            run_supervised_production_supervisor_loop(
                settings,
                sleep=fake_sleep,
                max_iterations=3,
            )
        )

    assert summary.attempts == 3
    assert summary.successful_cycles == 1
    assert summary.failures == 2
    assert summary.launched == 1
    assert summary.skipped == 0
    assert summary.observed_executions == 1
    assert summary.execution_alerts == 0
    assert summary.sent_notifications == 5
    assert sleeps == [3.0, 6.0]


def test_run_supervised_production_supervisor_loop_builds_and_reuses_stage_notifiers(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
    )
    system_notifier = object()
    approved_notifier = object()
    stable_notifier = object()
    execution_notifier = object()
    builder_calls = {
        "system": 0,
        "approved": 0,
        "stable": 0,
        "execution": 0,
    }
    cycle_calls = 0

    async def fake_sleep(seconds: float) -> None:
        _ = seconds

    def build_system_notifier(
        settings_arg: WorkerSettings, *, logger: object | None = None
    ) -> object:
        assert settings_arg is settings
        _ = logger
        builder_calls["system"] += 1
        return system_notifier

    def build_approved_notifier(
        settings_arg: WorkerSettings, *, logger: object | None = None
    ) -> object:
        assert settings_arg is settings
        _ = logger
        builder_calls["approved"] += 1
        return approved_notifier

    def build_stable_notifier(
        settings_arg: WorkerSettings, *, logger: object | None = None
    ) -> object:
        assert settings_arg is settings
        _ = logger
        builder_calls["stable"] += 1
        return stable_notifier

    def build_execution_notifier(
        settings_arg: WorkerSettings, *, logger: object | None = None
    ) -> object:
        assert settings_arg is settings
        _ = logger
        builder_calls["execution"] += 1
        return execution_notifier

    async def fake_run_production_supervisor_cycle_once(
        settings_arg: WorkerSettings,
        *,
        system_state_alert_notifier: object | None = None,
        approved_canary_alert_notifier: object | None = None,
        stable_launch_ready_alert_notifier: object | None = None,
        execution_alert_notifier: object | None = None,
        now: datetime | None = None,
        logger: object | None = None,
    ) -> ProductionSupervisorCycleSummary:
        nonlocal cycle_calls
        assert settings_arg is settings
        assert system_state_alert_notifier is system_notifier
        assert approved_canary_alert_notifier is approved_notifier
        assert stable_launch_ready_alert_notifier is stable_notifier
        assert execution_alert_notifier is execution_notifier
        assert now is None
        _ = logger
        cycle_calls += 1
        return ProductionSupervisorCycleSummary(
            checked_venues=1,
            degraded_venues=0,
            scanned_candidates=1,
            approved_candidates=1,
            saved_approved_snapshots=1,
            scanned_launch_ready_snapshots=1,
            launch_ready_candidates=1,
            saved_launch_ready_snapshots=1,
            launch_status="skipped",
            paper_trade_id=None,
            final_pair_state=None,
            observed_executions=0,
            saved_execution_observations=0,
            execution_alerts=0,
            sent_notifications=4,
            database_path=settings.database_path,
        )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.notifications.build_system_state_alert_notifier",
            build_system_notifier,
        )
        monkeypatch.setattr(
            "carryme_worker.notifications.build_approved_canary_alert_notifier",
            build_approved_notifier,
        )
        monkeypatch.setattr(
            "carryme_worker.notifications.build_stable_launch_ready_alert_notifier",
            build_stable_notifier,
        )
        monkeypatch.setattr(
            "carryme_worker.notifications.build_execution_alert_notifier",
            build_execution_notifier,
        )
        monkeypatch.setattr(
            "carryme_worker.poller.run_production_supervisor_cycle_once",
            fake_run_production_supervisor_cycle_once,
        )
        summary = asyncio.run(
            run_supervised_production_supervisor_loop(
                settings,
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.skipped == 2
    assert summary.sent_notifications == 8
    assert cycle_calls == 2
    assert builder_calls == {
        "system": 1,
        "approved": 1,
        "stable": 1,
        "execution": 1,
    }


def test_run_supervised_approved_canary_scan_loop_honors_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_interval_seconds=3,
    )

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def fake_scan_approved_canary_once(
        settings_arg: WorkerSettings,
        *,
        scanner: object | None = None,
        approval_service: object | None = None,
        store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ApprovedCanaryScanSummary:
        assert settings_arg is settings
        _ = scanner
        _ = approval_service
        _ = store
        _ = alert_sink
        _ = alert_notifier
        _ = logger
        assert now is None
        calls.append(1)
        return ApprovedCanaryScanSummary(
            scanned_candidates=2,
            approved_candidates=1,
            saved_snapshots=1,
            alert_events=1,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    calls: list[int] = []
    sleeps: list[float] = []

    from unittest.mock import patch

    with patch(
        "carryme_worker.poller.scan_approved_canary_once",
        side_effect=fake_scan_approved_canary_once,
    ):
        summary = asyncio.run(
            run_supervised_approved_canary_scan_loop(
                settings,
                store=ApprovedCanaryStore(settings.database_path),
                alert_sink=ApprovedCanaryAlertStore(settings.database_path),
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.scanned_candidates == 4
    assert summary.approved_candidates == 2
    assert summary.saved_snapshots == 2
    assert summary.alert_events == 2
    assert summary.sent_notifications == 2
    assert calls == [1, 1]
    assert sleeps == [3.0]


def test_run_supervised_approved_canary_scan_loop_reuses_shared_scanner_across_cycles(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        approved_canary_scan_interval_seconds=1,
    )

    class StubScanner:
        pass

    shared_scanner = StubScanner()
    build_calls: list[int] = []
    seen_scanners: list[object] = []

    async def fake_sleep(seconds: float) -> None:
        _ = seconds

    async def fake_scan_approved_canary_once(
        settings_arg: WorkerSettings,
        *,
        scanner: object | None = None,
        approval_service: object | None = None,
        store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ApprovedCanaryScanSummary:
        assert settings_arg is settings
        assert approval_service is not None
        assert store is not None
        assert alert_sink is not None
        _ = alert_notifier
        _ = logger
        assert now is None
        assert scanner is not None
        seen_scanners.append(scanner)
        return ApprovedCanaryScanSummary(
            scanned_candidates=0,
            approved_candidates=0,
            saved_snapshots=0,
            alert_events=0,
            sent_notifications=0,
            database_path=settings.database_path,
        )

    def fake_build_worker_universe_scanner(
        settings_arg: WorkerSettings,
        *,
        history_store: object | None = None,
    ) -> StubScanner:
        assert settings_arg is settings
        assert history_store is not None
        build_calls.append(1)
        return shared_scanner

    from unittest.mock import patch

    with (
        patch(
            "carryme_worker.poller.scan_approved_canary_once",
            side_effect=fake_scan_approved_canary_once,
        ),
        patch(
            "carryme_worker.poller._build_worker_universe_scanner",
            side_effect=fake_build_worker_universe_scanner,
        ),
    ):
        summary = asyncio.run(
            run_supervised_approved_canary_scan_loop(
                settings,
                store=ApprovedCanaryStore(settings.database_path),
                alert_sink=ApprovedCanaryAlertStore(settings.database_path),
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert build_calls == [1]
    assert seen_scanners == [shared_scanner, shared_scanner]


def test_observe_system_state_once_emits_and_notifies_alerts(tmp_path: Path) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_bearer_token="token",
    )

    class StubSystemStateService:
        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            assert "paradex" in configs
            return [
                VenueSystemState(
                    venue="extended",
                    enabled=False,
                    checked=False,
                    healthy=True,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=False,
                    status="maintenance",
                    blocking_reasons=["Paradex system state is maintenance"],
                ),
                VenueSystemState(
                    venue="hyperliquid",
                    enabled=False,
                    checked=False,
                    healthy=True,
                ),
            ]

    notified: list[str] = []

    class StubNotifier:
        async def notify(self, event: SystemStateAlertEvent) -> None:
            notified.append(event.alert_type)

    summary = asyncio.run(
        observe_system_state_once(
            settings,
            service=cast(SystemStateService, StubSystemStateService()),
            alert_sink=SystemStateAlertStore(settings.database_path),
            alert_notifier=StubNotifier(),
            now=datetime(2026, 3, 29, 20, 7, tzinfo=UTC),
        )
    )

    alerts = SystemStateAlertStore(settings.database_path).list_recent(limit=10, venue="paradex")

    assert summary.checked_venues == 1
    assert summary.degraded_venues == 1
    assert summary.saved_alerts == 1
    assert summary.sent_notifications == 1
    assert len(alerts) == 1
    assert alerts[0].alert_type == "venue_degraded"
    assert notified == ["venue_degraded"]


def test_observe_system_state_once_times_out_stuck_notifier(tmp_path: Path) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        system_state_alert_webhook_timeout_seconds=0.01,
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_bearer_token="token",
    )

    class StubSystemStateService:
        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            assert "paradex" in configs
            return [
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=False,
                    status="maintenance",
                    blocking_reasons=["Paradex system state is maintenance"],
                )
            ]

    class HangingNotifier:
        async def notify(self, event: SystemStateAlertEvent) -> None:
            _ = event
            await asyncio.Event().wait()

    summary = asyncio.run(
        observe_system_state_once(
            settings,
            service=cast(SystemStateService, StubSystemStateService()),
            alert_sink=SystemStateAlertStore(settings.database_path),
            alert_notifier=HangingNotifier(),
            now=datetime(2026, 3, 29, 20, 7, tzinfo=UTC),
        )
    )

    alerts = SystemStateAlertStore(settings.database_path).list_recent(limit=10, venue="paradex")

    assert summary.checked_venues == 1
    assert summary.degraded_venues == 1
    assert summary.saved_alerts == 1
    assert summary.sent_notifications == 0
    assert len(alerts) == 1
    assert alerts[0].alert_type == "venue_degraded"


def test_run_supervised_system_state_observation_loop_honors_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        system_state_observation_interval_seconds=3,
    )

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def fake_observe_system_state_once(
        settings_arg: WorkerSettings,
        *,
        service: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> SystemStateObservationSummary:
        assert settings_arg is settings
        _ = service
        _ = alert_sink
        _ = alert_notifier
        _ = logger
        assert now is None
        calls.append(1)
        return SystemStateObservationSummary(
            checked_venues=1,
            degraded_venues=1,
            saved_alerts=1,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    calls: list[int] = []
    sleeps: list[float] = []

    from unittest.mock import patch

    with patch(
        "carryme_worker.poller.observe_system_state_once",
        side_effect=fake_observe_system_state_once,
    ):
        summary = asyncio.run(
            run_supervised_system_state_observation_loop(
                settings,
                alert_sink=SystemStateAlertStore(settings.database_path),
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.checked_venues == 2
    assert summary.degraded_venues == 2
    assert summary.saved_alerts == 2
    assert summary.sent_notifications == 2
    assert calls == [1, 1]
    assert sleeps == [3.0]


def test_run_supervised_universe_scan_loop_honors_max_iterations(tmp_path: Path) -> None:
    class StubUniverseScanner:
        async def scan(self, **_: object) -> FundingUniverseScan:
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                ranking="route_adjusted_quality_pnl",
                target_notional=5000.0,
                overlap_count=1,
                overlaps=[],
                opportunities=[
                    FundingUniverseOpportunity(
                        opportunity=FundingArbOpportunity(
                            canonical_symbol="ARB-USD-PERP",
                            long_venue="paradex",
                            short_venue="extended",
                            long_fee_profile="pro",
                            short_fee_profile="default",
                            gross_daily_edge=0.004,
                            entry_cost_rate=0.00045,
                            round_trip_cost_rate=0.0009,
                            one_day_net_edge_after_entry=0.00355,
                            one_day_net_edge_after_round_trip=0.0031,
                            break_even_days_entry=0.2,
                            break_even_days_round_trip=0.3,
                            capacity=CapacityEstimate(
                                short_bid_notional=1400.0,
                                long_ask_notional=900.0,
                                max_entry_notional=900.0,
                                limiting_venue="paradex",
                            ),
                        ),
                        venue_markets={
                            "extended": FundingUniverseVenueMarket(
                                venue="extended",
                                symbol="ARB-USD",
                                mark_price=0.091,
                                daily_funding_rate=0.000312,
                                open_interest=200_000,
                                daily_volume=150_000,
                                bid_notional=1400.0,
                                ask_notional=1600.0,
                            ),
                            "paradex": FundingUniverseVenueMarket(
                                venue="paradex",
                                symbol="ARB-USD-PERP",
                                mark_price=0.0911,
                                daily_funding_rate=-0.0037,
                                open_interest=180_000,
                                daily_volume=140_000,
                                bid_notional=1200.0,
                                ask_notional=900.0,
                            ),
                        },
                        target_notional=5000.0,
                        deployable_notional=900.0,
                        estimated_one_day_pnl_after_entry=3.195,
                        estimated_one_day_pnl_after_round_trip=2.79,
                        quality_score=1.7,
                    )
                ],
            )

    async def fake_sleep(_: float) -> None:
        return None

    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        universe_scan_interval_seconds=1,
        min_candidate_entry_edge=0.001,
        min_candidate_capacity_notional=500.0,
    )

    summary = asyncio.run(
        run_supervised_universe_scan_loop(
            settings,
            scanner=StubUniverseScanner(),
            store=OpportunityHistoryStore(settings.database_path),
            sleep=fake_sleep,
            max_iterations=2,
        )
    )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.overlap_count == 2
    assert summary.scanned_opportunities == 2
    assert summary.saved_records == 2
    assert summary.alert_events == 2


def test_run_supervised_universe_scan_loop_rejects_non_positive_max_iterations(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(database_path=str(tmp_path / "history.sqlite3"))

    with pytest.raises(ValueError, match="max_iterations must be at least 1"):
        asyncio.run(run_supervised_universe_scan_loop(settings, max_iterations=0))


def test_run_supervised_universe_scan_loop_applies_backoff(tmp_path: Path) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        universe_scan_interval_seconds=2,
        universe_scan_max_backoff_seconds=5,
    )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    outcomes: list[UniverseScanSummary | Exception] = [
        RuntimeError("temporary universe failure"),
        UniverseScanSummary(
            overlap_count=1,
            scanned_opportunities=2,
            saved_records=2,
            alert_events=1,
            database_path=settings.database_path,
        ),
    ]

    async def fake_scan_funding_universe_once(
        settings_arg: WorkerSettings,
        *,
        scanner: object | None = None,
        store: object | None = None,
        alert_sink: object | None = None,
        now: datetime | None = None,
    ) -> UniverseScanSummary:
        assert settings_arg is settings
        _ = scanner
        _ = store
        _ = alert_sink
        assert now is None
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    from unittest.mock import patch

    with patch(
        "carryme_worker.poller.scan_funding_universe_once",
        side_effect=fake_scan_funding_universe_once,
    ):
        summary = asyncio.run(
            run_supervised_universe_scan_loop(
                settings,
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 1
    assert summary.failures == 1
    assert summary.overlap_count == 1
    assert summary.scanned_opportunities == 2
    assert summary.saved_records == 2
    assert summary.alert_events == 1
    assert sleeps == [2.0]


def test_run_supervised_universe_scan_loop_treats_connector_429_as_recoverable(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = WorkerSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        universe_scan_interval_seconds=2,
        universe_scan_max_backoff_seconds=5,
    )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    outcomes: list[UniverseScanSummary | Exception] = [
        ConnectorError("hyperliquid request failed with status 429", status_code=429),
        UniverseScanSummary(
            overlap_count=1,
            scanned_opportunities=2,
            saved_records=2,
            alert_events=1,
            database_path=settings.database_path,
        ),
    ]

    async def fake_scan_funding_universe_once(
        settings_arg: WorkerSettings,
        *,
        scanner: object | None = None,
        store: object | None = None,
        alert_sink: object | None = None,
        now: datetime | None = None,
    ) -> UniverseScanSummary:
        assert settings_arg is settings
        _ = scanner
        _ = store
        _ = alert_sink
        assert now is None
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    from unittest.mock import patch

    with (
        caplog.at_level("WARNING"),
        patch(
            "carryme_worker.poller.scan_funding_universe_once",
            side_effect=fake_scan_funding_universe_once,
        ),
    ):
        summary = asyncio.run(
            run_supervised_universe_scan_loop(
                settings,
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 1
    assert summary.failures == 1
    assert summary.overlap_count == 1
    assert summary.scanned_opportunities == 2
    assert summary.saved_records == 2
    assert summary.alert_events == 1
    assert sleeps == [2.0]
    assert "recoverable upstream/storage failure" in caplog.text
    assert "status 429" in caplog.text


def test_run_polling_loop_applies_backoff_and_saves_after_retry(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class FlakyScorer:
        def __init__(self) -> None:
            self.calls = 0

        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary failure")
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        poll_interval_seconds=2,
        max_backoff_seconds=10,
    )
    store = OpportunityHistoryStore(settings.database_path)

    summary = asyncio.run(
        run_polling_loop(
            settings,
            iterations=2,
            scorer=FlakyScorer(),
            store=store,
            sleep=fake_sleep,
        )
    )

    history = store.list_recent()

    assert summary.attempts == 2
    assert summary.successful_cycles == 1
    assert summary.failures == 1
    assert summary.saved_records == 1
    assert sleeps == [2.0]
    assert len(history) == 1


def test_run_polling_loop_resets_backoff_after_success(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class SequencedScorer:
        def __init__(self) -> None:
            self.calls = 0

        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            self.calls += 1
            if self.calls in {1, 3}:
                raise RuntimeError("temporary failure")
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        poll_interval_seconds=2,
        max_backoff_seconds=10,
    )
    store = OpportunityHistoryStore(settings.database_path)

    summary = asyncio.run(
        run_polling_loop(
            settings,
            iterations=4,
            scorer=SequencedScorer(),
            store=store,
            sleep=fake_sleep,
        )
    )

    assert summary.attempts == 4
    assert summary.successful_cycles == 2
    assert summary.failures == 2
    assert summary.saved_records == 2
    assert sleeps == [2.0, 2.0, 2.0]


def test_run_polling_loop_clamps_backoff_to_maximum(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class MostlyFailingScorer:
        def __init__(self) -> None:
            self.calls = 0

        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            self.calls += 1
            if self.calls < 4:
                raise RuntimeError("temporary failure")
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        poll_interval_seconds=2,
        max_backoff_seconds=5,
    )
    store = OpportunityHistoryStore(settings.database_path)

    summary = asyncio.run(
        run_polling_loop(
            settings,
            iterations=4,
            scorer=MostlyFailingScorer(),
            store=store,
            sleep=fake_sleep,
        )
    )

    assert summary.attempts == 4
    assert summary.successful_cycles == 1
    assert summary.failures == 3
    assert summary.saved_records == 1
    assert sleeps == [2.0, 4.0, 5.0]


def test_run_polling_loop_rejects_non_positive_iterations(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    with pytest.raises(ValueError, match="iterations must be at least 1"):
        asyncio.run(run_polling_loop(settings, iterations=0))


def test_observe_live_executions_once_persists_latest_live_snapshots(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    alert_store = ExecutionAlertStore(settings.database_path)
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            )

    summary = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            alert_sink=alert_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
        )
    )

    latest = observation_store.latest_for_paper_trade(7)

    assert summary.scanned_executions == 1
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1
    assert summary.saved_alerts == 0
    assert summary.sent_notifications == 0
    assert latest is not None
    assert latest.context == "worker_execution_monitor"
    assert latest.order_state.legs[0].observation_source == "rest_poll"


def test_observe_live_executions_once_triggers_auto_close_for_hedged_pairs(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=True,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="jup_extended_paradex",
                    canonical_symbol="JUP-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=25.0,
                    capacity_fraction=25.0 / 4500.0,
                    max_target_notional=25.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="JUP-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=25.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="JUP-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=25.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="JUP-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=25.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["JUP-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="bearer_token",
                        position_symbols=["JUP-USD-PERP"],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[],
                notes=[],
            )

    auto_close_calls: list[int] = []

    async def fake_auto_close(**kwargs: object) -> None:
        execution = cast(ExecutionJournalEntry, kwargs["execution"])
        auto_close_calls.append(execution.paper_trade_id or 0)
        return None

    hedged_pair_status = ExecutionPairStatus(
        execution_entry_id=1,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="hedged",
        recommended_action="monitor_open_hedge",
        order_state=ExecutionOrderState(
            execution_entry_id=1,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[],
            notes=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=1,
            paper_trade_id=7,
            preview_hash="preview-hash",
            status="accepted",
            recommended_action="no_action",
            matched_all_leg_symbols=True,
            venues=[],
            notes=[],
        ),
        notes=[],
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.build_execution_pair_status",
            lambda *args, **kwargs: hedged_pair_status,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._maybe_auto_close_open_hedged_execution",
            fake_auto_close,
        )
        summary = asyncio.run(
            observe_live_executions_once(
                settings,
                execution_store=execution_store,
                observation_store=observation_store,
                approved_store=ApprovedCanaryStore(settings.database_path),
                account_service=cast(AccountPreflightService, StubAccountService()),
                order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
                now=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
            )
        )

    assert summary.observed_executions == 1
    assert auto_close_calls == [7]


def test_observe_live_executions_once_captures_funding_checkpoint_for_open_hedge(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=False,
        execution_observation_max_age_seconds=7_200,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    snapshot_store = BalanceSnapshotStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="near_extended_paradex",
                    canonical_symbol="NEAR-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=25.0,
                    capacity_fraction=25.0 / 4500.0,
                    max_target_notional=25.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="NEAR-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=25.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="NEAR-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=25.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="NEAR-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=25.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        total_collateral=99.9,
                        available_to_trade=99.9,
                        free_collateral=99.9,
                        position_symbols=["NEAR-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="bearer_token",
                        total_collateral=199.8,
                        available_to_trade=199.8,
                        free_collateral=199.8,
                        position_symbols=["NEAR-USD-PERP"],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[],
                notes=[],
            )

    hedged_pair_status = ExecutionPairStatus(
        execution_entry_id=1,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="hedged",
        recommended_action="monitor_open_hedge",
        order_state=ExecutionOrderState(
            execution_entry_id=1,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[],
            notes=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=1,
            paper_trade_id=7,
            preview_hash="preview-hash",
            status="accepted",
            recommended_action="no_action",
            matched_all_leg_symbols=True,
            venues=[],
            notes=[],
        ),
        notes=[],
    )

    async def fake_auto_close(**kwargs: object) -> None:
        _ = kwargs
        return None

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.build_execution_pair_status",
            lambda *args, **kwargs: hedged_pair_status,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._maybe_auto_close_open_hedged_execution",
            fake_auto_close,
        )
        asyncio.run(
            observe_live_executions_once(
                settings,
                execution_store=execution_store,
                observation_store=observation_store,
                approved_store=ApprovedCanaryStore(settings.database_path),
                account_service=cast(AccountPreflightService, StubAccountService()),
                order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
                now=datetime(2026, 3, 29, 14, 5, tzinfo=UTC),
            )
        )
        asyncio.run(
            observe_live_executions_once(
                settings,
                execution_store=execution_store,
                observation_store=observation_store,
                approved_store=ApprovedCanaryStore(settings.database_path),
                account_service=cast(AccountPreflightService, StubAccountService()),
                order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
                now=datetime(2026, 3, 29, 14, 6, tzinfo=UTC),
            )
        )

    checkpoints = snapshot_store.list_recent(
        limit=10,
        paper_trade_id=7,
        stage="funding_window_checkpoint",
    )

    assert len(checkpoints) == 2
    assert {snapshot.venue for snapshot in checkpoints} == {"extended", "paradex"}
    assert all(snapshot.note is not None for snapshot in checkpoints)
    assert all("window_index=1" in cast(str, snapshot.note) for snapshot in checkpoints)




def test_observe_live_executions_once_uses_execution_timestamp_for_funding_checkpoint(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=False,
        execution_observation_max_age_seconds=7_200,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    snapshot_store = BalanceSnapshotStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 12, 59, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="near_extended_paradex",
                    canonical_symbol="NEAR-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=25.0,
                    capacity_fraction=25.0 / 4500.0,
                    max_target_notional=25.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="NEAR-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=25.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="NEAR-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=25.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="NEAR-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=25.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        total_collateral=99.9,
                        available_to_trade=99.9,
                        free_collateral=99.9,
                        position_symbols=["NEAR-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="bearer_token",
                        total_collateral=199.8,
                        available_to_trade=199.8,
                        free_collateral=199.8,
                        position_symbols=["NEAR-USD-PERP"],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[],
                notes=[],
            )

    hedged_pair_status = ExecutionPairStatus(
        execution_entry_id=1,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="hedged",
        recommended_action="monitor_open_hedge",
        order_state=ExecutionOrderState(
            execution_entry_id=1,
            paper_trade_id=7,
            preview_hash="preview-hash",
            legs=[],
            notes=[],
        ),
        reconciliation=ExecutionReconciliation(
            execution_entry_id=1,
            paper_trade_id=7,
            preview_hash="preview-hash",
            status="accepted",
            recommended_action="no_action",
            matched_all_leg_symbols=True,
            venues=[],
            notes=[],
        ),
        notes=[],
    )

    async def fake_auto_close(**kwargs: object) -> None:
        _ = kwargs
        return None

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller.build_execution_pair_status",
            lambda *args, **kwargs: hedged_pair_status,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._maybe_auto_close_open_hedged_execution",
            fake_auto_close,
        )
        asyncio.run(
            observe_live_executions_once(
                settings,
                execution_store=execution_store,
                observation_store=observation_store,
                approved_store=ApprovedCanaryStore(settings.database_path),
                account_service=cast(AccountPreflightService, StubAccountService()),
                order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
                now=datetime(2026, 3, 29, 14, 5, tzinfo=UTC),
            )
        )

    checkpoints = snapshot_store.list_recent(
        limit=10,
        paper_trade_id=7,
        stage="funding_window_checkpoint",
    )

    assert len(checkpoints) == 2
    assert all(snapshot.note is not None for snapshot in checkpoints)
    assert all("window_index=1" in cast(str, snapshot.note) for snapshot in checkpoints)

def test_maybe_auto_close_open_hedged_execution_skips_when_policy_disabled(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=False,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=15,
            preview_hash="auto-close-preview",
            confirmation_entry_id=20,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=15,
                created_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )

    class UnexpectedApprovedStore:
        def latest(self, label: str) -> ApprovedCanarySnapshot | None:
            _ = label
            raise AssertionError("policy-disabled auto-close must not query approved snapshots")

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("policy-disabled auto-close must not build pair-close context")
            ),
        )
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=_build_auto_close_pair_status(execution=execution),
                approved_store=cast(ApprovedCanaryStore, UnexpectedApprovedStore()),
                execution_store=execution_store,
                observation_store=observation_store,
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                logger=logging.getLogger("test"),
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert result is None


def test_maybe_auto_close_open_hedged_execution_skips_non_hedged_pair_status(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=True,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=16,
            preview_hash="non-hedged-preview",
            confirmation_entry_id=21,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=16,
                created_at=datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )

    class UnexpectedApprovedStore:
        def latest(self, label: str) -> ApprovedCanarySnapshot | None:
            _ = label
            raise AssertionError("non-hedged auto-close must not query approved snapshots")

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("non-hedged auto-close must not build pair-close context")
            ),
        )
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=_build_auto_close_pair_status(
                    execution=execution,
                    derived_state="cleanup_needed",
                    recommended_action="no_action",
                ),
                approved_store=cast(ApprovedCanaryStore, UnexpectedApprovedStore()),
                execution_store=execution_store,
                observation_store=observation_store,
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                logger=logging.getLogger("test"),
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert result is None


def test_maybe_auto_close_open_hedged_execution_times_out(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_auto_pair_close_enabled=True,
        execution_auto_pair_close_timeout_seconds=0.01,
        execution_auto_pair_close_max_hold_windows=10.0,
        execution_auto_pair_close_max_round_trip_break_even_hold_windows=10.0,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    approved_store = ApprovedCanaryStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=17,
            preview_hash="timeout-preview",
            confirmation_entry_id=22,
            paper_trade=_build_auto_close_paper_trade(
                entry_id=17,
                created_at=datetime(2026, 4, 4, 7, 0, tzinfo=UTC),
            ),
            legs=[_build_auto_close_execution_leg()],
        )
    )
    approved_store.append(
        _build_auto_close_snapshot(
            captured_at=datetime(2026, 4, 4, 10, 0, tzinfo=UTC),
            break_even_days_round_trip=0.2,
        )
    )

    async def slow_pair_close_context(**kwargs: object) -> tuple[object, object, object, object]:
        _ = kwargs
        await asyncio.sleep(0.05)
        raise AssertionError("timeout should fire before pair-close context completes")

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_preview_service_for_candidate",
            lambda *args, **kwargs: object(),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_cleanup_preview_router_for_candidate",
            lambda *args, **kwargs: object(),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_live_execution_coordinator_for_candidate",
            lambda *args, **kwargs: object(),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_cleanup_live_execution_router_for_candidate",
            lambda *args, **kwargs: object(),
        )
        monkeypatch.setattr(
            "carryme_worker.poller._build_pair_close_context_for_paper_trade",
            slow_pair_close_context,
        )
        monkeypatch.setattr(
            "carryme_worker.poller._execute_guarded_pair_close_from_confirmation",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("timed-out auto-close must not execute downstream close")
            ),
        )
        result = asyncio.run(
            _maybe_auto_close_open_hedged_execution(
                settings=settings,
                execution=execution,
                pair_status=_build_auto_close_pair_status(execution=execution),
                approved_store=approved_store,
                execution_store=execution_store,
                observation_store=observation_store,
                account_service=cast(AccountPreflightService, object()),
                order_state_service=cast(ExecutionOrderStateService, object()),
                logger=logging.getLogger("test"),
                now=datetime(2026, 4, 4, 10, 1, tzinfo=UTC),
            )
        )

    assert result is None


def test_observe_live_executions_once_skips_failed_observations(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    for paper_trade_id, external_reference in ((7, "order-1"), (8, "order-2")):
        execution_store.append(
            ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                adapter="paired_live:extended_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade_id,
                preview_hash=f"preview-{paper_trade_id}",
                confirmation_entry_id=paper_trade_id,
                paper_trade=PaperTradeEntry(
                    entry_id=paper_trade_id,
                    created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                    note="operator accepted candidate",
                    intent=FundingPairTradeIntent(
                        label=f"arb-{paper_trade_id}",
                        canonical_symbol="ARB-USD-PERP",
                        source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                        one_day_net_edge_after_entry=0.00055,
                        break_even_days_entry=0.45,
                        capacity_limit_notional=4500.0,
                        target_notional=11.0,
                        capacity_fraction=0.25,
                        max_target_notional=11.0,
                        long_leg=TradeLegIntent(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                            fee_profile="pro",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue="extended",
                            symbol="ARB-USD",
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference=external_reference,
                    )
                ],
            )
        )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            if entry.paper_trade_id == 7:
                raise RuntimeError("temporary venue failure")
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="order-2",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            )

    summary = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
        )
    )

    results = observation_store.list_recent(limit=10)

    assert summary.scanned_executions == 2
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1
    assert len(results) == 1
    assert results[0].paper_trade_id == 8


def test_observe_live_executions_once_pages_until_it_finds_live_entries(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_limit=1,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
            adapter="paired_mock:extended_then_paradex",
            mode="mock",
            status="submitted",
            paper_trade_id=9,
            preview_hash="paper-preview",
            confirmation_entry_id=10,
            paper_trade=PaperTradeEntry(
                entry_id=9,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="paper trade",
                intent=FundingPairTradeIntent(
                    label="paper_pair",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=True,
                    external_reference="mock-order-1",
                )
            ],
        )
    )
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            )

    summary = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
        )
    )

    assert summary.scanned_executions == 1
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1
    assert observation_store.latest_for_paper_trade(7) is not None


def test_observe_live_executions_once_skips_stale_live_entries(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_limit=5,
        execution_observation_max_age_seconds=600,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="stale-preview",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                note="stale live trade",
                intent=FundingPairTradeIntent(
                    label="stale_pair",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-stale",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            raise AssertionError(f"stale trade should not be probed: {paper_trade.entry_id}")

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            raise AssertionError(f"stale trade should not be observed: {entry.paper_trade_id}")

    summary = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
        )
    )

    assert summary.scanned_executions == 0
    assert summary.observed_executions == 0
    assert summary.saved_observations == 0
    assert observation_store.latest_for_paper_trade(7) is None


def test_observe_live_executions_once_keeps_monitoring_stale_open_hedges(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_limit=5,
        execution_observation_max_age_seconds=600,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                note="stale open hedge",
                intent=FundingPairTradeIntent(
                    label="stale_open_pair",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-stale-open",
                )
            ],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                legs=[],
                notes=[],
            ),
            pair_status=ExecutionPairStatus(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
                order_state=ExecutionOrderState(
                    execution_entry_id=execution.entry_id,
                    paper_trade_id=7,
                    preview_hash="stale-open-preview",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=execution.entry_id,
                    paper_trade_id=7,
                    preview_hash="stale-open-preview",
                    status="submitted",
                    recommended_action="monitor_open_hedge",
                    matched_all_leg_symbols=True,
                    venues=[],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[],
                notes=[],
            )

    summary = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
        )
    )

    assert summary.scanned_executions == 1
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1


def test_execution_requires_continued_monitoring_uses_latest_non_null_pair_status(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                note="stale open hedge",
                intent=FundingPairTradeIntent(
                    label="stale_open_pair",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-stale-open",
                )
            ],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                legs=[],
                notes=[],
            ),
            pair_status=ExecutionPairStatus(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                derived_state="hedged",
                recommended_action="monitor_open_hedge",
                order_state=ExecutionOrderState(
                    execution_entry_id=execution.entry_id,
                    paper_trade_id=7,
                    preview_hash="stale-open-preview",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=execution.entry_id,
                    paper_trade_id=7,
                    preview_hash="stale-open-preview",
                    status="submitted",
                    recommended_action="monitor_open_hedge",
                    matched_all_leg_symbols=True,
                    venues=[],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                legs=[],
                notes=[],
            ),
        )
    )

    assert _execution_requires_continued_monitoring(
        observation_store,
        execution=execution,
    )


def test_execution_requires_continued_monitoring_keeps_unknown_pair_status_history(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                note="stale open hedge",
                intent=FundingPairTradeIntent(
                    label="stale_open_pair",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 11, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-stale-open",
                )
            ],
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                legs=[],
                notes=[],
            ),
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
            context="worker_execution_monitor",
            execution_entry_id=execution.entry_id,
            paper_trade_id=7,
            preview_hash="stale-open-preview",
            order_state=ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=7,
                preview_hash="stale-open-preview",
                legs=[],
                notes=[],
            ),
        )
    )

    assert _execution_requires_continued_monitoring(
        observation_store,
        execution=execution,
    )


def test_observe_live_executions_once_includes_exact_max_age_cutoff(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_limit=5,
        execution_observation_max_age_seconds=600,
    )
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 12, 58, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="cutoff-preview",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 12, 58, tzinfo=UTC),
                note="cutoff live trade",
                intent=FundingPairTradeIntent(
                    label="cutoff_pair",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-cutoff",
                )
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-cutoff",
                        derived_state="unfilled",
                        order_status="OPEN",
                    )
                ],
                notes=[],
            )

    summary = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
        )
    )

    assert summary.scanned_executions == 1
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1
    assert observation_store.latest_for_paper_trade(7) is not None


def test_build_order_state_observers_respects_live_enabled_flags(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        extended_live_enabled=False,
        extended_api_key="extended-key",
        paradex_live_enabled=False,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
        hyperliquid_live_enabled=False,
        hyperliquid_account_address="0xdef",
        hyperliquid_api_wallet_private_key="hyperliquid-private",
    )

    assert _build_order_state_observers(settings) == {}


def test_observe_live_executions_once_emits_deduped_cleanup_alert(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    alert_store = ExecutionAlertStore(settings.database_path)
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order-1",
                ),
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="pdx-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                ],
                notes=[],
            )

    notified_events: list[str] = []

    class StubAlertNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            notified_events.append(event.alert_type)
            return 1

    for index in range(2):
        summary = asyncio.run(
            observe_live_executions_once(
                settings,
                execution_store=execution_store,
                observation_store=observation_store,
                alert_sink=alert_store,
                alert_notifier=StubAlertNotifier(),
                account_service=cast(AccountPreflightService, StubAccountService()),
                order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
                now=datetime(2026, 3, 29, 13, 6 + index, tzinfo=UTC),
            )
        )
        assert summary.saved_observations == 1
        assert summary.saved_alerts == (1 if index == 0 else 0)

    alerts = alert_store.list_recent(limit=10, paper_trade_id=7)

    assert len(alerts) == 1
    assert notified_events == ["cleanup_needed"]
    assert alerts[0].emitted_at == datetime(2026, 3, 29, 13, 6, tzinfo=UTC)
    assert alerts[0].alert_type == "cleanup_needed"
    assert alerts[0].paper_trade_id == 7
    assert alerts[0].preview_hash == "preview-hash"
    assert alerts[0].pair_status.derived_state == "cleanup_needed"


def test_observe_live_executions_once_retries_without_duplicate_alerts_after_atomic_failure(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    alert_store = ExecutionAlertStore(settings.database_path)
    execution_store = ExecutionJournalStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order-1",
                ),
            ],
        )
    )

    class FlakyAtomicObservationStore(ExecutionObservationStore):
        def __init__(self, database_path: str) -> None:
            super().__init__(database_path)
            self.calls = 0

        def _append_on_connection(
            self,
            connection: DatabaseConnection,
            entry: ExecutionObservationEntry,
        ) -> ExecutionObservationEntry:
            self.calls += 1
            if self.calls == 1:
                raise sqlite3.OperationalError("simulated observation write failure")
            return super()._append_on_connection(connection, entry)

    observation_store = FlakyAtomicObservationStore(settings.database_path)

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="pdx-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                ],
                notes=[],
            )

    first = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            alert_sink=alert_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
        )
    )
    second = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            alert_sink=alert_store,
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            now=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
        )
    )

    alerts = alert_store.list_recent(limit=10, paper_trade_id=7)
    latest = observation_store.latest_for_paper_trade(7)

    assert first.saved_observations == 0
    assert first.saved_alerts == 0
    assert second.saved_observations == 1
    assert second.saved_alerts == 1
    assert len(alerts) == 1
    assert latest is not None
    assert latest.context == "worker_execution_monitor"
    assert latest.pair_status is not None
    assert latest.pair_status.derived_state == "cleanup_needed"


def test_observe_live_executions_once_emits_deduped_review_required_alert(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    alert_store = ExecutionAlertStore(settings.database_path)
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order-1",
                ),
            ],
        )
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=False,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=False,
                        ready=False,
                        credential_mode="api_key",
                        position_symbols=[],
                    )
                ],
                blocking_reasons=["extended read access unavailable"],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="pdx-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                ],
                notes=[],
            )

    for index in range(2):
        summary = asyncio.run(
            observe_live_executions_once(
                settings,
                execution_store=execution_store,
                observation_store=observation_store,
                alert_sink=alert_store,
                account_service=cast(AccountPreflightService, StubAccountService()),
                order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
                now=datetime(2026, 3, 29, 13, 6 + index, tzinfo=UTC),
            )
        )
        assert summary.saved_observations == 1
        assert summary.saved_alerts == (1 if index == 0 else 0)

    alerts = alert_store.list_recent(limit=10, paper_trade_id=7)

    assert len(alerts) == 1
    assert alerts[0].alert_type == "review_required"
    assert alerts[0].paper_trade_id == 7
    assert alerts[0].preview_hash == "preview-hash"
    assert alerts[0].pair_status.derived_state == "review_required"


def test_observe_live_executions_once_realerts_after_recovery(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )
    alert_store = ExecutionAlertStore(settings.database_path)
    execution_store = ExecutionJournalStore(settings.database_path)
    observation_store = ExecutionObservationStore(settings.database_path)
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order-1",
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order-1",
                ),
            ],
        )
    )

    account_position_sequences = [["ARB-USD"], [], ["ARB-USD"]]

    class StubAccountService:
        def __init__(self) -> None:
            self.index = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            position_symbols = account_position_sequences[self.index]
            self.index += 1
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=position_symbols,
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="pdx-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    ),
                ],
                notes=[],
            )

    account_service = cast(AccountPreflightService, StubAccountService())
    order_state_service = cast(ExecutionOrderStateService, StubOrderStateService())

    first = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            alert_sink=alert_store,
            account_service=account_service,
            order_state_service=order_state_service,
            now=datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
        )
    )
    second = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            alert_sink=alert_store,
            account_service=account_service,
            order_state_service=order_state_service,
            now=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
        )
    )
    third = asyncio.run(
        observe_live_executions_once(
            settings,
            execution_store=execution_store,
            observation_store=observation_store,
            alert_sink=alert_store,
            account_service=account_service,
            order_state_service=order_state_service,
            now=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
        )
    )

    alerts = alert_store.list_recent(limit=10, paper_trade_id=7)

    assert first.saved_alerts == 1
    assert second.saved_alerts == 0
    assert third.saved_alerts == 1
    assert len(alerts) == 2
    assert [alert.emitted_at for alert in alerts] == [
        datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
        datetime(2026, 3, 29, 13, 6, tzinfo=UTC),
    ]


def test_worker_main_rejects_mutually_exclusive_modes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--once", "--iterations", "2"],
    )

    with pytest.raises(SystemExit, match="2"):
        worker_main()

    _, stderr = capsys.readouterr()
    assert "only supported with the looped worker modes" in stderr


def test_worker_main_rejects_observation_mode_with_other_modes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--observe-executions-once", "--iterations", "2"],
    )

    with pytest.raises(SystemExit, match="2"):
        worker_main()

    _, stderr = capsys.readouterr()
    assert "only supported with the looped worker modes" in stderr


def test_worker_main_rejects_universe_scan_mode_with_iterations(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--scan-universe-once", "--iterations", "2"],
    )

    with pytest.raises(SystemExit, match="2"):
        worker_main()

    _, stderr = capsys.readouterr()
    assert "only supported with the looped worker modes" in stderr


def test_worker_main_rejects_cache_launch_ready_canary_once_with_iterations(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--cache-launch-ready-canary-once", "--iterations", "2"],
    )

    with pytest.raises(SystemExit, match="2"):
        worker_main()

    _, stderr = capsys.readouterr()
    assert "only supported with the looped worker modes" in stderr


def test_worker_main_rejects_production_supervisor_once_with_iterations(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--run-production-supervisor-once", "--iterations", "2"],
    )

    with pytest.raises(SystemExit, match="2"):
        worker_main()

    _, stderr = capsys.readouterr()
    assert "only supported with the looped worker modes" in stderr


def test_run_supervised_execution_observation_loop_honors_max_iterations(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_interval_seconds=3,
    )

    class StubExecutionStore:
        pass

    class StubObservationStore:
        pass

    class StubAlertSink:
        pass

    class StableAccountService:
        pass

    class StableOrderStateService:
        pass

    calls: list[int] = []
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def fake_observe_live_executions_once(
        settings_arg: WorkerSettings,
        *,
        execution_store: object | None = None,
        observation_store: object | None = None,
        approved_store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        assert execution_store is stub_execution_store
        assert observation_store is stub_observation_store
        _ = approved_store
        assert alert_sink is stub_alert_sink
        assert alert_notifier is not None
        assert account_service is stable_account_service
        assert order_state_service is stable_order_state_service
        _ = logger
        assert now is None
        calls.append(1)
        return ExecutionObservationSummary(
            scanned_executions=2,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=1,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    stub_execution_store = cast(ExecutionJournalStore, StubExecutionStore())
    stub_observation_store = cast(ExecutionObservationStore, StubObservationStore())
    stub_alert_sink = cast(ExecutionAlertStore, StubAlertSink())
    stable_account_service = cast(AccountPreflightService, StableAccountService())
    stable_order_state_service = cast(ExecutionOrderStateService, StableOrderStateService())

    from unittest.mock import patch

    with patch(
        "carryme_worker.poller.observe_live_executions_once",
        side_effect=fake_observe_live_executions_once,
    ):
        summary = asyncio.run(
            run_supervised_execution_observation_loop(
                settings,
                execution_store=stub_execution_store,
                observation_store=stub_observation_store,
                alert_sink=stub_alert_sink,
                account_service=stable_account_service,
                order_state_service=stable_order_state_service,
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.scanned_executions == 4
    assert summary.observed_executions == 2
    assert summary.saved_observations == 2
    assert summary.saved_alerts == 2
    assert summary.sent_notifications == 2
    assert calls == [1, 1]
    assert sleeps == [3.0]


def test_run_supervised_execution_observation_loop_rejects_non_positive_max_iterations(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    with pytest.raises(ValueError, match="max_iterations must be at least 1"):
        asyncio.run(
            run_supervised_execution_observation_loop(
                settings,
                max_iterations=0,
            )
        )


def test_run_supervised_execution_observation_loop_applies_backoff(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_interval_seconds=2,
        execution_observation_max_backoff_seconds=5,
    )

    class StableAccountService:
        pass

    class StableOrderStateService:
        pass

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    outcomes: list[ExecutionObservationSummary | Exception] = [
        RuntimeError("temporary monitor failure"),
        ExecutionObservationSummary(
            scanned_executions=1,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=0,
            sent_notifications=0,
            database_path=settings.database_path,
        ),
    ]

    async def fake_observe_live_executions_once(
        settings_arg: WorkerSettings,
        *,
        execution_store: object | None = None,
        observation_store: object | None = None,
        approved_store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        _ = execution_store
        _ = observation_store
        _ = approved_store
        _ = alert_sink
        assert alert_notifier is not None
        assert account_service is stable_account_service
        assert order_state_service is stable_order_state_service
        _ = logger
        assert now is None
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    stable_account_service = cast(AccountPreflightService, StableAccountService())
    stable_order_state_service = cast(ExecutionOrderStateService, StableOrderStateService())

    from unittest.mock import patch

    with patch(
        "carryme_worker.poller.observe_live_executions_once",
        side_effect=fake_observe_live_executions_once,
    ):
        summary = asyncio.run(
            run_supervised_execution_observation_loop(
                settings,
                account_service=stable_account_service,
                order_state_service=stable_order_state_service,
                sleep=fake_sleep,
                max_iterations=2,
            )
        )

    assert summary.attempts == 2
    assert summary.successful_cycles == 1
    assert summary.failures == 1
    assert summary.scanned_executions == 1
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1
    assert summary.saved_alerts == 0
    assert summary.sent_notifications == 0
    assert sleeps == [2.0]


def test_run_supervised_execution_observation_loop_clamps_exponential_backoff(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_observation_interval_seconds=2,
        execution_observation_max_backoff_seconds=5,
    )

    class StableAccountService:
        pass

    class StableOrderStateService:
        pass

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    outcomes: list[ExecutionObservationSummary | Exception] = [
        RuntimeError("temporary monitor failure #1"),
        RuntimeError("temporary monitor failure #2"),
        RuntimeError("temporary monitor failure #3"),
        ExecutionObservationSummary(
            scanned_executions=1,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=0,
            sent_notifications=0,
            database_path=settings.database_path,
        ),
    ]

    async def fake_observe_live_executions_once(
        settings_arg: WorkerSettings,
        *,
        execution_store: object | None = None,
        observation_store: object | None = None,
        approved_store: object | None = None,
        alert_sink: object | None = None,
        alert_notifier: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        logger: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        _ = execution_store
        _ = observation_store
        _ = approved_store
        _ = alert_sink
        assert alert_notifier is not None
        assert account_service is stable_account_service
        assert order_state_service is stable_order_state_service
        assert logger is not None
        assert now is None
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    stable_account_service = cast(AccountPreflightService, StableAccountService())
    stable_order_state_service = cast(ExecutionOrderStateService, StableOrderStateService())

    from unittest.mock import patch

    with patch(
        "carryme_worker.poller.observe_live_executions_once",
        side_effect=fake_observe_live_executions_once,
    ):
        summary = asyncio.run(
            run_supervised_execution_observation_loop(
                settings,
                account_service=stable_account_service,
                order_state_service=stable_order_state_service,
                sleep=fake_sleep,
                max_iterations=4,
            )
        )

    assert summary.attempts == 4
    assert summary.successful_cycles == 1
    assert summary.failures == 3
    assert summary.scanned_executions == 1
    assert summary.observed_executions == 1
    assert summary.saved_observations == 1
    assert summary.saved_alerts == 0
    assert sleeps == [2.0, 4.0, 5.0]


def test_worker_main_prints_supervised_execution_observation_summary_as_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    async def fake_execution_supervised(
        settings: WorkerSettings,
        *,
        stop_event: asyncio.Event | None = None,
        max_iterations: int | None = None,
    ) -> ExecutionObservationLoopSummary:
        assert stop_event is not None
        assert max_iterations == 3
        return ExecutionObservationLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            scanned_executions=5,
            observed_executions=4,
            saved_observations=4,
            saved_alerts=1,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    monkeypatch.setattr("carryme_worker.main.WorkerSettings", lambda: settings)
    monkeypatch.setattr("carryme_worker.main.install_signal_handlers", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "carryme_worker.main.run_supervised_execution_observation_loop",
        fake_execution_supervised,
    )
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--observe-executions-supervise", "--iterations", "3"],
    )

    worker_main()

    stdout, _ = capsys.readouterr()
    assert json.loads(stdout) == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "scanned_executions": 5,
        "observed_executions": 4,
        "saved_observations": 4,
        "saved_alerts": 1,
        "sent_notifications": 1,
        "database_path": settings.database_path,
    }


def test_worker_main_prints_supervised_universe_scan_summary_as_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    async def fake_supervised_universe(
        settings: WorkerSettings,
        *,
        stop_event: asyncio.Event | None = None,
        max_iterations: int | None = None,
    ) -> UniverseScanLoopSummary:
        assert stop_event is not None
        assert max_iterations == 3
        return UniverseScanLoopSummary(
            attempts=3,
            successful_cycles=2,
            failures=1,
            overlap_count=9,
            scanned_opportunities=4,
            saved_records=4,
            alert_events=1,
            database_path=settings.database_path,
        )

    monkeypatch.setattr("carryme_worker.main.WorkerSettings", lambda: settings)
    monkeypatch.setattr("carryme_worker.main.install_signal_handlers", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "carryme_worker.main.run_supervised_universe_scan_loop",
        fake_supervised_universe,
    )
    monkeypatch.setattr(
        "sys.argv",
        ["carryme-worker", "--scan-universe-supervise", "--iterations", "3"],
    )

    worker_main()

    stdout, _ = capsys.readouterr()
    assert json.loads(stdout) == {
        "attempts": 3,
        "successful_cycles": 2,
        "failures": 1,
        "overlap_count": 9,
        "scanned_opportunities": 4,
        "saved_records": 4,
        "alert_events": 1,
        "database_path": settings.database_path,
    }


def test_summarize_candidates_counts_only_threshold_matches() -> None:
    records = [
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="match",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            ),
        ),
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="miss",
                left_venue="extended",
                left_symbol="ARB-USD",
                left_fee_profile="default",
                right_venue="paradex",
                right_symbol="ARB-USD-PERP",
                right_fee_profile="pro",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=-0.0002,
                one_day_net_edge_after_round_trip=-0.0007,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=1000.0,
                    long_ask_notional=800.0,
                    max_entry_notional=800.0,
                    limiting_venue="paradex",
                ),
            ),
        ),
    ]

    summary = summarize_candidates(
        records,
        min_one_day_net_edge_after_entry=0.0,
        min_capacity_notional=2500.0,
    )

    assert summary.total_records == 2
    assert summary.candidate_records == 1


def test_run_supervised_polling_loop_honors_max_iterations(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class StableScorer:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        poll_interval_seconds=2,
        min_candidate_entry_edge=0.0,
        min_candidate_capacity_notional=2500.0,
    )

    summary = asyncio.run(
        run_supervised_polling_loop(
            settings,
            scorer=StableScorer(),
            store=OpportunityHistoryStore(settings.database_path),
            sleep=fake_sleep,
            max_iterations=2,
        )
    )

    assert summary.attempts == 2
    assert summary.successful_cycles == 2
    assert summary.failures == 0
    assert summary.saved_records == 2
    assert summary.alert_events == 2
    assert sleeps == [2.0]


def test_run_supervised_polling_loop_skips_candidate_lookup_when_cycle_saves_nothing(
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "failing_pair",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class ValidationFailingScorer:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise ValueError("bad pair configuration")

    class GuardedHistoryStore(OpportunityHistoryStore):
        def list_recent(
            self,
            *,
            limit: int = 50,
            label: str | None = None,
        ) -> list[OpportunityRecord]:
            raise AssertionError("candidate lookup should be skipped when no rows were saved")

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    summary = asyncio.run(
        run_supervised_polling_loop(
            settings,
            scorer=ValidationFailingScorer(),
            store=GuardedHistoryStore(settings.database_path),
            max_iterations=1,
        )
    )

    assert summary.attempts == 1
    assert summary.successful_cycles == 1
    assert summary.failures == 0
    assert summary.saved_records == 0
    assert summary.alert_events == 0


def test_run_supervised_polling_loop_emits_candidate_alert_events(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class StableScorer:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    emitted: list[CandidateAlertEvent] = []

    class StubAlertSink:
        def append(self, event: CandidateAlertEvent) -> bool:
            emitted.append(event)
            return True

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        min_candidate_entry_edge=0.0,
        min_candidate_capacity_notional=2500.0,
    )

    summary = asyncio.run(
        run_supervised_polling_loop(
            settings,
            scorer=StableScorer(),
            store=OpportunityHistoryStore(settings.database_path),
            alert_sink=StubAlertSink(),
            max_iterations=1,
        )
    )

    assert summary.attempts == 1
    assert summary.successful_cycles == 1
    assert summary.failures == 0
    assert summary.saved_records == 1
    assert summary.alert_events == 1
    assert len(emitted) == 1
    assert emitted[0].record.pair.label == "strk_extended_hyperliquid"


def test_run_supervised_polling_loop_tolerates_alert_sink_failures(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class StableScorer:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    class FailingAlertSink:
        def append(self, event: CandidateAlertEvent) -> bool:
            raise RuntimeError("alert sink unavailable")

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        min_candidate_entry_edge=0.0,
        min_candidate_capacity_notional=2500.0,
    )

    with caplog.at_level("ERROR"):
        summary = asyncio.run(
            run_supervised_polling_loop(
                settings,
                scorer=StableScorer(),
                store=OpportunityHistoryStore(settings.database_path),
                alert_sink=FailingAlertSink(),
                max_iterations=1,
            )
        )

    assert summary.attempts == 1
    assert summary.successful_cycles == 1
    assert summary.failures == 0
    assert summary.saved_records == 1
    assert summary.alert_events == 0
    assert "failed to persist candidate alerts" in caplog.text


def test_run_supervised_polling_loop_resets_backoff_after_success(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text(
        """
        {
          "pairs": [
            {
              "label": "strk_extended_hyperliquid",
              "left_venue": "extended",
              "left_symbol": "STRK-USD",
              "left_fee_profile": "default",
              "right_venue": "hyperliquid",
              "right_symbol": "STRK",
              "right_fee_profile": "tier0"
            }
          ]
        }
        """
    )

    class SequencedScorer:
        def __init__(self) -> None:
            self.calls = 0

        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            self.calls += 1
            if self.calls in {1, 3}:
                raise RuntimeError("temporary failure")
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        poll_interval_seconds=2,
        max_backoff_seconds=10,
    )

    summary = asyncio.run(
        run_supervised_polling_loop(
            settings,
            scorer=SequencedScorer(),
            store=OpportunityHistoryStore(settings.database_path),
            sleep=fake_sleep,
            max_iterations=4,
        )
    )

    assert summary.attempts == 4
    assert summary.successful_cycles == 2
    assert summary.failures == 2
    assert summary.saved_records == 2
    assert sleeps == [2.0, 2.0, 2.0]


def test_run_supervised_polling_loop_rejects_non_positive_max_iterations(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    with pytest.raises(ValueError, match="max_iterations must be at least 1"):
        asyncio.run(run_supervised_polling_loop(settings, max_iterations=0))


def test_install_signal_handlers_registers_expected_signals() -> None:
    registered: list[str] = []

    class StubLoop:
        def add_signal_handler(self, sig: int, callback: object, *args: object) -> None:
            import signal

            registered.append(signal.Signals(sig).name)

    from unittest.mock import patch

    with patch.object(asyncio, "get_running_loop", return_value=StubLoop()):
        install_signal_handlers(
            asyncio.Event(),
            signals_to_handle=("SIGINT", "SIGTERM"),
        )

    assert registered == ["SIGINT", "SIGTERM"]


def test_worker_main_prints_supervised_summary_as_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    async def fake_supervised(
        settings: WorkerSettings,
        *,
        stop_event: asyncio.Event | None = None,
        max_iterations: int | None = None,
    ) -> PollLoopSummary:
        assert stop_event is not None
        assert max_iterations is None
        return PollLoopSummary(
            attempts=2,
            successful_cycles=1,
            failures=1,
            saved_records=1,
            database_path=settings.database_path,
            alert_events=0,
        )

    monkeypatch.setattr("carryme_worker.main.WorkerSettings", lambda: settings)
    monkeypatch.setattr("carryme_worker.main.install_signal_handlers", lambda *args, **kwargs: None)
    monkeypatch.setattr("carryme_worker.main.run_supervised_polling_loop", fake_supervised)
    monkeypatch.setattr("sys.argv", ["carryme-worker", "--supervise"])

    worker_main()

    stdout, _ = capsys.readouterr()
    assert json.loads(stdout) == {
        "attempts": 2,
        "successful_cycles": 1,
        "failures": 1,
        "saved_records": 1,
        "alert_events": 0,
        "database_path": settings.database_path,
    }


def test_worker_main_prints_execution_observation_summary_as_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    async def fake_observe(settings: WorkerSettings) -> ExecutionObservationSummary:
        return ExecutionObservationSummary(
            scanned_executions=3,
            observed_executions=2,
            saved_observations=2,
            saved_alerts=1,
            sent_notifications=1,
            database_path=settings.database_path,
        )

    monkeypatch.setattr("carryme_worker.main.WorkerSettings", lambda: settings)
    monkeypatch.setattr("carryme_worker.main.observe_live_executions_once", fake_observe)
    monkeypatch.setattr("sys.argv", ["carryme-worker", "--observe-executions-once"])

    worker_main()

    stdout, _ = capsys.readouterr()
    assert json.loads(stdout) == {
        "scanned_executions": 3,
        "observed_executions": 2,
        "saved_observations": 2,
        "saved_alerts": 1,
        "sent_notifications": 1,
        "database_path": settings.database_path,
    }


def test_worker_main_prints_universe_scan_summary_as_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
    )

    async def fake_scan(settings: WorkerSettings) -> UniverseScanSummary:
        return UniverseScanSummary(
            overlap_count=2,
            scanned_opportunities=4,
            saved_records=3,
            alert_events=2,
            database_path=settings.database_path,
        )

    monkeypatch.setattr("carryme_worker.main.WorkerSettings", lambda: settings)
    monkeypatch.setattr("carryme_worker.main.scan_funding_universe_once", fake_scan)
    monkeypatch.setattr("sys.argv", ["carryme-worker", "--scan-universe-once"])

    worker_main()

    stdout, _ = capsys.readouterr()
    assert json.loads(stdout) == {
        "overlap_count": 2,
        "scanned_opportunities": 4,
        "saved_records": 3,
        "alert_events": 2,
        "database_path": settings.database_path,
    }
