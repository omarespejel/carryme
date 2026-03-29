import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx
import pytest
from carryme_models import (
    CandidateAlertEvent,
    CapacityEstimate,
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionObservationEntry,
    ExecutionOrderState,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    OpportunityRecord,
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    TradeLegIntent,
    VenueAccountPreflight,
)
from carryme_runtime import AccountPreflightService, ExecutionOrderStateService
from carryme_storage import (
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    OpportunityHistoryStore,
)
from carryme_worker.config import WorkerSettings
from carryme_worker.main import (
    build_candidate_payload,
    build_cycle_payload,
    build_execution_observation_loop_payload,
    build_execution_observation_payload,
    build_health_payload,
    build_loop_payload,
)
from carryme_worker.main import (
    main as worker_main,
)
from carryme_worker.poller import (
    CandidateRecordSummary,
    ExecutionObservationLoopSummary,
    ExecutionObservationSummary,
    PollCycleSummary,
    PollLoopSummary,
    _build_order_state_observers,
    install_signal_handlers,
    observe_live_executions_once,
    poll_watchlist_once,
    run_polling_loop,
    run_supervised_execution_observation_loop,
    run_supervised_polling_loop,
    summarize_candidates,
)
from pydantic import ValidationError


def _clear_worker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARRYME_WORKER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_LOG_LEVEL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_MAX_BACKOFF_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_SCORE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_MIN_CANDIDATE_ENTRY_EDGE", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_MIN_CANDIDATE_CAPACITY_NOTIONAL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_STOP_SIGNALS", raising=False)


def test_worker_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_worker_env(monkeypatch)
    settings = WorkerSettings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.poll_interval_seconds == 30
    assert settings.max_backoff_seconds == 300
    assert settings.execution_observation_interval_seconds == 10
    assert settings.execution_observation_max_backoff_seconds == 60
    assert settings.score_timeout_seconds == 30.0
    assert settings.min_candidate_entry_edge == 0.0
    assert settings.min_candidate_capacity_notional == 0.0
    assert settings.stop_signals == ("SIGINT", "SIGTERM")
    assert settings.database_path == "data/carryme.sqlite3"
    assert Path(settings.watchlist_path).is_file()
    assert settings.watchlist_path.endswith("config/watchlists/default.json")


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


def test_worker_execution_observation_payload() -> None:
    payload = build_execution_observation_payload(
        ExecutionObservationSummary(
            scanned_executions=2,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=0,
            database_path="tmp/history.sqlite3",
        )
    )

    assert payload == {
        "scanned_executions": 2,
        "observed_executions": 1,
        "saved_observations": 1,
        "saved_alerts": 0,
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
    assert latest is not None
    assert latest.context == "worker_execution_monitor"
    assert latest.order_state.legs[0].observation_source == "rest_poll"


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
            connection: sqlite3.Connection,
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
        alert_sink: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        assert execution_store is stub_execution_store
        assert observation_store is stub_observation_store
        assert alert_sink is stub_alert_sink
        assert account_service is stable_account_service
        assert order_state_service is stable_order_state_service
        assert now is None
        calls.append(1)
        return ExecutionObservationSummary(
            scanned_executions=2,
            observed_executions=1,
            saved_observations=1,
            saved_alerts=1,
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
            database_path=settings.database_path,
        ),
    ]

    async def fake_observe_live_executions_once(
        settings_arg: WorkerSettings,
        *,
        execution_store: object | None = None,
        observation_store: object | None = None,
        alert_sink: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        _ = execution_store
        _ = observation_store
        _ = alert_sink
        assert account_service is stable_account_service
        assert order_state_service is stable_order_state_service
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
            database_path=settings.database_path,
        ),
    ]

    async def fake_observe_live_executions_once(
        settings_arg: WorkerSettings,
        *,
        execution_store: object | None = None,
        observation_store: object | None = None,
        alert_sink: object | None = None,
        account_service: object | None = None,
        order_state_service: object | None = None,
        now: datetime | None = None,
    ) -> ExecutionObservationSummary:
        assert settings_arg is settings
        _ = execution_store
        _ = observation_store
        _ = alert_sink
        assert account_service is stable_account_service
        assert order_state_service is stable_order_state_service
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
        "database_path": settings.database_path,
    }
