from datetime import UTC, datetime
from pathlib import Path

from carryme_models import CapacityEstimate, FundingArbOpportunity
from carryme_storage import OpportunityHistoryStore
from carryme_worker.config import WorkerSettings
from carryme_worker.main import build_cycle_payload, build_health_payload, build_loop_payload
from carryme_worker.poller import (
    PollCycleSummary,
    PollLoopSummary,
    poll_watchlist_once,
    run_polling_loop,
)


def test_worker_defaults() -> None:
    settings = WorkerSettings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.poll_interval_seconds == 30
    assert settings.max_backoff_seconds == 300
    assert settings.database_path == "data/carryme.sqlite3"
    assert settings.watchlist_path == "config/watchlists/default.json"


def test_worker_health_payload() -> None:
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
        PollCycleSummary(watched_pairs=2, saved_records=2, database_path="tmp/history.sqlite3")
    )

    assert payload == {
        "watched_pairs": 2,
        "saved_records": 2,
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

    import asyncio

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
    assert len(history) == 1
    assert history[0].pair.label == "strk_extended_hyperliquid"


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

    import asyncio

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
