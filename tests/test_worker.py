import asyncio
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from carryme_models import CapacityEstimate, FundingArbOpportunity
from carryme_storage import OpportunityHistoryStore
from carryme_worker.config import WorkerSettings
from carryme_worker.main import build_cycle_payload, build_health_payload
from carryme_worker.poller import PollCycleSummary, poll_watchlist_once
from pydantic import ValidationError


def _clear_worker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARRYME_WORKER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_LOG_LEVEL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_SCORE_TIMEOUT_SECONDS", raising=False)


def test_worker_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_worker_env(monkeypatch)
    settings = WorkerSettings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.poll_interval_seconds == 30
    assert settings.score_timeout_seconds == 30.0
    assert settings.database_path == "data/carryme.sqlite3"
    assert Path(settings.watchlist_path).is_file()
    assert settings.watchlist_path.endswith("config/watchlists/default.json")


def test_worker_rejects_missing_watchlist_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"

    with pytest.raises(ValidationError, match="watchlist_path"):
        WorkerSettings(watchlist_path=str(missing))


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
    assert "Failed to score pair extended/STRK-USD" in caplog.text


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
    assert "Failed to score pair extended/STRK-USD" in caplog.text
