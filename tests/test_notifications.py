import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from carryme_models import (
    ExecutionAlertEvent,
    ExecutionOrderState,
    ExecutionPairStatus,
    ExecutionReconciliation,
)
from carryme_worker.config import WorkerSettings
from carryme_worker.notifications import CompositeExecutionAlertNotifier
from carryme_worker.poller import _notify_execution_alert


def _execution_alert_event() -> ExecutionAlertEvent:
    order_state = ExecutionOrderState(
        execution_entry_id=1,
        paper_trade_id=7,
        preview_hash="preview-hash",
        legs=[],
        notes=[],
    )
    reconciliation = ExecutionReconciliation(
        execution_entry_id=1,
        paper_trade_id=7,
        preview_hash="preview-hash",
        status="submitted",
        recommended_action="cleanup leg mismatch",
        matched_all_leg_symbols=False,
        venues=[],
        notes=[],
    )
    pair_status = ExecutionPairStatus(
        execution_entry_id=1,
        paper_trade_id=7,
        preview_hash="preview-hash",
        derived_state="cleanup_needed",
        recommended_action="cleanup leg mismatch",
        order_state=order_state,
        reconciliation=reconciliation,
        notes=[],
    )
    return ExecutionAlertEvent(
        emitted_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        alert_type="cleanup_needed",
        paper_trade_id=7,
        preview_hash="preview-hash",
        pair_status=pair_status,
    )


def test_composite_execution_alert_notifier_isolates_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = _execution_alert_event()
    calls: list[str] = []

    class FailingNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            _ = event
            calls.append("fail")
            raise RuntimeError("broken notifier")

    class SuccessNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            _ = event
            calls.append("success")
            return 1

    composite = CompositeExecutionAlertNotifier(
        notifiers=(FailingNotifier(), SuccessNotifier()),
        timeout_seconds=0.1,
    )

    with caplog.at_level(logging.ERROR):
        delivered = asyncio.run(composite.notify(event))

    assert delivered == 1
    assert calls == ["fail", "success"]
    assert "execution notifier FailingNotifier failed" in caplog.text


def test_composite_execution_alert_notifier_raises_when_all_notifiers_fail() -> None:
    event = _execution_alert_event()
    calls: list[str] = []

    class FirstFailingNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            _ = event
            calls.append("first")
            raise RuntimeError("first failure")

    class SecondFailingNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            _ = event
            calls.append("second")
            raise RuntimeError("second failure")

    composite = CompositeExecutionAlertNotifier(
        notifiers=(FirstFailingNotifier(), SecondFailingNotifier()),
        timeout_seconds=0.1,
    )

    with pytest.raises(RuntimeError, match="second failure"):
        asyncio.run(composite.notify(event))

    assert calls == ["first", "second"]


def test_notify_execution_alert_times_out_generic_notifier(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.json"
    watchlist_path.write_text('{"pairs": []}')
    settings = WorkerSettings(
        watchlist_path=str(watchlist_path),
        database_path=str(tmp_path / "history.sqlite3"),
        execution_alert_webhook_timeout_seconds=0.001,
    )
    event = _execution_alert_event()

    class SlowNotifier:
        async def notify(self, event: ExecutionAlertEvent) -> int:
            _ = event
            await asyncio.sleep(0.05)
            return 1

    with pytest.raises(TimeoutError):
        asyncio.run(_notify_execution_alert(settings, SlowNotifier(), event))
