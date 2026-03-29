"""Execution-alert notification sinks for the worker."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

import httpx
from carryme_models import ExecutionAlertEvent

from carryme_worker.config import WorkerSettings

logger = logging.getLogger(__name__)


class ExecutionAlertNotifier(Protocol):
    """Async notifier for execution-monitor attention events."""

    async def notify(self, event: ExecutionAlertEvent) -> int: ...


@dataclass(frozen=True)
class LoggingExecutionAlertNotifier:
    """Emit execution alerts to the structured worker logs."""

    logger: logging.Logger

    async def notify(self, event: ExecutionAlertEvent) -> int:
        self.logger.warning(
            (
                "execution alert emitted: type=%s paper_trade_id=%s "
                "derived_state=%s recommended_action=%s preview_hash=%s"
            ),
            event.alert_type,
            event.paper_trade_id,
            event.pair_status.derived_state,
            event.pair_status.recommended_action,
            event.preview_hash,
        )
        return 1


@dataclass(frozen=True)
class WebhookExecutionAlertNotifier:
    """POST execution alerts to a configured external webhook."""

    webhook_url: str
    timeout_seconds: float = 10.0

    async def notify(self, event: ExecutionAlertEvent) -> int:
        payload = {
            "source": "carryme-worker",
            "event_type": "execution_alert",
            "alert": event.model_dump(mode="json"),
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()
        return 1


@dataclass(frozen=True)
class CompositeExecutionAlertNotifier:
    """Dispatch one execution alert to multiple downstream notifiers."""

    notifiers: tuple[ExecutionAlertNotifier, ...]
    timeout_seconds: float | None = None

    async def notify(self, event: ExecutionAlertEvent) -> int:
        delivered = 0
        last_error: Exception | None = None
        for notifier in self.notifiers:
            try:
                if self.timeout_seconds is None:
                    delivered += await notifier.notify(event)
                else:
                    delivered += await asyncio.wait_for(
                        notifier.notify(event),
                        timeout=self.timeout_seconds,
                    )
            except Exception as exc:
                last_error = exc
                logger.exception(
                    "execution notifier %s failed",
                    notifier.__class__.__name__,
                )
        if delivered == 0 and last_error is not None:
            raise last_error
        return delivered


def build_execution_alert_notifier(
    settings: WorkerSettings,
    *,
    logger: logging.Logger | None = None,
) -> CompositeExecutionAlertNotifier:
    """Build the default execution-alert notifier fanout for the worker."""

    base_logger = logger or logging.getLogger("carryme.worker")
    notifiers: list[ExecutionAlertNotifier] = [
        LoggingExecutionAlertNotifier(base_logger),
    ]
    if settings.execution_alert_webhook_url:
        notifiers.append(
            WebhookExecutionAlertNotifier(
                webhook_url=settings.execution_alert_webhook_url,
                timeout_seconds=settings.execution_alert_webhook_timeout_seconds,
            )
        )
    return CompositeExecutionAlertNotifier(
        tuple(notifiers),
        timeout_seconds=settings.execution_alert_webhook_timeout_seconds,
    )
