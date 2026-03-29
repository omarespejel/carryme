"""Execution-alert notification sinks for the worker."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

import httpx
from carryme_models import ExecutionAlertEvent

from carryme_worker.config import WorkerSettings


class ExecutionAlertNotifier(Protocol):
    """Async notifier for execution-monitor attention events."""

    async def notify(self, event: ExecutionAlertEvent) -> None: ...


@dataclass(frozen=True)
class LoggingExecutionAlertNotifier:
    """Emit execution alerts to the structured worker logs."""

    logger: logging.Logger

    async def notify(self, event: ExecutionAlertEvent) -> None:
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


@dataclass(frozen=True)
class WebhookExecutionAlertNotifier:
    """POST execution alerts to a configured external webhook."""

    webhook_url: str
    timeout_seconds: float = 10.0

    async def notify(self, event: ExecutionAlertEvent) -> None:
        payload = {
            "source": "carryme-worker",
            "event_type": "execution_alert",
            "alert": event.model_dump(mode="json"),
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()


@dataclass(frozen=True)
class CompositeExecutionAlertNotifier:
    """Dispatch one execution alert to multiple downstream notifiers."""

    notifiers: tuple[ExecutionAlertNotifier, ...]

    async def notify(self, event: ExecutionAlertEvent) -> None:
        for notifier in self.notifiers:
            await notifier.notify(event)


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
    return CompositeExecutionAlertNotifier(tuple(notifiers))
