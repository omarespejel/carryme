"""Alert notification sinks for the worker."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

import httpx
from carryme_models import (
    ApprovedCanaryAlertEvent,
    ExecutionAlertEvent,
    StableLaunchReadyAlertEvent,
    SystemStateAlertEvent,
)

from carryme_worker.config import WorkerSettings

logger = logging.getLogger(__name__)


class ExecutionAlertNotifier(Protocol):
    """Async notifier for execution-monitor attention events."""

    async def notify(self, event: ExecutionAlertEvent) -> int: ...


class ApprovedCanaryAlertNotifier(Protocol):
    """Async notifier for approved-canary availability events."""

    async def notify(self, event: ApprovedCanaryAlertEvent) -> None: ...


class SystemStateAlertNotifier(Protocol):
    """Async notifier for system-state transition events."""

    async def notify(self, event: SystemStateAlertEvent) -> None: ...


class StableLaunchReadyAlertNotifier(Protocol):
    """Async notifier for stable launch-ready transition events."""

    async def notify(self, event: StableLaunchReadyAlertEvent) -> None: ...


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
class LoggingApprovedCanaryAlertNotifier:
    """Emit approved-canary alerts to the structured worker logs."""

    logger: logging.Logger

    async def notify(self, event: ApprovedCanaryAlertEvent) -> None:
        current_label = event.current_snapshot.label if event.current_snapshot else None
        previous_label = event.previous_snapshot.label if event.previous_snapshot else None
        self.logger.warning(
            (
                "approved canary alert emitted: type=%s current_label=%s "
                "previous_label=%s max_snapshot_age_seconds=%s"
            ),
            event.alert_type,
            current_label,
            previous_label,
            event.max_snapshot_age_seconds,
        )


@dataclass(frozen=True)
class LoggingSystemStateAlertNotifier:
    """Emit system-state alerts to the structured worker logs."""

    logger: logging.Logger

    async def notify(self, event: SystemStateAlertEvent) -> None:
        previous_status = event.previous_state.status if event.previous_state is not None else None
        self.logger.warning(
            ("system state alert emitted: type=%s venue=%s current_status=%s previous_status=%s"),
            event.alert_type,
            event.venue,
            event.current_state.status,
            previous_status,
        )


@dataclass(frozen=True)
class LoggingStableLaunchReadyAlertNotifier:
    """Emit stable launch-ready alerts to the structured worker logs."""

    logger: logging.Logger

    async def notify(self, event: StableLaunchReadyAlertEvent) -> None:
        current_label = (
            event.current_stability.snapshot.label
            if event.current_stability is not None
            else None
        )
        previous_label = (
            event.previous_stability.snapshot.label
            if event.previous_stability is not None
            else None
        )
        self.logger.warning(
            (
                "stable launch-ready alert emitted: type=%s current_label=%s "
                "previous_label=%s min_snapshot_count=%s min_stable_seconds=%s"
            ),
            event.alert_type,
            current_label,
            previous_label,
            event.min_snapshot_count,
            event.min_stable_seconds,
        )


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
class WebhookApprovedCanaryAlertNotifier:
    """POST approved-canary alerts to a configured external webhook."""

    webhook_url: str
    timeout_seconds: float = 10.0

    async def notify(self, event: ApprovedCanaryAlertEvent) -> None:
        payload = {
            "source": "carryme-worker",
            "event_type": "approved_canary_alert",
            "alert": event.model_dump(mode="json"),
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()


@dataclass(frozen=True)
class WebhookSystemStateAlertNotifier:
    """POST system-state alerts to a configured external webhook."""

    webhook_url: str
    timeout_seconds: float = 10.0

    async def notify(self, event: SystemStateAlertEvent) -> None:
        payload = {
            "source": "carryme-worker",
            "event_type": "system_state_alert",
            "alert": event.model_dump(mode="json"),
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()


@dataclass(frozen=True)
class WebhookStableLaunchReadyAlertNotifier:
    """POST stable launch-ready alerts to a configured external webhook."""

    webhook_url: str
    timeout_seconds: float = 10.0

    async def notify(self, event: StableLaunchReadyAlertEvent) -> None:
        payload = {
            "source": "carryme-worker",
            "event_type": "stable_launch_ready_alert",
            "alert": event.model_dump(mode="json"),
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()


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


@dataclass(frozen=True)
class CompositeApprovedCanaryAlertNotifier:
    """Dispatch one approved-canary alert to multiple downstream notifiers."""

    notifiers: tuple[ApprovedCanaryAlertNotifier, ...]

    async def notify(self, event: ApprovedCanaryAlertEvent) -> None:
        for notifier in self.notifiers:
            await notifier.notify(event)


@dataclass(frozen=True)
class CompositeSystemStateAlertNotifier:
    """Dispatch one system-state alert to multiple downstream notifiers."""

    notifiers: tuple[SystemStateAlertNotifier, ...]

    async def notify(self, event: SystemStateAlertEvent) -> None:
        for notifier in self.notifiers:
            await notifier.notify(event)


@dataclass(frozen=True)
class CompositeStableLaunchReadyAlertNotifier:
    """Dispatch one stable launch-ready alert to multiple downstream notifiers."""

    notifiers: tuple[StableLaunchReadyAlertNotifier, ...]

    async def notify(self, event: StableLaunchReadyAlertEvent) -> None:
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
    return CompositeExecutionAlertNotifier(
        tuple(notifiers),
        timeout_seconds=settings.execution_alert_webhook_timeout_seconds,
    )


def build_approved_canary_alert_notifier(
    settings: WorkerSettings,
    *,
    logger: logging.Logger | None = None,
) -> CompositeApprovedCanaryAlertNotifier:
    """Build the default approved-canary alert notifier fanout for the worker."""

    base_logger = logger or logging.getLogger("carryme.worker")
    notifiers: list[ApprovedCanaryAlertNotifier] = [
        LoggingApprovedCanaryAlertNotifier(base_logger),
    ]
    if settings.approved_canary_alert_webhook_url:
        notifiers.append(
            WebhookApprovedCanaryAlertNotifier(
                webhook_url=settings.approved_canary_alert_webhook_url,
                timeout_seconds=settings.approved_canary_alert_webhook_timeout_seconds,
            )
        )
    return CompositeApprovedCanaryAlertNotifier(tuple(notifiers))


def build_system_state_alert_notifier(
    settings: WorkerSettings,
    *,
    logger: logging.Logger | None = None,
) -> CompositeSystemStateAlertNotifier:
    """Build the default system-state alert notifier fanout for the worker."""

    base_logger = logger or logging.getLogger("carryme.worker")
    notifiers: list[SystemStateAlertNotifier] = [
        LoggingSystemStateAlertNotifier(base_logger),
    ]
    if settings.system_state_alert_webhook_url:
        notifiers.append(
            WebhookSystemStateAlertNotifier(
                webhook_url=settings.system_state_alert_webhook_url,
                timeout_seconds=settings.system_state_alert_webhook_timeout_seconds,
            )
        )
    return CompositeSystemStateAlertNotifier(tuple(notifiers))


def build_stable_launch_ready_alert_notifier(
    settings: WorkerSettings,
    *,
    logger: logging.Logger | None = None,
) -> CompositeStableLaunchReadyAlertNotifier:
    """Build the default stable launch-ready alert notifier fanout for the worker."""

    base_logger = logger or logging.getLogger("carryme.worker")
    notifiers: list[StableLaunchReadyAlertNotifier] = [
        LoggingStableLaunchReadyAlertNotifier(base_logger),
    ]
    if settings.stable_launch_ready_alert_webhook_url:
        notifiers.append(
            WebhookStableLaunchReadyAlertNotifier(
                webhook_url=settings.stable_launch_ready_alert_webhook_url,
                timeout_seconds=settings.stable_launch_ready_alert_webhook_timeout_seconds,
            )
        )
    return CompositeStableLaunchReadyAlertNotifier(tuple(notifiers))
