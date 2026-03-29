"""Venue system-state probes for live execution gating."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, TypedDict

import httpx
from carryme_connectors import ConnectorError, ParadexPublicConnector
from carryme_models import PaperTradeEntry, PaperTradeSystemState, VenueSystemState


class VenueSystemStateConfig(TypedDict):
    """Configured public system-state settings for one venue."""

    enabled: bool


SystemStateConfigMap = dict[str, VenueSystemStateConfig]


class VenueSystemProbe(Protocol):
    """Protocol for one venue-specific public system-state probe."""

    async def probe(self, config: VenueSystemStateConfig) -> VenueSystemState: ...


@dataclass
class SystemStateService:
    """Probe venue health gates relevant to live execution."""

    probes: dict[str, VenueSystemProbe] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.probes:
            self.probes = {
                "extended": PassthroughSystemStateProbe(
                    venue="extended",
                    note="No dedicated Extended public system-state gate is configured.",
                ),
                "hyperliquid": PassthroughSystemStateProbe(
                    venue="hyperliquid",
                    note="No dedicated Hyperliquid public system-state gate is configured.",
                ),
                "paradex": ParadexSystemStateProbe(),
            }

    async def probe_venues(
        self,
        configs: SystemStateConfigMap,
    ) -> list[VenueSystemState]:
        """Probe all configured venue system-state gates."""

        tasks = [
            self.probes[venue].probe(configs.get(venue, {"enabled": False}))
            for venue in self.probes
        ]
        return list(await asyncio.gather(*tasks))

    async def probe_paper_trade(
        self,
        paper_trade: PaperTradeEntry,
        configs: SystemStateConfigMap,
    ) -> PaperTradeSystemState:
        """Probe only the venues touched by one saved paper trade."""

        all_statuses = {item.venue: item for item in await self.probe_venues(configs)}
        venue_names = [paper_trade.intent.long_leg.venue, paper_trade.intent.short_leg.venue]
        selected_names: list[str] = []
        for venue in venue_names:
            if venue not in selected_names:
                selected_names.append(venue)
        selected = [all_statuses[venue] for venue in selected_names]

        blocking_reasons: list[str] = []
        for status in selected:
            blocking_reasons.extend(status.blocking_reasons)

        return PaperTradeSystemState(
            paper_trade_id=paper_trade.entry_id or 0,
            label=paper_trade.intent.label,
            ready=not blocking_reasons,
            venues=selected,
            blocking_reasons=blocking_reasons,
        )


@dataclass
class PassthroughSystemStateProbe:
    """Report a non-blocking status for venues without a dedicated public gate."""

    venue: str
    note: str

    async def probe(self, config: VenueSystemStateConfig) -> VenueSystemState:
        enabled = bool(config["enabled"])
        return VenueSystemState(
            venue=self.venue,
            enabled=enabled,
            checked=False,
            healthy=True,
            status=None,
            notes=[self.note],
        )


@dataclass
class ParadexSystemStateProbe:
    """Probe the official Paradex public system-state endpoint."""

    base_url: str = "https://api.prod.paradex.trade"
    connector_factory: Callable[[httpx.AsyncClient], ParadexPublicConnector] = (
        ParadexPublicConnector
    )

    async def probe(self, config: VenueSystemStateConfig) -> VenueSystemState:
        enabled = bool(config["enabled"])
        if not enabled:
            return VenueSystemState(
                venue="paradex",
                enabled=False,
                checked=False,
                healthy=True,
                status=None,
                notes=["Paradex system-state probe skipped because live execution is disabled."],
            )

        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0) as client:
                payload = await self.connector_factory(client).fetch_system_state()
        except (ConnectorError, httpx.HTTPError) as exc:
            return VenueSystemState(
                venue="paradex",
                enabled=True,
                checked=True,
                healthy=False,
                status="error",
                blocking_reasons=[f"Paradex system-state probe failed: {exc}"],
                notes=[
                    "Paradex live execution is blocked until the public "
                    "/v1/system/state probe recovers."
                ],
            )

        status = payload.get("status")
        if not isinstance(status, str):
            return VenueSystemState(
                venue="paradex",
                enabled=True,
                checked=True,
                healthy=False,
                status="invalid",
                blocking_reasons=[
                    "Paradex system-state payload did not include a valid status field"
                ],
                notes=[
                    "Paradex live execution is blocked because the public "
                    "system-state payload is malformed."
                ],
            )

        healthy = status.lower() == "ok"
        blocking_reasons = [] if healthy else [f"Paradex system state is {status}"]
        return VenueSystemState(
            venue="paradex",
            enabled=True,
            checked=True,
            healthy=healthy,
            status=status,
            blocking_reasons=blocking_reasons,
            notes=[
                "Paradex public /v1/system/state must report status=ok before "
                "live execution is allowed."
            ],
        )
