"""Live opportunity scoring service for the operator API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

import httpx
from carryme_connectors import (
    ConnectorError,
    ExtendedPublicConnector,
    HyperliquidPublicConnector,
    ParadexPublicConnector,
    PublicVenueConnector,
)
from carryme_models import FundingArbOpportunity, NormalizedMarketSnapshot
from carryme_normalizers import get_fee_profile, normalize_market_snapshot
from carryme_scoring import score_funding_pair


class SnapshotFetcher(Protocol):
    """Interface for fetching normalized live market snapshots."""

    async def __call__(self, venue: str, symbol: str) -> NormalizedMarketSnapshot: ...


CONNECTOR_BASE_URLS: dict[str, str] = {
    "extended": "https://api.starknet.extended.exchange",
    "hyperliquid": "https://api.hyperliquid.xyz",
    "paradex": "https://api.prod.paradex.trade",
}


async def fetch_live_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
    """Fetch a live market snapshot and normalize it into canonical form."""

    key = venue.strip().lower()
    base_url = CONNECTOR_BASE_URLS.get(key)
    if base_url is None:
        raise ValueError(f"Unsupported venue: {venue}")

    async with httpx.AsyncClient(base_url=base_url, timeout=15.0) as client:
        connector = _build_connector(key, client)

        stats = await connector.fetch_market_stats(symbol)
        book = await connector.fetch_top_of_book(symbol)

    market = stats.model_copy(update={"top_of_book": book})
    return normalize_market_snapshot(key, market)


@dataclass
class OpportunityService:
    """Application service for live funding opportunity scoring."""

    fetch_snapshot: SnapshotFetcher = fetch_live_snapshot

    async def score_pair(
        self,
        *,
        left_venue: str,
        left_symbol: str,
        left_fee_profile: str,
        right_venue: str,
        right_symbol: str,
        right_fee_profile: str,
    ) -> FundingArbOpportunity:
        left_fee = get_fee_profile(left_venue, left_fee_profile)
        right_fee = get_fee_profile(right_venue, right_fee_profile)
        left, right = await asyncio.gather(
            self.fetch_snapshot(left_venue, left_symbol),
            self.fetch_snapshot(right_venue, right_symbol),
        )
        return score_funding_pair(
            left,
            right,
            left_fee,
            right_fee,
        )


def _build_connector(venue: str, client: httpx.AsyncClient) -> PublicVenueConnector:
    if venue == "extended":
        return ExtendedPublicConnector(client)
    if venue == "hyperliquid":
        return HyperliquidPublicConnector(client)
    if venue == "paradex":
        return ParadexPublicConnector(client)
    raise ValueError(f"Unsupported venue: {venue}")


__all__ = ["ConnectorError", "OpportunityService", "fetch_live_snapshot"]
