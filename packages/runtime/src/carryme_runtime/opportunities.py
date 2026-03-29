"""Live opportunity scoring services shared by the API and worker."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import httpx
from carryme_connectors import (
    ExtendedPublicConnector,
    HyperliquidPublicConnector,
    ParadexPublicConnector,
    PublicVenueConnector,
)
from carryme_models import FundingArbOpportunity, NormalizedMarketSnapshot
from carryme_normalizers import (
    NormalizationError,
    get_fee_profile,
    normalize_market_snapshot,
    normalize_symbol,
)
from carryme_scoring import score_funding_pair


class SnapshotFetcher(Protocol):
    """Interface for fetching normalized live market snapshots."""

    async def __call__(self, venue: str, symbol: str) -> NormalizedMarketSnapshot: ...


ConnectorFactory = Callable[[httpx.AsyncClient], PublicVenueConnector]


VENUE_REGISTRY: dict[str, tuple[str, ConnectorFactory]] = {
    "extended": ("https://api.starknet.extended.exchange", ExtendedPublicConnector),
    "hyperliquid": ("https://api.hyperliquid.xyz", HyperliquidPublicConnector),
    "paradex": ("https://api.prod.paradex.trade", ParadexPublicConnector),
}


class UpstreamDataError(ValueError):
    """Raised when a venue returns malformed or inconsistent market data."""


async def fetch_live_snapshot(venue: str, symbol: str) -> NormalizedMarketSnapshot:
    """Fetch a live market snapshot and normalize it into canonical form."""

    key = venue.strip().lower()
    venue_config = VENUE_REGISTRY.get(key)
    if venue_config is None:
        raise ValueError(f"Unsupported venue: {venue}")
    base_url, _connector_class = venue_config
    expected_identity = normalize_symbol(key, symbol)

    # Connector-level HTTP helpers already apply bounded retry/backoff for
    # transient 429/5xx/transport failures. Avoid duplicating that policy here.
    async with httpx.AsyncClient(base_url=base_url, timeout=15.0) as client:
        connector = _build_connector(key, client)
        stats, book = await asyncio.gather(
            connector.fetch_market_stats(symbol),
            connector.fetch_top_of_book(symbol),
        )

    market = stats.model_copy(update={"top_of_book": book})
    try:
        normalized = normalize_market_snapshot(key, market)
    except NormalizationError as exc:
        raise UpstreamDataError(
            f"Upstream market data could not be normalized for {key}:{symbol}: {exc}"
        ) from exc
    if normalized.identity.canonical_symbol != expected_identity.canonical_symbol:
        raise UpstreamDataError(
            f"Upstream market data symbol mismatch for {key}:{symbol}: "
            "expected "
            f"{expected_identity.canonical_symbol}, got {normalized.identity.canonical_symbol}"
        )
    return normalized


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
        left_identity = normalize_symbol(left_venue, left_symbol)
        right_identity = normalize_symbol(right_venue, right_symbol)
        if left_identity.canonical_symbol != right_identity.canonical_symbol:
            raise ValueError("Funding pairs must share the same canonical symbol")
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
    venue_config = VENUE_REGISTRY.get(venue)
    if venue_config is None:
        raise ValueError(f"Unsupported venue: {venue}")
    _base_url, connector_class = venue_config
    return connector_class(client)


__all__ = ["OpportunityService", "UpstreamDataError", "fetch_live_snapshot"]
