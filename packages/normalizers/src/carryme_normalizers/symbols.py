"""Canonical symbol normalization across venues."""

from __future__ import annotations

from carryme_models.market import MarketStats
from carryme_models.normalization import MarketIdentity, NormalizedMarketSnapshot

from carryme_normalizers.funding import normalize_funding_rate


class NormalizationError(ValueError):
    """Raised when venue-specific market data cannot be normalized."""


def normalize_symbol(venue: str, symbol: str) -> MarketIdentity:
    """Normalize a venue symbol into the shared perpetual identity format."""

    key = venue.strip().lower()
    if key == "extended":
        return normalize_extended_symbol(symbol)
    if key == "paradex":
        return normalize_paradex_symbol(symbol)
    if key == "hyperliquid":
        return normalize_hyperliquid_symbol(symbol)
    raise NormalizationError(f"Unsupported venue: {venue}")


def normalize_market_snapshot(venue: str, market: MarketStats) -> NormalizedMarketSnapshot:
    """Attach canonical identity and funding units to a market snapshot."""

    identity = normalize_symbol(venue, market.symbol)
    funding = normalize_funding_rate(venue, market.funding_rate)
    return NormalizedMarketSnapshot(identity=identity, market=market, funding=funding)


def normalize_extended_symbol(symbol: str) -> MarketIdentity:
    cleaned = symbol.strip().upper()
    parts = cleaned.split("-")
    if len(parts) != 2 or not all(parts):
        raise NormalizationError(f"Invalid Extended perp symbol: {symbol!r}")
    base_asset, quote_asset = parts
    return _identity("extended", cleaned, base_asset, quote_asset)


def normalize_paradex_symbol(symbol: str) -> MarketIdentity:
    cleaned = symbol.strip().upper()
    parts = cleaned.split("-")
    if len(parts) != 3 or not all(parts) or parts[2] != "PERP":
        raise NormalizationError(f"Invalid Paradex perp symbol: {symbol!r}")
    base_asset, quote_asset, _contract = parts
    return _identity("paradex", cleaned, base_asset, quote_asset)


def normalize_hyperliquid_symbol(symbol: str) -> MarketIdentity:
    cleaned = symbol.strip().upper()
    if not cleaned or "/" in cleaned or "-" in cleaned:
        raise NormalizationError(f"Invalid Hyperliquid perp symbol: {symbol!r}")
    return _identity("hyperliquid", cleaned, cleaned, "USD")


def _identity(venue: str, venue_symbol: str, base_asset: str, quote_asset: str) -> MarketIdentity:
    canonical_symbol = f"{base_asset}-{quote_asset}-PERP"
    return MarketIdentity(
        venue=venue,
        venue_symbol=venue_symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        canonical_symbol=canonical_symbol,
    )
