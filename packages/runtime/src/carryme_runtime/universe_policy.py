"""Deterministic symbol-policy tagging for funding-universe selection."""

from __future__ import annotations

from collections.abc import Iterable

POLICY_TAGS_BY_BASE_ASSET: dict[str, set[str]] = {
    "TRUMP": {"meme", "political"},
    "MELANIA": {"meme", "political"},
    "WLFI": {"political"},
    "MON": {"meme"},
    "PENGU": {"meme"},
    "WIF": {"meme"},
    "POPCAT": {"meme"},
    "FARTCOIN": {"meme"},
    "GOAT": {"meme"},
    "MOODENG": {"meme"},
    "PUMP": {"meme"},
    "SPX": {"meme"},
    "MEGA": {"meme"},
}


def policy_tags_for_symbol(canonical_symbol: str) -> list[str]:
    """Return deterministic policy tags for a canonical perp symbol."""

    base_asset = canonical_symbol.split("-", 1)[0].upper()
    return sorted(POLICY_TAGS_BY_BASE_ASSET.get(base_asset, set()))


def passes_symbol_policy(
    canonical_symbol: str,
    *,
    include_symbols: Iterable[str] | None = None,
    exclude_symbols: Iterable[str] | None = None,
    exclude_tags: Iterable[str] | None = None,
) -> bool:
    """Return whether a symbol passes the configured allow/deny policy."""

    normalized_symbol = canonical_symbol.strip().upper()
    include_set = _normalize_items(include_symbols)
    exclude_set = _normalize_items(exclude_symbols)
    exclude_tag_set = _normalize_items(exclude_tags)

    if include_set and normalized_symbol not in include_set:
        return False
    if normalized_symbol in exclude_set:
        return False

    tags = {tag.upper() for tag in policy_tags_for_symbol(normalized_symbol)}
    return not tags & exclude_tag_set


def _normalize_items(items: Iterable[str] | str | None) -> set[str]:
    if items is None:
        return set()
    if isinstance(items, str):
        items = [items]
    return {item.strip().upper() for item in items if item and item.strip()}
