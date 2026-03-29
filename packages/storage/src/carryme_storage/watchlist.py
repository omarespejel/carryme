"""Watchlist loading helpers."""

from __future__ import annotations

import json
from pathlib import Path

from carryme_models import FundingPairSpec


def load_watchlist(path: str | Path) -> list[FundingPairSpec]:
    """Load a funding-pair watchlist from JSON."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        pairs = payload.get("pairs")
        if not isinstance(pairs, list):
            raise ValueError("Watchlist JSON object must include a 'pairs' list")
        payload = pairs
    if not isinstance(payload, list):
        raise ValueError("Watchlist JSON must be a list or an object with a 'pairs' list")
    return [FundingPairSpec.model_validate(item) for item in payload]
