"""Watchlist loading helpers."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from carryme_models import FundingPairSpec, WatchlistDocument


def _parse_watchlist_payload(payload: object) -> list[FundingPairSpec]:
    """Parse a raw watchlist payload into validated funding pairs."""

    if isinstance(payload, dict):
        if "pairs" not in payload:
            raise ValueError("Watchlist JSON must be a list or an object with a 'pairs' list")
        payload = payload["pairs"]
    if not isinstance(payload, list):
        raise ValueError("Watchlist JSON must be a list or an object with a 'pairs' list")
    return [FundingPairSpec.model_validate(item) for item in payload]


def load_watchlist(path: str | Path) -> list[FundingPairSpec]:
    """Load a funding-pair watchlist from JSON."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return _parse_watchlist_payload(payload)


def save_watchlist(path: str | Path, pairs: list[FundingPairSpec]) -> list[FundingPairSpec]:
    """Persist a funding-pair watchlist atomically and return the saved pairs."""

    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    document = WatchlistDocument(pairs=pairs)
    payload = json.dumps(document.model_dump(mode="json"), indent=2)

    with tempfile.NamedTemporaryFile(
        "w",
        dir=target_path.parent,
        prefix=f".{target_path.name}.",
        suffix=".tmp",
        delete=False,
        encoding="utf-8",
    ) as temporary_file:
        temporary_file.write(f"{payload}\n")
        temporary_file.flush()
        os.fsync(temporary_file.fileno())
        temporary_path = Path(temporary_file.name)

    temporary_path.replace(target_path)
    return document.pairs


class WatchlistStore:
    """File-backed watchlist storage with atomic replacement semantics."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> list[FundingPairSpec]:
        """Load the current watchlist from disk."""

        return load_watchlist(self.path)

    def replace(self, pairs: list[FundingPairSpec]) -> list[FundingPairSpec]:
        """Atomically replace the current watchlist with the provided pairs."""

        return save_watchlist(self.path, pairs)
