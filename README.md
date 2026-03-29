# carryme

`carryme` will be a fee-aware, capacity-aware funding arbitrage control plane.

> **Status**: this repository now provides live funding-opportunity scoring,
> a bounded worker loop with persisted SQLite history, and API endpoints for
> health, fee references, live funding opportunities, ranked/latest history,
> and an operator dashboard.

The current workspace is intentionally narrow:
- `apps/api`: FastAPI endpoints for health, fee references, live scoring, ranked/latest history, and the operator dashboard
- `apps/worker`: worker CLI for deterministic health output, one-shot polling, and bounded scheduled loops
- `packages/models`: shared Pydantic models and settings
- `packages/connectors`: public REST connectors for market data
- `packages/normalizers`: venue-specific symbol, funding, and fee normalization
- `packages/scoring`: funding-arb opportunity scoring and capacity estimation
- `packages/runtime`: shared live fetch/normalize/score orchestration
- `packages/storage`: watchlist loading and persisted opportunity history

## Local development

```bash
uv sync --all-packages --group dev
uv run pytest
uv run ruff check .
uv run mypy $(find src apps packages tests -name '*.py' -type f)
```

## Run the skeleton services

```bash
uv run carryme-api
uv run carryme-worker
```
