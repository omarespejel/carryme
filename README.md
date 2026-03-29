# carryme

`carryme` will be a fee-aware, capacity-aware funding arbitrage control plane.

> **Status**: this repository currently contains only the workspace skeleton and
> health endpoints.

The current workspace is intentionally narrow:
- `apps/api`: FastAPI control-plane skeleton with health endpoints
- `apps/worker`: worker CLI skeleton that prints a deterministic health payload
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
