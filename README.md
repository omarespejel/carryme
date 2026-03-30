# carryme

`carryme` will be a fee-aware, capacity-aware funding arbitrage control plane.

> **Status**: this repository now provides live funding-opportunity scoring,
> a bounded worker loop with persisted SQLite history, and API endpoints for
> health, fee references, live funding opportunities, ranked/latest/candidate
> history, operator dashboards, and a supervised worker loop for continuous
> polling with signal-aware shutdown.

The current workspace is intentionally narrow:
- `apps/api`: FastAPI endpoints for health, fee references, live scoring, ranked/latest/candidate history, and operator dashboards
- `apps/worker`: worker CLI for deterministic health output, one-shot polling, bounded scheduled loops, and supervised execution
- `packages/models`: shared Pydantic models and settings
- `packages/connectors`: public REST connectors for market data
- `packages/normalizers`: venue-specific symbol, funding, and fee normalization
- `packages/scoring`: funding-arb opportunity scoring and capacity estimation
- `packages/runtime`: shared live fetch/normalize/score orchestration
- `packages/storage`: watchlist loading and persisted opportunity history

## Local development

```bash
uv sync --all-packages --group dev
uv run alembic upgrade head
uv run pytest
uv run ruff check .
uv run mypy $(find src apps packages tests -name '*.py' -type f)
```

## Run the skeleton services

```bash
uv run carryme-api
uv run carryme-worker
uv run carryme-worker --supervise
uv run carryme-worker --observe-executions-supervise
uv run carryme-worker --ready
```

Useful worker modes:

```bash
uv run carryme-worker --once
uv run carryme-worker --iterations 3
uv run carryme-worker --supervise
uv run carryme-worker --supervise --iterations 3
uv run carryme-worker --observe-executions-once
uv run carryme-worker --observe-executions-supervise
uv run carryme-worker --observe-executions-supervise --iterations 3
```

## Hosted deployment

The production target is Render with managed Postgres and split background workers.

1. Blueprint: [render.yaml](render.yaml)
2. Deployment guide: [docs/deploy/render.md](docs/deploy/render.md)
