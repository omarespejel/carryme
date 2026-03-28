# carryme

`carryme` is a fee-aware, capacity-aware funding arbitrage control plane.

The initial workspace is intentionally narrow:
- `apps/api`: FastAPI control-plane skeleton
- `apps/worker`: async worker skeleton
- `packages/models`: shared Pydantic models and settings
- `packages/connectors`: public REST connectors for market data
- `packages/normalizers`: venue-specific symbol, funding, and fee normalization

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
