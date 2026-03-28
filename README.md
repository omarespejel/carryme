# carryme

`carryme` will be a fee-aware, capacity-aware funding arbitrage control plane.

> **Status**: this repository currently contains only the workspace skeleton and
> health endpoints.

The current workspace is intentionally narrow:
- `apps/api`: FastAPI control-plane skeleton with health endpoints
- `apps/worker`: worker CLI skeleton that prints a deterministic health payload
- `packages/models`: shared Pydantic models and settings

## Local development

```bash
uv sync --all-packages --group dev
uv run pytest
uv run ruff check .
uv run mypy src apps packages tests
```

## Run the skeleton services

```bash
uv run carryme-api
uv run carryme-worker
```
