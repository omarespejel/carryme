# Render Deployment

`carryme` is ready to host on [Render](https://render.com/) with:

1. one FastAPI web service
2. a managed Postgres database
3. split long-running workers for scans, launch caching, launch execution, system-state, and execution monitoring

## Why Render

For the current system shape, Render is the right first host:

1. managed Postgres is built in
2. long-running background workers are a first-class primitive
3. the current control plane does not need Fly-level network control yet

## Services

The committed [render.yaml](../../render.yaml) defines:

1. `carryme-api`
2. `carryme-universe-scan`
3. `carryme-approved-canary-scan`
4. `carryme-launch-ready-cache`
5. `carryme-stable-launch`
6. `carryme-system-state`
7. `carryme-execution-monitor`
8. `carryme-postgres`

The system deliberately does not deploy the old monolithic supervisor loop as the primary hosted mode. Production cadence is split by responsibility:

1. market scanning
2. approval and launch caching
3. stable launch execution
4. system-state monitoring
5. execution monitoring

## Database

Hosted deployment is now `DATABASE_URL`-first.

Local development still works with a filesystem SQLite path like `data/carryme.sqlite3`.

Hosted deployment should use Postgres:

1. Render injects `DATABASE_URL` from `carryme-postgres`
2. the API and workers accept that directly
3. migrations run through Alembic:

```bash
uv run alembic upgrade head
```

## Required Secrets

Do not commit these. Configure them in Render secrets or an environment group and attach them to the services that need them.

The exact starting template is committed in [`.env.render.example`](../../.env.render.example).

### Extended

1. `CARRYME_API_EXTENDED_LIVE_ENABLED=true`
2. `CARRYME_API_EXTENDED_API_KEY`
3. `CARRYME_API_EXTENDED_STARK_PRIVATE_KEY`

### Paradex

1. `CARRYME_API_PARADEX_LIVE_ENABLED=true`
2. `CARRYME_API_PARADEX_ACCOUNT_ADDRESS`
3. `CARRYME_API_PARADEX_PRIVATE_KEY`
4. Optional: `CARRYME_API_PARADEX_BEARER_TOKEN`

### Hyperliquid

1. `CARRYME_API_HYPERLIQUID_LIVE_ENABLED=true`
2. `CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS`
3. Optional: `CARRYME_API_HYPERLIQUID_VAULT_ADDRESS`
4. `CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY`

### Optional Alert Sinks

1. `CARRYME_WORKER_STABLE_LAUNCH_READY_ALERT_WEBHOOK_URL`
2. `CARRYME_WORKER_SYSTEM_STATE_ALERT_WEBHOOK_URL`
3. `CARRYME_WORKER_EXECUTION_ALERT_WEBHOOK_URL`

## Health and Readiness

The API now exposes:

1. `GET /health`
2. `GET /v1/health`
3. `GET /ready`
4. `GET /v1/ready`

`/ready` performs a real database ping and returns `503` when the service should not receive traffic.

Workers expose a CLI readiness probe:

```bash
uv run carryme-worker --ready
```

## Local Smoke Before Deploy

```bash
uv sync --all-packages --group dev
uv run alembic upgrade head
uv run carryme-api
uv run carryme-worker --ready
uv run carryme-render-validate --skip-db-ping
```

To validate a real hosted-style database target before deploy:

```bash
export DATABASE_URL=postgresql+psycopg://...
export CARRYME_API_ENVIRONMENT=production
export CARRYME_WORKER_ENVIRONMENT=production
uv run carryme-render-validate
```

## Production Rollout Order

1. create the Render Postgres instance
2. apply the Blueprint
3. set live-trading secrets
4. verify `carryme-api /ready`
5. verify `uv run carryme-worker --ready` locally against the same Postgres URL if needed
6. keep live notional tiny until hosted canary cycles prove stable

## Render Console Checklist

1. create the Render project
2. provision the managed Postgres instance first
3. apply [render.yaml](../../render.yaml)
4. create one shared environment group from [`.env.render.example`](../../.env.render.example)
5. attach that group to:
   - `carryme-api`
   - `carryme-approved-canary-scan`
   - `carryme-launch-ready-cache`
   - `carryme-stable-launch`
   - `carryme-system-state`
   - `carryme-execution-monitor`
6. leave live venue flags `false` until `/ready` and worker readiness are green
7. only then enable live venue flags and paste trading secrets
