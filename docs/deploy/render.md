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

Before you host this, rotate any venue secrets that were ever pasted into chat or temporary notes. Hosted rollout should start from fresh keys.

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

## Service Secret Matrix

Use one shared environment group for common non-secret values, then attach only the required venue secrets to the services that actually need them.

### Shared Environment Group

Set these once in a shared Render environment group:

1. `CARRYME_API_ENVIRONMENT=production`
2. `CARRYME_WORKER_ENVIRONMENT=production`
3. `CARRYME_API_HOST=0.0.0.0`
4. `CARRYME_API_EXTENDED_LIVE_ENABLED=false`
5. `CARRYME_API_PARADEX_LIVE_ENABLED=false`
6. `CARRYME_API_HYPERLIQUID_LIVE_ENABLED=false`
7. optional webhook URLs

Do not put `DATABASE_URL` in the group. Render injects that per service from the managed Postgres instance.

### Per-Service Matrix

| Service | Needs `DATABASE_URL` | Needs venue enable flags | Needs live trading secrets | Needs alert webhooks |
|---|---|---|---|---|
| `carryme-api` | yes | yes | all venue secrets if you want operator-triggered live actions from the API | no |
| `carryme-universe-scan` | yes | no | no | no |
| `carryme-approved-canary-scan` | yes | yes | no | no |
| `carryme-launch-ready-cache` | yes | yes | no | `CARRYME_WORKER_STABLE_LAUNCH_READY_ALERT_WEBHOOK_URL` |
| `carryme-stable-launch` | yes | yes | yes, for the venues you intend to launch live on | no |
| `carryme-system-state` | yes | no | no | `CARRYME_WORKER_SYSTEM_STATE_ALERT_WEBHOOK_URL` |
| `carryme-execution-monitor` | yes | yes | yes, for the venues you intend to reconcile live on | `CARRYME_WORKER_EXECUTION_ALERT_WEBHOOK_URL` |

### Venue Secret Ownership

Use this to avoid over-sharing secrets across workers:

| Secret | `carryme-api` | `carryme-stable-launch` | `carryme-execution-monitor` | Other workers |
|---|---|---|---|---|
| `CARRYME_API_EXTENDED_API_KEY` | if API live actions enabled | yes | yes | no |
| `CARRYME_API_EXTENDED_STARK_PRIVATE_KEY` | if API live actions enabled | yes | yes | no |
| `CARRYME_API_PARADEX_ACCOUNT_ADDRESS` | if API live actions enabled | yes | yes | no |
| `CARRYME_API_PARADEX_PRIVATE_KEY` | if API live actions enabled | yes | yes | no |
| `CARRYME_API_PARADEX_BEARER_TOKEN` | optional | optional | optional | no |
| `CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS` | if API live actions enabled | yes | yes | no |
| `CARRYME_API_HYPERLIQUID_VAULT_ADDRESS` | optional | optional | optional | no |
| `CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY` | if API live actions enabled | yes | yes | no |

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

## First Rollout Sequence

This is the sequence I recommend for the first real hosted rollout.

### Phase 1: Infrastructure Only

1. merge [PR #148](https://github.com/omarespejel/carryme/pull/148)
2. merge [PR #150](https://github.com/omarespejel/carryme/pull/150)
3. create the Render Postgres instance
4. apply [render.yaml](/Users/espejelomar/StarkNet/ai-agents-starknet/carryme/render.yaml)
5. attach a shared environment group built from [.env.render.example](/Users/espejelomar/StarkNet/ai-agents-starknet/carryme/.env.render.example)
6. keep all `*_LIVE_ENABLED=false`
7. deploy with no venue secrets yet except what is strictly needed for auth testing

### Phase 2: Green Checks

1. verify `carryme-api` returns `200` on `/ready`
2. verify each worker is in a stable running state in Render logs
3. verify Alembic ran successfully on API boot
4. verify `carryme-universe-scan` is persisting records
5. verify `carryme-approved-canary-scan` and `carryme-launch-ready-cache` run without crashing

### Phase 3: Venue Activation

Enable only `Extended` and `Paradex` first.

1. set `CARRYME_API_EXTENDED_LIVE_ENABLED=true`
2. set `CARRYME_API_PARADEX_LIVE_ENABLED=true`
3. keep `CARRYME_API_HYPERLIQUID_LIVE_ENABLED=false`
4. add only the `Extended` and `Paradex` secrets to:
   - `carryme-api`
   - `carryme-stable-launch`
   - `carryme-execution-monitor`
5. leave `Hyperliquid` out until the hosted `Extended/Paradex` loop proves stable

### Phase 4: Hosted Canary

1. keep route approvals capped at tiny notional
2. do not exceed the current canary size ceiling
3. require stable launch-ready state before launch
4. review execution-monitor logs after every hosted canary
5. do not scale capital until repeated hosted cycles are clean

### Phase 5: Scale Decision

Only increase live notional after all of these are true:

1. hosted `/ready` stays green
2. no recurring worker crashes or reconnect loops
3. no unresolved cleanup/review alerts
4. repeated canary cycles show acceptable realized drag
5. route quality is supported by live hosted evidence, not just public market data

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
   - optionally `carryme-universe-scan` if you want the same non-secret defaults there too
6. leave live venue flags `false` until `/ready` and worker readiness are green
7. only then enable live venue flags and paste trading secrets

## What I Would Actually Deploy First

If you want the shortest sane production path:

1. Render managed Postgres
2. `carryme-api`
3. `carryme-universe-scan`
4. `carryme-approved-canary-scan`
5. `carryme-launch-ready-cache`
6. `carryme-system-state`
7. `carryme-execution-monitor`
8. `carryme-stable-launch` last

Reason:

1. you want observation and candidate materialization live before the launcher can spend money
2. that lets you prove hosted stability before you allow any live submissions
