# Carryme Production Automation Handoff

Read this before relying on memory or previous chat context.

## Current Goal

Make `carryme` automatically move small live funds between venues when the opportunity is real, route-approved, fresh, launchable, and safely monitorable.

This is not just an execution test. The system must hold profitable hedges long enough to earn funding and must close or clean up automatically when edge decays, profit gives back, hold limits are reached, or pair state becomes unsafe.

## Live Services To Verify

Use Render API or dashboard to verify current status before making launch recommendations:

- `carryme-api`
- `carryme-approved-canary-scan`
- `carryme-launch-ready-cache`
- `carryme-stable-launch`
- `carryme-execution-monitor`
- `carryme-system-state`
- `carryme-universe-scan`
- `carryme-postgres`

As of the agent handoff that introduced this file, `carryme-approved-canary-scan`, `carryme-launch-ready-cache`, and `carryme-stable-launch` were all live on merge commit `f2591db5d0caa13e8377a5be8f0df0245bd71591`. Treat this as historical context only; re-check live state before acting.

## Production Blockers Being Removed

Work through these in separate scoped PRs unless the user asks otherwise:

1. Stable launch profit-hold mode: stable launch must be able to open and hold instead of always passing `close_position=True`.
2. Deployment verification: `carryme-stable-launch` must exist and be live before claiming automation can move money.
3. Launch-ready credential gate: launch-ready must fail closed when selected venues are not live-enabled or are missing required credentials.
4. Dynamic opportunity promotion: better current routes need a safe approval/proposal path instead of being ignored because they are not statically approved.
5. Approved-scan coverage: approved route scanning must not silently skip labels because of the default 5-label limit.
6. Fast universe shortlist: broad scans must shortlist from lightweight market summaries before expensive orderbook snapshots.
7. Lightweight production summaries: API list endpoints must provide bounded summary responses for operations without returning full nested snapshots.

## Live-Money Defaults

- Keep canary caps small until repeated hosted cycles are clean.
- Prefer Extended + Paradex only unless Hyperliquid has been explicitly activated and tested.
- Keep route approvals as the last authorization boundary before live submission.
- Stable launch should fail closed unless execution monitoring and auto-close conditions are configured for held positions.
- Never recommend scaling capital from public market data alone; require hosted evidence from launched and monitored cycles.

## Fast Verification Commands

```bash
uv run ruff check .
uv run mypy src apps packages tests
uv run pytest -q tests/test_worker.py
uv run pytest -q tests/test_api.py tests/test_runtime.py tests/test_storage.py
```

Prefer targeted tests while iterating, then broaden before PR handoff.
