# Codex Agent Instructions

This repository is the `carryme` live funding-arbitrage control plane. Before changing code, read `.codex/START_HERE.md` and treat it as the active agent handoff for production automation work.

## Safety Rules

- Do not submit live-money changes without tests that cover the launch or close path being changed.
- Do not remove live-execution guards to make a trade go through faster.
- Do not widen notional caps, cooldowns, or approval scope without calling it out in the PR body.
- Do not assume a route is profitable because a raw funding edge is positive; account for fees, slippage, liquidity, launch freshness, and execution-monitor close behavior.
- Prefer fail-closed behavior for missing credentials, stale snapshots, unknown venue status, missing monitor state, or ambiguous route approvals.

## Production Pipeline

The hosted production path is split across Render workers:

1. `carryme-approved-canary-scan` refreshes approved live route snapshots.
2. `carryme-launch-ready-cache` converts fresh approved snapshots into launch-ready snapshots after launch gates pass.
3. `carryme-stable-launch` launches the latest stable launch-ready route.
4. `carryme-execution-monitor` observes live executions and applies close/cleanup automation when enabled.

For automated profit capture, all four stages must be healthy. A green scan/cache stage does not mean money will move.

## PR Discipline

- Keep branches scoped to one production risk or capability.
- Use TDD for behavior changes: add or update failing tests first, then implement.
- Run at least the targeted test file and `uv run ruff check .` before pushing.
- For broad launch/monitor changes, also run `uv run mypy src apps packages tests` when practical.
- Include exact validation commands and any skipped checks in the PR body.
