# Carryme Review Best Practices

This file is for Qodo/PR review agents. Keep findings concrete, production-oriented, and tied to changed code.

## Live-Money Launch Safety

Stable launch changes must preserve fail-closed behavior. Missing credentials, disabled live flags, stale snapshots, missing route approvals, ambiguous route identity, unknown venue health, or unobserved live executions should block unattended launch rather than degrade into a best-effort trade.

Bad pattern:

```python
if preflight_error:
    logger.warning("continuing anyway")
```

Good pattern:

```python
if preflight_error:
    return StableCanaryLaunchSummary(status="skipped", detail=str(preflight_error))
```

## Hold Mode Requires Real Close Automation

Opening and holding a hedge is only acceptable when the execution monitor can close or clean up the pair without manual intervention. If stable launch disables immediate close, review that auto-close is enabled, not shadow-only, and has deterministic triggers for edge decay, hold windows, profit giveback, and unsafe pair state.

## Route Approval Is The Authorization Boundary

Do not bypass `RouteApprovalService` for live submissions. Dynamic opportunity discovery can propose routes, but live execution still needs exact route approval covering label, canonical symbol, short venue, long venue, fee profiles, and max live notional.

## Funding And Fee Math Must Be Explicit

Funding edges must clearly distinguish entry-only edge from round-trip edge. Fee profiles, slippage, capacity, daily volume, and open interest filters must be explicit. Do not treat a positive gross funding spread as profitable without net costs.

## Venue Symbols And Units Are High Risk

Review symbol normalization, market names, funding-rate cadence, collateral units, and venue-specific precision carefully. Extended, Paradex, and Hyperliquid payloads can differ in symbol format and units.

## Stale Data Must Not Launch Trades

Launch-ready and stable-launch paths must reject stale approved snapshots and stale launch-ready snapshots. API endpoints used for operations should expose freshness fields clearly enough for an operator or agent to detect stale state.

## Scans Must Be Bounded

Universe and canary scans should shortlist before expensive orderbook snapshots where possible. API list endpoints should cap limits and avoid returning full nested payloads by default when a summary is enough.

## Tests Must Cover Money-Moving Behavior

Any PR that changes launch, close, cleanup, approval, or readiness behavior needs targeted tests for both allowed and blocked paths. Prefer deterministic store-backed tests over network-dependent tests.
