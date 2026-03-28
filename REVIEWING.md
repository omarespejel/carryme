# Review Guidance

This repository is a fee-aware, capacity-aware funding arbitrage control plane.

## Priorities
- correctness over cleverness
- deterministic execution over AI autonomy
- official venue APIs over UI-derived metrics
- explicit assumptions over hidden heuristics
- reproducibility over convenience

## What Reviewers Should Prioritize
1. Funding-unit mistakes
   - Do not mix hourly, 8h, and daily rates.
   - Make cadence and settlement timing explicit.
2. Fee-model mistakes
   - Do not assume UI/interactive fees apply to API trading.
   - Maker/taker, tier, and venue-specific fee modes must be explicit inputs.
3. Symbol-mapping mistakes
   - Do not silently equate symbols across venues.
   - Disputed aliases must be flagged instead of forced into comparability.
4. Capacity and execution mistakes
   - Do not rank opportunities on funding alone.
   - Depth, spread, OI, and stale-book conditions must affect scoring.
5. Data-integrity mistakes
   - Store raw payloads before normalization.
   - Avoid silent fallback behavior when a venue payload changes.
6. Reliability mistakes
   - Async collectors must be idempotent and retry safely.
   - Timeouts, backoff, and stale-data handling matter more than style.

## What Reviewers Should De-emphasize
- style-only comments unless they hide a correctness issue
- speculative abstractions before the system has real load and data
- premature performance rewrites before profiling

## AI-Specific Constraint
LLMs may help with summaries, reporting, and anomaly explanation, but never with the deterministic execution path or risk checks.
