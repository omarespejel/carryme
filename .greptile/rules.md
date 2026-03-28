# Greptile Review Rules

- Prioritize correctness over style.
- Treat fee calculations, funding-rate normalization, and symbol mapping as high-risk logic.
- Flag any silent fallback that can hide venue payload changes or stale market data.
- Prefer comments about deterministic behavior, idempotency, and replayability.
- Do not recommend AI-driven execution logic in the trading or risk path.
- If a change compares venues, verify that cadence, settlement timing, and fee mode are explicit.
