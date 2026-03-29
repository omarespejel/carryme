# carryme-worker

Worker service for polling configured funding-arbitrage watchlists, persisting
scored opportunities, and running bounded or supervised execution and
observation loops.

CLI modes:

- `uv run carryme-worker`: print deterministic health metadata
- `uv run carryme-worker --once`: score the configured watchlist once and persist rows
- `uv run carryme-worker --iterations 3`: run a bounded polling loop with backoff
- `uv run carryme-worker --supervise`: run the polling loop until signalled
- `uv run carryme-worker --supervise --iterations 3`: run the supervised polling loop for 3 cycles
- `uv run carryme-worker --observe-executions-once`: observe recent live executions once
- `uv run carryme-worker --observe-executions-supervise`: run the execution monitor until signalled
- `uv run carryme-worker --observe-executions-supervise --iterations 3`: run the execution monitor for 3 cycles
