# carryme-worker

Worker service for polling configured funding-arbitrage watchlists, persisting
scored opportunities, and running bounded or supervised execution loops.

CLI modes:

- `uv run carryme-worker`: print deterministic health metadata
- `uv run carryme-worker --once`: score the configured watchlist once and persist rows
- `uv run carryme-worker --iterations 3`: run a bounded polling loop with backoff
- `uv run carryme-worker --supervise`: run until signalled, with candidate-count logging
