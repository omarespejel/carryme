"""CLI entrypoint and runtime helpers for the carryme worker."""

import argparse
import asyncio

from carryme_models import AppDescriptor, ServiceHealth

from carryme_worker.config import WorkerSettings
from carryme_worker.poller import PollCycleSummary, poll_watchlist_once

APP_NAME = "carryme-worker"
APP_VERSION = "0.1.0"


def build_health_payload(settings: WorkerSettings) -> ServiceHealth:
    """Build a deterministic worker health payload."""

    return ServiceHealth(
        service=AppDescriptor(
            name=APP_NAME,
            version=APP_VERSION,
            environment=settings.environment,
        )
    )


def build_cycle_payload(summary: PollCycleSummary) -> dict[str, int | str]:
    """Build a deterministic summary payload for one poll cycle."""

    return {
        "watched_pairs": summary.watched_pairs,
        "saved_records": summary.saved_records,
        "failed_records": summary.failed_records,
        "database_path": summary.database_path,
    }


def main() -> None:
    """Run one poll cycle or print worker health."""

    parser = argparse.ArgumentParser(prog="carryme-worker")
    parser.add_argument("--once", action="store_true", help="Poll the configured watchlist once")
    args = parser.parse_args()

    settings = WorkerSettings()
    if args.once:
        summary = asyncio.run(poll_watchlist_once(settings))
        print(build_cycle_payload(summary))
        return

    payload = build_health_payload(settings)
    print(payload.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
