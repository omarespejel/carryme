"""CLI entrypoint for the carryme API service."""

import logging
import os
import subprocess
import time
from collections.abc import Callable, Sequence

import uvicorn

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_MIGRATION_MAX_ATTEMPTS = 5
DEFAULT_MIGRATION_RETRY_DELAY_SECONDS = 2.0
MIGRATION_MAX_ATTEMPTS_ENV = "CARRYME_API_MIGRATION_MAX_ATTEMPTS"
MIGRATION_RETRY_DELAY_SECONDS_ENV = "CARRYME_API_MIGRATION_RETRY_DELAY_SECONDS"
ALEMBIC_UPGRADE_COMMAND = ("alembic", "upgrade", "head")
logger = logging.getLogger(__name__)


def get_server_host() -> str:
    """Return the bind host for the API process."""

    return os.getenv("CARRYME_API_HOST", DEFAULT_HOST)


def get_server_port() -> int:
    """Return the bind port for the API process."""

    raw_port = os.getenv("PORT") or os.getenv("CARRYME_API_PORT")
    if raw_port is None:
        return DEFAULT_PORT
    return int(raw_port)


def _read_positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = int(raw_value)
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _read_non_negative_float_env(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = float(raw_value)
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def get_migration_max_attempts() -> int:
    """Return how many Alembic startup attempts Render should tolerate."""

    return _read_positive_int_env(
        MIGRATION_MAX_ATTEMPTS_ENV,
        DEFAULT_MIGRATION_MAX_ATTEMPTS,
    )


def get_migration_retry_delay_seconds() -> float:
    """Return the delay between failed Alembic startup attempts."""

    return _read_non_negative_float_env(
        MIGRATION_RETRY_DELAY_SECONDS_ENV,
        DEFAULT_MIGRATION_RETRY_DELAY_SECONDS,
    )


def run_alembic_upgrade_with_retries(
    *,
    run: Callable[[Sequence[str]], subprocess.CompletedProcess[object]] = subprocess.run,
    sleep: Callable[[float], None] = time.sleep,
    max_attempts: int | None = None,
    retry_delay_seconds: float | None = None,
) -> None:
    """Run Alembic with bounded retries for transient Render Postgres recovery."""

    attempt_limit = max_attempts if max_attempts is not None else get_migration_max_attempts()
    delay_seconds = (
        retry_delay_seconds
        if retry_delay_seconds is not None
        else get_migration_retry_delay_seconds()
    )
    if attempt_limit < 1:
        raise ValueError("max_attempts must be >= 1")
    if delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be >= 0")

    last_returncode = 1
    for attempt in range(1, attempt_limit + 1):
        completed = run(ALEMBIC_UPGRADE_COMMAND)
        if completed.returncode == 0:
            return
        last_returncode = completed.returncode
        if attempt >= attempt_limit:
            break
        logger.warning(
            "Alembic upgrade failed during API startup attempt %d/%d; "
            "retrying in %.1fs returncode=%d",
            attempt,
            attempt_limit,
            delay_seconds,
            completed.returncode,
        )
        if delay_seconds > 0:
            sleep(delay_seconds)

    logger.error(
        "Alembic upgrade failed during API startup after %d attempts returncode=%d",
        attempt_limit,
        last_returncode,
    )
    raise SystemExit(last_returncode)


def main() -> None:
    """Run the FastAPI service."""

    uvicorn.run(
        "carryme_api.app:app",
        host=get_server_host(),
        port=get_server_port(),
        reload=False,
    )


def migrate_and_start() -> None:
    """Run startup migrations with retry, then serve the API."""

    run_alembic_upgrade_with_retries()
    main()


if __name__ == "__main__":
    main()
