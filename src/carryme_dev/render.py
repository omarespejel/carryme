"""Render deployment validation helpers."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from unittest.mock import patch

from carryme_api.config import ApiSettings
from carryme_storage.db import Database, redact_database_url
from carryme_worker.config import WorkerSettings

REQUIRED_RENDER_ENV_VARS = (
    "DATABASE_URL",
    "CARRYME_API_ENVIRONMENT",
    "CARRYME_WORKER_ENVIRONMENT",
)

LIVE_VENUE_ENV_VARS: dict[str, tuple[str, tuple[str, ...]]] = {
    "extended": (
        "CARRYME_API_EXTENDED_LIVE_ENABLED",
        (
            "CARRYME_API_EXTENDED_API_KEY",
            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
        ),
    ),
    "paradex": (
        "CARRYME_API_PARADEX_LIVE_ENABLED",
        (
            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
            "CARRYME_API_PARADEX_PRIVATE_KEY",
        ),
    ),
    "hyperliquid": (
        "CARRYME_API_HYPERLIQUID_LIVE_ENABLED",
        (
            "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
            "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
        ),
    ),
}


def _is_truthy(value: str | None) -> bool:
    """Return whether an environment value should be treated as enabled."""

    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def build_render_validation_report(
    env: Mapping[str, str] | None = None,
    *,
    ping_database: bool = True,
) -> dict[str, object]:
    """Validate the current environment for Render-style deployment."""

    resolved_env = dict(os.environ if env is None else env)
    missing_required = [name for name in REQUIRED_RENDER_ENV_VARS if not resolved_env.get(name)]
    warnings: list[str] = []

    api_error: str | None = None
    worker_error: str | None = None
    api_settings: ApiSettings | None = None
    worker_settings: WorkerSettings | None = None

    with patch.dict(os.environ, resolved_env, clear=True):
        try:
            api_settings = ApiSettings()
        except Exception as error:  # pragma: no cover
            api_error = str(error)
        try:
            worker_settings = WorkerSettings()
        except Exception as error:  # pragma: no cover
            worker_error = str(error)

    database_url = resolved_env.get("DATABASE_URL", "data/carryme.sqlite3")
    api_database_target = (
        api_settings.database_target
        if api_settings is not None
        else redact_database_url(database_url)
    )
    worker_database_target = (
        worker_settings.database_target
        if worker_settings is not None
        else redact_database_url(database_url)
    )

    if resolved_env.get("CARRYME_API_ENVIRONMENT") not in {None, "production"}:
        warnings.append("CARRYME_API_ENVIRONMENT should be set to production on Render.")
    if resolved_env.get("CARRYME_WORKER_ENVIRONMENT") not in {None, "production"}:
        warnings.append("CARRYME_WORKER_ENVIRONMENT should be set to production on Render.")

    live_venues: dict[str, dict[str, object]] = {}
    live_missing: list[str] = []
    for venue, (flag_name, credential_names) in LIVE_VENUE_ENV_VARS.items():
        enabled = _is_truthy(resolved_env.get(flag_name))
        missing = [name for name in credential_names if enabled and not resolved_env.get(name)]
        live_venues[venue] = {
            "enabled": enabled,
            "missing_env": missing,
        }
        live_missing.extend(missing)

    database_ready = False
    database_error: str | None = None
    if ping_database:
        if "DATABASE_URL" not in resolved_env:
            database_error = "DATABASE_URL is not set."
        else:
            try:
                Database(resolved_env["DATABASE_URL"]).ping()
                database_ready = True
            except Exception as error:  # pragma: no cover
                database_error = str(error)
    else:
        database_ready = True

    status = "ready"
    if (
        missing_required
        or warnings
        or api_error is not None
        or worker_error is not None
        or live_missing
        or not database_ready
    ):
        status = "degraded"

    return {
        "status": status,
        "required_env": {
            "missing": missing_required,
            "warnings": warnings,
        },
        "database": {
            "target": api_database_target,
            "ready": database_ready,
            "error": database_error,
        },
        "api": {
            "valid": api_error is None,
            "database_target": api_database_target,
            "error": api_error,
        },
        "worker": {
            "valid": worker_error is None,
            "database_target": worker_database_target,
            "error": worker_error,
        },
        "live_venues": live_venues,
    }


def main() -> None:
    """Validate the current environment for Render deployment and exit non-zero on failure."""

    parser = argparse.ArgumentParser(prog="carryme-render-validate")
    parser.add_argument(
        "--skip-db-ping",
        action="store_true",
        help="Skip the live database connectivity check.",
    )
    args = parser.parse_args()

    report = build_render_validation_report(ping_database=not args.skip_db_ping)
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["status"] != "ready":
        raise SystemExit(1)
