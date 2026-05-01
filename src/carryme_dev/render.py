"""Render deployment validation helpers."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

import yaml
from carryme_api.config import ApiSettings
from carryme_storage.db import Database, redact_database_url
from carryme_worker.config import WorkerSettings

REQUIRED_RENDER_ENV_VARS = (
    "DATABASE_URL",
    "CARRYME_API_ENVIRONMENT",
    "CARRYME_WORKER_ENVIRONMENT",
)

REQUIRED_RENDER_DATABASES = ("carryme-postgres",)

REQUIRED_RENDER_SERVICE_SPECS: dict[str, dict[str, str]] = {
    "carryme-api": {
        "type": "web",
        "start_command": "uv run carryme-api",
    },
    "carryme-universe-scan": {
        "type": "worker",
        "start_command": "uv run carryme-worker --scan-universe-supervise",
    },
    "carryme-approved-canary-scan": {
        "type": "worker",
        "start_command": "uv run carryme-worker --scan-approved-canary-supervise",
    },
    "carryme-launch-ready-cache": {
        "type": "worker",
        "start_command": "uv run carryme-worker --cache-launch-ready-canary-supervise",
    },
    "carryme-stable-launch": {
        "type": "worker",
        "start_command": "uv run carryme-worker --launch-latest-stable-canary-supervise",
    },
    "carryme-system-state": {
        "type": "worker",
        "start_command": "uv run carryme-worker --observe-system-state-supervise",
    },
    "carryme-execution-monitor": {
        "type": "worker",
        "start_command": "uv run carryme-worker --observe-executions-supervise",
    },
}

LIVE_VENUE_ENV_VARS: dict[str, tuple[str, tuple[str, ...]]] = {
    "extended": (
        "CARRYME_API_EXTENDED_LIVE_ENABLED",
        (
            "CARRYME_API_EXTENDED_API_KEY",
            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
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

DEFAULT_DATABASE_PING_TIMEOUT_SECONDS = 5.0


def _is_truthy(value: str | None) -> bool:
    """Return whether an environment value should be treated as enabled."""

    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _paradex_missing_env(resolved_env: Mapping[str, str]) -> list[str]:
    """Return missing Paradex credentials, honoring bearer-token auth."""

    if not _is_truthy(resolved_env.get("CARRYME_API_PARADEX_LIVE_ENABLED")):
        return []

    missing: list[str] = []
    if not resolved_env.get("CARRYME_API_PARADEX_ACCOUNT_ADDRESS"):
        missing.append("CARRYME_API_PARADEX_ACCOUNT_ADDRESS")
    if not (
        resolved_env.get("CARRYME_API_PARADEX_PRIVATE_KEY")
        or resolved_env.get("CARRYME_API_PARADEX_BEARER_TOKEN")
    ):
        missing.append("CARRYME_API_PARADEX_PRIVATE_KEY|CARRYME_API_PARADEX_BEARER_TOKEN")
    return missing


def _discover_render_blueprint_path(start: Path | None = None) -> Path | None:
    """Search the current directory and parents for the repo Render blueprint."""

    current = (start or Path.cwd()).resolve()
    for directory in (current, *current.parents):
        candidate = directory / "render.yaml"
        if candidate.is_file():
            return candidate
    return None


def _load_render_blueprint(blueprint_path: str | Path) -> dict[str, object]:
    """Load one Render blueprint from YAML and normalize to a mapping."""

    path = Path(blueprint_path)
    loaded = yaml.safe_load(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError("render blueprint must be a top-level mapping")
    return loaded


def _parse_render_services(blueprint: Mapping[str, object]) -> dict[str, dict[str, str]]:
    """Extract service name, type, and start command from a parsed Render blueprint."""

    services_raw = blueprint.get("services")
    if not isinstance(services_raw, list):
        return {}

    services: dict[str, dict[str, str]] = {}
    for service_raw in services_raw:
        if not isinstance(service_raw, Mapping):
            continue
        name = service_raw.get("name")
        service_type = service_raw.get("type")
        start_command = service_raw.get("startCommand", "")
        if not isinstance(name, str) or not isinstance(service_type, str):
            continue
        services[name] = {
            "type": service_type,
            "start_command": start_command if isinstance(start_command, str) else "",
        }
    return services


def _command_executes_expected(*, service_type: str, actual: str, expected: str) -> bool:
    """Return whether one Render start command executes the required command."""

    expected_tokens = shlex.split(expected)
    actual_tokens = shlex.split(actual)

    if service_type == "worker":
        return actual_tokens == expected_tokens

    if actual_tokens == expected_tokens:
        return True

    if len(actual_tokens) >= 3 and actual_tokens[0] == "sh" and actual_tokens[1] == "-c":
        script = actual_tokens[2]
        for segment in re.split(r"\s*(?:&&|\|\||;)\s*", script):
            if shlex.split(segment) == expected_tokens:
                return True
    return False


def build_render_blueprint_validation_report(
    blueprint_path: str | Path = "render.yaml",
) -> dict[str, object]:
    """Validate that the checked-in Render blueprint contains every production stage."""

    path = Path(blueprint_path)
    if not path.is_file():
        return {
            "status": "degraded",
            "path": str(path),
            "missing_databases": list(REQUIRED_RENDER_DATABASES),
            "missing_services": list(REQUIRED_RENDER_SERVICE_SPECS),
            "invalid_services": [],
        }

    blueprint = _load_render_blueprint(path)
    services = _parse_render_services(blueprint)
    databases_raw = blueprint.get("databases")
    database_names: set[str] = set()
    if isinstance(databases_raw, list):
        for entry in databases_raw:
            if not isinstance(entry, Mapping):
                continue
            name = entry.get("name")
            if isinstance(name, str):
                database_names.add(name)
    missing_databases = [
        name
        for name in REQUIRED_RENDER_DATABASES
        if name not in database_names
    ]
    missing_services = [
        name for name in REQUIRED_RENDER_SERVICE_SPECS if name not in services
    ]
    invalid_services: list[dict[str, str]] = []
    for name, spec in REQUIRED_RENDER_SERVICE_SPECS.items():
        service = services.get(name)
        if service is None:
            continue
        if service["type"] != spec["type"]:
            invalid_services.append(
                {
                    "name": name,
                    "field": "type",
                    "expected": spec["type"],
                    "actual": service["type"],
                }
            )
        if not _command_executes_expected(
            service_type=spec["type"],
            actual=service["start_command"],
            expected=spec["start_command"],
        ):
            invalid_services.append(
                {
                    "name": name,
                    "field": "startCommand",
                    "expected": spec["start_command"],
                    "actual": service["start_command"],
                }
            )

    return {
        "status": (
            "ready"
            if not missing_databases and not missing_services and not invalid_services
            else "degraded"
        ),
        "path": str(path),
        "missing_databases": missing_databases,
        "missing_services": missing_services,
        "invalid_services": invalid_services,
    }


@contextmanager
def _override_environ(env: Mapping[str, str]) -> Iterator[None]:
    """Temporarily replace process environment variables with an explicit mapping."""

    original_env = os.environ.copy()
    os.environ.clear()
    os.environ.update(env)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original_env)


def _ping_database_with_timeout(database_url: str, timeout_seconds: float) -> None:
    """Ping the configured database with a bounded timeout on every platform."""

    if timeout_seconds <= 0:
        raise ValueError("database_ping_timeout_seconds must be positive")

    if hasattr(signal, "setitimer") and threading.current_thread() is threading.main_thread():
        def _handle_timeout(signum: int, frame: object) -> None:
            _ = signum, frame
            raise TimeoutError(
                f"Database ping timed out after {timeout_seconds:.1f}s"
            )

        previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, _handle_timeout)
        signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
        try:
            Database(database_url).ping()
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
        return

    ping_error: Exception | None = None
    done = threading.Event()

    def _ping_in_background() -> None:
        nonlocal ping_error
        try:
            Database(database_url).ping()
        except Exception as error:  # pragma: no cover
            ping_error = error
        finally:
            done.set()

    thread = threading.Thread(
        target=_ping_in_background,
        name="carryme-render-db-ping",
        daemon=True,
    )
    thread.start()
    if not done.wait(timeout_seconds):
        raise TimeoutError(f"Database ping timed out after {timeout_seconds:.1f}s")
    if ping_error is not None:
        raise ping_error


def build_render_validation_report(
    env: Mapping[str, str] | None = None,
    *,
    ping_database: bool = True,
    database_ping_timeout_seconds: float = DEFAULT_DATABASE_PING_TIMEOUT_SECONDS,
    blueprint_path: str | Path | None = None,
) -> dict[str, object]:
    """Validate the current environment for Render-style deployment."""

    resolved_env = dict(os.environ if env is None else env)
    missing_required = [name for name in REQUIRED_RENDER_ENV_VARS if not resolved_env.get(name)]
    warnings: list[str] = []

    api_error: str | None = None
    worker_error: str | None = None
    api_settings: ApiSettings | None = None
    worker_settings: WorkerSettings | None = None

    with _override_environ(resolved_env):
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

    paradex_missing = _paradex_missing_env(resolved_env)
    live_venues["paradex"] = {
        "enabled": _is_truthy(resolved_env.get("CARRYME_API_PARADEX_LIVE_ENABLED")),
        "missing_env": paradex_missing,
    }
    live_missing.extend(paradex_missing)

    database_ready: bool | None = False
    database_skipped = False
    database_error: str | None = None
    if ping_database:
        if "DATABASE_URL" not in resolved_env:
            database_error = "DATABASE_URL is not set."
        else:
            try:
                _ping_database_with_timeout(
                    resolved_env["DATABASE_URL"],
                    database_ping_timeout_seconds,
                )
                database_ready = True
            except Exception as error:  # pragma: no cover
                database_error = str(error)
    else:
        database_ready = None
        database_skipped = True

    effective_blueprint_path = (
        Path(blueprint_path)
        if blueprint_path is not None
        else _discover_render_blueprint_path()
    )
    blueprint_report = (
        build_render_blueprint_validation_report(effective_blueprint_path)
        if effective_blueprint_path is not None
        else {
            "status": "skipped",
            "path": None,
            "reason": "render.yaml not found from current working directory",
        }
    )

    status = "ready"
    if (
        missing_required
        or warnings
        or api_error is not None
        or worker_error is not None
        or live_missing
        or (database_ready is False)
        or blueprint_report["status"] == "degraded"
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
            "skipped": database_skipped,
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
        "render_blueprint": blueprint_report,
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
