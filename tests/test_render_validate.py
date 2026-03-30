from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from carryme_dev.render import build_render_validation_report


def test_render_validation_requires_database_url() -> None:
    report = build_render_validation_report({}, ping_database=False)

    assert report["status"] == "degraded"
    assert report["required_env"] == {
        "missing": [
            "DATABASE_URL",
            "CARRYME_API_ENVIRONMENT",
            "CARRYME_WORKER_ENVIRONMENT",
        ],
        "warnings": [],
    }


def test_render_validation_accepts_sqlite_database_url_for_smoke(tmp_path: Path) -> None:
    target = tmp_path / "render.sqlite3"
    report = build_render_validation_report(
        {
            "DATABASE_URL": f"sqlite:///{target}",
            "CARRYME_API_ENVIRONMENT": "production",
            "CARRYME_WORKER_ENVIRONMENT": "production",
            "CARRYME_API_EXTENDED_LIVE_ENABLED": "false",
            "CARRYME_API_PARADEX_LIVE_ENABLED": "false",
            "CARRYME_API_HYPERLIQUID_LIVE_ENABLED": "false",
        }
    )

    assert report["status"] == "ready"
    assert report["database"] == {
        "target": str(target),
        "ready": True,
        "skipped": False,
        "error": None,
    }
    assert report["api"] == {
        "valid": True,
        "database_target": str(target),
        "error": None,
    }
    assert report["worker"] == {
        "valid": True,
        "database_target": str(target),
        "error": None,
    }


def test_render_validation_flags_missing_live_credentials(tmp_path: Path) -> None:
    report = build_render_validation_report(
        {
            "DATABASE_URL": f"sqlite:///{tmp_path / 'render.sqlite3'}",
            "CARRYME_API_ENVIRONMENT": "production",
            "CARRYME_WORKER_ENVIRONMENT": "production",
            "CARRYME_API_EXTENDED_LIVE_ENABLED": "true",
        }
    )
    live_venues = cast(dict[str, dict[str, Any]], report["live_venues"])
    api_summary = cast(dict[str, Any], report["api"])
    worker_summary = cast(dict[str, Any], report["worker"])

    assert report["status"] == "degraded"
    assert live_venues["extended"] == {
        "enabled": True,
        "missing_env": [
            "CARRYME_API_EXTENDED_API_KEY",
            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
        ],
    }
    assert api_summary["valid"] is True
    assert worker_summary["valid"] is False
    assert "extended_api_key" in str(worker_summary["error"])


def test_render_validation_warns_on_non_production_env(tmp_path: Path) -> None:
    report = build_render_validation_report(
        {
            "DATABASE_URL": f"sqlite:///{tmp_path / 'render.sqlite3'}",
            "CARRYME_API_ENVIRONMENT": "staging",
            "CARRYME_WORKER_ENVIRONMENT": "development",
        },
        ping_database=False,
    )
    required_env = cast(dict[str, list[str]], report["required_env"])

    assert report["status"] == "degraded"
    assert required_env["warnings"] == [
        "CARRYME_API_ENVIRONMENT should be set to production on Render.",
        "CARRYME_WORKER_ENVIRONMENT should be set to production on Render.",
    ]


def test_render_validation_accepts_paradex_bearer_token_alone(tmp_path: Path) -> None:
    report = build_render_validation_report(
        {
            "DATABASE_URL": f"sqlite:///{tmp_path / 'render.sqlite3'}",
            "CARRYME_API_ENVIRONMENT": "production",
            "CARRYME_WORKER_ENVIRONMENT": "production",
            "CARRYME_API_PARADEX_LIVE_ENABLED": "true",
            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS": "0xabc",
            "CARRYME_API_PARADEX_BEARER_TOKEN": "token",
        }
    )
    live_venues = cast(dict[str, dict[str, Any]], report["live_venues"])
    api_summary = cast(dict[str, Any], report["api"])
    worker_summary = cast(dict[str, Any], report["worker"])

    assert report["status"] == "ready"
    assert live_venues["paradex"] == {
        "enabled": True,
        "missing_env": [],
    }
    assert api_summary["valid"] is True
    assert worker_summary["valid"] is True


def test_render_validation_flags_missing_hyperliquid_credentials(tmp_path: Path) -> None:
    report = build_render_validation_report(
        {
            "DATABASE_URL": f"sqlite:///{tmp_path / 'render.sqlite3'}",
            "CARRYME_API_ENVIRONMENT": "production",
            "CARRYME_WORKER_ENVIRONMENT": "production",
            "CARRYME_API_HYPERLIQUID_LIVE_ENABLED": "true",
        }
    )
    live_venues = cast(dict[str, dict[str, Any]], report["live_venues"])
    api_summary = cast(dict[str, Any], report["api"])
    worker_summary = cast(dict[str, Any], report["worker"])

    assert report["status"] == "degraded"
    assert live_venues["hyperliquid"] == {
        "enabled": True,
        "missing_env": [
            "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
            "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
        ],
    }
    assert api_summary["valid"] is True
    assert worker_summary["valid"] is False
    assert "hyperliquid_account_address" in str(worker_summary["error"])


def test_render_validation_honors_empty_explicit_env() -> None:
    with patch.dict(
        "os.environ",
        {
            "DATABASE_URL": "sqlite:///host.sqlite3",
            "CARRYME_API_ENVIRONMENT": "production",
            "CARRYME_WORKER_ENVIRONMENT": "production",
        },
        clear=True,
    ):
        report = build_render_validation_report({}, ping_database=False)

    assert report["status"] == "degraded"
    assert report["required_env"] == {
        "missing": [
            "DATABASE_URL",
            "CARRYME_API_ENVIRONMENT",
            "CARRYME_WORKER_ENVIRONMENT",
        ],
        "warnings": [],
    }
    assert report["database"] == {
        "target": "data/carryme.sqlite3",
        "ready": None,
        "skipped": True,
        "error": None,
    }
