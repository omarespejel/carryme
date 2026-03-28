import pytest
from carryme_worker.config import WorkerSettings
from carryme_worker.main import build_health_payload


def test_worker_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARRYME_WORKER_ENVIRONMENT", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_LOG_LEVEL", raising=False)
    monkeypatch.delenv("CARRYME_WORKER_POLL_INTERVAL_SECONDS", raising=False)
    settings = WorkerSettings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.poll_interval_seconds == 30


def test_worker_health_payload() -> None:
    payload = build_health_payload(WorkerSettings(environment="test"))

    assert payload.model_dump() == {
        "service": {
            "name": "carryme-worker",
            "version": "0.1.0",
            "environment": "test",
        },
        "status": "ok",
    }
