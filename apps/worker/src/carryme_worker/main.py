"""CLI entrypoint and runtime helpers for the carryme worker."""

from carryme_models import AppDescriptor, ServiceHealth

from carryme_worker.config import WorkerSettings

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


def main() -> None:
    """Print a startup marker for the worker skeleton."""

    settings = WorkerSettings()
    payload = build_health_payload(settings)
    print(payload.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
