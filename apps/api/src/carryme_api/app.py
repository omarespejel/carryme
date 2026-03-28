"""FastAPI application factory for carryme."""

from carryme_models import AppDescriptor, ServiceHealth
from fastapi import FastAPI

APP_NAME = "carryme-api"
APP_VERSION = "0.1.0"
APP_ENVIRONMENT = "development"


def create_app() -> FastAPI:
    """Create the FastAPI application."""

    app = FastAPI(title="carryme", version=APP_VERSION)

    @app.get("/health", response_model=ServiceHealth)
    def health() -> ServiceHealth:
        return ServiceHealth(
            service=AppDescriptor(
                name=APP_NAME,
                version=APP_VERSION,
                environment=APP_ENVIRONMENT,
            )
        )

    @app.get("/v1/health", response_model=ServiceHealth)
    def versioned_health() -> ServiceHealth:
        return health()

    return app


app = create_app()
