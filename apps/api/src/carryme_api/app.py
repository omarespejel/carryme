"""FastAPI application factory for carryme."""

import os

from carryme_models import AppDescriptor, ServiceHealth
from fastapi import FastAPI

APP_NAME = "carryme-api"
APP_VERSION = "0.1.0"
DEFAULT_APP_ENVIRONMENT = "development"
APP_ENVIRONMENT_VARIABLE = "CARRYME_API_ENVIRONMENT"


def get_app_environment() -> str:
    """Return the runtime environment exposed by the API health endpoints."""

    return os.getenv(APP_ENVIRONMENT_VARIABLE, DEFAULT_APP_ENVIRONMENT)


def create_app() -> FastAPI:
    """Create the FastAPI application."""

    app = FastAPI(title="carryme", version=APP_VERSION)

    @app.get("/health", response_model=ServiceHealth)
    def health() -> ServiceHealth:
        return ServiceHealth(
            service=AppDescriptor(
                name=APP_NAME,
                version=APP_VERSION,
                environment=get_app_environment(),
            )
        )

    @app.get("/v1/health", response_model=ServiceHealth)
    def versioned_health() -> ServiceHealth:
        return health()

    return app


app = create_app()
