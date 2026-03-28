"""Health and metadata models shared across services."""

from typing import Literal

from pydantic import BaseModel, Field


class AppDescriptor(BaseModel):
    """Static metadata describing a carryme service."""

    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    environment: str = Field(min_length=1)


class ServiceHealth(BaseModel):
    """Minimal health payload returned by services."""

    service: AppDescriptor
    status: Literal["ok"] = "ok"
