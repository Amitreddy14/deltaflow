"""Liveness endpoint: the process is up. Dependency checks live in `/ready`."""

from fastapi import APIRouter
from pydantic import BaseModel

from deltaflow import __version__
from deltaflow.config import get_settings

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    version: str
    environment: str


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", version=__version__, environment=settings.environment)
