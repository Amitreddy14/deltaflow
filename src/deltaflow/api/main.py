"""FastAPI application factory."""

from fastapi import FastAPI

from deltaflow import __version__
from deltaflow.api.routes import health


def create_app() -> FastAPI:
    app = FastAPI(
        title="Deltaflow",
        version=__version__,
        description="Control plane for the Deltaflow change data capture platform.",
    )
    app.include_router(health.router)
    return app


app = create_app()
