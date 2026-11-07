"""FastAPI application factory."""

from fastapi import FastAPI

from deltaflow import __version__
from deltaflow.api.routes import connector, health, metrics, pipeline, ready


def create_app() -> FastAPI:
    app = FastAPI(
        title="Deltaflow",
        version=__version__,
        description="Control plane for the Deltaflow change data capture platform.",
    )
    for module in (health, ready, connector, pipeline, metrics):
        app.include_router(module.router)
    return app


app = create_app()
