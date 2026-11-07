"""FastAPI application factory."""

from fastapi import Depends, FastAPI

from deltaflow import __version__
from deltaflow.api.deps import require_api_key
from deltaflow.api.routes import connector, health, metrics, pipeline, ready
from deltaflow.config import get_settings


class InsecureConfiguration(RuntimeError):
    """Raised at startup when production mode is on but the API would be unauthenticated."""


def create_app() -> FastAPI:
    settings = get_settings()
    locked = settings.auth_required
    if locked and not settings.api_key:
        # Fail closed: a production deploy with a forgotten key must not come up open.
        raise InsecureConfiguration(
            "DELTAFLOW_AUTH_REQUIRED is on but DELTAFLOW_API_KEY is not set; refusing to start"
        )
    app = FastAPI(
        title="Deltaflow",
        version=__version__,
        description="Control plane for the Deltaflow change data capture platform.",
        # The interactive docs advertise every route; keep them off on a public host.
        docs_url=None if locked else "/docs",
        redoc_url=None if locked else "/redoc",
        openapi_url=None if locked else "/openapi.json",
    )
    # /health is the only open route: it reveals nothing and load balancers need it.
    app.include_router(health.router)
    guarded = [Depends(require_api_key)] if locked else []
    for module in (ready, connector, pipeline, metrics):
        app.include_router(module.router, dependencies=guarded)
    return app


app = create_app()
