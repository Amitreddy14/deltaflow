"""Readiness: can the platform's dependencies be reached right now?"""

from __future__ import annotations

from fastapi import APIRouter, Response
from pydantic import BaseModel

from deltaflow.api.deps import OpsDep

router = APIRouter(tags=["health"])


class DependencyResponse(BaseModel):
    name: str
    ok: bool
    latency_ms: float
    detail: str


class ReadyResponse(BaseModel):
    status: str
    dependencies: list[DependencyResponse]


@router.get("/ready", response_model=ReadyResponse)
def ready(ops: OpsDep, response: Response) -> ReadyResponse:
    """200 when every dependency answers, 503 otherwise. `/health` stays a pure liveness check."""
    results = ops.readiness()
    all_ok = all(r.ok for r in results)
    if not all_ok:
        response.status_code = 503
    return ReadyResponse(
        status="ready" if all_ok else "not_ready",
        dependencies=[
            DependencyResponse(
                name=r.name, ok=r.ok, latency_ms=round(r.latency_ms, 1), detail=r.detail
            )
            for r in results
        ],
    )
