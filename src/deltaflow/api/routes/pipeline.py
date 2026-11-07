"""Read-only pipeline views: overall status, consumer lag, dead-letter depth."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deltaflow.api.deps import OpsDep
from deltaflow.ops import total_lag

router = APIRouter(tags=["pipeline"])


class PipelineResponse(BaseModel):
    status: str
    problems: list[str]
    connector: dict[str, Any] | None
    lag: dict[str, int]
    dead_letters: dict[str, int]


class LagPartition(BaseModel):
    topic: str
    partition: int
    committed: int
    end: int
    lag: int


class SinkLag(BaseModel):
    group: str
    total: int
    partitions: list[LagPartition]


@router.get("/pipeline", response_model=PipelineResponse)
def pipeline(ops: OpsDep) -> PipelineResponse:
    """One answer to "is it working?": connector, lag per sink, dead letters, and what is wrong."""
    summary = ops.pipeline()
    connector = summary.connector
    return PipelineResponse(
        status="healthy" if summary.healthy else "degraded",
        problems=summary.problems,
        connector=(
            {"name": connector.name, "state": connector.state, "running": connector.running}
            if connector
            else None
        ),
        lag=summary.lag,
        dead_letters=summary.dead_letters,
    )


@router.get("/lag", response_model=dict[str, SinkLag])
def lag(ops: OpsDep) -> dict[str, SinkLag]:
    """How far each sink's consumer group is behind the end of each topic partition."""
    try:
        by_sink = ops.lag()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"kafka unavailable: {exc}") from exc
    return {
        sink: SinkLag(
            group=rows[0].group if rows else "",
            total=total_lag(rows),
            partitions=[
                LagPartition(
                    topic=r.topic,
                    partition=r.partition,
                    committed=r.committed,
                    end=r.end,
                    lag=r.lag,
                )
                for r in rows
            ],
        )
        for sink, rows in by_sink.items()
    }


@router.get("/dlq", response_model=dict[str, int])
def dead_letters(ops: OpsDep) -> dict[str, int]:
    """Number of dead letters per sink. Inspect them with `deltaflow dlq show <sink>`."""
    try:
        return ops.dead_letters()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"kafka unavailable: {exc}") from exc
