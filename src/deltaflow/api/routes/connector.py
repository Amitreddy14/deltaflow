"""Steer the Debezium connector. Reading is open; changing it needs the API key (if configured)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from deltaflow.api.deps import OpsDep, connect_errors, require_api_key
from deltaflow.ops import ConnectorState

router = APIRouter(prefix="/connector", tags=["connector"])
protected = Depends(require_api_key)


class ConnectorResponse(BaseModel):
    name: str
    state: str
    running: bool
    tasks: list[dict[str, Any]]

    @classmethod
    def of(cls, state: ConnectorState) -> ConnectorResponse:
        return cls(name=state.name, state=state.state, running=state.running, tasks=state.tasks)


class ActionResponse(BaseModel):
    action: str
    connector: str
    detail: str = ""


@router.get("", response_model=ConnectorResponse)
def get_connector(ops: OpsDep) -> ConnectorResponse:
    with connect_errors():
        return ConnectorResponse.of(ops.connector_status())


@router.post("/register", response_model=ActionResponse, dependencies=[protected])
def register(ops: OpsDep) -> ActionResponse:
    """Create or update the connector from the packaged config (idempotent)."""
    with connect_errors():
        created = ops.register_connector()
    return ActionResponse(
        action="register",
        connector=ops.connector_name,
        detail="created" if created else "updated",
    )


@router.post("/pause", response_model=ActionResponse, dependencies=[protected])
def pause(ops: OpsDep) -> ActionResponse:
    """Stop capturing. Postgres keeps WAL for the replication slot meanwhile: keep pauses short."""
    with connect_errors():
        ops.pause_connector()
    return ActionResponse(action="pause", connector=ops.connector_name)


@router.post("/resume", response_model=ActionResponse, dependencies=[protected])
def resume(ops: OpsDep) -> ActionResponse:
    with connect_errors():
        ops.resume_connector()
    return ActionResponse(action="resume", connector=ops.connector_name)


@router.post("/restart", response_model=ActionResponse, dependencies=[protected])
def restart(ops: OpsDep) -> ActionResponse:
    with connect_errors():
        ops.restart_connector()
    return ActionResponse(action="restart", connector=ops.connector_name)
