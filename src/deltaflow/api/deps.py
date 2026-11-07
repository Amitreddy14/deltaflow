"""Shared FastAPI dependencies: the operations facade and optional API-key protection."""

from __future__ import annotations

import logging
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from typing import Annotated

import httpx
from fastapi import Depends, Header, HTTPException

from deltaflow import connect
from deltaflow.config import get_settings
from deltaflow.ops import Ops

log = logging.getLogger(__name__)


@lru_cache
def _ops() -> Ops:
    return Ops(get_settings())


def get_ops() -> Ops:
    """Overridden in tests (app.dependency_overrides) with a fake."""
    return _ops()


OpsDep = Annotated[Ops, Depends(get_ops)]


def _presented_key(x_api_key: str | None, authorization: str | None) -> str | None:
    """The key a caller sent: X-API-Key, or ``Authorization: Bearer <key>`` (what Prometheus
    and most HTTP tooling can send)."""
    if x_api_key:
        return x_api_key
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token.strip():
            return token.strip()
    return None


def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """Guard for state-changing endpoints. With no key configured access is open (local dev)."""
    expected = get_settings().api_key
    if not expected:
        return
    presented = _presented_key(x_api_key, authorization)
    if presented is None or not secrets.compare_digest(presented.encode(), expected.encode()):
        raise HTTPException(
            status_code=401,
            detail="missing or invalid API key (X-API-Key or Authorization: Bearer)",
            headers={"WWW-Authenticate": "Bearer"},
        )


@contextmanager
def connect_errors() -> Iterator[None]:
    """Translate Kafka Connect failures into HTTP errors."""
    try:
        yield
    except connect.ConnectorNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except connect.ConnectError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"kafka connect unreachable: {exc}") from exc
