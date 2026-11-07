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


def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    """Guard for state-changing endpoints. With no key configured access is open (local dev)."""
    expected = get_settings().api_key
    if not expected:
        return
    if x_api_key is None or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="missing or invalid X-API-Key")


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
