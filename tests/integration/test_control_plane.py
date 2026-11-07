"""The control plane against the real stack (run from the host).

Run:  pytest -m integration
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
import pytest
from fastapi.testclient import TestClient

from deltaflow.api.main import create_app
from deltaflow.config import get_settings

pytestmark = pytest.mark.integration

T = TypeVar("T")


def _eventually(fetch: Callable[[], T | None], *, timeout: float = 60.0) -> T:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = fetch()
        if result is not None:
            return result
        time.sleep(1.0)
    pytest.fail(f"condition not met within {timeout:.0f}s")


def test_readiness_status_and_metrics_reflect_the_live_stack() -> None:
    client = TestClient(create_app())

    ready = client.get("/ready")
    assert ready.status_code == 200, ready.json()
    assert {d["name"] for d in ready.json()["dependencies"]} == {
        "postgres",
        "kafka",
        "connect",
        "elasticsearch",
        "mongodb",
    }

    connector = client.get("/connector").json()
    assert connector["running"] is True and connector["name"] == "shop-postgres"

    text = client.get("/metrics").text
    assert 'deltaflow_dependency_up{dependency="kafka"} 1.0' in text
    assert 'deltaflow_connector_running{connector="shop-postgres"} 1.0' in text
    assert 'deltaflow_collector_error{collector="lag"} 0.0' in text


def test_lag_rises_with_new_changes_and_drains_to_zero() -> None:
    client = TestClient(create_app())
    settings = get_settings()
    email = f"it-{uuid.uuid4().hex[:10]}@example.com"
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO customers (email, full_name, city) VALUES (%s, 'Lag', 'X')", (email,)
        )
        conn.execute("DELETE FROM customers WHERE email = %s", (email,))

    def drained() -> dict[str, Any] | None:
        body = client.get("/lag").json()
        return body if all(sink["total"] == 0 for sink in body.values()) else None

    body = _eventually(drained)

    assert set(body) == {"es", "mongo"}
    assert any(p["topic"] == "deltaflow.public.customers" for p in body["es"]["partitions"])
    assert client.get("/dlq").status_code == 200


def test_pause_and_resume_the_connector() -> None:
    client = TestClient(create_app())

    def state() -> str:
        return str(client.get("/connector").json()["state"])

    try:
        assert client.post("/connector/pause").status_code == 200
        _eventually(lambda: True if state() == "PAUSED" else None)
    finally:
        assert client.post("/connector/resume").status_code == 200
    _eventually(lambda: True if client.get("/connector").json()["running"] else None)
