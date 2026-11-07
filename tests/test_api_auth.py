"""Production lock-down: fail-closed startup, protected reads, Bearer keys, no public docs."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient

from deltaflow.api.deps import get_ops
from deltaflow.api.main import InsecureConfiguration, create_app
from deltaflow.config import get_settings
from test_api_control import FakeOps

KEY = "k" * 32


@pytest.fixture
def locked(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[], TestClient]]:
    monkeypatch.setenv("DELTAFLOW_AUTH_REQUIRED", "true")
    monkeypatch.setenv("DELTAFLOW_API_KEY", KEY)
    get_settings.cache_clear()

    def make() -> TestClient:
        app = create_app()
        app.dependency_overrides[get_ops] = lambda: FakeOps()
        return TestClient(app)

    yield make
    get_settings.cache_clear()


def test_refuses_to_start_without_a_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DELTAFLOW_AUTH_REQUIRED", "true")
    monkeypatch.delenv("DELTAFLOW_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        with pytest.raises(InsecureConfiguration):
            create_app()
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("path", ["/ready", "/connector", "/pipeline", "/lag", "/dlq", "/metrics"])
def test_reads_need_the_key_in_production(locked: Callable[[], TestClient], path: str) -> None:
    client = locked()

    anonymous = client.get(path)

    assert anonymous.status_code == 401
    assert anonymous.headers["www-authenticate"] == "Bearer"
    assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get(path, headers={"X-API-Key": KEY}).status_code != 401


def test_health_stays_open(locked: Callable[[], TestClient]) -> None:
    assert locked().get("/health").status_code == 200


def test_bearer_token_is_accepted(locked: Callable[[], TestClient]) -> None:
    client = locked()

    assert client.get("/connector", headers={"Authorization": f"Bearer {KEY}"}).status_code == 200
    assert client.get("/connector", headers={"Authorization": f"bearer {KEY}"}).status_code == 200
    assert client.get("/connector", headers={"Authorization": f"Basic {KEY}"}).status_code == 401
    assert client.get("/connector", headers={"Authorization": "Bearer "}).status_code == 401


def test_docs_and_schema_are_not_published(locked: Callable[[], TestClient]) -> None:
    client = locked()

    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path, headers={"X-API-Key": KEY}).status_code == 404


def test_development_mode_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DELTAFLOW_AUTH_REQUIRED", raising=False)
    monkeypatch.delenv("DELTAFLOW_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        app = create_app()
        app.dependency_overrides[get_ops] = lambda: FakeOps()
        client = TestClient(app)
        assert client.get("/connector").status_code == 200
        assert client.get("/docs").status_code == 200
    finally:
        get_settings.cache_clear()
