from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from deltaflow.api.deps import get_ops
from deltaflow.api.main import create_app
from deltaflow.config import get_settings
from deltaflow.connect import ConnectError, ConnectorNotFound
from deltaflow.ops import ConnectorState, DependencyStatus, LagRow, PipelineSummary


class FakeOps:
    connector_name = "shop-postgres"

    def __init__(self) -> None:
        self.deps = [DependencyStatus("kafka", True, 4.0), DependencyStatus("mongodb", True, 2.0)]
        self.state = ConnectorState(
            "shop-postgres", "RUNNING", True, [{"id": 0, "state": "RUNNING"}]
        )
        self.error: Exception | None = None
        self.calls: list[str] = []
        self.lag_rows = {
            "es": [LagRow("deltaflow-es-sink", "deltaflow.public.orders", 0, 90, 100, 10)],
            "mongo": [],
        }
        self.dlq = {"es": 2, "mongo": 0}

    def readiness(self) -> list[DependencyStatus]:
        return self.deps

    def connector_status(self) -> ConnectorState:
        if self.error:
            raise self.error
        return self.state

    def _act(self, name: str) -> None:
        if self.error:
            raise self.error
        self.calls.append(name)

    def register_connector(self) -> bool:
        self._act("register")
        return True

    def pause_connector(self) -> None:
        self._act("pause")

    def resume_connector(self) -> None:
        self._act("resume")

    def restart_connector(self) -> None:
        self._act("restart")

    def lag(self) -> dict[str, list[LagRow]]:
        return self.lag_rows

    def dead_letters(self) -> dict[str, int]:
        return self.dlq

    def pipeline(self) -> PipelineSummary:
        return PipelineSummary(
            False, self.state, {"es": 10, "mongo": 0}, self.dlq, ["2 dead letter(s)"]
        )


@pytest.fixture
def fake() -> FakeOps:
    return FakeOps()


@pytest.fixture
def client(fake: FakeOps, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.delenv("DELTAFLOW_API_KEY", raising=False)
    get_settings.cache_clear()
    app = create_app()
    app.dependency_overrides[get_ops] = lambda: fake
    yield TestClient(app)
    get_settings.cache_clear()


def test_ready_is_200_when_every_dependency_answers(client: TestClient) -> None:
    response = client.get("/ready")

    assert response.status_code == 200 and response.json()["status"] == "ready"
    assert [d["name"] for d in response.json()["dependencies"]] == ["kafka", "mongodb"]


def test_ready_is_503_and_names_the_failing_dependency(client: TestClient, fake: FakeOps) -> None:
    fake.deps[1] = DependencyStatus("mongodb", False, 3000.0, "ServerSelectionTimeoutError: down")

    response = client.get("/ready")

    assert response.status_code == 503 and response.json()["status"] == "not_ready"
    assert response.json()["dependencies"][1]["detail"].startswith("ServerSelectionTimeoutError")


def test_health_stays_a_plain_liveness_check(client: TestClient, fake: FakeOps) -> None:
    fake.deps = [DependencyStatus("kafka", False, 1.0, "down")]

    assert client.get("/health").status_code == 200  # liveness does not depend on dependencies


def test_connector_status(client: TestClient) -> None:
    body = client.get("/connector").json()

    assert body["name"] == "shop-postgres" and body["running"] is True
    assert body["tasks"] == [{"id": 0, "state": "RUNNING"}]


@pytest.mark.parametrize(
    ("error", "status"),
    [(ConnectorNotFound("nope"), 404), (ConnectError("bad"), 502)],
)
def test_connect_errors_become_http_errors(
    client: TestClient, fake: FakeOps, error: Exception, status: int
) -> None:
    fake.error = error

    assert client.get("/connector").status_code == status
    assert client.post("/connector/pause").status_code == status


def test_unreachable_connect_is_a_502(client: TestClient, fake: FakeOps) -> None:
    import httpx

    fake.error = httpx.ConnectError("refused")

    response = client.get("/connector")

    assert response.status_code == 502 and "unreachable" in response.json()["detail"]


def test_actions_call_the_connector(client: TestClient, fake: FakeOps) -> None:
    for action in ("register", "pause", "resume", "restart"):
        response = client.post(f"/connector/{action}")
        assert response.status_code == 200 and response.json()["action"] == action

    assert fake.calls == ["register", "pause", "resume", "restart"]


def test_state_changing_calls_need_the_key_when_one_is_configured(
    client: TestClient, fake: FakeOps, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DELTAFLOW_API_KEY", "s3cret")
    get_settings.cache_clear()

    assert client.post("/connector/pause").status_code == 401
    assert client.post("/connector/pause", headers={"X-API-Key": "wrong"}).status_code == 401
    assert fake.calls == []  # nothing happened
    assert client.post("/connector/pause", headers={"X-API-Key": "s3cret"}).status_code == 200
    assert client.get("/connector").status_code == 200  # reading stays open
    assert client.get("/pipeline").status_code == 200


def test_pipeline_summarises_status_and_problems(client: TestClient) -> None:
    body = client.get("/pipeline").json()

    assert body["status"] == "degraded" and body["problems"] == ["2 dead letter(s)"]
    assert body["lag"] == {"es": 10, "mongo": 0} and body["dead_letters"]["es"] == 2
    assert body["connector"]["running"] is True


def test_lag_endpoint_lists_partitions_and_totals(client: TestClient) -> None:
    body = client.get("/lag").json()

    assert body["es"]["total"] == 10 and body["es"]["group"] == "deltaflow-es-sink"
    assert body["es"]["partitions"][0] == {
        "topic": "deltaflow.public.orders",
        "partition": 0,
        "committed": 90,
        "end": 100,
        "lag": 10,
    }
    assert body["mongo"]["total"] == 0 and body["mongo"]["partitions"] == []


def test_lag_and_dlq_return_503_when_kafka_is_down(client: TestClient, fake: FakeOps) -> None:
    def boom() -> Any:
        raise RuntimeError("no brokers")

    fake.lag = boom  # type: ignore[method-assign]
    fake.dead_letters = boom  # type: ignore[method-assign]

    assert client.get("/lag").status_code == 503
    assert client.get("/dlq").status_code == 503


def test_dlq_endpoint(client: TestClient) -> None:
    assert client.get("/dlq").json() == {"es": 2, "mongo": 0}


def test_metrics_are_prometheus_text_with_the_expected_series(client: TestClient) -> None:
    response = client.get("/metrics")

    assert response.status_code == 200 and response.headers["content-type"].startswith("text/plain")
    text = response.text
    assert 'deltaflow_dependency_up{dependency="kafka"} 1.0' in text
    assert 'deltaflow_connector_running{connector="shop-postgres"} 1.0' in text
    assert 'deltaflow_consumer_lag_messages{sink="es",topic="deltaflow.public.orders"} 10.0' in text
    assert 'deltaflow_dlq_messages{sink="es"} 2.0' in text
    assert 'deltaflow_collector_error{collector="lag"} 0.0' in text


def test_a_failing_part_sets_its_error_gauge_instead_of_failing_the_scrape(
    client: TestClient, fake: FakeOps
) -> None:
    fake.error = ConnectError("connect down")

    text = client.get("/metrics").text

    assert client.get("/metrics").status_code == 200
    assert 'deltaflow_collector_error{collector="connector"} 1.0' in text
    assert 'deltaflow_collector_error{collector="lag"} 0.0' in text  # others still reported
