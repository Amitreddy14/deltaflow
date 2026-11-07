from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from confluent_kafka import OFFSET_INVALID, TopicPartition

from deltaflow import ops
from deltaflow.config import Settings
from deltaflow.connect import ConnectError, ConnectorNotFound


def test_one_failing_dependency_is_reported_without_hiding_the_others() -> None:
    def broken() -> None:
        raise ConnectionError("refused")

    results = ops.check_dependencies({"kafka": lambda: None, "mongodb": broken})

    by_name = {r.name: r for r in results}
    assert by_name["kafka"].ok and by_name["kafka"].detail == ""
    assert not by_name["mongodb"].ok and "ConnectionError: refused" in by_name["mongodb"].detail
    assert [r.name for r in results] == ["kafka", "mongodb"]  # order preserved


def test_no_checks_means_no_results() -> None:
    assert ops.check_dependencies({}) == []


class FakeConsumer:
    """Two data topics (3 partitions and 1) plus an unrelated topic the pattern must skip."""

    def __init__(
        self, committed: dict[tuple[str, int], int], ends: dict[tuple[str, int], tuple[int, int]]
    ) -> None:
        self._committed, self._ends = committed, ends
        self.closed = False

    def list_topics(self, timeout: float) -> Any:
        def topic(*partitions: int) -> Any:
            return SimpleNamespace(error=None, partitions=dict.fromkeys(partitions))

        return SimpleNamespace(
            topics={
                "deltaflow.public.orders": topic(0, 1, 2),
                "deltaflow.public.customers": topic(0),
                "deltaflow.dlq.es": topic(0),
                "other": topic(0),
            }
        )

    def committed(self, partitions: list[TopicPartition], timeout: float) -> list[TopicPartition]:
        return [
            TopicPartition(
                tp.topic,
                tp.partition,
                self._committed.get((tp.topic, tp.partition), OFFSET_INVALID),
            )
            for tp in partitions
        ]

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float) -> tuple[int, int]:
        return self._ends.get((tp.topic, tp.partition), (0, 0))

    def close(self) -> None:
        self.closed = True


PATTERN = r"^deltaflow\.public\..+"


def test_lag_is_end_minus_committed_per_partition() -> None:
    consumer = FakeConsumer(
        committed={("deltaflow.public.orders", 0): 90, ("deltaflow.public.orders", 1): 50},
        ends={
            ("deltaflow.public.orders", 0): (0, 100),
            ("deltaflow.public.orders", 1): (0, 50),
            ("deltaflow.public.orders", 2): (10, 40),
            ("deltaflow.public.customers", 0): (0, 7),
        },
    )

    rows = ops.consumer_lag(consumer, "g", PATTERN)

    lag = {(r.topic, r.partition): r.lag for r in rows}
    assert lag[("deltaflow.public.orders", 0)] == 10
    assert lag[("deltaflow.public.orders", 1)] == 0
    assert len(rows) == 4  # the DLQ and unrelated topics are not data topics
    assert ops.total_lag(rows) == 10 + 0 + 30 + 7


def test_a_group_with_no_commit_is_measured_from_the_start_of_the_log() -> None:
    consumer = FakeConsumer(committed={}, ends={("deltaflow.public.orders", 2): (10, 40)})

    rows = ops.consumer_lag(consumer, "g", PATTERN)

    row = next(r for r in rows if r.partition == 2 and r.topic.endswith("orders"))
    assert (row.committed, row.end, row.lag) == (10, 40, 30)  # earliest retained, not 0


def test_no_matching_topics_means_no_rows() -> None:
    assert ops.consumer_lag(FakeConsumer({}, {}), "g", r"^nothing") == []


def test_lag_for_groups_uses_each_group_id_and_closes_consumers() -> None:
    made: list[dict[str, Any]] = []
    consumers: list[FakeConsumer] = []

    def make(config: dict[str, Any]) -> FakeConsumer:
        made.append(config)
        consumers.append(FakeConsumer({}, {}))
        return consumers[-1]

    result = ops.lag_for_groups("kafka:9092", ops.SINK_GROUPS, PATTERN, make_consumer=make)

    assert set(result) == {"es", "mongo"}
    assert [c["group.id"] for c in made] == ["deltaflow-es-sink", "deltaflow-mongo-sink"]
    assert all(c["enable.auto.commit"] is False for c in made)  # never disturbs the real group
    assert all(c.closed for c in consumers)


def test_dlq_depth_counts_messages_and_treats_a_missing_topic_as_empty() -> None:
    consumer = FakeConsumer({}, {("deltaflow.dlq.es", 0): (2, 7)})

    assert ops.dlq_depth(consumer, ["es", "mongo"]) == {"es": 5, "mongo": 0}


# ---- the facade, with a mocked Kafka Connect ----------------------------------------------


def _settings() -> Settings:
    return Settings(connect_url="http://connect:8083", _env_file=None)


def _ops(handler: Any, **kwargs: Any) -> ops.Ops:
    return ops.Ops(
        _settings(),
        checks={},
        client_factory=lambda: httpx.Client(
            base_url="http://connect:8083", transport=httpx.MockTransport(handler)
        ),
        **kwargs,
    )


def _status(connector: str = "RUNNING", task: str = "RUNNING") -> dict[str, Any]:
    return {
        "name": "shop-postgres",
        "connector": {"state": connector, "worker_id": "w"},
        "tasks": [{"id": 0, "state": task, "worker_id": "w"}],
    }


def test_connector_status_reports_running_only_when_every_task_is() -> None:
    healthy = _ops(lambda request: httpx.Response(200, json=_status()))
    assert healthy.connector_status().running is True

    half = _ops(lambda request: httpx.Response(200, json=_status(task="FAILED")))
    state = half.connector_status()
    assert state.running is False and state.tasks[0]["state"] == "FAILED"


def test_unknown_connector_raises_not_found() -> None:
    facade = _ops(lambda request: httpx.Response(404, json={"message": "no"}))

    with pytest.raises(ConnectorNotFound):
        facade.connector_status()


def test_pause_and_resume_call_the_connect_lifecycle_endpoints() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(202)

    facade = _ops(handler)
    facade.pause_connector()
    facade.resume_connector()

    assert seen == [
        ("PUT", "/connectors/shop-postgres/pause"),
        ("PUT", "/connectors/shop-postgres/resume"),
    ]


def test_lifecycle_failures_surface_as_connect_errors() -> None:
    facade = _ops(lambda request: httpx.Response(500, text="boom"))

    with pytest.raises(ConnectError, match="pause"):
        facade.pause_connector()


def test_register_renders_the_packaged_config_and_puts_it() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"], seen["body"] = request.url.path, request.read()
        return httpx.Response(201, json={})

    created = _ops(handler).register_connector()

    assert created is True and seen["path"] == "/connectors/shop-postgres/config"
    assert b"skipped.operations" in seen["body"] and b"${" not in seen["body"]


class _NoKafka:
    def __call__(self, config: dict[str, Any]) -> Any:
        raise RuntimeError("broker unreachable")


def test_pipeline_is_degraded_with_reasons_when_parts_fail() -> None:
    facade = _ops(
        lambda request: httpx.Response(200, json=_status(connector="UNASSIGNED")),
        make_consumer=_NoKafka(),
    )

    summary = facade.pipeline()

    assert summary.healthy is False
    joined = " | ".join(summary.problems)
    assert "UNASSIGNED" in joined
    assert "consumer lag unavailable" in joined and "dead-letter depth unavailable" in joined


def test_pipeline_is_healthy_when_running_with_no_dead_letters() -> None:
    def consumer(config: dict[str, Any]) -> FakeConsumer:
        return FakeConsumer(
            {("deltaflow.public.orders", 0): 100}, {("deltaflow.public.orders", 0): (0, 100)}
        )

    facade = _ops(lambda request: httpx.Response(200, json=_status()), make_consumer=consumer)

    summary = facade.pipeline()

    assert summary.healthy and summary.problems == []
    assert summary.lag == {"es": 0, "mongo": 0} and summary.dead_letters == {"es": 0, "mongo": 0}


def test_dead_letters_make_the_pipeline_degraded() -> None:
    def consumer(config: dict[str, Any]) -> FakeConsumer:
        return FakeConsumer({}, {("deltaflow.dlq.es", 0): (0, 3)})

    facade = _ops(lambda request: httpx.Response(200, json=_status()), make_consumer=consumer)

    summary = facade.pipeline()

    assert not summary.healthy and "3 dead letter(s) in the es DLQ" in summary.problems
