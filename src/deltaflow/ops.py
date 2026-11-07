"""Operational read-outs: dependency health, consumer lag and dead-letter depth.

Everything here only reads. The API (`deltaflow.api`) and the Prometheus endpoint are thin
layers over these functions, which take their collaborators as arguments so tests can supply fakes.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import httpx
import psycopg
from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from elasticsearch import Elasticsearch
from pymongo import MongoClient

from deltaflow import connect, deadletter
from deltaflow.config import Settings

log = logging.getLogger(__name__)

SINK_GROUPS: Mapping[str, str] = {"es": "deltaflow-es-sink", "mongo": "deltaflow-mongo-sink"}
CHECK_TIMEOUT_SECONDS = 3.0
KAFKA_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class DependencyStatus:
    name: str
    ok: bool
    latency_ms: float
    detail: str = ""


@dataclass(frozen=True, slots=True)
class LagRow:
    group: str
    topic: str
    partition: int
    committed: int  # next offset the group will read
    end: int  # next offset that will be written
    lag: int


Check = Callable[[], None]  # raises on failure


# ---- readiness ----------------------------------------------------------------------------


def _check_postgres(dsn: str) -> None:
    with psycopg.connect(dsn, connect_timeout=int(CHECK_TIMEOUT_SECONDS)) as conn:
        conn.execute("SELECT 1")


def _check_kafka(bootstrap_servers: str) -> None:
    AdminClient({"bootstrap.servers": bootstrap_servers}).list_topics(timeout=CHECK_TIMEOUT_SECONDS)


def _check_connect(url: str) -> None:
    httpx.get(url, timeout=CHECK_TIMEOUT_SECONDS).raise_for_status()


def _check_elasticsearch(url: str) -> None:
    with Elasticsearch(url, request_timeout=CHECK_TIMEOUT_SECONDS, max_retries=0) as es:
        es.info()


def _check_mongo(url: str) -> None:
    client: MongoClient[dict[str, Any]] = MongoClient(
        url, serverSelectionTimeoutMS=int(CHECK_TIMEOUT_SECONDS * 1000)
    )
    try:
        client.admin.command("ping")
    finally:
        client.close()


def default_checks(settings: Settings) -> dict[str, Check]:
    return {
        "postgres": lambda: _check_postgres(settings.postgres_dsn),
        "kafka": lambda: _check_kafka(settings.kafka_bootstrap_servers),
        "connect": lambda: _check_connect(settings.connect_url),
        "elasticsearch": lambda: _check_elasticsearch(settings.elasticsearch_url),
        "mongodb": lambda: _check_mongo(settings.mongo_url),
    }


def check_dependencies(
    checks: Mapping[str, Check], *, clock: Callable[[], float] = time.monotonic
) -> list[DependencyStatus]:
    """Run every check concurrently; one failure never hides the others."""

    def run(name: str, check: Check) -> DependencyStatus:
        started = clock()
        try:
            check()
        except Exception as exc:  # any failure means "not ready": report it, do not crash
            return DependencyStatus(name, False, (clock() - started) * 1000, _short(exc))
        return DependencyStatus(name, True, (clock() - started) * 1000)

    if not checks:
        return []
    with ThreadPoolExecutor(max_workers=len(checks)) as pool:
        futures = [pool.submit(run, name, check) for name, check in checks.items()]
        return [future.result() for future in futures]


def _short(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:200]


# ---- consumer lag -------------------------------------------------------------------------


def consumer_lag(
    consumer: Any, group: str, topic_pattern: str, *, timeout: float = KAFKA_TIMEOUT_SECONDS
) -> list[LagRow]:
    """Lag of one consumer group on every topic matching the pattern.

    `consumer` must use that group id. Reading committed offsets does not join the group, so
    asking never disturbs the real worker. A group that has committed nothing yet is measured
    from the start of the log, which is where a new sink begins reading.
    """
    pattern = re.compile(topic_pattern)
    metadata = consumer.list_topics(timeout=timeout)
    wanted = [
        TopicPartition(name, partition)
        for name, topic in sorted(metadata.topics.items())
        if pattern.match(name) and topic.error is None
        for partition in sorted(topic.partitions)
    ]
    if not wanted:
        return []
    rows = []
    for committed in consumer.committed(wanted, timeout=timeout):
        low, high = consumer.get_watermark_offsets(committed, timeout=timeout)
        position = (
            low if committed.offset == OFFSET_INVALID or committed.offset < 0 else committed.offset
        )
        rows.append(
            LagRow(
                group=group,
                topic=committed.topic,
                partition=committed.partition,
                committed=position,
                end=high,
                lag=max(0, high - position),
            )
        )
    return rows


def lag_for_groups(
    bootstrap_servers: str,
    groups: Mapping[str, str],
    topic_pattern: str,
    *,
    make_consumer: Callable[[dict[str, Any]], Any] = Consumer,
) -> dict[str, list[LagRow]]:
    result: dict[str, list[LagRow]] = {}
    for sink, group in groups.items():
        consumer = make_consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group,
                "enable.auto.commit": False,
            }
        )
        try:
            result[sink] = consumer_lag(consumer, group, topic_pattern)
        finally:
            consumer.close()
    return result


def total_lag(rows: Iterable[LagRow]) -> int:
    return sum(row.lag for row in rows)


# ---- dead-letter depth --------------------------------------------------------------------


def dlq_depth(consumer: Any, sinks: Iterable[str]) -> dict[str, int]:
    """Messages currently held in each sink's dead-letter topic (0 if the topic is absent)."""
    metadata = consumer.list_topics(timeout=KAFKA_TIMEOUT_SECONDS)
    depth: dict[str, int] = {}
    for sink in sinks:
        topic = metadata.topics.get(deadletter.dlq_topic(sink))
        if topic is None or topic.error is not None:
            depth[sink] = 0
            continue
        total = 0
        for partition in topic.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(deadletter.dlq_topic(sink), partition), timeout=KAFKA_TIMEOUT_SECONDS
            )
            total += max(0, high - low)
        depth[sink] = total
    return depth


def dlq_depth_for(
    bootstrap_servers: str,
    sinks: Iterable[str],
    *,
    make_consumer: Callable[[dict[str, Any]], Any] = Consumer,
) -> dict[str, int]:
    consumer = make_consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "deltaflow-ops",
            "enable.auto.commit": False,
        }
    )
    try:
        return dlq_depth(consumer, sinks)
    finally:
        consumer.close()


# ---- the facade the API uses --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ConnectorState:
    name: str
    state: str
    running: bool  # connector and every task RUNNING
    tasks: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PipelineSummary:
    healthy: bool
    connector: ConnectorState | None
    lag: dict[str, int]  # total lag per sink
    dead_letters: dict[str, int]
    problems: list[str]


class Ops:
    """Everything the control plane reads or does, behind one object (easy to fake in tests)."""

    def __init__(
        self,
        settings: Settings,
        *,
        checks: Mapping[str, Check] | None = None,
        client_factory: Callable[[], httpx.Client] | None = None,
        make_consumer: Callable[[dict[str, Any]], Any] = Consumer,
    ) -> None:
        self._settings = settings
        self._checks = checks if checks is not None else default_checks(settings)
        self._client_factory = client_factory or (
            lambda: httpx.Client(base_url=settings.connect_url, timeout=10.0)
        )
        self._make_consumer = make_consumer

    @property
    def connector_name(self) -> str:
        return self._settings.connector_name

    # readiness and lag
    def readiness(self) -> list[DependencyStatus]:
        return check_dependencies(self._checks)

    def lag(self) -> dict[str, list[LagRow]]:
        return lag_for_groups(
            self._settings.kafka_bootstrap_servers,
            SINK_GROUPS,
            self._settings.kafka_topic_pattern,
            make_consumer=self._make_consumer,
        )

    def dead_letters(self) -> dict[str, int]:
        return dlq_depth_for(
            self._settings.kafka_bootstrap_servers, SINK_GROUPS, make_consumer=self._make_consumer
        )

    # connector
    def connector_status(self) -> ConnectorState:
        name = self._settings.connector_name
        with self._client_factory() as client:
            status = connect.connector_status(client, name)
        return ConnectorState(
            name=name,
            state=status["connector"]["state"],
            running=connect.is_running(status),
            tasks=[
                {"id": t["id"], "state": t["state"], "worker_id": t.get("worker_id", "")}
                for t in status.get("tasks", [])
            ],
        )

    def register_connector(self) -> bool:
        name, config = connect.render_connector(
            connect.DEFAULT_CONNECTOR_FILE, connect.template_variables(os.environ)
        )
        with self._client_factory() as client:
            return connect.register_connector(client, name, config)

    def pause_connector(self) -> None:
        with self._client_factory() as client:
            connect.pause_connector(client, self._settings.connector_name)

    def resume_connector(self) -> None:
        with self._client_factory() as client:
            connect.resume_connector(client, self._settings.connector_name)

    def restart_connector(self) -> None:
        with self._client_factory() as client:
            connect.restart_connector(client, self._settings.connector_name)

    # one-call overview
    def pipeline(self) -> PipelineSummary:
        """Connector state, lag and dead letters in one answer; a failed part is a problem."""
        problems: list[str] = []
        connector: ConnectorState | None = None
        lag: dict[str, int] = {}
        dead: dict[str, int] = {}
        try:
            connector = self.connector_status()
            if not connector.running:
                problems.append(f"connector is {connector.state}, not fully RUNNING")
        except (connect.ConnectError, httpx.HTTPError) as exc:
            problems.append(f"connector status unavailable: {exc}")
        try:
            lag = {sink: total_lag(rows) for sink, rows in self.lag().items()}
        except Exception as exc:  # Kafka unreachable, timeouts, ...
            problems.append(f"consumer lag unavailable: {_short(exc)}")
        try:
            dead = self.dead_letters()
            problems += [f"{n} dead letter(s) in the {sink} DLQ" for sink, n in dead.items() if n]
        except Exception as exc:
            problems.append(f"dead-letter depth unavailable: {_short(exc)}")
        return PipelineSummary(not problems, connector, lag, dead, problems)
