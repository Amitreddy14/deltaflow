"""Prometheus metrics, computed fresh on every scrape."""

from __future__ import annotations

import logging
from collections.abc import Iterator

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from deltaflow.api.deps import OpsDep
from deltaflow.ops import Ops

log = logging.getLogger(__name__)
router = APIRouter(tags=["metrics"])


class DeltaflowCollector(Collector):
    """Reads the live state of the platform. A part that fails sets its error gauge, not a 500."""

    def __init__(self, ops: Ops) -> None:
        self._ops = ops

    def collect(self) -> Iterator[GaugeMetricFamily]:
        errors = GaugeMetricFamily(
            "deltaflow_collector_error",
            "1 if this part of the scrape failed, so its metrics are missing",
            labels=["collector"],
        )

        up = GaugeMetricFamily(
            "deltaflow_dependency_up", "1 if the dependency answered", labels=["dependency"]
        )
        took = GaugeMetricFamily(
            "deltaflow_dependency_check_seconds",
            "How long the dependency check took",
            labels=["dependency"],
        )
        try:
            for result in self._ops.readiness():
                up.add_metric([result.name], 1.0 if result.ok else 0.0)
                took.add_metric([result.name], result.latency_ms / 1000)
            errors.add_metric(["dependencies"], 0)
        except Exception:
            log.exception("dependency checks failed")
            errors.add_metric(["dependencies"], 1)
        yield up
        yield took

        running = GaugeMetricFamily(
            "deltaflow_connector_running",
            "1 if the connector and all its tasks are RUNNING",
            labels=["connector"],
        )
        try:
            state = self._ops.connector_status()
            running.add_metric([state.name], 1.0 if state.running else 0.0)
            errors.add_metric(["connector"], 0)
        except Exception:
            errors.add_metric(["connector"], 1)
        yield running

        lag = GaugeMetricFamily(
            "deltaflow_consumer_lag_messages",
            "Messages a sink's consumer group has yet to process, per topic",
            labels=["sink", "topic"],
        )
        try:
            for sink, rows in self._ops.lag().items():
                per_topic: dict[str, int] = {}
                for row in rows:
                    per_topic[row.topic] = per_topic.get(row.topic, 0) + row.lag
                for topic, value in sorted(per_topic.items()):
                    lag.add_metric([sink, topic], value)
            errors.add_metric(["lag"], 0)
        except Exception:
            errors.add_metric(["lag"], 1)
        yield lag

        dead = GaugeMetricFamily(
            "deltaflow_dlq_messages", "Dead letters held per sink", labels=["sink"]
        )
        try:
            for sink, count in self._ops.dead_letters().items():
                dead.add_metric([sink], count)
            errors.add_metric(["dlq"], 0)
        except Exception:
            errors.add_metric(["dlq"], 1)
        yield dead
        yield errors


@router.get("/metrics", include_in_schema=False)
def metrics(ops: OpsDep) -> Response:
    registry = CollectorRegistry()
    registry.register(DeltaflowCollector(ops))
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
