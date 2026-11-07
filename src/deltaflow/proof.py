"""Real-infrastructure wiring for `deltaflow chaos` and `deltaflow bench`.

The logic lives in `chaos.py` and `bench.py` (unit-tested with fakes). This module connects it
to docker compose, Postgres, Kafka Connect, Elasticsearch and MongoDB.
"""

from __future__ import annotations

import subprocess
import time
import uuid
from collections import Counter
from collections.abc import Callable
from functools import partial
from typing import Any

import httpx
import psycopg
from elasticsearch import Elasticsearch
from pymongo import MongoClient

from deltaflow import bench, chaos, connect, loadgen, verify
from deltaflow.config import Settings
from deltaflow.sinks.elasticsearch import index_name

BENCH_PREFIX = "bench-"


# ---- chaos --------------------------------------------------------------------------------


def docker_execute(step: chaos.Step) -> None:
    try:
        subprocess.run(step.command, check=True)
    except FileNotFoundError as exc:
        raise chaos.ChaosError("docker was not found on PATH") from exc
    except subprocess.CalledProcessError as exc:
        raise chaos.ChaosError(
            f"{' '.join(step.command)} failed with exit {exc.returncode}"
        ) from exc


def await_connector(
    settings: Settings,
    *,
    timeout: float,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Wait for the connector to be RUNNING, tolerating Connect being briefly unreachable.

    A connector or task that ends up FAILED after its dependencies restarted is restarted once
    per attempt, which is what an operator would do.
    """
    deadline = clock() + timeout
    last: Exception | None = None
    while True:
        try:
            with httpx.Client(base_url=settings.connect_url, timeout=10.0) as client:
                try:
                    connect.wait_until_running(
                        client, settings.connector_name, timeout=20, restart_after=10
                    )
                    return
                except connect.ConnectError as exc:
                    last = exc
                    if "FAILED" in str(exc):
                        connect.restart_connector(client, settings.connector_name)
        except (httpx.HTTPError, connect.ConnectError) as exc:
            last = exc  # Connect is still starting; try again
        if clock() >= deadline:
            raise chaos.ChaosError(f"connector not RUNNING after {timeout:.0f}s: {last}")
        sleep(2)


def docker_recover(settings: Settings, *, timeout: float) -> None:
    try:
        subprocess.run(["docker", "compose", "up", "-d", *chaos.SERVICES], check=True)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise chaos.ChaosError(f"could not bring services back up: {exc}") from exc
    await_connector(settings, timeout=timeout)


def deep_check(settings: Settings, *, wait: float) -> list[str]:
    """Row-by-row comparison of both sinks with Postgres, retrying while the sinks catch up."""
    es = Elasticsearch(settings.elasticsearch_url)
    mongo: MongoClient[dict[str, Any]] = MongoClient(
        settings.mongo_url, serverSelectionTimeoutMS=10000
    )

    def once() -> list[str]:
        sinks = {
            "elasticsearch": verify.elasticsearch_rows(es),
            "mongodb": verify.mongo_rows(mongo[settings.mongo_database]),
        }
        return verify.deep_mismatches(verify.source_rows(settings.postgres_dsn), sinks)

    try:
        return verify.wait_for(
            once, wait=wait, interval=3.0, sleep=time.sleep, clock=time.monotonic
        )
    finally:
        es.close()
        mongo.close()


def run_chaos_experiment(
    settings: Settings,
    *,
    rate: float,
    duration: float,
    seed: int | None,
    with_kafka: bool,
    recovery_timeout: float,
    baseline: bool,
    say: Callable[[str], None] = print,
) -> chaos.ChaosReport:
    if baseline:
        say("baseline: checking the pipeline is consistent before breaking anything")
        await_connector(settings, timeout=60)
        problems = deep_check(settings, wait=60)
        if problems:
            raise chaos.ChaosError(
                "the pipeline is not consistent before the experiment, so a result would "
                "prove nothing. Fix that first (deltaflow verify --deep --wait 60):\n  "
                + "\n  ".join(problems)
            )

    def generate() -> Counter[str]:
        counts = loadgen.run(settings.postgres_dsn, rate=rate, duration=duration, seed=seed)
        return Counter({op.value: n for op, n in counts.items()})

    return chaos.run_chaos(
        steps=chaos.default_steps(with_kafka=with_kafka),
        load=generate,
        execute=docker_execute,
        recover=lambda: docker_recover(settings, timeout=recovery_timeout),
        check=lambda: deep_check(settings, wait=recovery_timeout),
        say=say,
    )


# ---- bench --------------------------------------------------------------------------------


def run_bench(
    settings: Settings,
    *,
    rows: int,
    batch: int,
    probes: int,
    timeout: float,
    keep: bool,
    warmup: int = 3,
    say: Callable[[str], None] = print,
) -> tuple[list[bench.LatencySummary], bench.ThroughputResult]:
    run_id = uuid.uuid4().hex[:8]
    es = Elasticsearch(settings.elasticsearch_url)
    mongo: MongoClient[dict[str, Any]] = MongoClient(
        settings.mongo_url, serverSelectionTimeoutMS=10000
    )
    customers = mongo[settings.mongo_database]["customers"]
    index = index_name("customers")

    def es_count() -> int:
        es.indices.refresh(index=index, ignore_unavailable=True)
        return int(es.options(ignore_status=404).count(index=index).get("count", 0))

    def mongo_count() -> int:
        return customers.count_documents({"_deleted": False})

    def es_has(row_id: int) -> bool:
        response = es.options(ignore_status=404).get(index=index, id=str(row_id))
        return bool(response.body.get("found"))

    def mongo_has(row_id: int) -> bool:
        return customers.find_one({"_id": row_id, "_deleted": False}, {"_id": 1}) is not None

    try:
        with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:

            def source_count() -> int:
                row = conn.execute("SELECT count(*) FROM customers").fetchone()
                return int(row[0]) if row else 0

            baseline = source_count()
            if es_count() != baseline or mongo_count() != baseline:
                raise bench.BenchError(
                    "the sinks are not caught up with Postgres; run "
                    "`deltaflow verify --wait 60` first so the measurement starts clean"
                )

            say(f"latency: {warmup} warm-up + {probes} probes, one row at a time")
            samples: dict[str, list[float]] = {"elasticsearch": [], "mongodb": []}
            for i in range(warmup + probes):
                row = conn.execute(
                    "INSERT INTO customers (email, full_name, city) "
                    "VALUES (%s, 'Bench Probe', 'Chennai') RETURNING id",
                    (f"{BENCH_PREFIX}{run_id}-p{i}@example.com",),
                ).fetchone()
                assert row is not None
                row_id = int(row[0])
                seen = bench.measure_probe(
                    {
                        "elasticsearch": partial(es_has, row_id),
                        "mongodb": partial(mongo_has, row_id),
                    },
                    timeout=30.0,
                )
                if i >= warmup:
                    for sink, seconds in seen.items():
                        samples[sink].append(seconds)
                time.sleep(0.1)
            latency = [bench.summarise_latency(s, v) for s, v in samples.items()]

            say(f"throughput: writing {rows:,} rows in batches of {batch}")
            after_probes = source_count()

            def write_batches() -> None:
                for start in range(0, rows, batch):
                    conn.execute(
                        "INSERT INTO customers (email, full_name, city) "
                        "SELECT %s::text || g::text || '@example.com', 'Bench User', 'Chennai' "
                        "FROM generate_series(%s::int, %s::int) AS g",
                        (f"{BENCH_PREFIX}{run_id}-r", start, min(start + batch, rows) - 1),
                    )

            expected = after_probes + rows
            try:
                throughput = bench.measure_throughput(
                    rows=rows,
                    write_batches=write_batches,
                    counts={"elasticsearch": es_count, "mongodb": mongo_count},
                    expected={"elasticsearch": expected, "mongodb": expected},
                    timeout=timeout,
                )
            finally:
                if not keep:  # also after a failure, so a broken run leaves no rows behind
                    say("cleaning up the benchmark rows")
                    conn.execute("DELETE FROM customers WHERE email LIKE %s", (f"{BENCH_PREFIX}%",))
                    deadline = time.monotonic() + timeout
                    while (es_count() != baseline or mongo_count() != baseline) and (
                        time.monotonic() < deadline
                    ):
                        time.sleep(1.0)
        return latency, throughput
    finally:
        es.close()
        mongo.close()
