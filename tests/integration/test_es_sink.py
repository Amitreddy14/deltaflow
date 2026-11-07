"""End to end: Postgres changes become correct Elasticsearch documents, replays are harmless.

Needs the compose stack and a registered connector (the fixture handles the connector).
Run:  pytest -m integration
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
import pytest
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch, NotFoundError

from deltaflow.config import get_settings
from deltaflow.events import ChangeEvent, Op
from deltaflow.runners import kafka_consumer_config
from deltaflow.sinks.elasticsearch import ElasticsearchSink, ensure_index_template
from deltaflow.worker import SinkWorker

pytestmark = pytest.mark.integration

INDEX = "deltaflow-customers"
T = TypeVar("T")


def _eventually(fetch: Callable[[], T | None], *, timeout: float = 60.0) -> T:
    """Poll until `fetch` returns something other than None."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = fetch()
        if result is not None:
            return result
        time.sleep(0.5)
    pytest.fail(f"condition not met within {timeout:.0f}s")


def _get(es: Elasticsearch, doc_id: int, *, city: str | None = None) -> dict[str, Any] | None:
    try:
        doc: dict[str, Any] = dict(es.get(index=INDEX, id=str(doc_id)))
    except NotFoundError:
        return None
    if city is not None and doc["_source"].get("city") != city:
        return None
    return doc


def _gone(es: Elasticsearch, doc_id: int) -> bool | None:
    return True if _get(es, doc_id) is None else None


def _stale_create(doc_id: int, lsn: int, city: str) -> ChangeEvent:
    row = {"id": doc_id, "email": "stale@example.com", "full_name": "Stale", "city": city}
    return ChangeEvent(
        op=Op.CREATE,
        table="public.customers",
        key={"id": doc_id},
        before=None,
        after=row,
        lsn=lsn,
        ts_ms=0,
    )


def test_changes_reach_elasticsearch_and_stale_replays_are_ignored() -> None:
    settings = get_settings()
    es = Elasticsearch(settings.elasticsearch_url)
    ensure_index_template(es)

    consumer = Consumer(
        kafka_consumer_config(settings.kafka_bootstrap_servers, f"it-es-{uuid.uuid4().hex[:8]}")
    )
    consumer.subscribe([r"^deltaflow\.public\.customers$"])
    stop = threading.Event()
    failures: list[BaseException] = []

    def run() -> None:
        try:
            SinkWorker(consumer, ElasticsearchSink(es)).run(stop)
        except BaseException as exc:  # surfaced by the assertion below
            failures.append(exc)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        email = f"it-{uuid.uuid4().hex[:10]}@example.com"
        with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
            row = conn.execute(
                "INSERT INTO customers (email, full_name, city) VALUES (%s, 'ES User', 'Pune') "
                "RETURNING id",
                (email,),
            ).fetchone()
            assert row is not None
            customer_id: int = row[0]

            # 1. insert is indexed; the document version IS the source LSN
            created = _eventually(lambda: _get(es, customer_id, city="Pune"))
            assert created["_version"] == created["_source"]["cdc_lsn"]
            assert created["_source"]["email"] == email and created["_source"]["cdc_op"] == "c"

            # 2. update replaces the document with a higher version
            conn.execute("UPDATE customers SET city = 'Delhi' WHERE id = %s", (customer_id,))
            updated = _eventually(lambda: _get(es, customer_id, city="Delhi"))
            assert updated["_version"] > created["_version"]
            assert updated["_version"] == updated["_source"]["cdc_lsn"]

            # 3. replaying an OLDER event, or the SAME one again, changes nothing and raises nothing
            sink = ElasticsearchSink(es)
            sink.write([_stale_create(customer_id, created["_version"], "Pune")])
            sink.write([_stale_create(customer_id, updated["_version"], "Pune")])
            after_replay = _get(es, customer_id)
            assert after_replay is not None
            assert after_replay["_source"]["city"] == "Delhi"
            assert after_replay["_version"] == updated["_version"]
            assert sink.stale == 2

            # 4. delete removes the document
            conn.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
            _eventually(lambda: _gone(es, customer_id))

            # 5. a late replay of an old create cannot resurrect it (inside the gc_deletes window)
            sink.write([_stale_create(customer_id, created["_version"], "Pune")])
            assert _get(es, customer_id) is None

        assert not failures, f"sink worker crashed: {failures[0]!r}"
    finally:
        stop.set()
        worker.join(timeout=15)
        consumer.close()
        es.close()
