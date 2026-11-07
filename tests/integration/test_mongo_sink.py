"""End to end: Postgres changes become correct MongoDB state and history; replays are harmless.

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
from pymongo import MongoClient

from deltaflow import analytics
from deltaflow.config import get_settings
from deltaflow.events import ChangeEvent, Op
from deltaflow.runners import kafka_consumer_config
from deltaflow.sinks.mongo import HISTORY_COLLECTION, MongoSink, ensure_history_indexes
from deltaflow.worker import SinkWorker

pytestmark = pytest.mark.integration

T = TypeVar("T")


def _eventually(fetch: Callable[[], T | None], *, timeout: float = 60.0) -> T:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = fetch()
        if result is not None:
            return result
        time.sleep(0.5)
    pytest.fail(f"condition not met within {timeout:.0f}s")


def _stale_create(doc_id: int, lsn: int) -> ChangeEvent:
    row = {"id": doc_id, "email": "stale@example.com", "full_name": "Stale", "city": "Nowhere"}
    return ChangeEvent(
        op=Op.CREATE,
        table="public.customers",
        key={"id": doc_id},
        before=None,
        after=row,
        lsn=lsn,
        ts_ms=0,
    )


def test_changes_reach_mongodb_with_history_and_stale_replays_are_ignored() -> None:
    settings = get_settings()
    client: MongoClient[dict[str, Any]] = MongoClient(
        settings.mongo_url, serverSelectionTimeoutMS=5000
    )
    db = client[settings.mongo_database]
    ensure_history_indexes(db)
    customers = db["customers"]

    consumer = Consumer(
        kafka_consumer_config(settings.kafka_bootstrap_servers, f"it-mongo-{uuid.uuid4().hex[:8]}")
    )
    consumer.subscribe([r"^deltaflow\.public\.customers$"])
    stop = threading.Event()
    failures: list[BaseException] = []

    def run() -> None:
        try:
            SinkWorker(consumer, MongoSink(db)).run(stop)
        except BaseException as exc:  # surfaced by the assertion below
            failures.append(exc)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        email = f"it-{uuid.uuid4().hex[:10]}@example.com"
        with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
            row = conn.execute(
                "INSERT INTO customers (email, full_name, city) VALUES (%s, 'Mongo User', 'Pune') "
                "RETURNING id",
                (email,),
            ).fetchone()
            assert row is not None
            customer_id: int = row[0]

            # 1. insert -> current-state document keyed by the row's own id
            created = _eventually(lambda: customers.find_one({"_id": customer_id, "city": "Pune"}))
            assert created["email"] == email and created["_deleted"] is False
            assert created["_op"] == "c" and isinstance(created["_lsn"], int)

            # 2. update replaces it with a newer LSN
            conn.execute("UPDATE customers SET city = 'Delhi' WHERE id = %s", (customer_id,))
            updated = _eventually(lambda: customers.find_one({"_id": customer_id, "city": "Delhi"}))
            assert updated["_lsn"] > created["_lsn"] and updated["_op"] == "u"

            # 3. an older event replayed later changes nothing and raises nothing
            sink = MongoSink(db)
            sink.write([_stale_create(customer_id, created["_lsn"])])
            assert customers.find_one({"_id": customer_id}) == updated
            assert sink.stale == 1

            # 4. delete is a soft delete: flagged, last row kept, tombstone timestamp for the TTL
            conn.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
            deleted = _eventually(
                lambda: customers.find_one({"_id": customer_id, "_deleted": True})
            )
            assert deleted["email"] == email and deleted["_lsn"] > updated["_lsn"]
            assert "_deleted_at" in deleted

            # 5. a late replay of the old create cannot resurrect it, however late it arrives
            sink.write([_stale_create(customer_id, created["_lsn"])])
            assert customers.find_one({"_id": customer_id})["_deleted"] is True  # type: ignore[index]

        # 6. history holds the whole life of the row, in LSN order, with what changed
        history = list(
            db[HISTORY_COLLECTION]
            .find({"table": "customers", "doc_id": str(customer_id)})
            .sort("lsn")
        )
        assert [h["op"] for h in history] == ["c", "u", "d"]
        assert "city" in history[1]["changed"]
        assert history[0]["after"]["email"] == email  # the original, not the stale replay

        # 7. analytics run against the real server (these use $dateTrunc, MongoDB 5+)
        breakdown = analytics.run_report(db, "breakdown")
        assert any(r["table"] == "customers" and r["op"] == "c" for r in breakdown)
        assert analytics.run_report(db, "rate", unit="minute")
        analytics.run_report(db, "transitions")  # may be empty before any order updates

        assert not failures, f"sink worker crashed: {failures[0]!r}"
    finally:
        stop.set()
        worker.join(timeout=15)
        consumer.close()
        client.close()
