"""Phase 4 against the real stack: dead letters over Kafka, and TRUNCATE in both sinks.

Nothing here touches the shop data. The Kafka test uses private topics outside the
`deltaflow.public.*` pattern the running sinks subscribe to; the TRUNCATE tests use a
throwaway table name. Run:  pytest -m integration
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from elasticsearch import Elasticsearch
from pymongo import MongoClient

from deltaflow import deadletter
from deltaflow.config import get_settings
from deltaflow.events import ChangeEvent, Op
from deltaflow.runners import kafka_consumer_config
from deltaflow.sinks.base import SinkError
from deltaflow.sinks.elasticsearch import (
    TRUNCATE_INDEX,
    ElasticsearchSink,
    ensure_index_template,
    index_name,
)
from deltaflow.sinks.mongo import (
    HISTORY_COLLECTION,
    TRUNCATE_COLLECTION,
    MongoSink,
    ensure_history_indexes,
)
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


def _event_json(lsn: int, table: str = "orders") -> bytes:
    row = {"id": 1}
    body = {
        "op": "c",
        "before": None,
        "after": row,
        "source": {"schema": "public", "table": table, "lsn": lsn},
    }
    return json.dumps(body).encode()


class PoisonSink:
    """In-memory sink that permanently rejects one specific LSN."""

    def __init__(self, poison: int) -> None:
        self.poison = poison
        self.applied: list[int] = []

    def write(self, events: Sequence[ChangeEvent]) -> None:
        if any(e.lsn == self.poison for e in events):
            raise SinkError("mapping conflict")
        self.applied.extend(e.lsn for e in events)


def test_poison_messages_are_quarantined_and_the_rest_flow_through() -> None:
    settings = get_settings()
    suffix = uuid.uuid4().hex[:8]
    source, dlq_topic = f"deltaflow-it.src.{suffix}", f"deltaflow-it.dlq.{suffix}"
    group = f"it-dlq-{suffix}"
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for future in admin.create_topics(
        [NewTopic(source, num_partitions=1, replication_factor=1)]
    ).values():
        future.result()
    deadletter.ensure_topic(admin, dlq_topic)

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for value in (_event_json(1), b"garbage", _event_json(666), _event_json(2)):
        producer.produce(source, key=b'{"id": 1}', value=value)
    assert producer.flush(15) == 0

    consumer = Consumer(kafka_consumer_config(settings.kafka_bootstrap_servers, group))
    consumer.subscribe([source])
    sink = PoisonSink(poison=666)
    queue = deadletter.KafkaDeadLetterQueue(
        Producer({"bootstrap.servers": settings.kafka_bootstrap_servers}), dlq_topic, sink="it"
    )
    stop = threading.Event()
    worker = threading.Thread(
        target=lambda: SinkWorker(consumer, sink, dead_letters=queue).run(stop), daemon=True
    )
    worker.start()
    reader = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"it-dlq-reader-{suffix}",
            "enable.auto.commit": False,
        }
    )
    try:
        letters = _eventually(
            lambda: (
                found
                if len(found := deadletter.read_dead_letters(reader, dlq_topic)) >= 2
                else None
            )
        )
        by_stage = {letter.headers["dlq.stage"]: letter for letter in letters}

        assert sink.applied == [1, 2]  # good events on both sides of the poison went through
        assert by_stage["parse"].value == b"garbage"  # original bytes, untouched
        assert by_stage["sink"].value == _event_json(666)
        assert by_stage["sink"].headers["dlq.source.topic"] == source
        assert by_stage["sink"].headers["dlq.source.offset"] == "2"
        assert "mapping conflict" in by_stage["sink"].headers["dlq.reason"]
    finally:
        stop.set()
        worker.join(timeout=20)
        consumer.close()
        reader.close()

    # every message (good, garbage, poison) is committed: nothing will be read again
    checker = Consumer({**kafka_consumer_config(settings.kafka_bootstrap_servers, group)})
    try:
        (position,) = checker.committed([TopicPartition(source, 0)], timeout=15)
        assert position.offset == 4
    finally:
        checker.close()
        admin.delete_topics([source, dlq_topic])


def _create(table: str, row_id: int, lsn: int) -> ChangeEvent:
    return ChangeEvent(
        op=Op.CREATE,
        table=f"public.{table}",
        key={"id": row_id},
        before=None,
        after={"id": row_id, "name": f"row{row_id}"},
        lsn=lsn,
        ts_ms=int(time.time() * 1000),
    )


def _truncate(table: str, lsn: int) -> ChangeEvent:
    return ChangeEvent(
        op=Op.TRUNCATE,
        table=f"public.{table}",
        key={},
        before=None,
        after=None,
        lsn=lsn,
        ts_ms=int(time.time() * 1000),
    )


def test_truncate_in_elasticsearch_wipes_older_rows_and_blocks_stale_replays() -> None:
    settings = get_settings()
    table = f"it_trunc_{uuid.uuid4().hex[:8]}"
    index = index_name(table)
    es = Elasticsearch(settings.elasticsearch_url)
    try:
        ensure_index_template(es)
        sink = ElasticsearchSink(es)
        sink.load_state()

        sink.write([_create(table, 1, 100), _create(table, 2, 110)])
        sink.write([_truncate(table, 200)])
        sink.write([_create(table, 3, 300), _create(table, 4, 150)])  # 4 is older: superseded
        es.indices.refresh(index=index)
        ids = {
            hit["_id"] for hit in es.search(index=index, query={"match_all": {}})["hits"]["hits"]
        }
        assert ids == {"3"}

        restarted = ElasticsearchSink(es)  # a new process must remember the truncate
        restarted.load_state()
        restarted.write([_create(table, 5, 120)])
        es.indices.refresh(index=index)
        assert es.count(index=index)["count"] == 1
    finally:
        es.options(ignore_status=404).indices.delete(index=index)
        es.options(ignore_status=404).delete(index=TRUNCATE_INDEX, id=table)
        es.close()


def test_truncate_in_mongodb_tombstones_older_rows_and_blocks_stale_replays() -> None:
    settings = get_settings()
    table = f"it_trunc_{uuid.uuid4().hex[:8]}"
    client: MongoClient[dict[str, Any]] = MongoClient(
        settings.mongo_url, serverSelectionTimeoutMS=5000
    )
    db = client[settings.mongo_database]
    try:
        ensure_history_indexes(db)
        sink = MongoSink(db)
        sink.load_state()

        sink.write([_create(table, 1, 100), _create(table, 2, 110)])
        sink.write([_truncate(table, 200)])
        sink.write([_create(table, 3, 300), _create(table, 4, 150)])  # 4 is older: not applied

        live = sorted(d["_id"] for d in db[table].find({"_deleted": False}))
        assert live == [3]
        assert db[table].count_documents({"_deleted": True}) == 2  # rows 1 and 2 tombstoned
        assert db[table].find_one({"_id": 4}) is None

        restarted = MongoSink(db)
        restarted.load_state()
        restarted.write([_create(table, 5, 120)])
        assert db[table].find_one({"_id": 5}) is None

        ops = [h["op"] for h in db[HISTORY_COLLECTION].find({"table": table}).sort("lsn")]
        assert ops.count("t") == 1 and ops.count("c") == 5  # history keeps everything
    finally:
        db.drop_collection(table)
        db[TRUNCATE_COLLECTION].delete_one({"_id": table})
        db[HISTORY_COLLECTION].delete_many({"table": table})
        client.close()
