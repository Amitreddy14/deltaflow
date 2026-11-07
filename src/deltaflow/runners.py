"""Wire real infrastructure (Kafka consumer, destination clients) into a SinkWorker."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path

from confluent_kafka import Consumer, Producer

from deltaflow.clients import elasticsearch_client, mongo_client
from deltaflow.config import get_settings
from deltaflow.deadletter import KafkaDeadLetterQueue, dlq_topic, ensure_topic_on
from deltaflow.sinks.base import Sink
from deltaflow.sinks.elasticsearch import ElasticsearchSink, ensure_index_template
from deltaflow.sinks.mongo import MongoSink, ensure_history_indexes
from deltaflow.worker import SinkWorker

log = logging.getLogger(__name__)


def kafka_consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",  # a new sink replays history from the start
        "enable.auto.commit": False,  # offsets are committed by the worker, after the sink ack
        "topic.metadata.refresh.interval.ms": 5000,  # notice topics the connector creates later
    }


def heartbeat_writer(path: str | None) -> Callable[[], None] | None:
    """A function that touches `path`, or None when no heartbeat is configured.

    Failing to write is logged once and then ignored: a read-only file system must not take
    down a healthy worker.
    """
    if not path:
        return None
    target = Path(path)
    warned = False

    def beat() -> None:
        nonlocal warned
        try:
            target.touch()
        except OSError as exc:
            if not warned:
                log.warning("cannot write heartbeat file %s: %s", target, exc)
                warned = True

    return beat


def _consume_into(
    sink: Sink,
    *,
    name: str,
    bootstrap_servers: str,
    group_id: str,
    topic_pattern: str,
    batch_size: int,
    dead_letter_topic: str | None,
    stop: threading.Event,
) -> None:
    consumer = Consumer(kafka_consumer_config(bootstrap_servers, group_id))
    producer: Producer | None = None
    dead_letters: KafkaDeadLetterQueue | None = None
    if dead_letter_topic:
        ensure_topic_on(bootstrap_servers, dead_letter_topic)
        producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "acks": "all"}
        )
        dead_letters = KafkaDeadLetterQueue(producer, dead_letter_topic, sink=name)
    try:
        consumer.subscribe([topic_pattern])
        log.info(
            "%s started: group=%s topics=%s dlq=%s",
            name,
            group_id,
            topic_pattern,
            dead_letter_topic or "off",
        )
        SinkWorker(
            consumer,
            sink,
            batch_size=batch_size,
            dead_letters=dead_letters,
            heartbeat=heartbeat_writer(get_settings().heartbeat_file),
        ).run(stop)
    finally:
        consumer.close()
        if producer is not None:
            producer.flush(5)
        log.info("%s stopped", name)


def run_es_sink(
    *,
    bootstrap_servers: str,
    elasticsearch_url: str,
    group_id: str,
    topic_pattern: str,
    batch_size: int,
    stop: threading.Event,
    dead_letter_topic: str | None = dlq_topic("es"),
) -> None:
    es = elasticsearch_client(elasticsearch_url)
    try:
        ensure_index_template(es)
        sink = ElasticsearchSink(es)
        sink.load_state()
        _consume_into(
            sink,
            name="es-sink",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_pattern=topic_pattern,
            batch_size=batch_size,
            dead_letter_topic=dead_letter_topic,
            stop=stop,
        )
    finally:
        es.close()


def run_mongo_sink(
    *,
    bootstrap_servers: str,
    mongo_url: str,
    mongo_database: str,
    group_id: str,
    topic_pattern: str,
    batch_size: int,
    stop: threading.Event,
    dead_letter_topic: str | None = dlq_topic("mongo"),
) -> None:
    client = mongo_client(mongo_url)
    try:
        db = client[mongo_database]
        ensure_history_indexes(db)
        sink = MongoSink(db)
        sink.load_state()
        _consume_into(
            sink,
            name="mongo-sink",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_pattern=topic_pattern,
            batch_size=batch_size,
            dead_letter_topic=dead_letter_topic,
            stop=stop,
        )
    finally:
        client.close()
