"""Wire real infrastructure (Kafka consumer, destination clients) into a SinkWorker."""

from __future__ import annotations

import logging
import threading

from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from pymongo import MongoClient

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


def _consume_into(
    sink: Sink,
    *,
    name: str,
    bootstrap_servers: str,
    group_id: str,
    topic_pattern: str,
    batch_size: int,
    stop: threading.Event,
) -> None:
    consumer = Consumer(kafka_consumer_config(bootstrap_servers, group_id))
    try:
        consumer.subscribe([topic_pattern])
        log.info("%s started: group=%s topics=%s", name, group_id, topic_pattern)
        SinkWorker(consumer, sink, batch_size=batch_size).run(stop)
    finally:
        consumer.close()
        log.info("%s stopped", name)


def run_es_sink(
    *,
    bootstrap_servers: str,
    elasticsearch_url: str,
    group_id: str,
    topic_pattern: str,
    batch_size: int,
    stop: threading.Event,
) -> None:
    es = Elasticsearch(elasticsearch_url)
    try:
        ensure_index_template(es)
        _consume_into(
            ElasticsearchSink(es),
            name="es-sink",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_pattern=topic_pattern,
            batch_size=batch_size,
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
) -> None:
    client: MongoClient[dict[str, object]] = MongoClient(mongo_url, serverSelectionTimeoutMS=10000)
    try:
        db = client[mongo_database]
        ensure_history_indexes(db)
        _consume_into(
            MongoSink(db),
            name="mongo-sink",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_pattern=topic_pattern,
            batch_size=batch_size,
            stop=stop,
        )
    finally:
        client.close()
