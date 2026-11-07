"""Wire real infrastructure (Kafka consumer, Elasticsearch client) into a SinkWorker."""

from __future__ import annotations

import logging
import threading

from confluent_kafka import Consumer
from elasticsearch import Elasticsearch

from deltaflow.sinks.elasticsearch import ElasticsearchSink, ensure_index_template
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
    consumer = Consumer(kafka_consumer_config(bootstrap_servers, group_id))
    try:
        ensure_index_template(es)
        consumer.subscribe([topic_pattern])
        log.info("es-sink started: group=%s topics=%s", group_id, topic_pattern)
        SinkWorker(consumer, ElasticsearchSink(es), batch_size=batch_size).run(stop)
    finally:
        consumer.close()
        es.close()
        log.info("es-sink stopped")
