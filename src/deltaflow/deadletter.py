"""Dead-letter queue: quarantine events that can never succeed, without stopping the pipeline.

A message lands here for one of two reasons (ADR-0010):

* `parse`: it is not a change event we understand (bad JSON, missing key or source, ...).
* `sink`: the destination permanently rejected it (for example a mapping conflict).

The original key and value are kept byte for byte, with headers that say where the message came
from and why it was rejected, so it can be inspected and, once the cause is fixed, replayed.
A dead letter is only counted as handled after Kafka acknowledges it; if that fails the worker
stops without committing, so a message is never lost between the source topic and the DLQ.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

log = logging.getLogger(__name__)

DLQ_TOPIC_PREFIX = "deltaflow.dlq"
DLQ_RETENTION_MS = 30 * 24 * 3600 * 1000  # a month to notice and fix; then Kafka expires them
FLUSH_TIMEOUT_SECONDS = 15.0
MAX_REASON_BYTES = 2000

H_REASON = "dlq.reason"
H_STAGE = "dlq.stage"
H_SINK = "dlq.sink"
H_TOPIC = "dlq.source.topic"
H_PARTITION = "dlq.source.partition"
H_OFFSET = "dlq.source.offset"
H_FAILED_AT = "dlq.failed_at_ms"


def dlq_topic(sink: str) -> str:
    return f"{DLQ_TOPIC_PREFIX}.{sink}"


class DeadLetterError(RuntimeError):
    """The dead letter could not be stored. The worker must stop rather than lose the message."""


@dataclass(frozen=True, slots=True)
class DeadLetter:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    reason: str
    stage: str  # "parse" or "sink"


class DeadLetterQueue(Protocol):
    def send(self, letter: DeadLetter) -> None:
        """Store the letter durably, or raise DeadLetterError."""
        ...


class ProducerLike(Protocol):
    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = ...,
        key: bytes | None = ...,
        headers: list[tuple[str, str | bytes | None]] | None = ...,
        on_delivery: Callable[[Any, Any], None] | None = ...,
    ) -> None: ...
    def flush(self, timeout: float) -> int: ...


class KafkaDeadLetterQueue:
    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        *,
        sink: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._sink = sink
        self._clock = clock

    @property
    def topic(self) -> str:
        return self._topic

    def send(self, letter: DeadLetter) -> None:
        failures: list[Any] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                failures.append(error)

        reason = letter.reason.encode()[:MAX_REASON_BYTES]
        headers: list[tuple[str, str | bytes | None]] = [
            (H_REASON, reason),
            (H_STAGE, letter.stage.encode()),
            (H_SINK, self._sink.encode()),
            (H_TOPIC, letter.topic.encode()),
            (H_PARTITION, str(letter.partition).encode()),
            (H_OFFSET, str(letter.offset).encode()),
            (H_FAILED_AT, str(int(self._clock() * 1000)).encode()),
        ]
        try:
            self._producer.produce(
                self._topic,
                key=letter.key,
                value=letter.value,
                headers=headers,
                on_delivery=delivered,
            )
            remaining = self._producer.flush(FLUSH_TIMEOUT_SECONDS)
        except (KafkaException, BufferError) as exc:
            raise DeadLetterError(f"could not publish to {self._topic}: {exc}") from exc
        if remaining or failures:
            detail = failures[0] if failures else "timed out waiting for the broker"
            raise DeadLetterError(f"could not publish to {self._topic}: {detail}")
        log.warning(
            "dead-lettered %s[%d]@%d (%s): %s",
            letter.topic,
            letter.partition,
            letter.offset,
            letter.stage,
            letter.reason,
        )


def ensure_topic(admin: Any, topic: str) -> None:
    """Create the DLQ topic if it is missing (broker auto-creation is off, ADR-0003)."""
    new_topic = NewTopic(
        topic,
        num_partitions=1,
        replication_factor=1,
        config={"retention.ms": str(DLQ_RETENTION_MS), "cleanup.policy": "delete"},
    )
    for future in admin.create_topics([new_topic]).values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].name() != "TOPIC_ALREADY_EXISTS":
                raise DeadLetterError(f"could not create topic {topic}: {exc}") from exc


def ensure_topic_on(bootstrap_servers: str, topic: str) -> None:
    ensure_topic(AdminClient({"bootstrap.servers": bootstrap_servers}), topic)


def describe(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    """Turn a dead letter's Kafka headers into a plain dict (for `deltaflow dlq show`)."""
    return {k: (v or b"").decode(errors="replace") for k, v in headers or []}


@dataclass(frozen=True, slots=True)
class StoredLetter:
    """A dead letter read back from its topic, with the original bytes intact."""

    key: bytes | None
    value: bytes | None
    headers: dict[str, str]

    @property
    def source_topic(self) -> str:
        return self.headers.get(H_TOPIC, "")

    def summary(self) -> dict[str, str]:
        text = (self.value or b"").decode(errors="replace")
        return {
            "source": (
                f"{self.source_topic}[{self.headers.get(H_PARTITION, '?')}]"
                f"@{self.headers.get(H_OFFSET, '?')}"
            ),
            "stage": self.headers.get(H_STAGE, "?"),
            "reason": self.headers.get(H_REASON, "")[:70],
            "value": text[:60],
        }


def read_dead_letters(consumer: Any, topic: str, *, limit: int | None = None) -> list[StoredLetter]:
    """Read every dead letter currently in the topic (the topic is left untouched)."""
    from confluent_kafka import OFFSET_BEGINNING, TopicPartition

    metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
    if metadata is None or metadata.error is not None:
        return []
    partitions = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in metadata.partitions]
    consumer.assign(partitions)
    pending = sum(
        high - low
        for low, high in (consumer.get_watermark_offsets(tp, timeout=10) for tp in partitions)
    )
    letters: list[StoredLetter] = []
    while pending > 0 and (limit is None or len(letters) < limit):
        message = consumer.poll(2.0)
        if message is None:
            break  # nothing more arrived in time (for example, expired by retention)
        if message.error() is not None:
            raise DeadLetterError(f"could not read {topic}: {message.error()}")
        letters.append(StoredLetter(message.key(), message.value(), describe(message.headers())))
        pending -= 1
    return letters


def replay_dead_letters(producer: ProducerLike, letters: list[StoredLetter]) -> int:
    """Re-publish letters to the topic they came from; returns how many were sent.

    Safe to repeat: every sink is idempotent (ADR-0004), so a message that was already applied
    is skipped as stale. Messages whose cause is not fixed simply dead-letter again.
    """
    sent = 0
    for letter in letters:
        if not letter.source_topic:
            log.warning("skipping a letter without a source topic header")
            continue
        producer.produce(letter.source_topic, key=letter.key, value=letter.value)
        sent += 1
    if producer.flush(FLUSH_TIMEOUT_SECONDS):
        raise DeadLetterError("timed out re-publishing dead letters")
    return sent
