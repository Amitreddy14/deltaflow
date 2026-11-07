from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from deltaflow import deadletter
from deltaflow.deadletter import DeadLetter, DeadLetterError, KafkaDeadLetterQueue

LETTER = DeadLetter(
    topic="deltaflow.public.orders",
    partition=2,
    offset=41,
    key=b'{"id": 1}',
    value=b"garbage",
    reason="value is not valid JSON",
    stage="parse",
)


class FakeProducer:
    def __init__(self, *, remaining: int = 0, delivery_error: Any = None) -> None:
        self.remaining = remaining
        self.delivery_error = delivery_error
        self.sent: list[dict[str, Any]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.sent.append({"topic": topic, **kwargs})
        self._callback = kwargs.get("on_delivery")

    def flush(self, timeout: float) -> int:
        if self._callback:
            self._callback(self.delivery_error, None)
        return self.remaining


def test_dead_letter_keeps_original_bytes_and_explains_itself_in_headers() -> None:
    producer = FakeProducer()
    queue = KafkaDeadLetterQueue(producer, "deltaflow.dlq.es", sink="es-sink", clock=lambda: 1.5)

    queue.send(LETTER)

    (sent,) = producer.sent
    assert sent["topic"] == "deltaflow.dlq.es"
    assert sent["key"] == b'{"id": 1}' and sent["value"] == b"garbage"
    headers = deadletter.describe(sent["headers"])
    assert headers["dlq.stage"] == "parse" and headers["dlq.sink"] == "es-sink"
    assert headers["dlq.source.topic"] == "deltaflow.public.orders"
    assert headers["dlq.source.partition"] == "2" and headers["dlq.source.offset"] == "41"
    assert headers["dlq.reason"] == "value is not valid JSON"
    assert headers["dlq.failed_at_ms"] == "1500"


def test_unacknowledged_dead_letter_is_an_error() -> None:
    queue = KafkaDeadLetterQueue(FakeProducer(remaining=1), "t", sink="es")
    with pytest.raises(DeadLetterError, match="timed out"):
        queue.send(LETTER)

    failing = KafkaDeadLetterQueue(FakeProducer(delivery_error="broker said no"), "t", sink="es")
    with pytest.raises(DeadLetterError, match="broker said no"):
        failing.send(LETTER)


def test_very_long_reasons_are_truncated() -> None:
    producer = FakeProducer()
    long = replace(LETTER, reason="x" * 10_000)
    KafkaDeadLetterQueue(producer, "t", sink="es").send(long)

    assert len(deadletter.describe(producer.sent[0]["headers"])["dlq.reason"]) == 2000


def test_topic_names_are_per_sink_and_outside_the_data_topic_pattern() -> None:
    assert deadletter.dlq_topic("es") == "deltaflow.dlq.es"
    assert not deadletter.dlq_topic("es").startswith("deltaflow.public.")


class _Future:
    def __init__(self, error: KafkaException | None = None) -> None:
        self._error = error

    def result(self) -> None:
        if self._error:
            raise self._error


class _Admin:
    def __init__(self, error: KafkaException | None = None) -> None:
        self.error = error
        self.created: list[Any] = []

    def create_topics(self, topics: list[Any]) -> dict[str, _Future]:
        self.created += topics
        return {t.topic: _Future(self.error) for t in topics}


def _kafka_error(code: int) -> KafkaException:
    return KafkaException(KafkaError(code))


def test_ensure_topic_creates_with_retention_and_tolerates_existing() -> None:
    admin = _Admin()
    deadletter.ensure_topic(admin, "deltaflow.dlq.es")
    (topic,) = admin.created
    assert topic.topic == "deltaflow.dlq.es" and topic.config["cleanup.policy"] == "delete"

    deadletter.ensure_topic(_Admin(_kafka_error(KafkaError.TOPIC_ALREADY_EXISTS)), "t")
    with pytest.raises(DeadLetterError):
        deadletter.ensure_topic(_Admin(_kafka_error(KafkaError._TRANSPORT)), "t")


class _Msg:
    def __init__(self, key: bytes, value: bytes, headers: list[tuple[str, bytes]]) -> None:
        self._key, self._value, self._headers = key, value, headers

    def error(self) -> None:
        return None

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers


class _Meta:
    def __init__(self) -> None:
        self.error = None
        self.partitions = {0: object()}


class _Topics:
    topics: dict[str, Any]

    def __init__(self, topic: str, meta: Any) -> None:
        self.topics = {topic: meta}


class FakeReader:
    def __init__(self, messages: list[_Msg], exists: bool = True) -> None:
        self.messages = messages
        self.exists = exists
        self.assigned: list[Any] = []

    def list_topics(self, topic: str, timeout: float) -> _Topics:
        return _Topics(topic, _Meta() if self.exists else None)

    def assign(self, partitions: list[Any]) -> None:
        self.assigned = partitions

    def get_watermark_offsets(self, tp: Any, timeout: float) -> tuple[int, int]:
        return 0, len(self.messages)

    def poll(self, timeout: float) -> _Msg | None:
        return self.messages.pop(0) if self.messages else None


def _stored(n: int) -> list[_Msg]:
    return [
        _Msg(b"k", f"v{i}".encode(), [("dlq.source.topic", b"deltaflow.public.orders")])
        for i in range(n)
    ]


def test_read_dead_letters_returns_everything_with_headers() -> None:
    letters = deadletter.read_dead_letters(FakeReader(_stored(3)), "deltaflow.dlq.es")

    assert [letter.value for letter in letters] == [b"v0", b"v1", b"v2"]
    assert letters[0].source_topic == "deltaflow.public.orders"
    assert letters[0].summary()["value"] == "v0"


def test_read_respects_limit_and_missing_topic() -> None:
    assert len(deadletter.read_dead_letters(FakeReader(_stored(5)), "t", limit=2)) == 2
    assert deadletter.read_dead_letters(FakeReader([], exists=False), "t") == []


def test_replay_republishes_to_the_source_topic_and_skips_orphans() -> None:
    letters = deadletter.read_dead_letters(FakeReader(_stored(2)), "t")
    letters.append(deadletter.StoredLetter(b"k", b"v", {}))  # no source header
    producer = FakeProducer()
    producer._callback = None

    sent = deadletter.replay_dead_letters(producer, letters)

    assert sent == 2
    assert {s["topic"] for s in producer.sent} == {"deltaflow.public.orders"}
    assert producer.sent[0]["value"] == b"v0"
