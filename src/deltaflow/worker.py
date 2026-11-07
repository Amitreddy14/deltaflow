"""Generic Kafka-to-sink consumer loop with commit-after-acknowledge semantics (ADR-0004).

Offsets are committed only after the sink has durably applied the batch. A crash anywhere
before that point causes redelivery, never loss; idempotent sinks make redelivery harmless.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, Literal, Protocol

from confluent_kafka import TopicPartition

from deltaflow.events import ChangeEvent, parse_change_event
from deltaflow.sinks.base import RetryableSinkError, Sink

log = logging.getLogger(__name__)

# Broker conditions that are normal while the connector has not created its topics yet,
# or when a partition is simply drained. Anything else is a real consumer failure.
_IGNORABLE_KAFKA_ERRORS = {"_PARTITION_EOF", "UNKNOWN_TOPIC_OR_PART", "_UNKNOWN_TOPIC"}

ParseFn = Callable[..., ChangeEvent | None]


class WorkerError(RuntimeError):
    """The Kafka consumer reported an unrecoverable error."""


class ConsumerLike(Protocol):
    """The slice of confluent_kafka.Consumer the worker uses (lets tests supply a fake)."""

    def consume(self, num_messages: int, timeout: float) -> list[Any]: ...
    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[False]) -> Any: ...


class SinkWorker:
    def __init__(
        self,
        consumer: ConsumerLike,
        sink: Sink,
        *,
        batch_size: int = 200,
        poll_timeout: float = 1.0,
        max_retries: int = 5,
        backoff_base: float = 0.5,
        backoff_max: float = 10.0,
        parse: ParseFn = parse_change_event,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._consumer = consumer
        self._sink = sink
        self._batch_size = batch_size
        self._poll_timeout = poll_timeout
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._parse = parse
        self._sleep = sleep

    def run_once(self) -> int:
        """Process one batch. Returns the number of Kafka messages handled (0 if idle)."""
        messages = self._consumer.consume(self._batch_size, self._poll_timeout)
        usable: list[Any] = []
        for message in messages:
            error = message.error()
            if error is None:
                usable.append(message)
            elif error.name() not in _IGNORABLE_KAFKA_ERRORS:
                raise WorkerError(f"kafka consumer error: {error}")
        if not usable:
            return 0

        events: list[ChangeEvent] = []
        for message in usable:
            event = self._parse(
                message.key(),
                message.value(),
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )
            if event is not None:
                events.append(event)

        if events:
            self._write_with_retry(events)
        self._commit(usable)  # only reached once the sink acknowledged everything
        return len(usable)

    def run(self, stop: threading.Event) -> None:
        """Loop until `stop` is set. Exceptions propagate: the process should crash and restart."""
        while not stop.is_set():
            self.run_once()

    def _write_with_retry(self, events: Sequence[ChangeEvent]) -> None:
        for attempt in range(self._max_retries + 1):
            try:
                self._sink.write(events)
                return
            except RetryableSinkError as exc:
                if attempt == self._max_retries:
                    raise
                delay = min(self._backoff_max, self._backoff_base * 2**attempt)
                log.warning("sink unavailable (%s); retry %d in %.1fs", exc, attempt + 1, delay)
                self._sleep(delay)

    def _commit(self, messages: Sequence[Any]) -> None:
        """Commit the next offset to read for every partition touched by this batch."""
        highest: dict[tuple[str, int], int] = {}
        for message in messages:
            position = (message.topic(), message.partition())
            highest[position] = max(highest.get(position, -1), message.offset())
        offsets = [TopicPartition(t, p, offset + 1) for (t, p), offset in sorted(highest.items())]
        self._consumer.commit(offsets=offsets, asynchronous=False)
