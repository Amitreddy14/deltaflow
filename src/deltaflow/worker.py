"""Generic Kafka-to-sink consumer loop with commit-after-acknowledge semantics (ADR-0004).

Offsets are committed only after the sink has durably applied the batch. A crash anywhere
before that point causes redelivery, never loss; idempotent sinks make redelivery harmless.

With a dead-letter queue configured (ADR-0010), a message that can never succeed (unparseable,
or permanently rejected by the sink) is quarantined and the pipeline moves on. Transient
failures are never dead-lettered: they are retried and, if the destination stays down, stop the
worker. A burst of consecutive dead letters also stops it, because that points at a broken sink
rather than bad data.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, Literal, Protocol

from confluent_kafka import TopicPartition

from deltaflow.deadletter import DeadLetter, DeadLetterQueue
from deltaflow.events import ChangeEvent, EventParseError, parse_change_event
from deltaflow.sinks.base import RetryableSinkError, Sink, SinkError

log = logging.getLogger(__name__)

# Broker conditions that are normal while the connector has not created its topics yet,
# or when a partition is simply drained. Anything else is a real consumer failure.
_IGNORABLE_KAFKA_ERRORS = {"_PARTITION_EOF", "UNKNOWN_TOPIC_OR_PART", "_UNKNOWN_TOPIC"}

ParseFn = Callable[..., ChangeEvent | None]


class WorkerError(RuntimeError):
    """The worker cannot continue safely (consumer failure, or a flood of dead letters)."""


class _ShutdownRequested(Exception):
    """Stop was requested while a batch was still being retried; it will be redelivered."""


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
        sleep: Callable[[float], None] | None = None,
        dead_letters: DeadLetterQueue | None = None,
        max_consecutive_dead_letters: int = 50,
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
        self._dead_letters = dead_letters
        self._max_consecutive = max_consecutive_dead_letters
        self._consecutive_dead = 0
        self._stop: threading.Event | None = None
        self.dead_lettered = 0

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

        pairs: list[tuple[Any, ChangeEvent]] = []
        for message in usable:
            try:
                event = self._parse(
                    message.key(),
                    message.value(),
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
            except EventParseError as exc:
                if self._dead_letters is None:
                    raise
                self._dead_letter(message, "parse", str(exc))
                continue
            if event is not None:
                pairs.append((message, event))

        if pairs:
            self._write_isolating(pairs)
        self._commit(usable)  # only reached once every message is applied or dead-lettered
        return len(usable)

    def run(self, stop: threading.Event) -> None:
        """Loop until `stop` is set, finishing (and committing) the batch in flight first.

        If stop arrives while a batch is waiting out a retry, the batch is abandoned
        uncommitted: Kafka redelivers it after restart and idempotent sinks make that harmless.
        Other exceptions propagate: the process should crash and be restarted.
        """
        self._stop = stop
        try:
            while not stop.is_set():
                self.run_once()
        except _ShutdownRequested:
            log.info("shutdown requested during a retry; the open batch will be redelivered")
        finally:
            self._stop = None

    def _write_isolating(self, pairs: Sequence[tuple[Any, ChangeEvent]]) -> None:
        """Write the batch; if the sink permanently rejects it, bisect to find the bad event(s).

        Re-writing a good half is safe because sinks are idempotent. Each bad event ends up
        alone in a batch of one and is dead-lettered; everything else is applied.
        """
        try:
            self._write_with_retry([event for _, event in pairs])
        except RetryableSinkError:
            raise  # the destination is down, not the data: never quarantine because of that
        except SinkError as exc:
            if self._dead_letters is None:
                raise
            if len(pairs) == 1:
                self._dead_letter(pairs[0][0], "sink", str(exc))
                return
            log.warning("batch of %d rejected (%s); isolating the bad event(s)", len(pairs), exc)
            middle = len(pairs) // 2
            self._write_isolating(pairs[:middle])
            self._write_isolating(pairs[middle:])
        else:
            self._consecutive_dead = 0

    def _dead_letter(self, message: Any, stage: str, reason: str) -> None:
        assert self._dead_letters is not None
        self._dead_letters.send(
            DeadLetter(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                key=message.key(),
                value=message.value(),
                reason=reason,
                stage=stage,
            )
        )
        self.dead_lettered += 1
        self._consecutive_dead += 1
        if self._consecutive_dead > self._max_consecutive:
            raise WorkerError(
                f"{self._consecutive_dead} consecutive dead letters: the sink looks broken, "
                "not the data. Stopping instead of quarantining everything."
            )

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
                self._pause(delay)

    def _pause(self, delay: float) -> None:
        if self._sleep is not None:
            self._sleep(delay)
        elif self._stop is not None:
            self._stop.wait(delay)  # wakes immediately when shutdown is requested
        else:
            time.sleep(delay)
        self._check_stop()

    def _check_stop(self) -> None:
        if self._stop is not None and self._stop.is_set():
            raise _ShutdownRequested

    def _commit(self, messages: Sequence[Any]) -> None:
        """Commit the next offset to read for every partition touched by this batch."""
        highest: dict[tuple[str, int], int] = {}
        for message in messages:
            position = (message.topic(), message.partition())
            highest[position] = max(highest.get(position, -1), message.offset())
        offsets = [TopicPartition(t, p, offset + 1) for (t, p), offset in sorted(highest.items())]
        self._consumer.commit(offsets=offsets, asynchronous=False)
