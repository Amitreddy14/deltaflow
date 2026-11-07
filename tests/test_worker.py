import threading
import time
from collections.abc import Sequence
from typing import Any

import pytest
from confluent_kafka import TopicPartition

from deltaflow.events import ChangeEvent
from deltaflow.sinks.base import RetryableSinkError, SinkError
from deltaflow.worker import SinkWorker, WorkerError


def _value(op: str = "c", lsn: int = 1) -> bytes:
    row = {"id": 1}
    body = {
        "op": op,
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "source": {"schema": "public", "table": "orders", "lsn": lsn},
    }
    return str(body).replace("'", '"').replace("None", "null").encode()


class Err:
    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        return self._name


class Msg:
    def __init__(
        self,
        offset: int,
        value: bytes | None = None,
        *,
        partition: int = 0,
        topic: str = "deltaflow.public.orders",
        error: Err | None = None,
    ) -> None:
        self._offset, self._value, self._partition, self._topic = offset, value, partition, topic
        self._error = error

    def error(self) -> Err | None:
        return self._error

    def key(self) -> bytes:
        return b'{"id": 1}'

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self, *batches: list[Msg]) -> None:
        self.batches = list(batches)
        self.commits: list[list[TopicPartition]] = []

    def consume(self, num_messages: int, timeout: float) -> list[Any]:
        return self.batches.pop(0) if self.batches else []

    def commit(self, *, offsets: list[TopicPartition], asynchronous: bool) -> None:
        assert asynchronous is False  # commit must complete before we read on
        self.commits.append(offsets)


class RecordingSink:
    def __init__(self, failures: list[Exception] | None = None) -> None:
        self.failures = failures or []
        self.batches: list[list[ChangeEvent]] = []

    def write(self, events: Sequence[ChangeEvent]) -> None:
        if self.failures:
            raise self.failures.pop(0)
        self.batches.append(list(events))


def _worker(consumer: FakeConsumer, sink: RecordingSink, **kwargs: Any) -> SinkWorker:
    return SinkWorker(consumer, sink, sleep=lambda _: None, **kwargs)


def test_commits_next_offset_only_after_the_sink_acknowledges() -> None:
    consumer = FakeConsumer([Msg(10, _value()), Msg(11, _value("u", 2))])
    sink = RecordingSink()

    handled = _worker(consumer, sink).run_once()

    assert handled == 2 and [len(b) for b in sink.batches] == [2]
    (commit,) = consumer.commits
    assert [(tp.topic, tp.partition, tp.offset) for tp in commit] == [
        ("deltaflow.public.orders", 0, 12)  # last offset + 1
    ]


def test_nothing_is_committed_when_the_sink_fails() -> None:
    consumer = FakeConsumer([Msg(5, _value())])
    sink = RecordingSink([SinkError("rejected")])

    with pytest.raises(SinkError):
        _worker(consumer, sink).run_once()

    assert consumer.commits == []  # the message will be redelivered after a restart


def test_transient_failures_are_retried_with_backoff_then_succeed() -> None:
    consumer = FakeConsumer([Msg(5, _value())])
    sink = RecordingSink([RetryableSinkError("down"), RetryableSinkError("down")])
    delays: list[float] = []
    worker = SinkWorker(consumer, sink, backoff_base=1.0, sleep=delays.append)

    worker.run_once()

    assert delays == [1.0, 2.0] and len(sink.batches) == 1 and len(consumer.commits) == 1


def test_retries_are_bounded() -> None:
    consumer = FakeConsumer([Msg(5, _value())])
    sink = RecordingSink([RetryableSinkError("down")] * 10)

    with pytest.raises(RetryableSinkError):
        _worker(consumer, sink, max_retries=2).run_once()

    assert consumer.commits == []


def test_skipped_messages_still_advance_the_offset() -> None:
    """Tombstones and heartbeats produce no events, but must not be re-read forever."""
    consumer = FakeConsumer([Msg(7, None), Msg(8, b'{"ts_ms": 1}')])
    sink = RecordingSink()

    _worker(consumer, sink).run_once()

    assert sink.batches == []  # nothing to write
    assert [tp.offset for tp in consumer.commits[0]] == [9]


def test_commits_the_highest_offset_per_partition() -> None:
    consumer = FakeConsumer(
        [
            Msg(3, _value(), partition=0),
            Msg(9, _value(), partition=1),
            Msg(4, _value(), partition=0),
        ]
    )

    _worker(consumer, RecordingSink()).run_once()

    assert [(tp.partition, tp.offset) for tp in consumer.commits[0]] == [(0, 5), (1, 10)]


def test_idle_poll_does_nothing() -> None:
    consumer = FakeConsumer()

    assert _worker(consumer, RecordingSink()).run_once() == 0
    assert consumer.commits == []


def test_missing_topics_are_tolerated_but_real_errors_are_not() -> None:
    quiet = FakeConsumer([Msg(0, error=Err("UNKNOWN_TOPIC_OR_PART"))])
    assert _worker(quiet, RecordingSink()).run_once() == 0

    broken = FakeConsumer([Msg(0, error=Err("_ALL_BROKERS_DOWN"))])
    with pytest.raises(WorkerError):
        _worker(broken, RecordingSink()).run_once()


def test_unparseable_message_stops_the_worker_without_committing() -> None:
    consumer = FakeConsumer([Msg(1, b"garbage")])

    with pytest.raises(ValueError, match="not valid JSON"):
        _worker(consumer, RecordingSink()).run_once()

    assert consumer.commits == []


def test_run_loops_until_stopped() -> None:
    stop = threading.Event()
    consumer = FakeConsumer([Msg(0, _value())], [Msg(1, _value())])
    sink = RecordingSink()
    original = sink.write

    def write_then_maybe_stop(events: Sequence[ChangeEvent]) -> None:
        original(events)
        if len(sink.batches) == 2:
            stop.set()

    sink.write = write_then_maybe_stop  # type: ignore[method-assign]

    _worker(consumer, sink).run(stop)

    assert len(sink.batches) == 2 and len(consumer.commits) == 2


# ---- Phase 4: dead letters, isolation, shutdown -------------------------------------------


class RecordingDlq:
    def __init__(self, fail: bool = False) -> None:
        self.letters: list[Any] = []
        self.fail = fail

    def send(self, letter: Any) -> None:
        if self.fail:
            raise RuntimeError("dlq down")
        self.letters.append(letter)


class PoisonSink:
    """Rejects any batch that contains an event whose LSN is in `poison`."""

    def __init__(self, poison: set[int]) -> None:
        self.poison = poison
        self.applied: list[int] = []
        self.calls = 0

    def write(self, events: Sequence[ChangeEvent]) -> None:
        self.calls += 1
        if any(e.lsn in self.poison for e in events):
            raise SinkError("mapping conflict")
        self.applied.extend(e.lsn for e in events)


def test_unparseable_message_is_dead_lettered_and_the_rest_proceed() -> None:
    consumer = FakeConsumer([Msg(1, b"garbage"), Msg(2, _value(lsn=7))])
    sink, dlq = RecordingSink(), RecordingDlq()

    handled = _worker(consumer, sink, dead_letters=dlq).run_once()

    assert handled == 2 and len(sink.batches) == 1
    (letter,) = dlq.letters
    assert letter.stage == "parse" and letter.value == b"garbage" and letter.offset == 1
    assert "not valid JSON" in letter.reason
    assert [tp.offset for tp in consumer.commits[0]] == [3]  # both messages are done


def test_poison_event_is_isolated_by_bisection_and_everything_else_is_applied() -> None:
    consumer = FakeConsumer([Msg(i, _value(lsn=i + 100)) for i in range(8)])
    sink, dlq = PoisonSink({105}), RecordingDlq()

    _worker(consumer, sink, dead_letters=dlq).run_once()  # type: ignore[arg-type]

    assert sorted(sink.applied) == [100, 101, 102, 103, 104, 106, 107]
    (letter,) = dlq.letters
    assert letter.stage == "sink" and letter.offset == 5 and "mapping conflict" in letter.reason
    assert [tp.offset for tp in consumer.commits[0]] == [8]


def test_without_a_dead_letter_queue_a_rejected_batch_still_halts_the_worker() -> None:
    consumer = FakeConsumer([Msg(0, _value(lsn=1))])

    with pytest.raises(SinkError):
        _worker(consumer, PoisonSink({1})).run_once()  # type: ignore[arg-type]

    assert consumer.commits == []


def test_a_down_destination_is_never_dead_lettered() -> None:
    consumer = FakeConsumer([Msg(0, _value())])
    dlq = RecordingDlq()

    with pytest.raises(RetryableSinkError):
        _worker(
            consumer,
            RecordingSink([RetryableSinkError("down")] * 10),
            max_retries=1,
            dead_letters=dlq,
        ).run_once()

    assert dlq.letters == [] and consumer.commits == []


def test_a_flood_of_dead_letters_stops_the_worker() -> None:
    """If everything is rejected the sink is broken, not the data; do not quarantine it all."""
    consumer = FakeConsumer([Msg(i, _value(lsn=i + 1)) for i in range(10)])
    sink, dlq = PoisonSink(set(range(1, 11))), RecordingDlq()

    with pytest.raises(WorkerError, match="consecutive dead letters"):
        _worker(consumer, sink, dead_letters=dlq, max_consecutive_dead_letters=3).run_once()  # type: ignore[arg-type]

    assert len(dlq.letters) == 4 and consumer.commits == []


def test_a_good_write_resets_the_dead_letter_streak() -> None:
    batches = [[Msg(0, _value(lsn=1))], [Msg(1, _value(lsn=2))], [Msg(2, _value(lsn=3))]]
    consumer = FakeConsumer(*batches)
    sink, dlq = PoisonSink({1, 3}), RecordingDlq()
    worker = _worker(consumer, sink, dead_letters=dlq, max_consecutive_dead_letters=1)  # type: ignore[arg-type]

    for _ in batches:
        worker.run_once()  # 1 bad, 1 good, 1 bad: never two in a row

    assert len(dlq.letters) == 2 and sink.applied == [2]


def test_if_the_dead_letter_cannot_be_stored_nothing_is_committed() -> None:
    consumer = FakeConsumer([Msg(1, b"garbage")])

    with pytest.raises(RuntimeError, match="dlq down"):
        _worker(consumer, RecordingSink(), dead_letters=RecordingDlq(fail=True)).run_once()

    assert consumer.commits == []  # the message is not lost: it will be read again


def test_shutdown_during_a_retry_abandons_the_batch_uncommitted() -> None:
    stop = threading.Event()
    consumer = FakeConsumer([Msg(5, _value())])
    sink = RecordingSink([RetryableSinkError("down")] * 10)
    worker = SinkWorker(consumer, sink, backoff_base=30.0, max_retries=5)  # would wait 30 s

    threading.Timer(0.2, stop.set).start()
    begin = time.monotonic()
    worker.run(stop)  # returns cleanly instead of raising

    assert time.monotonic() - begin < 5 and consumer.commits == []


def test_shutdown_lets_the_batch_in_flight_finish_and_commit() -> None:
    stop = threading.Event()
    consumer = FakeConsumer([Msg(0, _value())], [Msg(1, _value())])
    sink = RecordingSink()
    original = sink.write

    def write_and_request_stop(events: Sequence[ChangeEvent]) -> None:
        stop.set()  # arrives mid-batch
        original(events)

    sink.write = write_and_request_stop  # type: ignore[method-assign]

    _worker(consumer, sink).run(stop)

    assert len(sink.batches) == 1 and len(consumer.commits) == 1  # finished, committed, exited


def test_heartbeat_beats_before_every_batch_even_when_idle() -> None:
    stop = threading.Event()
    beats: list[int] = []
    consumer = FakeConsumer()  # always idle

    def beat() -> None:
        beats.append(1)
        if len(beats) == 3:
            stop.set()

    SinkWorker(consumer, RecordingSink(), heartbeat=beat, sleep=lambda _: None).run(stop)

    assert len(beats) == 3  # an idle worker still proves its loop is turning
