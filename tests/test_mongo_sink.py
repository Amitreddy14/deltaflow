"""Unit tests for the Mongo sink against a small fake that follows MongoDB's documented rules.

The fake models the one behaviour the design depends on: an upsert whose filter does not match
an existing document tries to insert it, and fails with E11000 (duplicate key) if that `_id`
already exists. The real server is exercised by tests/integration/test_mongo_sink.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from pymongo.errors import BulkWriteError, ConnectionFailure

from deltaflow.events import ChangeEvent, Op
from deltaflow.sinks import mongo
from deltaflow.sinks.base import RetryableSinkError, SinkError

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)


class FakeCollection:
    def __init__(self) -> None:
        self.docs: dict[Any, dict[str, Any]] = {}
        self.indexes: list[tuple[Any, dict[str, Any]]] = []
        self.fail_with: Exception | None = None
        self.write_error_code = 11000

    def create_index(self, keys: Any, **options: Any) -> None:
        self.indexes.append((keys, options))

    def bulk_write(self, operations: list[Any], ordered: bool = True) -> None:
        if self.fail_with:
            raise self.fail_with
        assert ordered is False  # the LSN guard makes order irrelevant; we rely on that
        errors: list[dict[str, Any]] = []
        for index, operation in enumerate(operations):
            doc_id, guard = operation._filter["_id"], operation._filter["_lsn"]["$lt"]
            existing = self.docs.get(doc_id)
            if existing is None or existing["_lsn"] < guard:
                self.docs[doc_id] = dict(operation._doc)
            else:  # filter matched nothing, upsert tries to insert a duplicate _id
                errors.append({"index": index, "code": self.write_error_code, "errmsg": "dup"})
        if errors:
            raise BulkWriteError({"writeErrors": errors})

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> None:
        errors = []
        for index, document in enumerate(documents):
            if document["_id"] in self.docs:
                errors.append({"index": index, "code": self.write_error_code, "errmsg": "dup"})
            else:
                self.docs[document["_id"]] = document
        if errors:
            raise BulkWriteError({"writeErrors": errors})


class FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())


def _event(
    op: Op = Op.CREATE,
    lsn: int = 100,
    *,
    key: dict[str, Any] | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
) -> ChangeEvent:
    row = {"id": 7, "city": "Pune"}
    if op is Op.DELETE:
        before = before or row
    else:
        after = after or row
    return ChangeEvent(
        op=op,
        table="public.customers",
        key=key or {"id": 7},
        before=before,
        after=after,
        lsn=lsn,
        ts_ms=1_700_000_000_000,
        topic="deltaflow.public.customers",
        partition=1,
        offset=9,
    )


def _update(lsn: int, old_city: str, new_city: str) -> ChangeEvent:
    return _event(
        Op.UPDATE,
        lsn,
        before={"id": 7, "city": old_city},
        after={"id": 7, "city": new_city},
    )


def _sink() -> tuple[mongo.MongoSink, FakeDatabase]:
    db = FakeDatabase()
    return mongo.MongoSink(db, clock=lambda: NOW), db  # type: ignore[arg-type]


def test_state_operation_is_guarded_by_lsn_and_keyed_by_the_row() -> None:
    operation = mongo.state_operation(_event(lsn=500), NOW)

    assert operation._filter == {"_id": 7, "_lsn": {"$lt": 500}}
    assert operation._upsert is True
    document = operation._doc
    assert document["city"] == "Pune" and document["_lsn"] == 500
    assert document["_op"] == "c" and document["_deleted"] is False
    assert "_deleted_at" not in document


def test_delete_becomes_a_soft_delete_that_keeps_the_last_row() -> None:
    document = mongo.state_operation(_event(Op.DELETE, 900), NOW)._doc

    assert document["_deleted"] is True and document["_deleted_at"] == NOW
    assert document["city"] == "Pune" and document["_lsn"] == 900 and document["_op"] == "d"


def test_composite_keys_are_joined_into_the_id() -> None:
    event = _event(key={"b": 2, "a": 1}, after={"a": 1, "b": 2})

    assert mongo.mongo_id(event) == "1|2"


def test_history_document_describes_the_change() -> None:
    update = _event(
        Op.UPDATE,
        7,
        before={"id": 7, "city": "Pune", "n": 1},
        after={"id": 7, "city": "Delhi", "n": 1},
    )

    document = mongo.history_document(update)

    assert document["_id"] == "customers|7|7"
    assert document["op"] == "u" and document["changed"] == ["city"]
    assert document["ts"] == datetime.fromtimestamp(1_700_000_000, tz=UTC)
    assert document["kafka"] == {"topic": "deltaflow.public.customers", "partition": 1, "offset": 9}


def test_insert_writes_state_and_history() -> None:
    sink, db = _sink()

    sink.write([_event(lsn=10)])

    assert db["customers"].docs[7]["_lsn"] == 10
    assert list(db["changes"].docs) == ["customers|7|10"]
    assert sink.applied == 1 and sink.stale == 0


def test_newer_events_replace_older_ones() -> None:
    sink, db = _sink()
    sink.write([_event(lsn=10)])

    sink.write(
        [_event(Op.UPDATE, 20, before={"id": 7, "city": "Pune"}, after={"id": 7, "city": "Delhi"})]
    )

    assert db["customers"].docs[7]["city"] == "Delhi" and db["customers"].docs[7]["_lsn"] == 20


def test_final_state_is_the_same_whatever_order_events_arrive_in() -> None:
    events = [
        _event(Op.CREATE, 10),
        _update(20, "Pune", "Delhi"),
        _update(30, "Delhi", "Goa"),
    ]
    forward, forward_db = _sink()
    backward, backward_db = _sink()

    forward.write(events)
    backward.write(list(reversed(events)))

    assert forward_db["customers"].docs[7] == backward_db["customers"].docs[7]
    assert backward_db["customers"].docs[7]["city"] == "Goa"
    assert len(backward_db["changes"].docs) == 3  # history is complete either way


def test_a_stale_event_is_ignored_without_error() -> None:
    sink, db = _sink()
    sink.write(
        [_event(Op.UPDATE, 50, before={"id": 7, "city": "x"}, after={"id": 7, "city": "Goa"})]
    )

    sink.write([_event(Op.CREATE, 10)])  # must not raise

    assert db["customers"].docs[7]["city"] == "Goa" and db["customers"].docs[7]["_lsn"] == 50
    assert sink.stale == 1


def test_redelivering_a_batch_changes_nothing() -> None:
    sink, db = _sink()
    batch = [_event(Op.CREATE, 10), _event(Op.DELETE, 20)]
    sink.write(batch)
    snapshot = {k: dict(v) for k, v in db["customers"].docs.items()}

    sink.write(batch)  # same batch again, as after a crash before the offset commit

    assert db["customers"].docs == snapshot
    assert len(db["changes"].docs) == 2
    assert sink.history_duplicates == 2 and sink.stale == 2


def test_a_late_old_event_cannot_resurrect_a_deleted_row() -> None:
    sink, db = _sink()
    sink.write([_event(Op.CREATE, 10), _event(Op.DELETE, 20)])

    sink.write([_event(Op.CREATE, 10)])

    assert db["customers"].docs[7]["_deleted"] is True


def test_other_write_errors_are_permanent() -> None:
    sink, db = _sink()
    db["customers"].write_error_code = 121  # document validation failure

    with pytest.raises(SinkError) as raised:
        sink.write([_event()])
        sink.write([_event()])

    assert not isinstance(raised.value, RetryableSinkError)


def test_connection_failures_are_retryable() -> None:
    sink, db = _sink()
    db["customers"].fail_with = ConnectionFailure("down")

    with pytest.raises(RetryableSinkError):
        sink.write([_event()])


def test_indexes_are_created_once_per_table() -> None:
    sink, db = _sink()

    sink.write([_event(lsn=1)])
    sink.write([_event(lsn=2)])

    assert len(db["customers"].indexes) == 1
    keys, options = db["customers"].indexes[0]
    assert keys == "_deleted_at" and options["expireAfterSeconds"] == mongo.TOMBSTONE_TTL_SECONDS


def test_empty_batch_is_a_no_op() -> None:
    sink, db = _sink()

    sink.write([])

    assert db.collections == {}
