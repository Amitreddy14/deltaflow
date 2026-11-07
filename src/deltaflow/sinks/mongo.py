"""MongoDB sink: current state plus an append-only change history (ADR-0005, ADR-0009).

Two collections kinds are written for every batch:

* One *current state* collection per source table (`orders`, `customers`, ...), one document
  per row. A write replaces the document only if the event's LSN is newer than the stored one
  (`_lsn < event.lsn`). Deletes are *soft*: the document stays, flagged `_deleted`, carrying the
  delete's LSN. That makes every write commutative and gives permanent protection against an old
  event resurrecting a deleted row. A TTL index purges tombstones after seven days.
* One `changes` history collection, one document per event, keyed `table|id|lsn`. Replays hit
  the same key and are rejected as duplicates, so history is complete and never doubled.

When the guard rejects a write MongoDB reports a duplicate key (E11000), because the upsert
then tries to insert a document whose `_id` already exists. That is the expected, harmless
signal "a newer or identical version is already stored".
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from pymongo import ReplaceOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError, ConnectionFailure

from deltaflow.events import ChangeEvent
from deltaflow.sinks.base import RetryableSinkError, SinkError

log = logging.getLogger(__name__)

HISTORY_COLLECTION = "changes"
DUPLICATE_KEY = 11000
TOMBSTONE_TTL_SECONDS = 7 * 24 * 3600


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def mongo_id(event: ChangeEvent) -> Any:
    """The row's own primary key when it has one column, otherwise the joined key string."""
    if len(event.key) == 1:
        return next(iter(event.key.values()))
    return event.doc_id


def changed_columns(before: Mapping[str, Any] | None, after: Mapping[str, Any] | None) -> list[str]:
    if not before or not after:
        return []
    return sorted(c for c in before.keys() | after.keys() if before.get(c) != after.get(c))


def state_operation(event: ChangeEvent, now: datetime) -> ReplaceOne[dict[str, Any]]:
    """Replace the row's current-state document, but only if this event is newer."""
    row = event.before if event.is_delete else event.after
    assert row is not None  # the parser guarantees a row image for every event
    document: dict[str, Any] = {
        **row,
        "_id": mongo_id(event),
        "_lsn": event.lsn,
        "_op": event.op.value,
        "_ts": _utc(event.ts_ms),
        "_deleted": event.is_delete,
    }
    if event.is_delete:
        document["_deleted_at"] = now  # drives the tombstone TTL
    return ReplaceOne({"_id": document["_id"], "_lsn": {"$lt": event.lsn}}, document, upsert=True)


def history_document(event: ChangeEvent) -> dict[str, Any]:
    return {
        "_id": f"{event.table_name}|{event.doc_id}|{event.lsn}",
        "table": event.table_name,
        "doc_id": event.doc_id,
        "key": dict(event.key),
        "op": event.op.value,
        "lsn": event.lsn,
        "ts": _utc(event.ts_ms),
        "before": event.before,
        "after": event.after,
        "changed": changed_columns(event.before, event.after),
        "kafka": {"topic": event.topic, "partition": event.partition, "offset": event.offset},
    }


def ensure_history_indexes(db: Database[Any]) -> None:
    history = db[HISTORY_COLLECTION]
    history.create_index([("table", 1), ("ts", -1)], name="table_ts")
    history.create_index([("table", 1), ("doc_id", 1), ("lsn", 1)], name="row_history")


def ensure_state_indexes(db: Database[Any], table_name: str) -> None:
    db[table_name].create_index(
        "_deleted_at", expireAfterSeconds=TOMBSTONE_TTL_SECONDS, name="tombstone_ttl"
    )


def _write_errors(exc: BulkWriteError) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = exc.details.get("writeErrors", [])
    return errors


def _split_duplicates(exc: BulkWriteError) -> int:
    """Count duplicate-key rejections; raise if any other write error is present."""
    errors = _write_errors(exc)
    others = [e for e in errors if e.get("code") != DUPLICATE_KEY]
    if others or not errors:
        raise SinkError(f"mongodb rejected writes: {others[0] if others else exc.details}") from exc
    return len(errors)


class MongoSink:
    def __init__(
        self,
        db: Database[Any],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._db = db
        self._clock = clock
        self._indexed: set[str] = set()
        self.applied = 0
        self.stale = 0
        self.history_written = 0
        self.history_duplicates = 0

    def write(self, events: Sequence[ChangeEvent]) -> None:
        if not events:
            return
        try:
            self._ensure_indexes(events)
            self._write_history(events)
            self._write_state(events)
        except ConnectionFailure as exc:  # includes server-selection timeouts and failovers
            raise RetryableSinkError(f"mongodb unreachable: {exc}") from exc
        log.info("applied %d events (%d stale or duplicate skipped)", len(events), self.stale)

    def _ensure_indexes(self, events: Sequence[ChangeEvent]) -> None:
        for table_name in {e.table_name for e in events} - self._indexed:
            ensure_state_indexes(self._db, table_name)
            self._indexed.add(table_name)

    def _write_history(self, events: Sequence[ChangeEvent]) -> None:
        documents = [history_document(e) for e in events]
        try:
            self._db[HISTORY_COLLECTION].insert_many(documents, ordered=False)
            self.history_written += len(documents)
        except BulkWriteError as exc:
            duplicates = _split_duplicates(exc)
            self.history_duplicates += duplicates
            self.history_written += len(documents) - duplicates

    def _write_state(self, events: Sequence[ChangeEvent]) -> None:
        now = self._clock()
        by_table: dict[str, list[ReplaceOne[dict[str, Any]]]] = {}
        for event in events:
            by_table.setdefault(event.table_name, []).append(state_operation(event, now))
        for table_name, operations in by_table.items():
            try:
                # Unordered is safe: the LSN guard makes the writes commutative.
                self._db[table_name].bulk_write(operations, ordered=False)
                self.applied += len(operations)
            except BulkWriteError as exc:
                stale = _split_duplicates(exc)
                self.stale += stale
                self.applied += len(operations) - stale
