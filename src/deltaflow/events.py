"""Normalised change events, parsed from Debezium's JSON envelope."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class EventParseError(ValueError):
    """A Kafka message is not a change event we understand. Never silently dropped."""


class Op(StrEnum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # initial snapshot row
    TRUNCATE = "t"  # table emptied; carries no row and no key (ADR-0010)


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """One committed row change.

    `lsn` is the PostgreSQL log position of the change. It is strictly increasing for the
    changes of any one row, so sinks use it as the version that orders writes (ADR-0004).
    """

    op: Op
    table: str  # "schema.table", e.g. "public.orders"
    key: Mapping[str, Any]  # primary key columns
    before: Mapping[str, Any] | None
    after: Mapping[str, Any] | None
    lsn: int
    ts_ms: int
    topic: str = ""
    partition: int = -1
    offset: int = -1

    @property
    def table_name(self) -> str:
        return self.table.rsplit(".", 1)[-1]

    @property
    def doc_id(self) -> str:
        """Stable document id derived from the primary key (sorted, '|' for composite keys).

        A TRUNCATE concerns the whole table, so its id is the wildcard "*".
        """
        if self.is_truncate:
            return "*"
        return "|".join(str(self.key[name]) for name in sorted(self.key))

    @property
    def is_delete(self) -> bool:
        return self.op is Op.DELETE

    @property
    def is_truncate(self) -> bool:
        return self.op is Op.TRUNCATE


def _load_json(raw: bytes, what: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise EventParseError(f"{what} is not valid JSON: {exc}") from exc


def parse_change_event(
    key: bytes | None,
    value: bytes | None,
    *,
    topic: str = "",
    partition: int = -1,
    offset: int = -1,
) -> ChangeEvent | None:
    """Parse one Kafka message.

    Returns None for messages that are legitimately not changes: the null-valued tombstone
    Kafka receives after every delete, and Debezium heartbeats. Anything else that cannot be
    understood raises EventParseError, so it is visible instead of silently skipped; the
    worker routes those to the dead-letter topic. A TRUNCATE is a valid event with no key.
    """
    if value is None:
        return None  # tombstone

    payload = _load_json(value, "value")
    if not isinstance(payload, dict):
        raise EventParseError("value is not a JSON object")
    if "op" not in payload:
        return None  # heartbeat

    try:
        op = Op(payload["op"])
    except ValueError as exc:
        raise EventParseError(f"unsupported operation {payload['op']!r}") from exc

    key_doc: Any = {}
    if op is not Op.TRUNCATE:
        if key is None:
            raise EventParseError("message has no key")
        key_doc = _load_json(key, "key")
        if not isinstance(key_doc, dict) or not key_doc:
            raise EventParseError("key is not a non-empty JSON object")

    source = payload.get("source")
    if not isinstance(source, dict):
        raise EventParseError("event has no source block")
    try:
        table = f"{source['schema']}.{source['table']}"
        lsn = int(source["lsn"])
    except (KeyError, TypeError, ValueError) as exc:
        raise EventParseError(f"source block is missing schema, table or lsn: {exc!r}") from exc

    before, after = payload.get("before"), payload.get("after")
    for name, row in (("before", before), ("after", after)):
        if row is not None and not isinstance(row, dict):
            raise EventParseError(f"{name} image is not a JSON object")
    if op is Op.TRUNCATE:
        before = after = None
    elif op is Op.DELETE and before is None:
        raise EventParseError("delete event has no before image (is REPLICA IDENTITY FULL set?)")
    elif op is not Op.DELETE and after is None:
        raise EventParseError(f"{op.name.lower()} event has no after image")

    ts_ms = payload.get("ts_ms") or source.get("ts_ms") or 0
    return ChangeEvent(
        op=op,
        table=table,
        key=key_doc,
        before=before,
        after=after,
        lsn=lsn,
        ts_ms=int(ts_ms),
        topic=topic,
        partition=partition,
        offset=offset,
    )
