import json
from typing import Any

import pytest

from deltaflow.events import EventParseError, Op, parse_change_event

KEY = b'{"id": 7}'


def _value(op: str, before: Any, after: Any, **source: Any) -> bytes:
    src = {"schema": "public", "table": "orders", "lsn": 100, "ts_ms": 5}
    src.update(source)
    return json.dumps(
        {"op": op, "before": before, "after": after, "source": src, "ts_ms": 9}
    ).encode()


def test_parses_insert() -> None:
    event = parse_change_event(KEY, _value("c", None, {"id": 7, "status": "pending"}))

    assert event is not None
    assert event.op is Op.CREATE
    assert event.table == "public.orders" and event.table_name == "orders"
    assert event.doc_id == "7"
    assert event.lsn == 100 and event.ts_ms == 9
    assert event.after == {"id": 7, "status": "pending"} and event.before is None
    assert not event.is_delete


def test_parses_update_and_snapshot() -> None:
    update = parse_change_event(KEY, _value("u", {"id": 7, "s": 1}, {"id": 7, "s": 2}))
    snapshot = parse_change_event(KEY, _value("r", None, {"id": 7}))

    assert update is not None and update.op is Op.UPDATE and update.before == {"id": 7, "s": 1}
    assert snapshot is not None and snapshot.op is Op.READ


def test_parses_delete_with_full_before_image() -> None:
    event = parse_change_event(KEY, _value("d", {"id": 7, "status": "paid"}, None))

    assert event is not None and event.is_delete
    assert event.before == {"id": 7, "status": "paid"} and event.after is None


def test_carries_kafka_position() -> None:
    event = parse_change_event(
        KEY, _value("c", None, {"id": 7}), topic="deltaflow.public.orders", partition=2, offset=41
    )

    assert event is not None
    assert (event.topic, event.partition, event.offset) == ("deltaflow.public.orders", 2, 41)


def test_composite_key_id_is_sorted_and_joined() -> None:
    event = parse_change_event(b'{"b": 2, "a": 1}', _value("c", None, {"a": 1, "b": 2}))

    assert event is not None and event.doc_id == "1|2"


def test_tombstone_and_heartbeat_are_not_changes() -> None:
    assert parse_change_event(KEY, None) is None
    assert parse_change_event(b'{"serverName": "x"}', b'{"ts_ms": 1}') is None


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        (KEY, b"not json", "not valid JSON"),
        (KEY, b"[1, 2]", "not a JSON object"),
        (KEY, _value("t", None, None), "unsupported operation"),
        (None, _value("c", None, {"id": 7}), "no key"),
        (b"[]", _value("c", None, {"id": 7}), "key is not"),
        (KEY, _value("c", None, {"id": 7}, lsn=None), "missing schema, table or lsn"),
        (KEY, _value("d", None, None), "no before image"),
        (KEY, _value("u", {"id": 7}, None), "no after image"),
        (KEY, b'{"op": "c", "after": {"id": 7}}', "no source block"),
    ],
)
def test_malformed_messages_raise_instead_of_being_dropped(
    key: bytes | None, value: bytes, message: str
) -> None:
    with pytest.raises(EventParseError, match=message):
        parse_change_event(key, value)
