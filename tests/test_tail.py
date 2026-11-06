import json

from deltaflow.tail import format_event


def _event(op: str, before: dict[str, object] | None, after: dict[str, object] | None) -> bytes:
    return json.dumps({"op": op, "before": before, "after": after, "source": {"lsn": 42}}).encode()


def test_insert_shows_the_new_row() -> None:
    line = format_event(b'{"id": 1}', _event("c", None, {"id": 1, "city": "Pune"}))

    assert line.startswith("INSERT")
    assert "lsn=42" in line
    assert "Pune" in line


def test_update_shows_only_changed_columns() -> None:
    before = {"id": 1, "city": "Pune", "name": "A"}
    after = {"id": 1, "city": "Delhi", "name": "A"}

    line = format_event(b'{"id": 1}', _event("u", before, after))

    assert line.startswith("UPDATE")
    assert "Pune -> Delhi" in line
    assert "name" not in line


def test_delete_and_tombstone() -> None:
    delete = format_event(b'{"id": 1}', _event("d", {"id": 1}, None))
    tombstone = format_event(b'{"id": 1}', None)

    assert delete.startswith("DELETE")
    assert tombstone.startswith("TOMBSTONE")


def test_non_change_messages_are_labelled() -> None:
    assert format_event(b'{"serverName": "x"}', b'{"ts_ms": 1}').startswith("OTHER")
