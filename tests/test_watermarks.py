from deltaflow.events import ChangeEvent, Op
from deltaflow.sinks.base import TruncateWatermarks


def _row(lsn: int, table: str = "orders") -> ChangeEvent:
    return ChangeEvent(
        op=Op.CREATE, table=f"public.{table}", key={"id": 1}, before=None,
        after={"id": 1}, lsn=lsn, ts_ms=0,
    )  # fmt: skip


def test_only_events_older_than_the_mark_are_superseded() -> None:
    marks = TruncateWatermarks()
    marks.advance("orders", 500)

    assert marks.superseded(_row(499))
    assert not marks.superseded(_row(500))  # not strictly older
    assert not marks.superseded(_row(501))


def test_marks_are_per_table_and_never_go_backwards() -> None:
    marks = TruncateWatermarks()
    marks.advance("orders", 500)
    marks.advance("orders", 300)

    assert marks.superseded(_row(450))  # still 500
    assert not marks.superseded(_row(1, table="customers"))


def test_load_merges_with_the_maximum() -> None:
    marks = TruncateWatermarks()
    marks.advance("orders", 700)
    marks.load({"orders": 500, "customers": 200})

    assert marks.superseded(_row(600)) and len(marks) == 2
