from collections.abc import Mapping

from deltaflow import verify

SOURCE = {"customers": 5, "products": 3, "orders": 10}


def test_matching_counts_have_no_mismatches() -> None:
    sinks = {"elasticsearch": dict(SOURCE), "mongodb": dict(SOURCE)}

    assert verify.mismatches(SOURCE, sinks) == []


def test_each_disagreeing_table_and_sink_is_reported() -> None:
    sinks = {
        "elasticsearch": {**SOURCE, "orders": 9},
        "mongodb": {**SOURCE, "customers": 4, "orders": 11},
    }

    problems = verify.mismatches(SOURCE, sinks)

    assert problems == [
        "customers: source=5 mongodb=4",
        "orders: source=10 elasticsearch=9",
        "orders: source=10 mongodb=11",
    ]


def test_waits_until_sinks_catch_up() -> None:
    behind = {"elasticsearch": {**SOURCE, "orders": 8}, "mongodb": dict(SOURCE)}
    caught_up = {"elasticsearch": dict(SOURCE), "mongodb": dict(SOURCE)}
    states = iter([behind, behind, caught_up])
    ticks = iter(range(0, 100, 2))

    def collect() -> tuple[Mapping[str, int], Mapping[str, Mapping[str, int]]]:
        return SOURCE, next(states)

    _, _, problems = verify.wait_until_consistent(
        collect, wait=30, interval=2, sleep=lambda _: None, clock=lambda: float(next(ticks))
    )

    assert problems == []


def test_gives_up_after_the_wait_and_reports_the_mismatch() -> None:
    behind = {"elasticsearch": {**SOURCE, "orders": 8}, "mongodb": dict(SOURCE)}
    ticks = iter(range(0, 1000, 10))

    _, _, problems = verify.wait_until_consistent(
        lambda: (SOURCE, behind),
        wait=20,
        interval=2,
        sleep=lambda _: None,
        clock=lambda: float(next(ticks)),
    )

    assert problems == ["orders: source=10 elasticsearch=8"]


# ---- deep verification ----------------------------------------------------------------------


def _rows(**tables: dict[int, tuple[object, ...]]) -> dict[str, dict[int, tuple[object, ...]]]:
    return {name: tables.get(name, {}) for name in verify.COMPARE_COLUMNS}


def test_identical_rows_have_no_deep_mismatches() -> None:
    rows = _rows(customers={1: ("a@x", "A", "Pune")})

    assert verify.deep_mismatches(rows, {"elasticsearch": rows, "mongodb": rows}) == []


def test_deep_mismatch_names_missing_extra_and_differing_rows() -> None:
    source = _rows(
        orders={1: (1, 1, 1, "paid", 100), 2: (1, 1, 2, "paid", 200), 3: (1, 1, 1, "paid", 100)}
    )
    sink = _rows(
        orders={2: (1, 1, 2, "shipped", 200), 3: (1, 1, 1, "paid", 100), 9: (1, 1, 1, "paid", 1)}
    )

    lines = verify.deep_mismatches(source, {"mongodb": sink})

    (line,) = lines
    assert line.startswith("orders/mongodb:")
    assert "1 missing [1]" in line and "1 extra [9]" in line and "1 differing" in line
    assert "'paid'" in line and "'shipped'" in line  # shows the source and sink values


def test_equal_counts_with_different_content_are_caught() -> None:
    """The case counts cannot see: same number of rows, one holds a stale value."""
    source = _rows(customers={1: ("a@x", "A", "Pune"), 2: ("b@x", "B", "Delhi")})
    stale = _rows(customers={1: ("a@x", "A", "Pune"), 2: ("b@x", "B", "Mumbai")})

    assert verify.deep_mismatches(source, {"elasticsearch": stale})


def test_sample_size_is_limited() -> None:
    source = _rows(customers=dict.fromkeys(range(50), ("e", "n", "c")))

    (line,) = verify.deep_mismatches(source, {"es": _rows()}, show=3)

    assert "50 missing [0, 1, 2]" in line


def test_wait_for_retries_until_clean_or_out_of_time() -> None:
    answers = [["bad"], ["bad"], []]
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    problems = verify.wait_for(
        lambda: answers.pop(0), wait=30, interval=2, sleep=sleep, clock=lambda: now[0]
    )

    assert problems == [] and now[0] == 4.0

    stuck = verify.wait_for(lambda: ["bad"], wait=5, interval=2, sleep=sleep, clock=lambda: now[0])
    assert stuck == ["bad"]
