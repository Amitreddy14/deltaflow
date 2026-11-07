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
