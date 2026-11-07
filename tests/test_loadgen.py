import random
from collections import Counter
from collections.abc import Callable

import psycopg
import pytest

from deltaflow import loadgen


def test_every_operation_has_a_weight_and_a_handler() -> None:
    assert set(loadgen.WEIGHTS) == set(loadgen.Op)
    assert set(loadgen._HANDLERS) == set(loadgen.Op)


def test_choose_operation_is_deterministic_for_a_seed() -> None:
    first = [loadgen.choose_operation(random.Random(7)) for _ in range(20)]
    second = [loadgen.choose_operation(random.Random(7)) for _ in range(20)]

    assert first == second


def test_choose_operation_follows_the_weights() -> None:
    rng = random.Random(1)
    counts = Counter(loadgen.choose_operation(rng) for _ in range(10_000))

    assert counts[loadgen.Op.INSERT_ORDER] > counts[loadgen.Op.DELETE_ORDER] * 3
    assert all(counts[op] > 0 for op in loadgen.Op)


def test_orders_only_move_forward_or_cancel() -> None:
    rng = random.Random(3)
    seen = {loadgen.next_status("pending", rng) for _ in range(200)}

    assert seen == {"paid", "cancelled"}
    assert loadgen.next_status("shipped", random.Random(0)) in {"delivered", "cancelled"}


def test_run_duration_is_measured_after_connecting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: a slow connect (130 s on Windows IPv6 fallback) must not use up the run time."""
    now = [0.0]

    class SlowConnection:
        def __enter__(self) -> "SlowConnection":
            now[0] += 130.0  # connecting takes far longer than the whole duration
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    calls: list[loadgen.Op] = []

    def fake_handler_for(op: loadgen.Op) -> Callable[[object, random.Random], None]:
        return lambda conn, rng: calls.append(op)

    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: SlowConnection())
    monkeypatch.setattr(loadgen, "_HANDLERS", {op: fake_handler_for(op) for op in loadgen.Op})

    counts = loadgen.run(
        "ignored",
        rate=1.0,
        duration=3.0,
        seed=1,
        sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
        clock=lambda: now[0],
    )

    assert sum(counts.values()) == 3
