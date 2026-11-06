import random
from collections import Counter

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
