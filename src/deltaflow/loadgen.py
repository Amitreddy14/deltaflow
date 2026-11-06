"""Generate a realistic stream of inserts, updates and deletes against the source database."""

from __future__ import annotations

import random
import time
from collections import Counter
from collections.abc import Callable
from enum import StrEnum

import psycopg

_FIRST = ["Aarav", "Diya", "Vivaan", "Ananya", "Rohan", "Meera", "Kabir", "Isha"]
_LAST = ["Reddy", "Sharma", "Iyer", "Nair", "Patel", "Khan"]
_CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Mumbai", "Delhi", "Pune"]
_NEXT_STATUS = {"pending": "paid", "paid": "shipped", "shipped": "delivered"}


class Op(StrEnum):
    INSERT_ORDER = "insert_order"
    ADVANCE_ORDER = "advance_order"
    UPDATE_PRODUCT = "update_product"
    UPDATE_CUSTOMER = "update_customer"
    INSERT_CUSTOMER = "insert_customer"
    DELETE_ORDER = "delete_order"


# Roughly what a shop does: mostly new orders and order progress, rarely deletes.
WEIGHTS: dict[Op, int] = {
    Op.INSERT_ORDER: 35,
    Op.ADVANCE_ORDER: 30,
    Op.UPDATE_PRODUCT: 15,
    Op.UPDATE_CUSTOMER: 8,
    Op.INSERT_CUSTOMER: 7,
    Op.DELETE_ORDER: 5,
}


def choose_operation(rng: random.Random) -> Op:
    return rng.choices(list(WEIGHTS), weights=list(WEIGHTS.values()), k=1)[0]


def next_status(current: str, rng: random.Random) -> str:
    """Advance an order; a small share of in-flight orders get cancelled instead."""
    if current in _NEXT_STATUS and rng.random() < 0.1:
        return "cancelled"
    return _NEXT_STATUS[current]


def _insert_order(conn: psycopg.Connection, rng: random.Random) -> None:
    customer = conn.execute("SELECT id FROM customers ORDER BY random() LIMIT 1").fetchone()
    product = conn.execute(
        "SELECT id, price_cents FROM products ORDER BY random() LIMIT 1"
    ).fetchone()
    if customer is None or product is None:
        raise RuntimeError("source tables are empty; was the seed script applied?")
    quantity = rng.randint(1, 5)
    conn.execute(
        "INSERT INTO orders (customer_id, product_id, quantity, status, total_cents) "
        "VALUES (%s, %s, %s, 'pending', %s)",
        (customer[0], product[0], quantity, quantity * product[1]),
    )


def _advance_order(conn: psycopg.Connection, rng: random.Random) -> None:
    row = conn.execute(
        "SELECT id, status FROM orders WHERE status IN ('pending','paid','shipped') "
        "ORDER BY random() LIMIT 1"
    ).fetchone()
    if row is None:
        _insert_order(conn, rng)
        return
    conn.execute("UPDATE orders SET status = %s WHERE id = %s", (next_status(row[1], rng), row[0]))


def _update_product(conn: psycopg.Connection, rng: random.Random) -> None:
    row = conn.execute("SELECT id, price_cents FROM products ORDER BY random() LIMIT 1").fetchone()
    if row is None:
        return
    if rng.random() < 0.7:  # restock / sell-down
        conn.execute(
            "UPDATE products SET stock = GREATEST(0, stock + %s) WHERE id = %s",
            (rng.randint(-5, 25), row[0]),
        )
    else:  # price change of up to +/-10%
        price = max(100, int(row[1] * rng.uniform(0.9, 1.1)))
        conn.execute("UPDATE products SET price_cents = %s WHERE id = %s", (price, row[0]))


def _update_customer(conn: psycopg.Connection, rng: random.Random) -> None:
    row = conn.execute("SELECT id FROM customers ORDER BY random() LIMIT 1").fetchone()
    if row is not None:
        conn.execute("UPDATE customers SET city = %s WHERE id = %s", (rng.choice(_CITIES), row[0]))


def _insert_customer(conn: psycopg.Connection, rng: random.Random) -> None:
    conn.execute(
        "INSERT INTO customers (email, full_name, city) VALUES (%s, %s, %s)",
        (
            f"user{rng.getrandbits(40):010x}@example.com",
            f"{rng.choice(_FIRST)} {rng.choice(_LAST)}",
            rng.choice(_CITIES),
        ),
    )


def _delete_order(conn: psycopg.Connection, rng: random.Random) -> None:
    row = conn.execute(
        "SELECT id FROM orders ORDER BY (status = 'cancelled') DESC, random() LIMIT 1"
    ).fetchone()
    if row is not None:
        conn.execute("DELETE FROM orders WHERE id = %s", (row[0],))


_HANDLERS: dict[Op, Callable[[psycopg.Connection, random.Random], None]] = {
    Op.INSERT_ORDER: _insert_order,
    Op.ADVANCE_ORDER: _advance_order,
    Op.UPDATE_PRODUCT: _update_product,
    Op.UPDATE_CUSTOMER: _update_customer,
    Op.INSERT_CUSTOMER: _insert_customer,
    Op.DELETE_ORDER: _delete_order,
}


def run(
    dsn: str,
    *,
    rate: float = 5.0,
    duration: float | None = 30.0,
    seed: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Counter[Op]:
    """Apply roughly `rate` operations per second for `duration` seconds (None = until Ctrl+C).

    Each operation is its own committed transaction, so each produces its own change event.
    """
    if rate <= 0:
        raise ValueError("rate must be positive")
    rng = random.Random(seed)
    counts: Counter[Op] = Counter()
    started = clock()
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            while duration is None or clock() - started < duration:
                op = choose_operation(rng)
                _HANDLERS[op](conn, rng)
                counts[op] += 1
                sleep(1.0 / rate)
        except KeyboardInterrupt:
            pass
    return counts
