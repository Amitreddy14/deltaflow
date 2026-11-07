"""End to end: SQL writes in Postgres become ordered change events on Kafka.

Needs the compose stack:  docker compose up -d --wait
Run:                      pytest -m integration
Skipped automatically when the stack is not reachable.
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from typing import Any

import httpx
import psycopg
import pytest
from confluent_kafka import Consumer

from deltaflow import connect
from deltaflow.config import get_settings

pytestmark = pytest.mark.integration

TOPIC = "deltaflow.public.customers"


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module", autouse=True)
def running_connector() -> None:
    if not all(_reachable("127.0.0.1", port) for port in (5432, 29092, 8083)):
        pytest.skip("compose stack is not running (docker compose up -d --wait)")
    name, config = connect.render_connector(
        connect.DEFAULT_CONNECTOR_FILE, connect.template_variables({})
    )
    with httpx.Client(base_url=get_settings().connect_url, timeout=15.0) as client:
        connect.register_connector(client, name, config)
        connect.wait_until_running(client, name)


def _collect(customer_id: int, expected: int, timeout: float = 45.0) -> list[dict[str, Any] | None]:
    """Read the topic from the start and return this customer's events, in arrival order."""
    consumer = Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap_servers,
            "group.id": f"it-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    found: list[dict[str, Any] | None] = []
    deadline = time.monotonic() + timeout
    try:
        while len(found) < expected and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            raw_key = message.key()
            assert raw_key is not None
            key = json.loads(raw_key)
            if key.get("id") != customer_id:
                continue
            value = message.value()
            found.append(json.loads(value) if value is not None else None)
    finally:
        consumer.close()
    return found


def test_insert_update_delete_arrive_in_order() -> None:
    email = f"it-{uuid.uuid4().hex[:10]}@example.com"
    with psycopg.connect(get_settings().postgres_dsn, autocommit=True) as conn:
        row = conn.execute(
            "INSERT INTO customers (email, full_name, city) VALUES (%s, 'IT User', 'Pune') "
            "RETURNING id",
            (email,),
        ).fetchone()
        assert row is not None
        customer_id: int = row[0]
        conn.execute("UPDATE customers SET city = 'Delhi' WHERE id = %s", (customer_id,))
        conn.execute("DELETE FROM customers WHERE id = %s", (customer_id,))

    events = _collect(customer_id, expected=4)  # create, update, delete, tombstone

    assert len(events) == 4, f"expected 4 messages, got {len(events)}: {events}"
    create, update, delete, tombstone = events
    assert create is not None and update is not None and delete is not None
    assert tombstone is None  # Kafka tombstone for log compaction

    assert [create["op"], update["op"], delete["op"]] == ["c", "u", "d"]
    assert create["before"] is None and create["after"]["city"] == "Pune"
    assert update["before"]["city"] == "Pune" and update["after"]["city"] == "Delhi"
    # REPLICA IDENTITY FULL: the delete carries the entire old row, not just the key.
    assert delete["after"] is None and delete["before"]["email"] == email

    lsns = [e["source"]["lsn"] for e in (create, update, delete)]
    assert lsns == sorted(lsns) and len(set(lsns)) == 3, "LSN must strictly increase per row"
