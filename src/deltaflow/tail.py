"""Print change events from a Kafka topic in a compact, readable form."""

from __future__ import annotations

import json
import uuid
from typing import Any

from confluent_kafka import Consumer

_OP_NAMES = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}


def format_event(key: bytes | None, value: bytes | None) -> str:
    """One line per event. A null value is Kafka's tombstone that follows a delete."""
    key_text = json.dumps(json.loads(key)) if key else "-"
    if value is None:
        return f"TOMBSTONE key={key_text}"
    event: dict[str, Any] = json.loads(value)
    if "op" not in event:  # e.g. heartbeat messages
        return f"OTHER key={key_text} value={json.dumps(event)[:100]}"
    op = _OP_NAMES.get(event["op"], event["op"])
    lsn = event.get("source", {}).get("lsn")
    before, after = event.get("before"), event.get("after")
    changed = ""
    if before and after:
        diff = {k: f"{before[k]} -> {after[k]}" for k in after if before.get(k) != after[k]}
        changed = " " + json.dumps(diff)
    elif after:
        changed = f" {json.dumps(after)[:100]}"
    elif before:
        changed = f" was {json.dumps(before)[:100]}"
    return f"{op:<8} lsn={lsn} key={key_text}{changed}"


def tail(bootstrap_servers: str, topic: str, *, max_messages: int, from_beginning: bool) -> int:
    """Print up to max_messages events (or until 5 s of silence). Returns count printed."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"deltaflow-tail-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    printed = 0
    try:
        idle = 0
        while printed < max_messages and idle < 5:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            idle = 0
            print(format_event(message.key(), message.value()))
            printed += 1
    finally:
        consumer.close()
    return printed
