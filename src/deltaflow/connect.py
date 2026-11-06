"""Register and monitor the Debezium connector through the Kafka Connect REST API."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from string import Template
from typing import Any

import httpx

DEFAULT_CONNECTOR_FILE = Path("infra/debezium/shop-connector.json")
_TEMPLATE_DEFAULTS = {
    "POSTGRES_USER": "deltaflow",
    "POSTGRES_PASSWORD": "deltaflow",
    "POSTGRES_DB": "shop",
}


class ConnectError(RuntimeError):
    """Kafka Connect rejected a request or the connector is unhealthy."""


def template_variables(env: Mapping[str, str]) -> dict[str, str]:
    """Values for ${...} placeholders: environment first, then local-dev defaults."""
    return {key: env.get(key, default) for key, default in _TEMPLATE_DEFAULTS.items()}


def render_connector(path: Path, variables: Mapping[str, str]) -> tuple[str, dict[str, Any]]:
    """Load a connector file and fill in placeholders. Returns (name, config).

    Uses strict substitution, so a placeholder with no value fails loudly instead of
    silently registering a connector with a literal "${...}" password.
    """
    document = json.loads(Template(path.read_text(encoding="utf-8")).substitute(variables))
    return str(document["name"]), dict(document["config"])


def register_connector(client: httpx.Client, name: str, config: Mapping[str, Any]) -> bool:
    """Create or update a connector. PUT is idempotent. Returns True if newly created."""
    response = client.put(f"/connectors/{name}/config", json=dict(config))
    if response.status_code not in (200, 201):
        raise ConnectError(f"register {name!r} failed: {response.status_code} {response.text}")
    return response.status_code == 201


def connector_status(client: httpx.Client, name: str) -> dict[str, Any]:
    response = client.get(f"/connectors/{name}/status")
    if response.status_code != 200:
        raise ConnectError(f"status {name!r} failed: {response.status_code} {response.text}")
    body: dict[str, Any] = response.json()
    return body


def is_running(status: Mapping[str, Any]) -> bool:
    """True when the connector and every one of its tasks report RUNNING."""
    tasks = status.get("tasks", [])
    return bool(
        status["connector"]["state"] == "RUNNING"
        and tasks
        and all(task["state"] == "RUNNING" for task in tasks)
    )


def wait_until_running(
    client: httpx.Client,
    name: str,
    *,
    timeout: float = 90.0,
    interval: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Poll until the connector and its tasks are RUNNING. Fails fast on FAILED."""
    deadline = clock() + timeout
    while True:
        try:
            status = connector_status(client, name)
        except ConnectError:
            status = {}  # not visible yet right after creation
        if status:
            states = [status["connector"]["state"], *(t["state"] for t in status.get("tasks", []))]
            if "FAILED" in states:
                trace = next((t.get("trace", "") for t in status.get("tasks", [])), "")
                raise ConnectError(f"connector {name!r} FAILED: {trace[:600]}")
            if is_running(status):
                return status
        if clock() >= deadline:
            raise ConnectError(f"connector {name!r} not RUNNING after {timeout:.0f}s: {status}")
        sleep(interval)
