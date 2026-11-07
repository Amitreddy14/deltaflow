"""Shared fixtures for tests that need the docker compose stack."""

from __future__ import annotations

import socket
from urllib.parse import urlparse

import httpx
import pytest

from deltaflow import connect
from deltaflow.config import get_settings


def reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _endpoints() -> list[tuple[str, int]]:
    """Host and port of every service the tests talk to, taken from the active settings."""
    settings = get_settings()
    postgres = urlparse(settings.postgres_dsn)
    connect_url = urlparse(settings.connect_url)
    es_url = urlparse(settings.elasticsearch_url)
    mongo_url = urlparse(settings.mongo_url)
    kafka_host, _, kafka_port = settings.kafka_bootstrap_servers.rpartition(":")
    return [
        (postgres.hostname or "127.0.0.1", postgres.port or 5432),
        (kafka_host, int(kafka_port)),
        (connect_url.hostname or "127.0.0.1", connect_url.port or 8083),
        (es_url.hostname or "127.0.0.1", es_url.port or 9200),
        (mongo_url.hostname or "127.0.0.1", mongo_url.port or 27017),
    ]


@pytest.fixture(scope="module", autouse=True)
def running_connector() -> None:
    """Skip when the stack is down; otherwise make sure the Debezium connector is RUNNING."""
    if not all(reachable(host, port) for host, port in _endpoints()):
        pytest.skip("compose stack is not running (docker compose up -d --wait)")
    name, config = connect.render_connector(
        connect.DEFAULT_CONNECTOR_FILE, connect.template_variables({})
    )
    with httpx.Client(base_url=get_settings().connect_url, timeout=15.0) as client:
        connect.register_connector(client, name, config)
        connect.wait_until_running(client, name)
