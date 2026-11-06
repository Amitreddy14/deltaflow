import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from deltaflow import connect

CONNECTOR_FILE = Path(__file__).resolve().parents[1] / "infra" / "debezium" / "shop-connector.json"


def _client(handler: Any) -> httpx.Client:
    return httpx.Client(base_url="http://connect", transport=httpx.MockTransport(handler))


def test_template_variables_prefer_env_then_defaults() -> None:
    variables = connect.template_variables({"POSTGRES_USER": "alice"})

    assert variables["POSTGRES_USER"] == "alice"
    assert variables["POSTGRES_DB"] == "shop"


def test_shipped_connector_renders_with_no_unresolved_placeholders() -> None:
    name, config = connect.render_connector(CONNECTOR_FILE, connect.template_variables({}))

    assert name == "shop-postgres"
    assert config["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert config["plugin.name"] == "pgoutput"
    assert config["publication.name"] == "deltaflow_pub"
    assert config["key.converter.schemas.enable"] == "false"
    assert "${" not in json.dumps(config)


def test_render_fails_loudly_on_missing_placeholder(tmp_path: Path) -> None:
    template = tmp_path / "c.json"
    template.write_text('{"name": "x", "config": {"p": "${NOPE}"}}', encoding="utf-8")

    with pytest.raises(KeyError):
        connect.render_connector(template, {})


def test_register_reports_created_vs_updated() -> None:
    statuses = iter([201, 200])
    client = _client(lambda request: httpx.Response(next(statuses), json={}))

    assert connect.register_connector(client, "c", {"a": "1"}) is True
    assert connect.register_connector(client, "c", {"a": "1"}) is False


def test_register_raises_on_rejection() -> None:
    client = _client(lambda request: httpx.Response(400, text="bad config"))

    with pytest.raises(connect.ConnectError, match="bad config"):
        connect.register_connector(client, "c", {})


def test_is_running_requires_connector_and_all_tasks() -> None:
    running = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
    task_down = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "UNASSIGNED"}]}
    no_tasks = {"connector": {"state": "RUNNING"}, "tasks": []}

    assert connect.is_running(running)
    assert not connect.is_running(task_down)
    assert not connect.is_running(no_tasks)


def test_wait_until_running_polls_until_ready() -> None:
    responses = iter(
        [
            httpx.Response(404, json={}),  # not visible yet
            httpx.Response(
                200, json={"connector": {"state": "RUNNING"}, "tasks": [{"state": "UNASSIGNED"}]}
            ),
            httpx.Response(
                200, json={"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
            ),
        ]
    )
    client = _client(lambda request: next(responses))

    status = connect.wait_until_running(client, "c", sleep=lambda _: None)

    assert status["tasks"][0]["state"] == "RUNNING"


def test_wait_until_running_fails_fast_on_failed_task() -> None:
    body = {
        "connector": {"state": "RUNNING"},
        "tasks": [{"state": "FAILED", "trace": "replication slot error"}],
    }
    client = _client(lambda request: httpx.Response(200, json=body))

    with pytest.raises(connect.ConnectError, match="replication slot error"):
        connect.wait_until_running(client, "c", sleep=lambda _: None)


def test_wait_until_running_times_out() -> None:
    client = _client(lambda request: httpx.Response(404, json={}))
    ticks = iter(range(0, 1000, 40))

    with pytest.raises(connect.ConnectError, match="not RUNNING"):
        connect.wait_until_running(
            client, "c", timeout=90, sleep=lambda _: None, clock=lambda: float(next(ticks))
        )
