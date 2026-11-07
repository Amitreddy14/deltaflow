from __future__ import annotations

from typing import Any

import pytest

from deltaflow import clients
from deltaflow.config import Settings


def test_elasticsearch_credentials_go_in_basic_auth_not_the_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def fake(url: str, **kwargs: Any) -> object:
        seen.update(url=url, **kwargs)
        return object()

    monkeypatch.setattr(clients, "Elasticsearch", fake)
    cfg = Settings(
        elasticsearch_url="http://es:9200",
        elasticsearch_user="elastic",
        elasticsearch_password="pw",
    )

    clients.elasticsearch_client(settings=cfg, request_timeout=3)

    assert seen == {
        "url": "http://es:9200",
        "basic_auth": ("elastic", "pw"),
        "request_timeout": 3,
    }


def test_no_credentials_means_no_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}
    monkeypatch.setattr(clients, "Elasticsearch", lambda url, **kw: seen.update(kw) or object())

    clients.elasticsearch_client("http://x:9200", settings=Settings())

    assert "basic_auth" not in seen


def test_url_override_wins_over_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}
    monkeypatch.setattr(clients, "Elasticsearch", lambda url, **kw: seen.update(url=url) or 1)

    clients.elasticsearch_client("http://other:9200", settings=Settings())

    assert seen["url"] == "http://other:9200"


def test_mongo_client_uses_settings_url_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake(url: str, **kwargs: Any) -> object:
        seen.update(url=url, **kwargs)
        return object()

    monkeypatch.setattr(clients, "MongoClient", fake)

    clients.mongo_client(settings=Settings(mongo_url="mongodb://u:p@m:27017"), timeout_ms=1234)

    assert seen == {"url": "mongodb://u:p@m:27017", "serverSelectionTimeoutMS": 1234}
