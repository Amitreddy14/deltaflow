"""Construction of datastore clients, so authentication is configured in exactly one place.

Elasticsearch credentials come from ``DELTAFLOW_ELASTICSEARCH_USER`` / ``..._PASSWORD`` and are
passed as ``basic_auth`` (never embedded in the URL, where they would leak into logs and
tracebacks). MongoDB credentials travel in the connection string, which is how the driver
expects them; generate URL-safe passwords (``deltaflow gen-env`` does).
"""

from __future__ import annotations

from typing import Any

from elasticsearch import Elasticsearch
from pymongo import MongoClient

from deltaflow.config import Settings, get_settings


def elasticsearch_client(
    url: str | None = None, *, settings: Settings | None = None, **kwargs: Any
) -> Elasticsearch:
    cfg = settings or get_settings()
    if cfg.elasticsearch_user and cfg.elasticsearch_password:
        kwargs.setdefault("basic_auth", (cfg.elasticsearch_user, cfg.elasticsearch_password))
    return Elasticsearch(url or cfg.elasticsearch_url, **kwargs)


def mongo_client(
    url: str | None = None,
    *,
    settings: Settings | None = None,
    timeout_ms: int = 10000,
) -> MongoClient[dict[str, Any]]:
    cfg = settings or get_settings()
    return MongoClient(url or cfg.mongo_url, serverSelectionTimeoutMS=timeout_ms)
