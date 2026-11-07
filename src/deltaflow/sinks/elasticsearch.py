"""Elasticsearch sink: current row state, kept correct by external versioning (ADR-0004/0008).

Every write carries the event's LSN as the document version with version_type=external.
Elasticsearch accepts a write only if its version is greater than the stored one, so a
redelivered, replayed or out-of-order event can never overwrite newer data. Rejections come
back as HTTP 409 and are the expected, harmless signal that the event was already applied.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from elastic_transport import TransportError
from elasticsearch import Elasticsearch, helpers

from deltaflow.events import ChangeEvent
from deltaflow.sinks.base import RetryableSinkError, SinkError

log = logging.getLogger(__name__)

INDEX_PREFIX = "deltaflow"
TEMPLATE_NAME = "deltaflow"

_TEXT_WITH_KEYWORD = {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
_DATE = {"type": "date", "ignore_malformed": True}  # a bad timestamp must not poison the sink

INDEX_TEMPLATE: dict[str, Any] = {
    "index_patterns": [f"{INDEX_PREFIX}-*"],
    "priority": 100,
    "template": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,  # single node: replicas would leave indices yellow forever
            # Elasticsearch remembers a delete's version only this long. A replayed older event
            # arriving after that window could resurrect a deleted row (ADR-0004).
            "index.gc_deletes": "10m",
        },
        "mappings": {
            "dynamic": True,  # additive source columns appear automatically
            "properties": {
                "cdc_lsn": {"type": "long"},
                "cdc_op": {"type": "keyword"},
                "cdc_ts_ms": {"type": "date", "format": "epoch_millis", "ignore_malformed": True},
                "id": {"type": "long"},
                "customer_id": {"type": "long"},
                "product_id": {"type": "long"},
                "email": {"type": "keyword"},
                "city": {"type": "keyword"},
                "status": {"type": "keyword"},
                "category": {"type": "keyword"},
                "sku": {"type": "keyword"},
                "full_name": _TEXT_WITH_KEYWORD,
                "name": _TEXT_WITH_KEYWORD,
                "created_at": _DATE,
                "updated_at": _DATE,
            },
        },
    },
}


def index_name(table_name: str, prefix: str = INDEX_PREFIX) -> str:
    return f"{prefix}-{table_name}"


def to_action(event: ChangeEvent, prefix: str = INDEX_PREFIX) -> dict[str, Any]:
    """Translate one event into an Elasticsearch bulk action, versioned by its LSN."""
    action: dict[str, Any] = {
        "_index": index_name(event.table_name, prefix),
        "_id": event.doc_id,
        "_version": event.lsn,
        "_version_type": "external",
    }
    if event.is_delete:
        return {**action, "_op_type": "delete"}
    assert event.after is not None  # guaranteed by the parser for every non-delete
    document = {
        **event.after,
        "cdc_lsn": event.lsn,
        "cdc_op": event.op.value,
        "cdc_ts_ms": event.ts_ms,
    }
    return {**action, "_op_type": "index", "_source": document}


def ensure_index_template(es: Elasticsearch) -> None:
    es.indices.put_index_template(name=TEMPLATE_NAME, **INDEX_TEMPLATE)


BulkFn = Callable[..., tuple[int, Any]]


class ElasticsearchSink:
    def __init__(
        self,
        es: Elasticsearch,
        *,
        prefix: str = INDEX_PREFIX,
        bulk: BulkFn = helpers.bulk,
    ) -> None:
        self._es = es
        self._prefix = prefix
        self._bulk = bulk
        self.applied = 0
        self.stale = 0

    def write(self, events: Sequence[ChangeEvent]) -> None:
        if not events:
            return
        actions = [to_action(event, self._prefix) for event in events]
        try:
            _, errors = self._bulk(self._es, actions, raise_on_error=False, stats_only=False)
        except TransportError as exc:
            raise RetryableSinkError(f"elasticsearch unreachable: {exc}") from exc

        stale, retryable, permanent = self._classify(errors)
        self.stale += stale
        self.applied += len(events) - stale - len(retryable) - len(permanent)
        if retryable:
            raise RetryableSinkError(
                f"{len(retryable)} bulk items failed transiently: {retryable[0]}"
            )
        if permanent:
            raise SinkError(f"{len(permanent)} bulk items rejected: {permanent[0]}")
        log.info("applied %d events (%d stale or duplicate skipped)", len(events) - stale, stale)

    @staticmethod
    def _classify(errors: Iterable[dict[str, Any]]) -> tuple[int, list[Any], list[Any]]:
        stale, retryable, permanent = 0, [], []
        for item in errors:
            detail = next(iter(item.values()))  # item is {"index": {...}} or {"delete": {...}}
            status = int(detail.get("status", 0))
            if status == 409:
                stale += 1  # version conflict: a newer (or identical) version is already stored
            elif status == 404:
                continue  # deleting a row that is not indexed: the desired end state already holds
            elif status == 429 or status >= 500:
                retryable.append(detail)
            else:
                permanent.append(detail)
        return stale, retryable, permanent


def search_documents(
    es: Elasticsearch, table_name: str, text: str, *, size: int = 5, prefix: str = INDEX_PREFIX
) -> list[dict[str, Any]]:
    """Full-text search across every field of a table's index. Returns hits (id, score, doc)."""
    response = es.search(
        index=index_name(table_name, prefix),
        query={"simple_query_string": {"query": text}},
        size=size,
    )
    return [
        {"id": hit["_id"], "score": hit["_score"], "doc": hit["_source"]}
        for hit in response["hits"]["hits"]
    ]
