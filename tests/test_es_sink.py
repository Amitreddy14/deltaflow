from typing import Any

import pytest
from elastic_transport import ConnectionError as EsConnectionError
from elastic_transport import ConnectionTimeout

from deltaflow.events import ChangeEvent, Op
from deltaflow.sinks import elasticsearch as es_sink
from deltaflow.sinks.base import RetryableSinkError, SinkError


def _event(op: Op = Op.CREATE, lsn: int = 100, **row: Any) -> ChangeEvent:
    image = {"id": 7, "status": "paid", **row}
    return ChangeEvent(
        op=op,
        table="public.orders",
        key={"id": 7},
        before=image if op is Op.DELETE else None,
        after=None if op is Op.DELETE else image,
        lsn=lsn,
        ts_ms=1234,
    )


class FakeBulk:
    """Stands in for elasticsearch.helpers.bulk; records calls and returns scripted errors."""

    def __init__(self, errors: list[dict[str, Any]] | None = None, raises: Exception | None = None):
        self.errors = errors or []
        self.raises = raises
        self.calls: list[list[dict[str, Any]]] = []

    def __call__(self, client: object, actions: list[dict[str, Any]], **kwargs: Any) -> Any:
        assert kwargs["raise_on_error"] is False  # failures must come back, not raise
        self.calls.append(actions)
        if self.raises:
            raise self.raises
        return len(actions) - len(self.errors), self.errors


def _sink(bulk: FakeBulk) -> es_sink.ElasticsearchSink:
    return es_sink.ElasticsearchSink(object(), bulk=bulk)  # type: ignore[arg-type]


def test_index_action_is_versioned_by_lsn() -> None:
    action = es_sink.to_action(_event(Op.UPDATE, lsn=555))

    assert action["_op_type"] == "index"
    assert action["_index"] == "deltaflow-orders" and action["_id"] == "7"
    assert action["_version"] == 555 and action["_version_type"] == "external"
    assert action["_source"]["status"] == "paid"
    assert action["_source"]["cdc_lsn"] == 555 and action["_source"]["cdc_op"] == "u"


def test_delete_action_carries_the_same_version_and_no_source() -> None:
    action = es_sink.to_action(_event(Op.DELETE, lsn=900))

    assert action["_op_type"] == "delete"
    assert action["_version"] == 900 and action["_version_type"] == "external"
    assert "_source" not in action


def test_write_sends_all_events_in_one_ordered_bulk() -> None:
    bulk = FakeBulk()
    events = [_event(Op.CREATE, 1), _event(Op.UPDATE, 2), _event(Op.DELETE, 3)]

    _sink(bulk).write(events)

    assert len(bulk.calls) == 1
    assert [a["_version"] for a in bulk.calls[0]] == [1, 2, 3]  # order preserved


def test_empty_batch_makes_no_request() -> None:
    bulk = FakeBulk()

    _sink(bulk).write([])

    assert bulk.calls == []


def test_version_conflicts_are_the_expected_idempotency_signal() -> None:
    conflict = {"index": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}}
    sink = _sink(FakeBulk(errors=[conflict]))

    sink.write([_event(), _event(lsn=101)])  # must not raise

    assert sink.stale == 1 and sink.applied == 1


def test_deleting_a_missing_document_is_fine() -> None:
    sink = _sink(FakeBulk(errors=[{"delete": {"status": 404, "result": "not_found"}}]))

    sink.write([_event(Op.DELETE, 5)])  # must not raise


@pytest.mark.parametrize("status", [429, 500, 503])
def test_overload_and_server_errors_are_retryable(status: int) -> None:
    sink = _sink(FakeBulk(errors=[{"index": {"status": status, "error": "busy"}}]))

    with pytest.raises(RetryableSinkError):
        sink.write([_event()])


def test_rejected_documents_are_permanent_errors() -> None:
    sink = _sink(FakeBulk(errors=[{"index": {"status": 400, "error": "mapper_parsing_exception"}}]))

    with pytest.raises(SinkError) as raised:
        sink.write([_event()])

    assert not isinstance(raised.value, RetryableSinkError)


@pytest.mark.parametrize("error", [EsConnectionError("down"), ConnectionTimeout("slow")])
def test_unreachable_elasticsearch_is_retryable(error: Exception) -> None:
    with pytest.raises(RetryableSinkError):
        _sink(FakeBulk(raises=error)).write([_event()])


def test_template_installs_single_node_friendly_settings() -> None:
    calls: list[dict[str, Any]] = []

    class Indices:
        def put_index_template(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    class Client:
        indices = Indices()

    es_sink.ensure_index_template(Client())  # type: ignore[arg-type]

    (call,) = calls
    assert call["name"] == "deltaflow" and call["index_patterns"] == ["deltaflow-*"]
    settings = call["template"]["settings"]
    assert settings["number_of_replicas"] == 0
    assert settings["index.gc_deletes"] == "10m"
    props = call["template"]["mappings"]["properties"]
    assert props["status"]["type"] == "keyword" and props["created_at"]["ignore_malformed"] is True


# ---- TRUNCATE (ADR-0010) -------------------------------------------------------------------


class FakeIndices:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    def refresh(self, **kwargs: Any) -> None:
        self.log.append("refresh")


class FakeEs:
    """Records the calls a truncate makes, in order."""

    def __init__(self, *, hits: list[dict[str, Any]] | None = None) -> None:
        self.log: list[str] = []
        self.calls: dict[str, dict[str, Any]] = {}
        self.indices = FakeIndices(self.log)
        self.hits = hits or []
        self.index_error: Exception | None = None
        self.delete_error: Exception | None = None

    def index(self, **kwargs: Any) -> None:
        self.log.append("index")
        self.calls["index"] = kwargs
        if self.index_error:
            raise self.index_error

    def delete_by_query(self, **kwargs: Any) -> None:
        self.log.append("delete_by_query")
        self.calls["delete_by_query"] = kwargs
        if self.delete_error:
            raise self.delete_error

    def search(self, **kwargs: Any) -> Any:
        return {"hits": {"hits": self.hits}}


def _truncate_event(lsn: int = 500) -> ChangeEvent:
    return ChangeEvent(
        op=Op.TRUNCATE, table="public.orders", key={}, before=None, after=None, lsn=lsn, ts_ms=1
    )


def _es_sink(es: FakeEs, bulk: FakeBulk) -> es_sink.ElasticsearchSink:
    return es_sink.ElasticsearchSink(es, bulk=bulk)  # type: ignore[arg-type]


def test_truncate_records_the_watermark_then_deletes_only_older_documents() -> None:
    es, bulk = FakeEs(), FakeBulk()

    _es_sink(es, bulk).write([_truncate_event(500)])

    assert es.log == ["index", "refresh", "delete_by_query"]  # mark first, make visible, delete
    assert es.calls["index"]["version"] == 500 and es.calls["index"]["version_type"] == "external"
    assert es.calls["index"]["id"] == "orders"
    delete = es.calls["delete_by_query"]
    assert delete["index"] == "deltaflow-orders"
    assert delete["query"] == {"range": {"cdc_lsn": {"lt": 500}}}  # newer rows survive
    assert delete["conflicts"] == "proceed" and delete["ignore_unavailable"] is True
    assert bulk.calls == []  # a truncate is not a bulk action


def test_rows_older_than_a_truncate_are_superseded_whatever_the_arrival_order() -> None:
    es, bulk = FakeEs(), FakeBulk()
    sink = _es_sink(es, bulk)

    sink.write([_truncate_event(500)])
    sink.write([_event(Op.CREATE, lsn=400), _event(Op.UPDATE, lsn=600)])

    (actions,) = bulk.calls
    assert [a["_version"] for a in actions] == [600]  # the pre-truncate event never reaches ES
    assert sink.stale == 1 and sink.truncated == 1


def test_truncate_and_older_rows_in_one_batch_resolve_the_same_way() -> None:
    es, bulk = FakeEs(), FakeBulk()
    sink = _es_sink(es, bulk)

    sink.write([_event(Op.CREATE, lsn=100), _truncate_event(500), _event(Op.CREATE, lsn=700)])

    (actions,) = bulk.calls
    assert [a["_version"] for a in actions] == [700]


def test_watermarks_survive_a_restart() -> None:
    es = FakeEs(hits=[{"_id": "orders", "_source": {"lsn": 500}}])
    bulk = FakeBulk()
    sink = _es_sink(es, bulk)

    sink.load_state()
    sink.write([_event(Op.CREATE, lsn=499)])

    assert es.log == ["refresh"]  # watermarks written moments ago must be made searchable first
    assert bulk.calls == [] and sink.stale == 1


def test_an_already_recorded_newer_truncate_is_not_an_error() -> None:
    from elasticsearch import ApiError

    es = FakeEs()
    es.index_error = ApiError("conflict", _meta(409), {})

    _es_sink(es, FakeBulk()).write([_truncate_event(500)])

    assert "delete_by_query" in es.log  # still cleans up; the delete is idempotent


def _meta(status: int) -> Any:
    from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig

    return ApiResponseMeta(
        status=status,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig("http", "x", 9200),
    )


def test_truncate_failures_are_classified() -> None:
    from elasticsearch import ApiError

    down = FakeEs()
    down.delete_error = EsConnectionError("refused")
    with pytest.raises(RetryableSinkError):
        _es_sink(down, FakeBulk()).write([_truncate_event()])

    busy = FakeEs()
    busy.delete_error = ApiError("busy", _meta(429), {})
    with pytest.raises(RetryableSinkError):
        _es_sink(busy, FakeBulk()).write([_truncate_event()])

    bad = FakeEs()
    bad.delete_error = ApiError("bad", _meta(400), {})
    with pytest.raises(SinkError):
        _es_sink(bad, FakeBulk()).write([_truncate_event()])
