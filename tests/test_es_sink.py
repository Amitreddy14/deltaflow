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
