import pytest

from deltaflow.cli import build_parser


def test_register_connector_defaults() -> None:
    args = build_parser().parse_args(["register-connector"])

    assert args.command == "register-connector"
    assert str(args.config).endswith("shop-connector.json")
    assert args.no_wait is False


def test_loadgen_options() -> None:
    args = build_parser().parse_args(["loadgen", "--rate", "20", "--duration", "0", "--seed", "1"])

    assert (args.rate, args.duration, args.seed) == (20.0, 0.0, 1)


def test_tail_requires_a_topic() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["tail"])


def test_run_es_sink_defaults() -> None:
    args = build_parser().parse_args(["run-es-sink"])

    assert args.group_id == "deltaflow-es-sink"
    assert args.topic_pattern.startswith("^deltaflow")
    assert args.batch_size == 200


def test_search_takes_a_table_and_text() -> None:
    args = build_parser().parse_args(["search", "customers", "Reddy", "--size", "3"])

    assert (args.table, args.text, args.size) == ("customers", "Reddy", 3)
