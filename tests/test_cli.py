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


def test_run_mongo_sink_defaults() -> None:
    args = build_parser().parse_args(["run-mongo-sink"])

    assert args.group_id == "deltaflow-mongo-sink"  # its own consumer group, independent of ES
    assert args.mongo_database == "deltaflow"


def test_analytics_only_accepts_known_reports() -> None:
    assert build_parser().parse_args(["analytics", "transitions"]).report == "transitions"
    with pytest.raises(SystemExit):
        build_parser().parse_args(["analytics", "bogus"])


def test_verify_accepts_a_wait() -> None:
    assert build_parser().parse_args(["verify", "--wait", "30"]).wait == 30.0


def test_format_table_aligns_columns_and_handles_no_rows() -> None:
    from deltaflow.cli import format_table

    table = format_table([{"status": "paid", "n": 12}, {"status": "shipped", "n": 3}])

    assert table.splitlines() == [
        "status   n ",
        "-------  --",
        "paid     12",
        "shipped  3 ",
    ]
    assert format_table([]) == "(no rows)"


def test_sinks_have_their_own_dead_letter_topics_which_can_be_disabled() -> None:
    parser = build_parser()

    assert parser.parse_args(["run-es-sink"]).dlq_topic == "deltaflow.dlq.es"
    assert parser.parse_args(["run-mongo-sink"]).dlq_topic == "deltaflow.dlq.mongo"
    assert parser.parse_args(["run-es-sink", "--dlq-topic", ""]).dlq_topic == ""


def test_dlq_command_takes_an_action_and_a_sink() -> None:
    args = build_parser().parse_args(["dlq", "show", "es", "--max", "5"])

    assert (args.action, args.sink, args.limit) == ("show", "es", 5)
    with pytest.raises(SystemExit):
        build_parser().parse_args(["dlq", "purge", "es"])
