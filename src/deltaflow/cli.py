"""Command line entry point: `deltaflow <command>`."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from deltaflow import analytics, connect, deadletter, loadgen, runners, tail, verify
from deltaflow.config import get_settings


def build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    parser = argparse.ArgumentParser(prog="deltaflow", description="Deltaflow CDC platform tools")
    commands = parser.add_subparsers(dest="command", required=True)

    reg = commands.add_parser("register-connector", help="create or update the Debezium connector")
    reg.add_argument("--config", type=Path, default=connect.DEFAULT_CONNECTOR_FILE)
    reg.add_argument("--connect-url", default=settings.connect_url)
    reg.add_argument("--no-wait", action="store_true", help="do not wait for RUNNING")

    status = commands.add_parser("connector-status", help="show connector and task state")
    status.add_argument("--name", default="shop-postgres")
    status.add_argument("--connect-url", default=settings.connect_url)

    load = commands.add_parser("loadgen", help="generate inserts/updates/deletes in Postgres")
    load.add_argument("--dsn", default=settings.postgres_dsn)
    load.add_argument("--rate", type=float, default=5.0, help="operations per second")
    load.add_argument("--duration", type=float, default=30.0, help="seconds; 0 = until Ctrl+C")
    load.add_argument("--seed", type=int, default=None)

    watch = commands.add_parser("tail", help="print change events from a Kafka topic")
    watch.add_argument("topic", help="e.g. deltaflow.public.orders")
    watch.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    watch.add_argument("--max", type=int, default=20, dest="max_messages")
    watch.add_argument("--from-beginning", action="store_true")

    for name, help_text, default_group in (
        ("run-es-sink", "stream change events into Elasticsearch", "deltaflow-es-sink"),
        ("run-mongo-sink", "stream change events into MongoDB", "deltaflow-mongo-sink"),
    ):
        sink = commands.add_parser(name, help=help_text)
        sink.add_argument("--group-id", default=default_group)
        sink.add_argument("--topic-pattern", default=settings.kafka_topic_pattern)
        sink.add_argument("--batch-size", type=int, default=200)
        sink.add_argument(
            "--dlq-topic",
            default=deadletter.dlq_topic("es" if name == "run-es-sink" else "mongo"),
            help="dead-letter topic; pass an empty string to stop on bad events instead",
        )
        sink.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
        if name == "run-es-sink":
            sink.add_argument("--elasticsearch-url", default=settings.elasticsearch_url)
        else:
            sink.add_argument("--mongo-url", default=settings.mongo_url)
            sink.add_argument("--mongo-database", default=settings.mongo_database)

    find = commands.add_parser("search", help="full-text search a table's Elasticsearch index")
    find.add_argument("table", help="orders, customers or products")
    find.add_argument("text", help='query, e.g. "Reddy" or "status:shipped"')
    find.add_argument("--size", type=int, default=5)
    find.add_argument("--elasticsearch-url", default=settings.elasticsearch_url)

    report = commands.add_parser("analytics", help="run an analytics report on MongoDB")
    report.add_argument("report", choices=analytics.REPORTS)
    report.add_argument("--unit", choices=analytics.UNITS, default="hour", help="for 'rate'")
    report.add_argument("--since-minutes", type=int, default=None, help="only recent history")
    report.add_argument("--mongo-url", default=settings.mongo_url)
    report.add_argument("--mongo-database", default=settings.mongo_database)

    dlq = commands.add_parser("dlq", help="inspect or replay dead-lettered events")
    dlq.add_argument("action", choices=("show", "replay"))
    dlq.add_argument("sink", choices=("es", "mongo"))
    dlq.add_argument("--max", type=int, default=20, dest="limit")
    dlq.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)

    check = commands.add_parser(
        "verify", help="compare row counts: Postgres vs Elasticsearch vs Mongo"
    )
    check.add_argument(
        "--wait", type=float, default=0.0, help="seconds to wait for sinks to catch up"
    )
    check.add_argument("--dsn", default=settings.postgres_dsn)
    check.add_argument("--elasticsearch-url", default=settings.elasticsearch_url)
    check.add_argument("--mongo-url", default=settings.mongo_url)
    check.add_argument("--mongo-database", default=settings.mongo_database)
    return parser


def _stop_on_signal() -> threading.Event:
    logging.basicConfig(
        level=get_settings().log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())  # finish the current batch, then exit cleanly
    return stop


def _run_es_sink(args: argparse.Namespace) -> None:
    runners.run_es_sink(
        bootstrap_servers=args.bootstrap_servers,
        elasticsearch_url=args.elasticsearch_url,
        group_id=args.group_id,
        topic_pattern=args.topic_pattern,
        batch_size=args.batch_size,
        dead_letter_topic=args.dlq_topic or None,
        stop=_stop_on_signal(),
    )


def _run_mongo_sink(args: argparse.Namespace) -> None:
    runners.run_mongo_sink(
        bootstrap_servers=args.bootstrap_servers,
        mongo_url=args.mongo_url,
        mongo_database=args.mongo_database,
        group_id=args.group_id,
        topic_pattern=args.topic_pattern,
        batch_size=args.batch_size,
        dead_letter_topic=args.dlq_topic or None,
        stop=_stop_on_signal(),
    )


def _dlq(args: argparse.Namespace) -> None:
    import uuid

    from confluent_kafka import Consumer, Producer

    topic = deadletter.dlq_topic(args.sink)
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"deltaflow-dlq-cli-{uuid.uuid4().hex[:8]}",  # never commits, never shared
            "enable.auto.commit": False,
        }
    )
    try:
        limit = args.limit if args.action == "show" else None
        letters = deadletter.read_dead_letters(consumer, topic, limit=limit)
    finally:
        consumer.close()
    if args.action == "show":
        print(format_table([letter.summary() for letter in letters]))
        print(f"{len(letters)} dead letter(s) in {topic}")
        return
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    sent = deadletter.replay_dead_letters(producer, letters)
    print(f"re-published {sent} dead letter(s) from {topic} to their source topics")


def _search(args: argparse.Namespace) -> None:
    from elasticsearch import Elasticsearch

    from deltaflow.sinks.elasticsearch import search_documents

    with Elasticsearch(args.elasticsearch_url) as es:
        hits = search_documents(es, args.table, args.text, size=args.size)
    for hit in hits:
        print(f"id={hit['id']} score={hit['score']:.2f} {json.dumps(hit['doc'])[:160]}")
    print(f"{len(hits)} hit(s)")


def format_table(rows: list[dict[str, Any]]) -> str:
    """Left-aligned text table; values are stringified (dates as ISO)."""
    if not rows:
        return "(no rows)"
    columns = list(rows[0])
    cells = [[_cell(row.get(c)) for c in columns] for row in rows]
    widths = [max(len(c), *(len(r[i]) for r in cells)) for i, c in enumerate(columns)]
    lines = ["  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))]
    lines.append("  ".join("-" * w for w in widths))
    lines += ["  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)) for r in cells]
    return "\n".join(lines)


def _cell(value: Any) -> str:
    return value.isoformat(timespec="seconds") if isinstance(value, datetime) else str(value)


def _analytics(args: argparse.Namespace) -> None:
    from pymongo import MongoClient

    since = (
        datetime.now(UTC) - timedelta(minutes=args.since_minutes) if args.since_minutes else None
    )
    client: MongoClient[dict[str, Any]] = MongoClient(args.mongo_url, serverSelectionTimeoutMS=5000)
    try:
        rows = analytics.run_report(
            client[args.mongo_database], args.report, unit=args.unit, since=since
        )
    finally:
        client.close()
    print(format_table(rows))


def _verify(args: argparse.Namespace) -> int:
    from elasticsearch import Elasticsearch
    from pymongo import MongoClient

    es = Elasticsearch(args.elasticsearch_url)
    mongo: MongoClient[dict[str, Any]] = MongoClient(args.mongo_url, serverSelectionTimeoutMS=5000)
    try:
        db = mongo[args.mongo_database]
        source, sinks, problems = verify.wait_until_consistent(
            lambda: (
                verify.source_counts(args.dsn),
                {
                    "elasticsearch": verify.elasticsearch_counts(es),
                    "mongodb": verify.mongo_counts(db),
                },
            ),
            wait=args.wait,
            interval=2.0,
            sleep=time.sleep,
            clock=time.monotonic,
        )
    finally:
        es.close()
        mongo.close()
    rows = [
        {
            "table": t,
            "postgres": source[t],
            "elasticsearch": sinks["elasticsearch"][t],
            "mongodb": sinks["mongodb"][t],
        }
        for t in verify.TABLES
    ]
    print(format_table(rows))
    if problems:
        print("\nMISMATCH:\n  " + "\n  ".join(problems), file=sys.stderr)
        return 1
    print("\nOK: every sink matches the source")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "register-connector":
            variables = connect.template_variables(os.environ)
            name, config = connect.render_connector(args.config, variables)
            with httpx.Client(base_url=args.connect_url, timeout=15.0) as client:
                created = connect.register_connector(client, name, config)
                print(f"connector {name!r} {'created' if created else 'updated'}")
                if not args.no_wait:
                    connect.wait_until_running(client, name)
                    print(f"connector {name!r} is RUNNING")
        elif args.command == "connector-status":
            with httpx.Client(base_url=args.connect_url, timeout=15.0) as client:
                status = connect.connector_status(client, args.name)
            tasks = ", ".join(f"task{t['id']}={t['state']}" for t in status["tasks"])
            print(f"{args.name}: connector={status['connector']['state']} {tasks}")
        elif args.command == "loadgen":
            duration = None if args.duration == 0 else args.duration
            counts = loadgen.run(args.dsn, rate=args.rate, duration=duration, seed=args.seed)
            print("done: " + ", ".join(f"{op.value}={n}" for op, n in sorted(counts.items())))
        elif args.command == "tail":
            tail.tail(
                args.bootstrap_servers,
                args.topic,
                max_messages=args.max_messages,
                from_beginning=args.from_beginning,
            )
        elif args.command == "run-es-sink":
            _run_es_sink(args)
        elif args.command == "run-mongo-sink":
            _run_mongo_sink(args)
        elif args.command == "dlq":
            _dlq(args)
        elif args.command == "search":
            _search(args)
        elif args.command == "analytics":
            _analytics(args)
        elif args.command == "verify":
            return _verify(args)
    except (connect.ConnectError, httpx.HTTPError, RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
