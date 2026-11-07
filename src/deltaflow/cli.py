"""Command line entry point: `deltaflow <command>`."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from collections.abc import Sequence
from pathlib import Path

import httpx

from deltaflow import connect, loadgen, runners, tail
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

    sink = commands.add_parser("run-es-sink", help="stream change events into Elasticsearch")
    sink.add_argument("--group-id", default="deltaflow-es-sink")
    sink.add_argument("--topic-pattern", default=settings.kafka_topic_pattern)
    sink.add_argument("--batch-size", type=int, default=200)
    sink.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    sink.add_argument("--elasticsearch-url", default=settings.elasticsearch_url)

    find = commands.add_parser("search", help="full-text search a table's Elasticsearch index")
    find.add_argument("table", help="orders, customers or products")
    find.add_argument("text", help='query, e.g. "Reddy" or "status:shipped"')
    find.add_argument("--size", type=int, default=5)
    find.add_argument("--elasticsearch-url", default=settings.elasticsearch_url)
    return parser


def _run_es_sink(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=get_settings().log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())  # finish the current batch, then exit cleanly
    runners.run_es_sink(
        bootstrap_servers=args.bootstrap_servers,
        elasticsearch_url=args.elasticsearch_url,
        group_id=args.group_id,
        topic_pattern=args.topic_pattern,
        batch_size=args.batch_size,
        stop=stop,
    )


def _search(args: argparse.Namespace) -> None:
    from elasticsearch import Elasticsearch

    from deltaflow.sinks.elasticsearch import search_documents

    with Elasticsearch(args.elasticsearch_url) as es:
        hits = search_documents(es, args.table, args.text, size=args.size)
    for hit in hits:
        print(f"id={hit['id']} score={hit['score']:.2f} {json.dumps(hit['doc'])[:160]}")
    print(f"{len(hits)} hit(s)")


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
        elif args.command == "search":
            _search(args)
    except (connect.ConnectError, httpx.HTTPError, RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
