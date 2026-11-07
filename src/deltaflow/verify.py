"""Compare row counts in the source database with every sink: "do they all agree?"."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import psycopg
from elasticsearch import Elasticsearch, NotFoundError
from psycopg import sql
from pymongo.database import Database

from deltaflow.sinks.elasticsearch import index_name

TABLES = ("customers", "products", "orders")
Counts = Mapping[str, int]


def source_counts(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn) as conn:
        return {
            table: int(
                conn.execute(
                    sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))
                ).fetchone()[0]  # type: ignore[index]
            )
            for table in TABLES
        }


def elasticsearch_counts(es: Elasticsearch) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in TABLES:
        index = index_name(table)
        try:
            es.indices.refresh(index=index)  # make just-written documents countable
            counts[table] = int(es.count(index=index)["count"])
        except NotFoundError:
            counts[table] = 0
    return counts


def mongo_counts(db: Database[Any]) -> dict[str, int]:
    return {table: db[table].count_documents({"_deleted": False}) for table in TABLES}


def mismatches(source: Counts, sinks: Mapping[str, Counts]) -> list[str]:
    """Human-readable lines for every table where a sink disagrees with the source."""
    lines = []
    for table in TABLES:
        for sink_name, counts in sinks.items():
            if counts.get(table) != source.get(table):
                lines.append(f"{table}: source={source.get(table)} {sink_name}={counts.get(table)}")
    return lines


def wait_until_consistent(
    collect: Callable[[], tuple[Counts, Mapping[str, Counts]]],
    *,
    wait: float,
    interval: float,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> tuple[Counts, Mapping[str, Counts], list[str]]:
    """Collect counts, retrying for up to `wait` seconds while sinks catch up."""
    deadline = clock() + wait
    while True:
        source, sinks = collect()
        problems = mismatches(source, sinks)
        if not problems or clock() >= deadline:
            return source, sinks, problems
        sleep(interval)
