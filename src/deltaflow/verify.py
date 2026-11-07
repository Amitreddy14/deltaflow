"""Compare row counts in the source database with every sink: "do they all agree?"."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import psycopg
from elasticsearch import Elasticsearch, NotFoundError, helpers
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


# ---- deep verification: the same rows with the same values, not just the same counts --------

# Plain text and integer columns only. Those survive the trip through Debezium's JSON unchanged,
# whereas timestamps and decimals are re-encoded on the way and would need conversion to compare.
COMPARE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "customers": ("email", "full_name", "city"),
    "products": ("sku", "name", "category", "price_cents", "stock"),
    "orders": ("customer_id", "product_id", "quantity", "status", "total_cents"),
}
Rows = Mapping[int, tuple[Any, ...]]  # primary key -> compared column values
TableRows = Mapping[str, Rows]


def source_rows(dsn: str) -> dict[str, dict[int, tuple[Any, ...]]]:
    result: dict[str, dict[int, tuple[Any, ...]]] = {}
    with psycopg.connect(dsn) as conn:
        for table, columns in COMPARE_COLUMNS.items():
            query = sql.SQL("SELECT id, {} FROM {}").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in columns), sql.Identifier(table)
            )
            result[table] = {int(r[0]): tuple(r[1:]) for r in conn.execute(query).fetchall()}
    return result


def elasticsearch_rows(es: Elasticsearch) -> dict[str, dict[int, tuple[Any, ...]]]:
    result: dict[str, dict[int, tuple[Any, ...]]] = {}
    for table, columns in COMPARE_COLUMNS.items():
        index = index_name(table)
        try:
            es.indices.refresh(index=index)
            hits = helpers.scan(es, index=index, query={"query": {"match_all": {}}}, size=1000)
            result[table] = {
                int(hit["_id"]): tuple(hit["_source"].get(c) for c in columns) for hit in hits
            }
        except NotFoundError:
            result[table] = {}
    return result


def mongo_rows(db: Database[Any]) -> dict[str, dict[int, tuple[Any, ...]]]:
    result: dict[str, dict[int, tuple[Any, ...]]] = {}
    for table, columns in COMPARE_COLUMNS.items():
        documents = db[table].find({"_deleted": False}, dict.fromkeys(columns, 1))
        result[table] = {int(d["_id"]): tuple(d.get(c) for c in columns) for d in documents}
    return result


def deep_mismatches(
    source: TableRows, sinks: Mapping[str, TableRows], *, show: int = 5
) -> list[str]:
    """One line per table and sink that differs: missing ids, extra ids, differing values."""
    lines: list[str] = []
    for table in COMPARE_COLUMNS:
        expected = source.get(table, {})
        for sink_name, tables in sinks.items():
            actual = tables.get(table, {})
            missing = sorted(expected.keys() - actual.keys())
            extra = sorted(actual.keys() - expected.keys())
            differing = sorted(
                i for i in expected.keys() & actual.keys() if expected[i] != actual[i]
            )
            if not (missing or extra or differing):
                continue
            parts = []
            if missing:
                parts.append(f"{len(missing)} missing {missing[:show]}")
            if extra:
                parts.append(f"{len(extra)} extra {extra[:show]}")
            if differing:
                sample = [(i, expected[i], actual[i]) for i in differing[:show]]
                parts.append(f"{len(differing)} differing (id, source, sink) {sample}")
            lines.append(f"{table}/{sink_name}: " + "; ".join(parts))
    return lines


def wait_for(
    check: Callable[[], list[str]],
    *,
    wait: float,
    interval: float,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> list[str]:
    """Run `check` (returns problems) repeatedly for up to `wait` seconds until it is clean."""
    deadline = clock() + wait
    while True:
        problems = check()
        if not problems or clock() >= deadline:
            return problems
        sleep(interval)
