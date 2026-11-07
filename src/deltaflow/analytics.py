"""Analytics over the MongoDB change history and current state.

These are the questions the analytics store exists to answer. The history collection makes
questions about *how data changed* possible, which current-state tables (and the search
index) cannot answer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pymongo.database import Database

from deltaflow.sinks.mongo import HISTORY_COLLECTION

UNITS = ("minute", "hour", "day")


def _since(since: datetime | None) -> dict[str, Any]:
    return {"ts": {"$gte": since}} if since else {}


def status_transitions_pipeline(since: datetime | None = None) -> list[dict[str, Any]]:
    """How often orders moved from one status to another."""
    return [
        {"$match": {"table": "orders", "op": "u", **_since(since)}},
        {"$match": {"$expr": {"$ne": ["$before.status", "$after.status"]}}},
        {
            "$group": {
                "_id": {"from": "$before.status", "to": "$after.status"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1, "_id.from": 1, "_id.to": 1}},
        {"$project": {"_id": 0, "from": "$_id.from", "to": "$_id.to", "count": 1}},
    ]


def change_rate_pipeline(unit: str, since: datetime | None = None) -> list[dict[str, Any]]:
    """Changes per time bucket and table."""
    if unit not in UNITS:
        raise ValueError(f"unit must be one of {UNITS}")
    return [
        {"$match": _since(since)},
        {
            "$group": {
                "_id": {"bucket": {"$dateTrunc": {"date": "$ts", "unit": unit}}, "table": "$table"},
                "changes": {"$sum": 1},
            }
        },
        {"$sort": {"_id.bucket": 1, "_id.table": 1}},
        {"$project": {"_id": 0, "bucket": "$_id.bucket", "table": "$_id.table", "changes": 1}},
    ]


def operation_breakdown_pipeline(since: datetime | None = None) -> list[dict[str, Any]]:
    """Inserts, updates, deletes and snapshot reads per table."""
    return [
        {"$match": _since(since)},
        {"$group": {"_id": {"table": "$table", "op": "$op"}, "count": {"$sum": 1}}},
        {"$sort": {"_id.table": 1, "_id.op": 1}},
        {"$project": {"_id": 0, "table": "$_id.table", "op": "$_id.op", "count": 1}},
    ]


def revenue_by_status_pipeline() -> list[dict[str, Any]]:
    """Current orders and revenue per status (deleted rows excluded)."""
    return [
        {"$match": {"_deleted": False}},
        {
            "$group": {
                "_id": "$status",
                "orders": {"$sum": 1},
                "revenue_cents": {"$sum": "$total_cents"},
            }
        },
        {"$sort": {"revenue_cents": -1}},
        {"$project": {"_id": 0, "status": "$_id", "orders": 1, "revenue_cents": 1}},
    ]


def customers_by_city_pipeline() -> list[dict[str, Any]]:
    return [
        {"$match": {"_deleted": False}},
        {"$group": {"_id": "$city", "customers": {"$sum": 1}}},
        {"$sort": {"customers": -1, "_id": 1}},
        {"$project": {"_id": 0, "city": "$_id", "customers": 1}},
    ]


def run_report(
    db: Database[Any], name: str, *, unit: str = "hour", since: datetime | None = None
) -> list[dict[str, Any]]:
    if name == "transitions":
        return list(db[HISTORY_COLLECTION].aggregate(status_transitions_pipeline(since)))
    if name == "rate":
        return list(db[HISTORY_COLLECTION].aggregate(change_rate_pipeline(unit, since)))
    if name == "breakdown":
        return list(db[HISTORY_COLLECTION].aggregate(operation_breakdown_pipeline(since)))
    if name == "revenue":
        return list(db["orders"].aggregate(revenue_by_status_pipeline()))
    if name == "cities":
        return list(db["customers"].aggregate(customers_by_city_pipeline()))
    raise ValueError(f"unknown report {name!r}")


REPORTS = ("transitions", "rate", "breakdown", "revenue", "cities")
