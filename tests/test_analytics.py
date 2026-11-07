from datetime import UTC, datetime

import pytest

from deltaflow import analytics


def test_transitions_only_count_real_status_changes_on_orders() -> None:
    pipeline = analytics.status_transitions_pipeline()

    assert pipeline[0]["$match"] == {"table": "orders", "op": "u"}
    assert pipeline[1]["$match"]["$expr"] == {"$ne": ["$before.status", "$after.status"]}
    assert pipeline[2]["$group"]["_id"] == {"from": "$before.status", "to": "$after.status"}


def test_since_filter_is_applied_when_given() -> None:
    since = datetime(2026, 11, 7, tzinfo=UTC)

    assert analytics.status_transitions_pipeline(since)[0]["$match"]["ts"] == {"$gte": since}
    assert analytics.change_rate_pipeline("hour", since)[0]["$match"] == {"ts": {"$gte": since}}
    assert analytics.change_rate_pipeline("hour")[0]["$match"] == {}


@pytest.mark.parametrize("unit", analytics.UNITS)
def test_rate_buckets_by_the_requested_unit(unit: str) -> None:
    group = analytics.change_rate_pipeline(unit)[1]["$group"]

    assert group["_id"]["bucket"]["$dateTrunc"]["unit"] == unit


def test_rate_rejects_unknown_units() -> None:
    with pytest.raises(ValueError, match="unit must be one of"):
        analytics.change_rate_pipeline("fortnight")


def test_current_state_reports_exclude_deleted_rows() -> None:
    assert analytics.revenue_by_status_pipeline()[0]["$match"] == {"_deleted": False}
    assert analytics.customers_by_city_pipeline()[0]["$match"] == {"_deleted": False}


def test_every_advertised_report_is_known_and_unknown_ones_fail() -> None:
    assert set(analytics.REPORTS) == {"transitions", "rate", "breakdown", "revenue", "cities"}
    with pytest.raises(ValueError, match="unknown report"):
        analytics.run_report(None, "nope")  # type: ignore[arg-type]
