"""The proof tooling against the live stack (read-only: it changes nothing)."""

from __future__ import annotations

import pytest

from deltaflow import proof, verify
from deltaflow.config import get_settings

pytestmark = pytest.mark.integration


def test_deep_check_passes_on_a_consistent_pipeline() -> None:
    problems = proof.deep_check(get_settings(), wait=60)

    assert problems == [], "\n".join(problems)


def test_deep_check_sees_the_same_rows_as_the_source() -> None:
    settings = get_settings()

    source = verify.source_rows(settings.postgres_dsn)

    assert set(source) == set(verify.COMPARE_COLUMNS)
    assert all(rows for rows in source.values())  # the seed data is present
