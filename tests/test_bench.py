from __future__ import annotations

import pytest

from deltaflow import bench


def test_percentile_interpolates_between_samples() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0]

    assert bench.percentile(values, 0) == 1.0
    assert bench.percentile(values, 50) == 3.0
    assert bench.percentile(values, 100) == 5.0
    assert bench.percentile(values, 25) == 2.0
    assert bench.percentile([1.0, 2.0], 50) == 1.5
    assert bench.percentile([7.0], 99) == 7.0
    with pytest.raises(ValueError):
        bench.percentile([], 50)


def test_latency_summary_is_in_milliseconds() -> None:
    summary = bench.summarise_latency("es", [0.010, 0.030, 0.020])

    assert summary.samples == 3 and summary.p50_ms == pytest.approx(20)
    assert summary.max_ms == pytest.approx(30)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_probe_reports_each_sink_when_it_first_sees_the_row() -> None:
    clock = Clock()
    polls = {"es": 0, "mongo": 0}

    def visible_after(name: str, n: int) -> bool:
        polls[name] += 1
        return polls[name] > n

    seen = bench.measure_probe(
        {"es": lambda: visible_after("es", 2), "mongo": lambda: visible_after("mongo", 5)},
        timeout=10,
        poll=0.1,
        clock=clock,
        sleep=clock.sleep,
    )

    assert seen["es"] == pytest.approx(0.2) and seen["mongo"] == pytest.approx(0.5)
    assert seen["es"] < seen["mongo"]  # one slow sink does not delay the other's reading


def test_probe_times_out_naming_the_missing_sink() -> None:
    clock = Clock()

    with pytest.raises(bench.BenchError, match="mongo"):
        bench.measure_probe(
            {"es": lambda: True, "mongo": lambda: False},
            timeout=1,
            poll=0.1,
            clock=clock,
            sleep=clock.sleep,
        )


def test_throughput_times_each_sink_until_it_holds_every_row() -> None:
    clock = Clock()
    state = {"es": 0, "mongo": 0}

    def write() -> None:
        clock.now += 2.0  # the writes took two seconds

    def grow(name: str, step: int) -> int:
        state[name] += step
        return state[name]

    result = bench.measure_throughput(
        rows=1000,
        write_batches=write,
        counts={"es": lambda: grow("es", 500), "mongo": lambda: grow("mongo", 250)},
        expected={"es": 1000, "mongo": 1000},
        timeout=60,
        poll=1.0,
        clock=clock,
        sleep=clock.sleep,
    )

    assert result.write_seconds == 2.0 and result.write_rate == 500
    assert result.drain_seconds["es"] < result.drain_seconds["mongo"]
    assert result.pipeline_rate == pytest.approx(1000 / result.drain_seconds["mongo"])


def test_throughput_times_out_with_what_each_sink_has() -> None:
    clock = Clock()

    with pytest.raises(bench.BenchError, match="have, want"):
        bench.measure_throughput(
            rows=10,
            write_batches=lambda: None,
            counts={"es": lambda: 1},
            expected={"es": 10},
            timeout=5,
            poll=1.0,
            clock=clock,
            sleep=clock.sleep,
        )


def test_markdown_report_has_a_row_per_measurement() -> None:
    latency = [
        bench.summarise_latency("Elasticsearch", [0.05, 0.06]),
        bench.summarise_latency("MongoDB", [0.04]),
    ]
    throughput = bench.ThroughputResult(
        rows=5000, write_seconds=2.0, drain_seconds={"Elasticsearch": 5.0, "MongoDB": 4.0}
    )

    text = bench.markdown_report(latency, throughput)

    assert "Latency to Elasticsearch" in text and "Latency to MongoDB" in text
    assert "5,000 at 2,500/s" in text
    assert "Pipeline throughput (slowest sink) | 1,000 rows/s" in text
