"""Measure the pipeline: end-to-end latency of single changes and sustained throughput.

* **Latency** is measured with probes, one at a time on an otherwise quiet pipeline: insert a
  row, then poll each sink until the row is visible. The poll uses a real-time get (no search
  refresh delay), so the number is "commit in Postgres to readable in the sink".
* **Throughput** inserts many rows in batches, then times how long until both sinks hold all of
  them. That includes Debezium, Kafka and both sink workers working at once.

Numbers depend entirely on the machine and on this deployment (one broker, one worker per sink,
default batch sizes), so they describe this setup and are not a general benchmark.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass


class BenchError(RuntimeError):
    """A sink did not catch up in time, or the pipeline is not ready to be measured."""


def percentile(sorted_values: Sequence[float], p: float) -> float:
    """Linear-interpolated percentile (p in 0..100) of an already sorted, non-empty sequence."""
    if not sorted_values:
        raise ValueError("no values")
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * p / 100
    low, high = math.floor(rank), math.ceil(rank)
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (rank - low)


@dataclass(frozen=True, slots=True)
class LatencySummary:
    sink: str
    samples: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


def summarise_latency(sink: str, seconds: Sequence[float]) -> LatencySummary:
    ordered = sorted(seconds)
    return LatencySummary(
        sink=sink,
        samples=len(ordered),
        p50_ms=percentile(ordered, 50) * 1000,
        p95_ms=percentile(ordered, 95) * 1000,
        p99_ms=percentile(ordered, 99) * 1000,
        max_ms=ordered[-1] * 1000,
    )


def measure_probe(
    visible: dict[str, Callable[[], bool]],
    *,
    timeout: float,
    poll: float = 0.005,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    started: float | None = None,
) -> dict[str, float]:
    """Poll every sink until its `visible()` is true; returns seconds from `started` per sink.

    Sinks are polled in the same loop, so one slow sink does not delay the other's reading.
    """
    begin = clock() if started is None else started
    seen: dict[str, float] = {}
    while len(seen) < len(visible):
        now = clock()
        if now - begin > timeout:
            pending = sorted(set(visible) - set(seen))
            raise BenchError(f"probe not visible in {pending} after {timeout:.0f}s")
        for sink, is_visible in visible.items():
            if sink not in seen and is_visible():
                seen[sink] = clock() - begin
        if len(seen) < len(visible):
            sleep(poll)
    return seen


@dataclass(frozen=True, slots=True)
class ThroughputResult:
    rows: int
    write_seconds: float
    drain_seconds: dict[str, float]  # per sink: from first write until it held every row

    @property
    def write_rate(self) -> float:
        return self.rows / self.write_seconds

    def end_to_end_rate(self, sink: str) -> float:
        return self.rows / self.drain_seconds[sink]

    @property
    def pipeline_rate(self) -> float:
        """Rows per second until the *slowest* sink held everything."""
        return self.rows / max(self.drain_seconds.values())


def measure_throughput(
    *,
    rows: int,
    write_batches: Callable[[], None],
    counts: dict[str, Callable[[], int]],
    expected: dict[str, int],
    timeout: float,
    poll: float = 0.25,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ThroughputResult:
    """Run `write_batches`, then time until every sink's count reaches its expected value."""
    started = clock()
    write_batches()
    write_seconds = clock() - started
    drained: dict[str, float] = {}
    while len(drained) < len(counts):
        if clock() - started > timeout:
            pending = {s: (counts[s](), expected[s]) for s in counts if s not in drained}
            raise BenchError(f"sinks did not catch up in {timeout:.0f}s (have, want): {pending}")
        for sink, count in counts.items():
            if sink not in drained and count() >= expected[sink]:
                drained[sink] = clock() - started
        if len(drained) < len(counts):
            sleep(poll)
    return ThroughputResult(rows=rows, write_seconds=write_seconds, drain_seconds=drained)


def markdown_report(latency: Sequence[LatencySummary], throughput: ThroughputResult | None) -> str:
    lines = [
        "| Measurement | Result |",
        "|---|---|",
    ]
    for item in latency:
        lines.append(
            f"| Latency to {item.sink} (p50 / p95 / p99, {item.samples} probes) "
            f"| {item.p50_ms:.0f} / {item.p95_ms:.0f} / {item.p99_ms:.0f} ms |"
        )
    if throughput is not None:
        lines.append(
            f"| Rows written to Postgres | {throughput.rows:,} at {throughput.write_rate:,.0f}/s |"
        )
        for sink in sorted(throughput.drain_seconds):
            lines.append(
                f"| Throughput into {sink} (end to end) "
                f"| {throughput.end_to_end_rate(sink):,.0f} rows/s "
                f"({throughput.drain_seconds[sink]:.1f}s for {throughput.rows:,} rows) |"
            )
        lines.append(
            f"| Pipeline throughput (slowest sink) | {throughput.pipeline_rate:,.0f} rows/s |"
        )
    return "\n".join(lines)
