"""Chaos proof: crash parts of the pipeline while the database keeps changing, then check that
every sink still holds exactly the source's data.

The scenario is a plain timeline of `docker compose` commands run while the load generator
writes to Postgres. `kill` is SIGKILL, a real crash with no chance to finish or commit.
Afterwards everything is brought back, the connector is waited on, and the sinks are compared
with the source row by row (see `verify.deep_mismatches`). The run ends in PASS or FAIL.

The orchestration takes its collaborators as arguments, so the logic is unit-tested without
Docker; `deltaflow chaos` supplies the real ones.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

SERVICES = ("kafka", "connect", "es-sink", "mongo-sink")


class ChaosError(RuntimeError):
    """The experiment itself failed (not the pipeline): a command or the load generator broke."""


@dataclass(frozen=True, slots=True)
class Step:
    at: float  # seconds after the load starts
    label: str
    command: tuple[str, ...]


def _compose(*args: str) -> tuple[str, ...]:
    return ("docker", "compose", *args)


def default_steps(*, with_kafka: bool = False) -> list[Step]:
    """Crash both sinks and Kafka Connect at different moments, then restore the sinks."""
    steps = [
        Step(3, "SIGKILL es-sink", _compose("kill", "es-sink")),
        Step(8, "SIGKILL mongo-sink", _compose("kill", "mongo-sink")),
        Step(12, "restart kafka connect (Debezium)", _compose("restart", "connect")),
        Step(22, "start es-sink", _compose("start", "es-sink")),
        Step(26, "start mongo-sink", _compose("start", "mongo-sink")),
    ]
    if with_kafka:
        steps.append(Step(30, "restart the Kafka broker", _compose("restart", "kafka")))
    return steps


@dataclass(slots=True)
class ChaosReport:
    operations: Counter[str] = field(default_factory=Counter)
    faults: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    seconds: float = 0.0

    @property
    def passed(self) -> bool:
        return not self.problems


def run_chaos(
    *,
    steps: list[Step],
    load: Callable[[], Counter[str]],
    execute: Callable[[Step], None],
    recover: Callable[[], None],
    check: Callable[[], list[str]],
    say: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> ChaosReport:
    """Run `load` in the background, execute the steps on schedule, recover, then check.

    `recover` always runs, even if the experiment breaks halfway, so a failed run does not
    leave the stack with services killed.
    """
    report = ChaosReport()
    outcome: dict[str, object] = {}

    def generate() -> None:
        try:
            outcome["operations"] = load()
        except BaseException as exc:  # reported by the main thread
            outcome["error"] = exc

    started = clock()
    worker = threading.Thread(target=generate, daemon=True)
    worker.start()
    try:
        for step in sorted(steps, key=lambda s: s.at):
            sleep(max(0.0, step.at - (clock() - started)))
            if "error" in outcome:
                break
            say(f"[{clock() - started:5.1f}s] {step.label}")
            execute(step)
            report.faults.append(step.label)
        worker.join()
        if "error" in outcome:
            raise ChaosError(f"load generator failed: {outcome['error']!r}")
        operations = outcome.get("operations")
        if isinstance(operations, Counter):
            report.operations = operations
    finally:
        say("recovering: bringing every service back up")
        recover()
    say("checking every sink against the source, row by row")
    report.problems = check()
    report.seconds = clock() - started
    return report
