from __future__ import annotations

import time
from collections import Counter
from typing import Any

import pytest

from deltaflow import chaos
from deltaflow.chaos import ChaosError, Step


class Clock:
    """A fake clock where sleeping advances time, so schedules run instantly."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _run(
    steps: list[Step],
    *,
    load: Any = None,
    execute: Any = None,
    recover: Any = None,
    check: Any = None,
    clock: Clock | None = None,
) -> tuple[chaos.ChaosReport, list[str]]:
    clock = clock or Clock()
    events: list[str] = []
    report = chaos.run_chaos(
        steps=steps,
        load=load or (lambda: Counter({"insert_order": 3})),
        execute=execute or (lambda step: events.append(f"exec:{step.label}")),
        recover=recover or (lambda: events.append("recover")),
        check=check or (lambda: events.append("check") or []),  # type: ignore[func-returns-value]
        say=lambda _: None,
        sleep=clock.sleep,
        clock=clock,
    )
    return report, events


def test_steps_run_in_time_order_then_recovery_then_the_check() -> None:
    steps = [Step(8, "second", ("b",)), Step(3, "first", ("a",))]

    report, events = _run(steps)

    assert events == ["exec:first", "exec:second", "recover", "check"]
    assert report.faults == ["first", "second"] and report.passed
    assert report.operations == Counter({"insert_order": 3})


def test_steps_wait_for_their_scheduled_time() -> None:
    clock = Clock()
    seen: list[float] = []

    _run(
        [Step(3, "a", ("a",)), Step(10, "b", ("b",))],
        execute=lambda step: seen.append(clock.now),
        clock=clock,
    )

    assert seen == [3.0, 10.0]


def test_problems_found_by_the_check_fail_the_run() -> None:
    report, _ = _run([Step(1, "a", ("a",))], check=lambda: ["orders/mongodb: 2 missing [1, 2]"])

    assert not report.passed and report.problems == ["orders/mongodb: 2 missing [1, 2]"]


def test_recovery_still_runs_when_a_command_fails() -> None:
    events: list[str] = []

    def explode(step: Step) -> None:
        raise ChaosError("docker compose kill failed")

    with pytest.raises(ChaosError, match="kill failed"):
        _run([Step(1, "a", ("a",))], execute=explode, recover=lambda: events.append("recover"))

    assert events == ["recover"]  # a broken experiment must not leave services killed


def test_a_failing_load_generator_is_an_experiment_error_not_a_pass() -> None:
    def broken() -> Counter[str]:
        raise RuntimeError("postgres refused")

    events: list[str] = []
    with pytest.raises(ChaosError, match="load generator failed"):
        _run([Step(1, "a", ("a",))], load=broken, recover=lambda: events.append("recover"))

    assert events == ["recover"]


def test_load_runs_concurrently_with_the_steps() -> None:
    started = time.monotonic()
    order: list[str] = []

    def slow_load() -> Counter[str]:
        time.sleep(0.2)
        order.append("load done")
        return Counter()

    chaos.run_chaos(
        steps=[Step(0, "fault", ("x",))],
        load=slow_load,
        execute=lambda step: order.append("fault"),
        recover=lambda: order.append("recover"),
        check=lambda: [],
        say=lambda _: None,
    )

    assert order == ["fault", "load done", "recover"]  # fault injected while load still running
    assert time.monotonic() - started < 5


def test_default_scenario_crashes_both_sinks_and_connect_then_restores() -> None:
    steps = chaos.default_steps()
    commands = [" ".join(s.command[2:]) for s in steps]

    assert commands == [
        "kill es-sink",
        "kill mongo-sink",
        "restart connect",
        "start es-sink",
        "start mongo-sink",
    ]
    assert all(s.command[:2] == ("docker", "compose") for s in steps)
    assert [s.at for s in steps] == sorted(s.at for s in steps)


def test_kafka_restart_is_opt_in() -> None:
    assert not any("kafka" in s.command for s in chaos.default_steps())
    assert any(s.command[-1] == "kafka" for s in chaos.default_steps(with_kafka=True))
