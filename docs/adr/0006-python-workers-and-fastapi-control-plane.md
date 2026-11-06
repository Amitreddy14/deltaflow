# ADR-0006: Python sink workers and a FastAPI control plane in one package

- Status: Accepted
- Date: 2026-11-06

## Context

The sink logic and the operator API share the event model, settings and Kafka helpers.

## Decision

One installable Python package (`deltaflow`, `src/` layout) with:

- `deltaflow.api`: FastAPI control plane.
- `deltaflow.workers` (from Phase 2): one entry point per sink, run as separate processes or containers.
- Shared `config`, event model and Kafka helpers.

Tooling: `ruff` for lint and format, `mypy --strict`, `pytest`. Kafka via `confluent-kafka`
(introduced in Phase 1 for the `tail` tool and integration tests).

## Consequences

- One version, one test suite, one Docker image run with different commands.
- Workers scale independently of the API because they are separate processes.
