# ADR-0002: Use Debezium (log-based CDC) for capture

- Status: Accepted
- Date: 2026-11-06

## Context

Options for getting changes out of PostgreSQL:

1. **Application dual writes.** Fragile; stores drift when one write fails.
2. **Polling / trigger tables.** Adds load, misses hard deletes without triggers, and has latency.
3. **Log-based CDC with Debezium.** Reads the WAL via logical decoding; sees every committed change including deletes, with source LSNs and transactional ordering.
4. **A custom Python WAL reader** (psycopg + pgoutput). Possible, but re-implements snapshotting, schema tracking and type mapping.

## Decision

Use **Debezium's PostgreSQL connector on Kafka Connect** for capture. Put our engineering into
what sits around it: sink workers, idempotency, dead-lettering, replay, control plane and
observability.

## Consequences

- Industry-standard, production-proven capture with the initial snapshot built in.
- The JVM-based Connect worker is an extra memory cost (about 512 MB heap in our compose file).
- The connector owns a replication slot, so an abandoned connector can make Postgres retain WAL. Slot lag monitoring is required (see DESIGN section 7).
- A custom reader remains a possible future alternative behind the same event interface.
