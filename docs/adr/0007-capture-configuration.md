# ADR-0007: REPLICA IDENTITY FULL, explicit publication, schemaless JSON events

- Status: Accepted
- Date: 2026-11-06

## Context

Three capture settings shape every downstream consumer.

1. **What an UPDATE or DELETE carries.** By default Postgres logs only the primary key of the old row, so `before` is nearly empty.
2. **Which tables are visible to the connector.** Debezium can create its own publication (all tables) or use one we define.
3. **How events are serialised.** Kafka Connect's JSON converter can embed a full schema in every message, or send plain JSON.

## Decision

1. `REPLICA IDENTITY FULL` on every captured table. Updates and deletes carry the complete old row. This feeds the MongoDB history and audit trail and lets a delete be processed without a lookup. Cost: more WAL per update, acceptable at this scale.
2. An **explicit publication** (`deltaflow_pub`) created in SQL, with `publication.autocreate.mode=disabled`. The connector can only see the three listed tables.
3. `JsonConverter` with `schemas.enable=false`. Events are plain JSON envelopes (`before`, `after`, `source.lsn`, `op`, `ts_ms`). Smaller messages and easy to read, at the cost of no registry-enforced schema; schema evolution is handled in the sink workers (Phase 4).

Also: `tombstones.on.delete=true` (the null-valued message after a delete is kept, as required for log compaction and for sinks to observe the end of a key's life) and a 10 s heartbeat so the replication slot keeps advancing on a quiet table.

## Consequences

- Every event is self-contained, so sinks are simple.
- Adding a table needs three edits: the table, `ALTER ... REPLICA IDENTITY FULL`, and the publication plus connector `table.include.list`. This is documented in `docs/phase-1-capture.md`.
- Moving to Avro or Protobuf with a schema registry later changes only the converter settings and the worker deserialiser.
