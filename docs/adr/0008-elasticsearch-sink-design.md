# ADR-0008: Elasticsearch sink design

- Status: Accepted
- Date: 2026-11-07

## Context

ADR-0004 chose LSN-guarded idempotent sinks. This ADR records how that is realised for
Elasticsearch, and the smaller choices around it.

## Decision

1. **Versioning.** Every bulk action carries `version = event.lsn` and `version_type = external`. Elasticsearch rejects a write unless its version is strictly greater than the stored one, answering HTTP 409. The sink treats 409 as success (the event is already applied or superseded) and counts it as `stale`.
2. **Deletes are versioned too** and are real deletes. A delete leaves a version marker for `index.gc_deletes`, which we raise from 60 s to 10 minutes, so a late replay of an older event cannot resurrect the row inside that window. The residual risk beyond the window is documented, not hidden.
3. **One index per table**, named `deltaflow-<table>` (the schema is not part of the name, as the source has a single schema). Document id is the primary key; composite keys join with `|`.
4. **Documents** hold the row's columns plus `cdc_lsn`, `cdc_op` and `cdc_ts_ms`, so any document shows which change produced it.
5. **Index template** (`deltaflow-*`) is installed by the worker at start-up: keyword fields for exact filters, text+keyword for names, `dynamic: true` so added source columns just appear, and `ignore_malformed` on dates so one odd timestamp cannot stop the pipeline. One shard, zero replicas, because the target is a single node and replicas would keep every index yellow.
6. **Error classification.** 409 and a 404 on delete: success. 429, 5xx and connection failures: retryable, with bounded exponential backoff. Anything else (for example 400 mapping errors): permanent, the worker stops without committing. Phase 4 routes permanent failures to a dead-letter topic.
7. **Batching.** One bulk request per Kafka batch, in message order. Because messages are keyed by primary key, all changes to a row are in one partition and stay ordered inside the bulk.
8. **Topic discovery.** Workers subscribe with a regex so they pick up topics the connector creates after they start (broker auto-creation is off, ADR-0003).

## Consequences

- Redelivery, restarts and consumer-group resets are safe by construction; the tests prove it.
- A permanently rejected document halts that worker until Phase 4's dead-letter handling exists. This is deliberate: failing loudly beats silently losing data.
- `TRUNCATE` on a source table is not translated and halts the worker (it has no row key). It is handled in Phase 4.
