# ADR-0009: MongoDB sink design

- Status: Accepted
- Date: 2026-11-07

## Context

ADR-0004 chose idempotent, LSN-guarded sinks and ADR-0005 gave MongoDB the analytics role.
Elasticsearch offers external versioning out of the box; MongoDB does not, so the guard has to be
built from ordinary operations. This ADR records how, and what MongoDB stores.

## Decision

1. **Two kinds of collection.**
   - A *current-state* collection per table (`customers`, `orders`, ...): one document per row, `_id` = primary key (composite keys join with `|`).
   - One *history* collection, `changes`: one document per change event, append-only.
2. **LSN guard on state.** Each write is `ReplaceOne({_id, _lsn: {$lt: event.lsn}}, doc, upsert=True)`.
   - Row missing: the filter matches nothing, so the upsert inserts it.
   - Row present with an older LSN: it is replaced.
   - Row present with an equal or newer LSN: the filter matches nothing, the upsert then tries to insert the same `_id`, and MongoDB answers duplicate key `E11000`. The sink treats exactly that error as "stale, already superseded" and counts it as `stale`. Any other write error is permanent.
3. **Deletes are soft.** A delete replaces the document with its last known image plus `_deleted: true` and `_deleted_at`. It goes through the same guard, so an old create or update replayed later cannot resurrect the row. A TTL index on `_deleted_at` removes the tombstone after 7 days, which bounds storage; the residual risk beyond that window is documented, as with `index.gc_deletes` in ADR-0008.
4. **Writes commute.** Because every write is guarded by the LSN, the final state does not depend on the order in which a batch is applied, so bulk writes are `ordered=False` for throughput.
5. **History is idempotent.** The history document's `_id` is `table|docid|lsn`. Redelivery produces duplicate-key errors on insert, which are counted as `history_duplicates` and ignored. Each history document holds `op`, `before`, `after`, `lsn`, `ts_ms` and, for updates, the list of `changed` columns.
6. **Analytics read the history.** Aggregations (`status_transitions`, `change_rate`, `operation_breakdown`) run over `changes`; `revenue_by_status` and `customers_by_city` run over current state with `_deleted: false`.
7. **Errors.** Connection failures are retryable with bounded backoff (shared worker logic from ADR-0008). Everything else is permanent and halts the worker until Phase 4 adds dead-letter handling.
8. **Verification.** `deltaflow verify` compares row counts: Postgres rows vs live Elasticsearch documents vs non-deleted Mongo documents.

## Consequences

- Redelivery, restarts and consumer-group resets are safe, and each sink keeps its own consumer group, so one sink being down never blocks the other.
- Soft deletes mean raw collection counts include tombstones; consumers must filter `_deleted: false` (the analytics and `verify` do).
- The guard depends on `E11000` being the only duplicate-key source on the state collections, which holds because `_id` is the only unique index there.
- The unit tests run against a fake collection that implements the guard semantics. The integration test (`tests/integration/test_mongo_sink.py`) is the check against a real MongoDB.
