# ADR-0010: Dead letters, TRUNCATE and graceful shutdown

- Status: Accepted
- Date: 2026-11-07
- Refines: ADR-0004, ADR-0008, ADR-0009 (their "halt the worker" consequences)

## Context

Until Phase 3 any message the pipeline could not process stopped a worker: an unparseable message,
a document the sink rejected for good, or a `TRUNCATE`. Stopping is safe but one bad message
blocks every good one behind it. Phase 4 keeps the safety and removes the blockage.

## Decision

### 1. Dead-letter queue

- **What is quarantined.** Only messages that can never succeed: `parse` (not a change event) and `sink` (permanently rejected by the destination). Transient failures are retried with backoff and, if the destination stays down, stop the worker. A down destination is never a reason to quarantine data.
- **Where.** One Kafka topic per sink: `deltaflow.dlq.es`, `deltaflow.dlq.mongo`. They sit outside the `deltaflow.public.*` pattern the sinks subscribe to, so a dead letter is never consumed as data. One partition, 30 day retention, created at start-up (broker auto-creation is off, ADR-0003).
- **What is stored.** The original key and value byte for byte, plus headers: `dlq.stage`, `dlq.reason`, `dlq.sink`, `dlq.source.topic/partition/offset`, `dlq.failed_at_ms`.
- **Never lose a message.** A message counts as handled only after Kafka acknowledges the dead letter. If publishing fails, the worker stops without committing, and the message is read again.
- **Finding the poison event.** The sink write is all-or-nothing per batch, so when a batch is permanently rejected the worker bisects it. Halves are rewritten (safe, sinks are idempotent) until each bad event is alone in a batch of one and is dead-lettered. Everything else is applied. Cost is O(k log n) writes for k bad events in a batch of n.
- **Circuit breaker.** More than 50 consecutive dead letters (configurable) stops the worker with an error. That pattern means the sink is broken (for example bad credentials answered as a permanent error), not that the data is. A successful write resets the count.
- **Operations.** `deltaflow dlq show <sink>` lists letters; `deltaflow dlq replay <sink>` re-publishes them to their source topics. Replay is safe to repeat because sinks are idempotent; letters whose cause is not fixed simply dead-letter again. `--dlq-topic ""` turns the feature off (the worker then stops on bad events, as in Phase 3).

### 2. TRUNCATE

- Debezium is configured with `skipped.operations: none`, so truncates reach Kafka (its default skips them). A truncate is a keyless event, one per table, carrying the table's LSN.
- **The ordering problem.** With no key it lands in an arbitrary partition, so row events older than the truncate may be read after it, especially on replay. Applying the truncate "in order" is therefore not enough.
- **Watermark.** Each sink stores, per table, the LSN of the latest truncate and drops any row event older than it as superseded. The outcome no longer depends on arrival order. The mark is persisted (Elasticsearch index `deltaflow-_truncates`, Mongo collection `_truncates`), loaded at start-up, and can only increase.
- **Elasticsearch.** Store the mark (external version = LSN), refresh the index so recent documents are searchable, then `delete_by_query` documents with `cdc_lsn < truncate LSN`. Newer rows survive.
- **MongoDB.** Guarded upsert of the mark, then tombstone every live row with `_lsn < truncate LSN`, raising its `_lsn` to the truncate's. Same soft-delete model as a row delete (ADR-0009), purged by the TTL. The truncate is also written to history (`op: "t"`, `doc_id: "*"`).
- Truncate events are idempotent: replaying one, or an older one, changes nothing.

### 3. Graceful shutdown

- `SIGTERM` and `SIGINT` set a stop flag. The batch in flight is finished and committed, then the worker exits with status 0.
- If the stop arrives while a batch waits out a retry, the wait is interrupted and the batch is abandoned uncommitted. Kafka redelivers it; idempotent sinks make that harmless.
- The consumer is closed (leaving the group promptly) and the DLQ producer is flushed. Compose gives the sinks `stop_grace_period: 30s`.

## Consequences

- One poison message no longer blocks a table; it is visible, inspectable and replayable.
- A flood of rejections still halts the pipeline loudly instead of draining it into the DLQ.
- Dead letters are per sink: a message one sink accepted and the other rejected appears only in the rejecting sink's DLQ.
- A truncate is permanent for older events by design. Re-inserting rows afterwards works because new events have a higher LSN than the mark.
- Not covered, and listed in the roadmap: consumer-lag and health reporting (Phase 5), automated kill-and-recover proof (Phase 6).
