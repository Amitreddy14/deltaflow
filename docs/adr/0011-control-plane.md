# ADR-0011: Control plane design

- Status: Accepted
- Date: 2026-11-07

## Context

Operators need to answer three questions without shelling into containers: is it reachable, is it
keeping up, and is anything wrong? They also need a few safe levers over the connector.

## Decision

1. **Liveness and readiness are separate.** `/health` says the process is up and never touches a dependency, so an orchestrator does not restart the API because Elasticsearch is down. `/ready` checks Postgres, Kafka, Kafka Connect, Elasticsearch and MongoDB (concurrently, 3 s timeout each, one failure never hides the others) and answers 503 if any is down.
2. **Lag is read, never joined.** Lag per sink is `end offset - committed offset` per partition, read with a consumer that has the sink's group id but never subscribes. Reading committed offsets does not join the group, so it cannot trigger a rebalance or steal partitions from the real worker. A group that has committed nothing is measured from the start of the log, where a new sink begins.
3. **Dead-letter depth** is the watermark difference of each DLQ topic: no messages are consumed to count them.
4. **One facade (`Ops`).** The API, the metrics endpoint and tests all go through one object, so routes stay thin and tests substitute a fake instead of patching clients. Connector actions reuse the Phase 1 Connect helpers.
5. **`/pipeline` answers "is it working?"** `healthy` only when the connector and all tasks are RUNNING, no dead letters exist, and every part of the answer could be obtained. A part that fails is reported as a problem, not hidden and not a 500.
6. **Metrics are computed per scrape** by a custom Prometheus collector, so there is no background state to go stale. A failing part sets `deltaflow_collector_error{collector=...}` to 1 and the rest still reports. Series: `deltaflow_dependency_up`, `deltaflow_dependency_check_seconds`, `deltaflow_connector_running`, `deltaflow_consumer_lag_messages{sink,topic}`, `deltaflow_dlq_messages{sink}`. Lag is aggregated per topic to keep cardinality low; partitions are available from `/lag`.
7. **Access control is minimal on purpose.** Reads are open. Register, pause, resume and restart require `X-API-Key` when `DELTAFLOW_API_KEY` is set; comparison is constant time. Unset means open, for local development only. Real deployment hardening (TLS, network exposure, secret handling) belongs to Phase 7.
8. **Pause is a sharp tool.** While the connector is paused Postgres keeps WAL for the replication slot. The endpoint documents this; `/pipeline` flags a paused connector as degraded so it is not forgotten.

## Consequences

- Every check is a real call, so `/ready` and `/metrics` can take up to about 3 s when something is down. Prometheus' default 10 s scrape timeout still fits because checks run in parallel.
- Per-scrape computation means each scrape opens short-lived Kafka consumers. At a 15 s interval that is negligible for this scale; a larger deployment would cache briefly.
- Pausing the connector for a long time is still possible. Slot-lag monitoring is a Phase 7 concern.
