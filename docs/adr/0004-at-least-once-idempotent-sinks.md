# ADR-0004: At-least-once delivery with LSN-guarded idempotent sinks

- Status: Accepted
- Date: 2026-11-06

## Context

Kafka offers exactly-once only within Kafka. Writing to Elasticsearch and MongoDB cannot be
made atomic with an offset commit.

## Decision

Commit offsets only after the sink acknowledges the write (at-least-once). Make every sink
write idempotent and order-safe using the source **LSN** as a version:

- Elasticsearch: `version_type=external`, `version=lsn`.
- MongoDB: upsert guarded by `_lsn < incoming lsn`; history keyed uniquely by `(table, key, lsn)`.

## Consequences

- Redelivery, restarts and replays are safe by construction.
- Sinks must store the LSN with each document.
- Elasticsearch keeps delete versions for a limited time (`index.gc_deletes`, 60 s by default; Deltaflow raises it to 10 minutes, see ADR-0008), so a very late replayed update after a delete could resurrect a row. Mitigation: the replay tooling targets a quiesced consumer group, and this limit is documented. The tests cover the in-window case.
