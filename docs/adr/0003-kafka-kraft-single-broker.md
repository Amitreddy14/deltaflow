# ADR-0003: Kafka in KRaft mode, single broker

- Status: Accepted
- Date: 2026-11-06

## Context

Kafka no longer needs ZooKeeper. The target environments (a laptop and one free-tier VM)
cannot run a multi-broker cluster.

## Decision

Run one Kafka broker in **KRaft** mode (combined broker and controller) using the official
`apache/kafka` image. Internal replication factors are set to 1. **Automatic topic creation is disabled**: every topic is created explicitly (the connector creates its data topics with 3 partitions, so consumer parallelism and per-key ordering are still demonstrated). Auto-created topics get the broker default `cleanup.policy=delete`, and Kafka Connect refuses to start when its internal offsets topic has that policy (found the hard way in Phase 1).

## Consequences

- One container, no ZooKeeper, low memory.
- No fault tolerance for Kafka itself. This is acceptable for the project's scope and is stated plainly in the README.
- Application code does not depend on broker count, so it moves to a real cluster unchanged.
