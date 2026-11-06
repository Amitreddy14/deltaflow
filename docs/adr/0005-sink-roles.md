# ADR-0005: Elasticsearch for search, MongoDB for analytics

- Status: Accepted
- Date: 2026-11-06

## Context

The brief names both Elasticsearch and MongoDB as destinations. They should do different jobs
rather than duplicate each other.

## Decision

- **Elasticsearch** holds the *current state* of each row, mapped for full-text and filtered search.
- **MongoDB** holds the *current state* (for fast lookups) plus an **append-only change history** collection, which supports analytics such as order-status transitions, changes per hour and audit trails.

Each sink is its own Kafka consumer group.

## Consequences

- A failure or slowdown in one sink never affects the other.
- Adding a third sink later is a new consumer group and a small adapter, with no change to capture.
