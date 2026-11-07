# Changelog

All notable changes, by phase. Versions follow [Semantic Versioning](https://semver.org/);
`v1.0.0` is the first release of the complete platform.

## [1.0.0] - Phase 7: deploy and polish

### Added
- `docker-compose.prod.yml`: Caddy with automatic HTTPS as the only public service; private
  datastores, Elasticsearch and MongoDB authentication, memory limits, log rotation, restart
  policies, hardened app containers, Prometheus (optional profile, loopback only).
- `deltaflow gen-env`: generates `.env.prod` and the Prometheus key file with random secrets;
  never overwrites.
- `DELTAFLOW_AUTH_REQUIRED`: fail-closed API mode that protects every route except `/health`
  and disables `/docs`; `Authorization: Bearer` accepted alongside `X-API-Key`.
- Elasticsearch basic auth settings and a single client factory (`deltaflow.clients`).
- Release workflow publishing multi-arch images to GitHub Container Registry on `v*` tags.
- Deployment guide (Oracle Cloud Always Free), backup script, `SECURITY.md`, ADR-0013.

### Changed
- CI validates the production compose file.
- Postgres WAL retention for the replication slot is capped in production (see ADR-0013).

## Phase 6: proof
- `deltaflow chaos` (SIGKILL sinks, restart Debezium under load, end in `PASS`/`FAIL`),
  `verify --deep` row comparison, `deltaflow bench`, full-stack integration job in CI.
- Sink workers write a heartbeat file; compose healthchecks use it.

## Phase 5: control plane
- FastAPI endpoints for readiness, connector control, pipeline status, consumer lag, dead-letter
  depth and Prometheus metrics; optional API key for state-changing routes.

## Phase 4: reliability
- Dead-letter topics with batch bisection, retry with backoff, TRUNCATE handling with persisted
  watermarks, graceful shutdown, manual offset commits after sink acknowledgement.

## Phase 3: MongoDB sink
- Guarded upserts, soft deletes with TTL, append-only history, analytics reports.

## Phase 2: Elasticsearch sink
- Event model, consumer framework, external-versioning idempotency, index templates, search CLI.

## Phase 1: capture
- Sample schema, load generator, Debezium connector and registration tooling.

## Phase 0: foundation
- Design document, ADRs, compose stack, FastAPI skeleton, CI.
