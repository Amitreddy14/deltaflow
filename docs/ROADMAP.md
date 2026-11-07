# Roadmap

Each phase is one branch (`phase-N-name`), one PR into `main` with green CI, and one tag
(`phase-N`). Nothing starts until the previous phase is merged.

| Phase | Scope | Exit criteria |
|---|---|---|
| **0. Foundation** ✅ | Design doc, ADRs, repo scaffold, Docker Compose stack, FastAPI skeleton, CI | `docker compose up --wait` is healthy; CI green; `/health` responds |
| **1. Capture** ✅ | Sample source schema (`customers`, `products`, `orders`) and seed data, load generator, Debezium connector config and registration script | Insert, update and delete in Postgres appear as events on Kafka topics |
| **2. Event model + Elasticsearch sink** ✅ | `ChangeEvent` normalisation, consumer framework, ES sink with external-version idempotency, index templates | Row changes appear in Elasticsearch; replaying the topic changes nothing |
| **3. MongoDB sink** | Guarded upsert for current state, append-only history, analytics aggregations | Same changes land in Mongo; stale events are ignored; history is complete |
| **4. Reliability** | Manual offset commits, retry with backoff, dead-letter topics, graceful shutdown, tombstone and snapshot handling | Poison events are quarantined; sinks survive restarts without loss |
| **5. Control plane** | FastAPI endpoints: connector register/pause/resume, pipeline status, consumer lag, readiness checks, Prometheus metrics | Operator can see and steer the pipeline over HTTP |
| **6. Proof** | Integration suite in CI, chaos demo (kill workers mid-stream), load test, benchmarks in README | Demo script ends with `PASS`: sinks equal the source |
| **7. Deploy + polish** | Production compose with auth, deployment guide for free hosting, backup notes, README, diagrams | Reproducible deployment from the guide; repo reads like a product |

Phase 7 follows the same approach as Revenant: ship production deploy files and a guide,
and perform the real deployment when a free cloud account (for example Oracle Cloud Always
Free, whose ARM VM has enough memory for the full stack) is available.
