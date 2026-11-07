# Phase 3: MongoDB sink and analytics

Goal: the same changes that reach Elasticsearch also reach MongoDB as current state plus a full
history, stale events are ignored, and the history can answer analytical questions.

## What was added

| Path | Purpose |
|---|---|
| `src/deltaflow/sinks/mongo.py` | LSN-guarded state writes, soft deletes with TTL, idempotent history |
| `src/deltaflow/analytics.py` | Aggregation pipelines and `run_report` |
| `src/deltaflow/verify.py` | Count comparison: Postgres vs Elasticsearch vs MongoDB |
| `src/deltaflow/runners.py` | `run_mongo_sink`, sharing the consumer helper with the ES sink |
| `deltaflow run-mongo-sink`, `deltaflow analytics`, `deltaflow verify` | CLI entry points |
| `mongo-sink` service in `docker-compose.yml` | Runs the sink in its own consumer group, `deltaflow-mongo-sink` |
| `tests/integration/test_mongo_sink.py` | Insert, update, stale replay, soft delete, no resurrection, history, analytics on a real MongoDB |

Design reasoning: [ADR-0009](adr/0009-mongodb-sink-design.md).

## What lands in MongoDB

| Collection | Content |
|---|---|
| `customers`, `products`, `orders` | Current state, one document per row, plus `_lsn`, `_op`, `_deleted` |
| `changes` | Every change event: `table`, `doc_id`, `op`, `before`, `after`, `changed`, `lsn`, `ts_ms` |

## Run it

```powershell
pip install -e ".[dev]"                 # new dependency: pymongo
docker compose up -d --wait --build     # also starts mongo-sink
deltaflow register-connector            # skip if already RUNNING
docker compose logs mongo-sink --tail 20
```

Generate traffic, then check that all three stores agree:

```powershell
deltaflow loadgen --rate 5 --duration 30
deltaflow verify --wait 30
```

`verify` exits with code 1 and prints the differing tables if anything is off.

## Analytics

```powershell
deltaflow analytics breakdown                    # operations per table
deltaflow analytics rate --unit minute           # changes per minute
deltaflow analytics transitions                  # order status transitions (pending -> shipped ...)
deltaflow analytics revenue                      # revenue by order status
deltaflow analytics cities                       # customers per city
deltaflow analytics rate --since-minutes 10      # only recent history
```

## Prove it survives failure

**1. Sink down while the database keeps changing:**

```powershell
docker compose stop mongo-sink
deltaflow loadgen --rate 10 --duration 20
docker compose start mongo-sink
deltaflow verify --wait 30
```

**2. Replay the entire history (state and history must not change):**

```powershell
docker compose stop mongo-sink
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group deltaflow-mongo-sink --reset-offsets --to-earliest --all-topics --execute
docker compose start mongo-sink
docker compose logs mongo-sink --tail 10     # expect many "stale" and history duplicates, no errors
deltaflow verify --wait 30
```

## Troubleshooting

- **`mongo-sink` exits at start:** check `DELTAFLOW_MONGO_URL` in `.env`. Inside compose the sink uses the `mongodb` service name; from your PowerShell it must use `127.0.0.1` and your `MONGO_HOST_PORT`.
- **`ModuleNotFoundError: pymongo`:** run `pip install -e ".[dev]"` again, and rebuild the image with `--build`.
- **Counts differ right after loadgen:** sinks are asynchronous; use `verify --wait 30`.
