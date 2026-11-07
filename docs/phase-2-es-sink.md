# Phase 2: Event model and Elasticsearch sink

Goal: every change in PostgreSQL ends up as the correct document in Elasticsearch, and
replaying or redelivering events can never make it wrong.

## What was added

| Path | Purpose |
|---|---|
| `src/deltaflow/events.py` | `ChangeEvent` and `parse_change_event`; tombstones and heartbeats are skipped, malformed messages raise |
| `src/deltaflow/worker.py` | Generic consumer loop: batch, write, retry with backoff, **commit offsets only after the sink acknowledges** |
| `src/deltaflow/sinks/base.py` | The `Sink` contract and error types |
| `src/deltaflow/sinks/elasticsearch.py` | Bulk writes versioned by LSN, index template, search helper |
| `src/deltaflow/runners.py` | Wires a real Kafka consumer and Elasticsearch client into the worker |
| `deltaflow run-es-sink`, `deltaflow search` | CLI entry points |
| `es-sink` service in `docker-compose.yml` | Runs the sink, restarts on failure |
| `tests/integration/test_es_sink.py` | Insert, update, stale replay, delete and late replay against the real stack |

Design reasoning: [ADR-0008](adr/0008-elasticsearch-sink-design.md).

## How a change flows

```
INSERT/UPDATE/DELETE --> Postgres WAL --> Debezium --> Kafka topic (keyed by primary key)
   --> SinkWorker: parse -> bulk write {version = LSN, external} -> commit offset
   --> Elasticsearch index deltaflow-<table>
```

## Run it

After merging, rebuild so the new service and dependencies are in the image:

```powershell
docker compose up -d --wait --build
deltaflow register-connector          # skip if the connector is already RUNNING
docker compose logs es-sink --tail 20 # expect "es-sink started" and "applied N events"
```

Compare the source and the index (the numbers must match once the sink has caught up):

```powershell
docker compose exec postgres psql -U deltaflow -d shop -Atc "select count(*) from orders"
Invoke-RestMethod "http://127.0.0.1:9200/deltaflow-orders/_count"
```

Search:

```powershell
deltaflow search customers "Reddy"
deltaflow search orders "status:shipped" --size 3
```

Watch live updates while generating traffic:

```powershell
deltaflow loadgen --rate 5 --duration 30
docker compose logs es-sink -f --tail 5
```

## Prove it survives failure (a small chaos test)

**1. Sink down while the database keeps changing:**

```powershell
docker compose stop es-sink
deltaflow loadgen --rate 10 --duration 20
docker compose start es-sink
```

After a few seconds the counts match again: Kafka held the events and the sink resumed from its
last committed offset.

**2. Replay the entire history:**

```powershell
docker compose stop es-sink
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group deltaflow-es-sink --reset-offsets --to-earliest --all-topics --execute
docker compose start es-sink
docker compose logs es-sink --tail 10
```

The log shows a large number of events "stale or duplicate skipped" and the counts and contents
do not change. That is the LSN guard doing its job.

## Verify

```powershell
python -m pytest                     # unit tests (65+), no infrastructure needed
python -m pytest -m integration -v   # needs the stack; includes the Elasticsearch end-to-end test
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `es-sink` keeps restarting | `docker compose logs es-sink`. A mapping rejection (HTTP 400) is a permanent error by design; the log names the document and reason. |
| Index is empty | The connector must be RUNNING (`deltaflow connector-status`) so topics exist. The sink finds topics automatically within about 5 seconds. |
| Counts differ briefly | Elasticsearch refreshes about once per second, and the sink may be catching up. Wait a few seconds. |
| `TRUNCATE` on a table halts the sink | Known limit until Phase 4 (dead-letter handling). |
