# Phase 4: Reliability

Goal: bad data and restarts never lose events and never block good ones.

## What was added

| Path | Purpose |
|---|---|
| `src/deltaflow/deadletter.py` | Dead-letter publisher (acknowledged before commit), topic creation, read and replay helpers |
| `src/deltaflow/worker.py` | Parse errors and permanent sink rejections are quarantined; bisection finds the bad event; circuit breaker; graceful shutdown |
| `src/deltaflow/events.py` | `TRUNCATE` event (`op: "t"`, no key) |
| `src/deltaflow/sinks/base.py` | `TruncateWatermarks`: order-independent truncate semantics |
| `src/deltaflow/sinks/elasticsearch.py`, `mongo.py` | Truncate handling with persisted watermarks (`load_state()` at start-up) |
| `infra/debezium/shop-connector.json` | `skipped.operations: none` so truncates are captured |
| `docker-compose.yml` | `stop_grace_period: 30s` for both sinks |
| `deltaflow dlq show/replay <es|mongo>` | Inspect and replay dead letters |
| `tests/integration/test_reliability.py` | Dead letters over real Kafka; truncate in real Elasticsearch and MongoDB (no shop data touched) |

Design reasoning: [ADR-0010](adr/0010-reliability.md).

## Upgrade steps

```powershell
docker compose up -d --wait --build
deltaflow register-connector      # re-applies the connector config so truncates are captured
docker compose logs es-sink --tail 5     # expect "dlq=deltaflow.dlq.es"
docker compose logs mongo-sink --tail 5  # expect "dlq=deltaflow.dlq.mongo"
```

## Demo 1: a poison message is quarantined, the pipeline keeps moving

Write garbage straight into a data topic (a message Debezium would never produce):

```powershell
docker compose exec kafka bash -c "echo 'k:not-json' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic deltaflow.public.orders --property parse.key=true --property key.separator=:"
deltaflow loadgen --rate 5 --duration 10
deltaflow dlq show es
deltaflow dlq show mongo
deltaflow verify --wait 30
```

Expected: each sink dead-lettered the message (stage `parse`, reason "value is not valid JSON"),
neither stopped, and `verify` ends with `OK`.

## Demo 2: TRUNCATE, then restore

`orders` has no dependent tables, so it can be truncated and restored from a backup copy:

```powershell
docker compose exec postgres psql -U deltaflow -d shop -c "CREATE TABLE orders_backup AS TABLE orders"
docker compose exec postgres psql -U deltaflow -d shop -c "TRUNCATE orders"
deltaflow verify --wait 30          # orders = 0 in Postgres, Elasticsearch and MongoDB
docker compose exec postgres psql -U deltaflow -d shop -c "INSERT INTO orders SELECT * FROM orders_backup"
deltaflow verify --wait 30          # everything is back, in all three
docker compose exec postgres psql -U deltaflow -d shop -c "DROP TABLE orders_backup"
```

(`orders_backup` is not in the publication, so its changes are not captured.)

## Demo 3: graceful shutdown

```powershell
deltaflow loadgen --rate 20 --duration 15
docker compose stop es-sink          # returns within seconds
docker compose logs es-sink --tail 5 # ends with "es-sink stopped"
docker compose ps -a es-sink         # Exited (0)
docker compose start es-sink
deltaflow verify --wait 30
```

## Inspect and replay

```powershell
deltaflow dlq show es --max 10
deltaflow dlq replay es              # re-publishes every dead letter to its source topic
```

Replay only helps once the cause is fixed (for example after correcting a mapping); otherwise the
letters dead-letter again. It is safe to repeat.

## Troubleshooting

- **Worker stops with "consecutive dead letters":** the sink is rejecting everything. Read the first letter's `dlq.reason` in `dlq show`; it is usually credentials, a template or a mapping problem. Fix it, then `dlq replay`.
- **`TRUNCATE` produced nothing:** re-run `deltaflow register-connector`; without `skipped.operations: none` Debezium drops truncates.
- **Want the Phase 3 behaviour back:** start a sink with `--dlq-topic ""`.
