# Phase 1: Capture

Goal: every insert, update and delete in PostgreSQL appears as an ordered event on a Kafka topic.

## What was added

| Path | Purpose |
|---|---|
| `infra/postgres/init/01-schema.sql` | `customers`, `products`, `orders`; `updated_at` triggers; `REPLICA IDENTITY FULL`; `deltaflow_pub` publication |
| `infra/postgres/init/02-seed.sql` | 50 customers, 20 products, 100 orders (deterministic) |
| `infra/debezium/shop-connector.json` | Debezium connector (credentials filled from environment) |
| `src/deltaflow/connect.py` | Register the connector idempotently and wait for it to be RUNNING |
| `src/deltaflow/loadgen.py` | Weighted random inserts, updates and deletes |
| `src/deltaflow/tail.py` | Human-readable Kafka event viewer |
| `src/deltaflow/cli.py` | `deltaflow register-connector / connector-status / loadgen / tail` |
| `tests/integration/test_capture.py` | End-to-end proof against the compose stack |

Topics created by Debezium (3 partitions each, keyed by primary key):
`deltaflow.public.customers`, `deltaflow.public.products`, `deltaflow.public.orders`.

## Run it

The init scripts only run on a **new** Postgres volume, so start from a clean slate:

```bash
docker compose down -v
docker compose up -d --wait
pip install -e ".[dev]"

deltaflow register-connector          # creates the connector, waits for RUNNING
deltaflow connector-status
```

Watch events (terminal 1), then make changes (terminal 2):

```bash
deltaflow tail deltaflow.public.orders --from-beginning --max 5   # initial snapshot, op SNAPSHOT
deltaflow tail deltaflow.public.orders --max 50                   # live; leave running

deltaflow loadgen --rate 5 --duration 30                          # random traffic
```

Or by hand:

```bash
docker compose exec postgres psql -U deltaflow -d shop -c \
  "UPDATE customers SET city = 'Pune' WHERE id = 1;"
```

Expected `tail` output:

```
SNAPSHOT lsn=26521136 key={"id": 1} {"id": 1, "customer_id": 1, ... "status": "paid", ...}
UPDATE   lsn=26583904 key={"id": 7} {"status": "pending -> paid", "updated_at": "... -> ..."}
DELETE   lsn=26590112 key={"id": 3} was {"id": 3, "customer_id": 4, ...}
TOMBSTONE key={"id": 3}
```

## Event shape

```json
{
  "before": {"id": 7, "status": "pending", "...": "..."},
  "after":  {"id": 7, "status": "paid",    "...": "..."},
  "source": {"lsn": 26583904, "table": "orders", "ts_ms": 1762412000000, "...": "..."},
  "op": "u",
  "ts_ms": 1762412000123
}
```

`op` is `r` (snapshot), `c`, `u` or `d`. After every delete Kafka also receives a message with a
null value (the tombstone).

## Verify

```bash
pytest -m integration        # needs the stack; skipped automatically otherwise
```

The test inserts, updates and deletes one customer and asserts: four messages arrive in order
(create, update, delete, tombstone); the update has correct before and after; the delete carries
the full old row; and the LSN strictly increases.

## Adding a table later

1. Create the table in SQL and `ALTER TABLE ... REPLICA IDENTITY FULL`.
2. `ALTER PUBLICATION deltaflow_pub ADD TABLE ...`.
3. Add it to `table.include.list` in the connector file and re-run `deltaflow register-connector`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `register-connector`: connection refused | Connect takes about 30 s to start. Run `docker compose up -d --wait` first. |
| Connector `FAILED`, "publication does not exist" | The volume was created before Phase 1. Run `docker compose down -v` and start again. |
| Postgres disk grows while the connector is stopped | The replication slot retains WAL by design. Delete the connector and drop the slot: `SELECT pg_drop_replication_slot('deltaflow_slot');` |
| No topics yet | Topics appear after the snapshot starts; check `deltaflow connector-status`. |
