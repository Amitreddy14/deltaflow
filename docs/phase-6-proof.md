# Phase 6: Proof

Goal: show, not claim, that the pipeline stays correct when parts of it crash.

## What was added

| Path | Purpose |
|---|---|
| `src/deltaflow/chaos.py` | Fault timeline and orchestration: crash, recover, check, report |
| `src/deltaflow/proof.py` | Wiring to docker compose, Kafka Connect, Postgres, Elasticsearch, MongoDB |
| `src/deltaflow/verify.py` | `--deep`: row-by-row comparison (missing, extra, differing rows) |
| `src/deltaflow/bench.py` | Latency probes, throughput timing, Markdown report |
| `deltaflow chaos`, `deltaflow bench`, `deltaflow verify --deep` | CLI entry points |
| `.github/workflows/ci.yml` | New `integration` job: full stack, integration tests, chaos |
| `tests/integration/test_proof.py` | Deep check passes on the live stack |

Design reasoning: [ADR-0012](adr/0012-proof-strategy.md).

## The chaos proof

```powershell
docker compose up -d --wait --build
deltaflow register-connector            # skip if already RUNNING
deltaflow chaos
```

What it does, on a timeline while the load generator writes about 20 changes per second for 40 s:

| Time | Fault |
|---|---|
| 3 s | `kill es-sink` (SIGKILL: a crash, no clean shutdown) |
| 8 s | `kill mongo-sink` |
| 12 s | `restart connect` (Debezium goes down and resumes from the replication slot) |
| 22 s | `start es-sink` |
| 26 s | `start mongo-sink` |

Then it brings every service back, waits for the connector, and compares Postgres with both
sinks **row by row**. It ends with `PASS` (exit 0) or `FAIL` plus the differing rows (exit 1).
Exit 2 means the experiment could not run (for example the pipeline was already inconsistent, or
Docker was not found).

Options: `--duration`, `--rate`, `--seed`, `--with-kafka` (also restarts the broker, harsher),
`--recovery-timeout`, `--skip-baseline`.

**Why this is evidence:** while a sink is dead, Kafka keeps the events; on restart the worker
resumes from its last committed offset and may re-read events it already applied; the LSN guards
make that harmless. A crash between "write to the sink" and "commit the offset" is exactly the
case at-least-once delivery has to survive, and SIGKILL hits it at random moments.

**What it does not cover:** disk loss, network partitions, a long Postgres outage, or more than
one sample of the possible interleavings. Run it several times; CI runs it on every pull request.

## Deep verification

```powershell
deltaflow verify --deep --wait 60
```

Counts can agree while a row holds a stale value. `--deep` compares the id and key columns of
every row in Postgres, Elasticsearch and MongoDB and names what differs.

## Benchmark

```powershell
deltaflow bench                       # 30 latency probes, 5,000 rows
deltaflow bench --rows 20000 --json bench.json
```

It measures, on the running stack:

- **Latency:** insert one row, then poll each sink until it is readable (real-time get, so no search refresh delay). p50, p95, p99.
- **Throughput:** insert rows in batches of 100 and time until both sinks hold all of them. That covers Debezium, Kafka and both workers at once.

It removes its rows afterwards (`--keep` leaves them) and prints a Markdown table to paste into
the README. Numbers describe your machine and this deployment (one broker, one worker per sink,
default batch size of 200); they are not a general benchmark. Close heavy programs before running,
and run it twice, as the first run warms caches.

## CI

The `integration` job starts the compose stack on the runner and runs, in order: connector
registration, `pytest -m integration`, `deltaflow chaos --duration 40 --rate 15`. On failure it
prints service state and logs; it always tears the stack down. It runs after the fast
lint/type/unit job passes.

## Troubleshooting

- **`experiment could not run: ... not consistent before`:** a sink is behind or broken. `deltaflow verify --deep --wait 60`, check `docker compose logs es-sink mongo-sink`.
- **`docker was not found`:** run from a terminal where `docker` works, in the repository folder (the commands use the project's compose file).
- **`FAIL` with missing rows in one sink:** keep the output; it names the table, sink and ids. Check that sink's logs and `deltaflow dlq show <sink>`.
- **Chaos leaves a service stopped:** `docker compose up -d`.
