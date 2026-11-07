# ADR-0012: How the platform proves it is correct

- Status: Accepted
- Date: 2026-11-07

## Context

Phases 2 to 4 argue that sinks are idempotent and survive crashes. Arguments are not evidence. The
claim a reader cares about is simple: after things break, does every sink hold exactly the
source's data? Counts alone cannot answer that, and a happy-path demo proves nothing about it.

## Decision

1. **Prove it by crashing it.** `deltaflow chaos` runs the load generator against Postgres and, on a fixed timeline, SIGKILLs both sink workers, restarts Kafka Connect (Debezium), then restores the sinks. `kill` is a real crash: no graceful stop, no final commit. `--with-kafka` adds a broker restart.
2. **Compare rows, not counts.** `verify --deep` and the chaos check load every row's id and key columns from Postgres, Elasticsearch and MongoDB and report missing, extra and differing rows. Equal counts with a stale value in one row (the failure idempotent guards exist to prevent) are caught; a test pins that case. Compared columns are plain text and integers, which survive Debezium's JSON unchanged; timestamps and decimals are re-encoded in transit and would need conversion, so they are left out on purpose.
3. **The experiment must be sound.** Before breaking anything the chaos run checks the pipeline is already consistent; otherwise a failure would be blamed on the faults. If it is not, the run stops with exit code 2 (experiment could not run), which is distinct from FAIL (1) and PASS (0). Services are always brought back, even if the experiment breaks halfway.
4. **A passing run ends in `PASS`** and exit code 0, so it works as a CI gate. The orchestration (timeline, ordering, recovery, error handling) is unit-tested with fake collaborators; only the thin Docker, HTTP and database wiring is untested outside the real stack.
5. **CI runs everything.** A job starts the full compose stack on the runner, registers the connector, runs `pytest -m integration`, then runs the chaos proof. Logs are printed on failure and the stack is always torn down.
6. **Benchmarks are measured, not claimed.** `deltaflow bench` measures single-change latency (probe rows polled with real-time gets, so no search-refresh delay) and sustained throughput (batched inserts timed until both sinks hold every row), then removes its rows. It prints a Markdown table. Results are specific to the machine and to this deployment (one broker, one worker per sink) and are labelled that way.

## Consequences

- A green chaos run is real evidence for the delivery guarantee under those faults. It is not a proof for all failures: it does not cover disk loss, network partitions, or long Postgres outages, and a single run samples one interleaving. Running it repeatedly (CI does on every PR) is what builds confidence.
- The deep comparison reads whole tables, so it suits the demo-sized dataset, not production-sized tables.
- The CI integration job is the slowest and most infrastructure-dependent check; it is separate from the fast lint, type and unit job so quick feedback stays quick.
