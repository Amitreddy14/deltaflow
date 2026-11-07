# Phase 5: Control plane

Goal: an operator can see what the pipeline is doing, and steer it, over HTTP.

## What was added

| Path | Purpose |
|---|---|
| `src/deltaflow/ops.py` | Dependency checks, consumer lag, dead-letter depth, and the `Ops` facade |
| `src/deltaflow/api/routes/ready.py` | `GET /ready` |
| `src/deltaflow/api/routes/connector.py` | `GET /connector`, `POST /connector/{register,pause,resume,restart}` |
| `src/deltaflow/api/routes/pipeline.py` | `GET /pipeline`, `GET /lag`, `GET /dlq` |
| `src/deltaflow/api/routes/metrics.py` | `GET /metrics` (Prometheus text format) |
| `src/deltaflow/api/deps.py` | Facade dependency, optional API key, Connect error mapping |
| `src/deltaflow/connect.py` | `pause_connector`, `resume_connector`, `ConnectorNotFound` |
| `infra/prometheus/prometheus.yml`, `prometheus` service | Optional scraper (`--profile monitoring`) |
| `tests/integration/test_control_plane.py` | Readiness, lag draining, and pause/resume against the live stack |

Design reasoning: [ADR-0011](adr/0011-control-plane.md). Interactive docs: `/docs`.

## Endpoints

| Method and path | Answers | Auth |
|---|---|---|
| `GET /health` | Process is alive (touches nothing else) | open |
| `GET /ready` | Can I reach Postgres, Kafka, Connect, Elasticsearch, MongoDB? 200 or 503 | open |
| `GET /connector` | Connector and task states, `running` flag | open |
| `POST /connector/register` | Create or update the connector from the packaged config | key |
| `POST /connector/pause` / `resume` / `restart` | Lifecycle control | key |
| `GET /pipeline` | `healthy` or `degraded`, with the list of problems | open |
| `GET /lag` | Per sink and partition: committed, end, lag | open |
| `GET /dlq` | Dead letters per sink | open |
| `GET /metrics` | Prometheus metrics | open |

"key" means `X-API-Key` is required when `DELTAFLOW_API_KEY` is set.

## Run it

```powershell
docker compose up -d --wait --build
```

Your API port is `API_HOST_PORT` from `.env` (default 8000; yours is 8001):

```powershell
Invoke-RestMethod http://127.0.0.1:8001/ready
Invoke-RestMethod http://127.0.0.1:8001/pipeline
Invoke-RestMethod http://127.0.0.1:8001/lag | ConvertTo-Json -Depth 5
Invoke-RestMethod http://127.0.0.1:8001/metrics
```

## Demos

**1. Watch lag grow and drain:**

```powershell
docker compose stop mongo-sink
deltaflow loadgen --rate 10 --duration 15
(Invoke-RestMethod http://127.0.0.1:8001/lag).mongo.total     # positive and growing
Invoke-RestMethod http://127.0.0.1:8001/pipeline              # es total stays near 0
docker compose start mongo-sink
Start-Sleep 15
(Invoke-RestMethod http://127.0.0.1:8001/lag).mongo.total     # back to 0
```

**2. A dependency goes down:**

```powershell
docker compose stop elasticsearch
Invoke-WebRequest http://127.0.0.1:8001/ready -SkipHttpErrorCheck | Select-Object StatusCode, Content
docker compose start elasticsearch
```

Expected: HTTP 503 naming `elasticsearch`; `/health` still returns 200.

**3. Pause and resume the connector:**

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8001/connector/pause
Invoke-RestMethod http://127.0.0.1:8001/pipeline        # degraded: connector is PAUSED
Invoke-RestMethod -Method Post http://127.0.0.1:8001/connector/resume
```

Keep pauses short: Postgres retains WAL for the replication slot while the connector is paused.

**4. Protect the control endpoints:** add `DELTAFLOW_API_KEY=choose-a-long-random-value` to `.env`,
run `docker compose up -d`, then:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8001/connector/restart          # 401
Invoke-RestMethod -Method Post http://127.0.0.1:8001/connector/restart -Headers @{ "X-API-Key" = "choose-a-long-random-value" }
```

**5. Prometheus (optional):**

```powershell
docker compose --profile monitoring up -d
```

Open http://127.0.0.1:9090, then try `deltaflow_consumer_lag_messages` or `deltaflow_dependency_up == 0`.

## Metrics

| Series | Meaning |
|---|---|
| `deltaflow_dependency_up{dependency}` | 1 if the dependency answered |
| `deltaflow_dependency_check_seconds{dependency}` | Check duration |
| `deltaflow_connector_running{connector}` | 1 if connector and all tasks RUNNING |
| `deltaflow_consumer_lag_messages{sink,topic}` | Messages not yet processed |
| `deltaflow_dlq_messages{sink}` | Dead letters held |
| `deltaflow_collector_error{collector}` | 1 if that part of the scrape failed |

Useful alerts: `deltaflow_connector_running == 0`, `deltaflow_dlq_messages > 0`,
`deltaflow_consumer_lag_messages > 1000` for 5 minutes.

## Troubleshooting

- **`/ready` is 503 only from the host, fine in Docker (or the reverse):** the API uses `DELTAFLOW_*_URL` settings; inside compose they point at service names, from your shell at `127.0.0.1` and your `*_HOST_PORT` values.
- **`/lag` shows lag that never drains:** the sink is not running; check `docker compose ps` and its logs.
- **`/connector` returns 404:** run `POST /connector/register` or `deltaflow register-connector`.
- **A 401 on actions:** `DELTAFLOW_API_KEY` is set; send it as `X-API-Key`.
