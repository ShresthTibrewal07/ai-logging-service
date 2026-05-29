# AI Logging Service

Distributed log ingestion and anomaly detection system built with FastAPI, Kafka, PostgreSQL, and Redis.

It accepts logs over HTTP, pushes them through Kafka, processes them in a background worker, stores the results in Postgres, and exposes APIs for querying logs, anomalies, alerts, and DLQ entries.

## Highlights

- Event-driven log pipeline using FastAPI + Kafka
- Background consumer for asynchronous processing
- Anomaly detection using keyword checks and error-spike rules
- Retry and dead-letter queue flow for failed messages
- PostgreSQL-backed queries for logs, alerts, and DLQ records
- Docker Compose setup for running the full stack locally

## Architecture

```text
client -> FastAPI ingestion API -> Kafka -> consumer-service -> Postgres
                                                                                 -> retry topic -> DLQ
```

## Tech Stack

- FastAPI
- Kafka
- PostgreSQL
- Redis
- SQLAlchemy
- Pydantic v2
- Docker Compose

## Run locally

```bash
docker compose up -d --build
curl http://localhost:8000/health
```

API docs:

```text
http://localhost:8000/docs
```

## Sample usage

Send a log:

```bash
curl -X POST http://localhost:8000/logs \
    -H "Content-Type: application/json" \
    -d '{
        "service_name": "payment-service",
        "level": "ERROR",
        "message": "Database unavailable: connection pool exhausted",
        "trace_id": "trace-abc123",
        "environment": "production",
        "metadata": {
            "request_id": "req-1",
            "latency_ms": 1250
        }
    }'
```

Query stored logs:

```bash
curl "http://localhost:8000/logs?limit=20"
curl "http://localhost:8000/logs/anomalies?since_minutes=60"
```

Generate demo traffic:

```bash
python simulate_logs.py --count 200 --delay 0.05
```

## Main API endpoints

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/logs` | Ingest one log |
| `POST` | `/logs/bulk` | Ingest a batch of logs |
| `GET` | `/logs` | Query processed logs |
| `GET` | `/logs/anomalies` | View detected anomalies |
| `GET` | `/logs/metrics/summary` | View aggregated metrics |
| `GET` | `/alerts` | List active alerts |
| `GET` | `/dlq` | Inspect dead-letter records |
| `GET` | `/health` | Health check |

## Notes

- Kafka topics are created on startup.
- Database tables are created on startup.
- The repo includes a simulator for generating realistic test traffic.
- `app/` and `db/` are compatibility wrappers used by the Docker commands.

## Why this project matters

This project demonstrates practical backend engineering patterns that show up in real systems: async processing, service decoupling, failure handling with retries and DLQs, anomaly detection, and a usable local developer workflow.
