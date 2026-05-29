"""
app/main.py — FastAPI Application

Routes:
  POST /logs          — ingest a single log
  POST /logs/bulk     — ingest up to 500 logs in one call
  GET  /logs          — query stored logs
  GET  /logs/anomalies — list recent anomalies
  GET  /logs/metrics  — per-service / per-level counts
  GET  /alerts        — active (unacknowledged) alerts
  POST /alerts/{id}/ack  — acknowledge an alert
  GET  /dlq           — list DLQ entries
  POST /dlq/{id}/resolve — mark a DLQ entry resolved
  GET  /health        — liveness probe
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.config import settings
from app.ingestion import ensure_topics, flush_producer, publish_log
from app.schemas import (
    AlertResponse,
    BulkLogIngest,
    DLQEntryResponse,
    LogIngest,
    LogResponse,
    MetricPoint,
)
from db.database import get_db, health_check, init_db
from db.repository import AlertRepository, DLQRepository, LogRepository

logging.basicConfig(
    level  = getattr(logging, settings.LOG_LEVEL),
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Lifespan ────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    ensure_topics()
    logger.info("Starting AI Log Monitoring Service | env=%s", settings.APP_ENV)
    yield
    logger.info("Shutting down — flushing Kafka producer...")
    flush_producer()


# ── App ─────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "AI Log Monitoring System",
    description = "Distributed log ingestion, AI anomaly detection, and alerting",
    version     = "1.0.0",
    lifespan    = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


# ── Health ──────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    db_ok = health_check()
    return {
        "status"  : "ok" if db_ok else "degraded",
        "database": "up" if db_ok else "down",
        "kafka"   : "producer-ready",
        "env"     : settings.APP_ENV,
    }


# ── Log Ingestion ───────────────────────────────────────────────────────────────

@app.post("/logs", status_code=status.HTTP_202_ACCEPTED, tags=["Ingestion"])
def ingest_log(payload: LogIngest):
    """
    Accept a single log from a microservice.
    Publishes to Kafka; returns immediately (async processing).
    """
    publish_log(payload)
    return {"status": "accepted", "service": payload.service_name, "level": payload.level}


@app.post("/logs/bulk", status_code=status.HTTP_202_ACCEPTED, tags=["Ingestion"])
def ingest_bulk(payload: BulkLogIngest):
    """
    Accept up to 500 logs in one HTTP call.
    Each log is individually published to Kafka.
    """
    for log in payload.logs:
        publish_log(log)
    return {"status": "accepted", "count": len(payload.logs)}


# ── Log Queries ─────────────────────────────────────────────────────────────────

@app.get("/logs", response_model=list[LogResponse], tags=["Queries"])
def list_logs(
    service_name: Optional[str] = Query(None, description="Filter by service name"),
    level       : Optional[str] = Query(None, description="Filter by log level"),
    limit       : int           = Query(100, ge=1, le=1000),
    offset      : int           = Query(0,   ge=0),
    db          : Session       = Depends(get_db),
):
    """Paginated list of processed logs, newest first."""
    repo = LogRepository(db)
    logs = repo.get_recent(service_name=service_name, level=level, limit=limit, offset=offset)
    return logs


@app.get("/logs/anomalies", response_model=list[LogResponse], tags=["Queries"])
def list_anomalies(
    since_minutes: int     = Query(60, ge=1,  le=1440),
    limit        : int     = Query(50, ge=1,  le=500),
    db           : Session = Depends(get_db),
):
    """Return logs flagged as anomalies in the last N minutes."""
    repo = LogRepository(db)
    return repo.get_anomalies(since_minutes=since_minutes, limit=limit)


@app.get("/logs/{log_id}", response_model=LogResponse, tags=["Queries"])
def get_log(log_id: str, db: Session = Depends(get_db)):
    repo  = LogRepository(db)
    entry = repo.get_by_id(log_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Log not found")
    return entry


@app.get("/logs/metrics/summary", response_model=list[MetricPoint], tags=["Queries"])
def metrics_summary(
    since_minutes: int     = Query(60, ge=1, le=1440),
    db           : Session = Depends(get_db),
):
    """Per-service / per-level counts bucketed by minute."""
    repo = LogRepository(db)
    rows = repo.get_metrics(since_minutes=since_minutes)
    return rows


# ── Alerts ──────────────────────────────────────────────────────────────────────

@app.get("/alerts", response_model=list[AlertResponse], tags=["Alerts"])
def list_alerts(
    limit: int     = Query(20, ge=1, le=200),
    db   : Session = Depends(get_db),
):
    """Return unacknowledged alerts."""
    repo = AlertRepository(db)
    return repo.get_active(limit=limit)


@app.post("/alerts/{alert_id}/ack", tags=["Alerts"])
def acknowledge_alert(alert_id: str, db: Session = Depends(get_db)):
    repo = AlertRepository(db)
    ok   = repo.acknowledge(alert_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"status": "acknowledged", "id": alert_id}


# ── DLQ ─────────────────────────────────────────────────────────────────────────

@app.get("/dlq", response_model=list[DLQEntryResponse], tags=["DLQ"])
def list_dlq(
    limit: int     = Query(50, ge=1, le=500),
    db   : Session = Depends(get_db),
):
    """Return unresolved DLQ entries."""
    repo = DLQRepository(db)
    return repo.get_unresolved(limit=limit)


@app.post("/dlq/{entry_id}/resolve", tags=["DLQ"])
def resolve_dlq(entry_id: str, db: Session = Depends(get_db)):
    repo = DLQRepository(db)
    ok   = repo.resolve(entry_id)
    if not ok:
        raise HTTPException(status_code=404, detail="DLQ entry not found")
    return {"status": "resolved", "id": entry_id}
