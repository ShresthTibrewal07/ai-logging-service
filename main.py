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
    GET  /reports/alerts — generate aggregated alert report
    POST /reports/alerts/send — dispatch alert report via configured channels
    GET  /reports/scheduler/status — inspect periodic report scheduler state
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
from app.scheduled_reporting import get_scheduler_status, start_report_scheduler, stop_report_scheduler
from app.schemas import (
    AlertReportDispatchResponse,
    AlertReportResponse,
    AlertReportSchedulerStatusResponse,
    AlertReportSendRequest,
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
    start_report_scheduler()
    logger.info("Starting AI Log Monitoring Service | env=%s", settings.APP_ENV)
    yield
    await stop_report_scheduler()
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


@app.get("/reports/alerts", response_model=AlertReportResponse, tags=["Reports"])
def get_alert_report(
    since_hours         : int           = Query(24, ge=1, le=24 * 30),
    include_acknowledged: bool          = Query(False),
    service_name        : Optional[str] = Query(None),
    recent_limit        : int           = Query(10, ge=1, le=50),
    db                  : Session       = Depends(get_db),
):
    from app.reporting import build_alert_report

    return build_alert_report(
        db=db,
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
        recent_limit=recent_limit,
    )


@app.post("/reports/alerts/send", response_model=AlertReportDispatchResponse, tags=["Reports"])
def send_alert_report(payload: AlertReportSendRequest, db: Session = Depends(get_db)):
    from app.reporting import dispatch_alert_report

    return dispatch_alert_report(db=db, payload=payload)


@app.get("/reports/scheduler/status", response_model=AlertReportSchedulerStatusResponse, tags=["Reports"])
def report_scheduler_status():
    return get_scheduler_status()


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


# ── Prometheus Metrics ──────────────────────────────────────────────────────────

@app.get("/metrics", include_in_schema=False)
def prometheus_metrics(db: Session = Depends(get_db)):
    """Scraped by Prometheus every 15s. Uses persisted data so charts survive restarts."""
    from fastapi.responses import PlainTextResponse
    from sqlalchemy import text as sql_text

    def comment(lines: list[str], name: str, help_text: str, metric_type: str):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")

    lines: list[str] = []

    ingested = db.execute(
        sql_text(
            """
            SELECT service_name, level, COUNT(*) AS count
            FROM logs
            GROUP BY service_name, level
            ORDER BY service_name, level
            """
        )
    ).fetchall()
    comment(lines, "log_ingested_total", "Total log messages ingested by service and level", "counter")
    for row in ingested:
        lines.append(
            f'log_ingested_total{{service="{row.service_name}",level="{row.level}"}} {row.count}'
        )

    anomalies = db.execute(
        sql_text(
            """
            SELECT service_name, level, COUNT(*) AS count
            FROM logs
            WHERE is_anomaly = true
            GROUP BY service_name, level
            ORDER BY service_name, level
            """
        )
    ).fetchall()
    comment(lines, "log_anomalies_total", "Total anomalies detected by service and level", "counter")
    for row in anomalies:
        lines.append(
            f'log_anomalies_total{{service="{row.service_name}",level="{row.level}"}} {row.count}'
        )

    comment(lines, "log_processing_duration_ms", "Consumer log processing latency in milliseconds", "histogram")
    buckets = [5, 10, 25, 50, 100, 250, 500, 1000, 2500]
    counts = {bucket: 0 for bucket in buckets}
    counts["+Inf"] = 0
    durations = db.execute(
        sql_text(
            """
            SELECT EXTRACT(EPOCH FROM (processed_at - received_at)) * 1000 AS duration_ms
            FROM logs
            WHERE processed_at IS NOT NULL AND received_at IS NOT NULL
            """
        )
    ).fetchall()
    duration_values = [max(float(row.duration_ms or 0), 0.0) for row in durations]
    for duration in duration_values:
        for bucket in buckets:
            if duration <= bucket:
                counts[bucket] += 1
        counts["+Inf"] += 1
    for bucket in buckets:
        lines.append(f'log_processing_duration_ms_bucket{{le="{bucket}"}} {counts[bucket]}')
    lines.append(f'log_processing_duration_ms_bucket{{le="+Inf"}} {counts["+Inf"]}')
    lines.append(f"log_processing_duration_ms_sum {sum(duration_values):.2f}")
    lines.append(f"log_processing_duration_ms_count {len(duration_values)}")

    dlq_unresolved = db.execute(
        sql_text("SELECT COUNT(*) FROM dead_letter_queue WHERE resolved = false")
    ).scalar() or 0
    comment(lines, "dlq_entries_total", "Current number of unresolved DLQ entries", "gauge")
    lines.append(f"dlq_entries_total {dlq_unresolved}")

    active_alerts = db.execute(
        sql_text("SELECT COUNT(*) FROM alerts WHERE acknowledged = false")
    ).scalar() or 0
    comment(lines, "active_alerts_total", "Current number of unacknowledged alerts", "gauge")
    lines.append(f"active_alerts_total {active_alerts}")

    comment(lines, "kafka_consumer_lag", "Estimated Kafka consumer lag in messages", "gauge")
    lines.append("kafka_consumer_lag 0")

    comment(lines, "redis_cache_hits_total", "Total Redis cache hits", "counter")
    lines.append("redis_cache_hits_total 0")

    comment(lines, "redis_cache_misses_total", "Total Redis cache misses", "counter")
    lines.append("redis_cache_misses_total 0")

    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


# ── AI Summarisation ────────────────────────────────────────────────────────────

@app.get("/ai/summarise", tags=["AI"])
async def ai_summarise(
    service_name : Optional[str] = Query(None),
    since_minutes: int           = Query(30, ge=1, le=1440),
    db           : Session       = Depends(get_db),
):
    """
    Generate an AI incident summary for recent anomaly logs.
    Uses OpenAI if OPENAI_API_KEY is set, otherwise rule-based fallback.
    Result is cached in Redis for 5 minutes.
    """
    try:
        from app.summariser import summarise_incident
        repo = LogRepository(db)
        logs = repo.get_anomalies(since_minutes=since_minutes, limit=30)
        # Convert ORM objects to LogIngest-compatible objects for summariser
        from app.schemas import LogIngest, LogLevel
        log_ingests = [
            LogIngest(
                service_name=l.service_name,
                level=l.level,
                message=l.message,
                environment=l.environment or "production",
                trace_id=l.trace_id,
                timestamp=l.timestamp,
            )
            for l in logs
        ]
        result = await summarise_incident(log_ingests, service_name, since_minutes)
        return result
    except Exception as exc:
        return {
            "summary": f"Summarisation unavailable: {exc}",
            "severity": "LOW",
            "likely_cause": "Configure OPENAI_API_KEY in .env for AI analysis",
            "affected_services": [],
            "recommended_actions": ["Check /logs/anomalies for raw data"],
            "confidence": 0.0,
        }


# ── Stats (used by dashboard) ───────────────────────────────────────────────────

@app.get("/stats", tags=["System"])
def stats(db: Session = Depends(get_db)):
    """Aggregated counts for dashboard header cards."""
    from sqlalchemy import text as sql_text
    with db as session:
        total    = session.execute(sql_text("SELECT COUNT(*) FROM logs")).scalar() or 0
        anomalies= session.execute(sql_text("SELECT COUNT(*) FROM logs WHERE is_anomaly=true")).scalar() or 0
        alerts   = session.execute(sql_text("SELECT COUNT(*) FROM alerts WHERE acknowledged=false")).scalar() or 0
        dlq      = session.execute(sql_text("SELECT COUNT(*) FROM dead_letter_queue WHERE resolved=false")).scalar() or 0
    return {
        "total_logs"      : total,
        "total_anomalies" : anomalies,
        "active_alerts"   : alerts,
        "dlq_unresolved"  : dlq,
    }
