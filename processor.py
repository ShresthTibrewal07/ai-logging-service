"""
app/processor.py — Log Processor (Phase 1 + Phase 2)

Pipeline per message:
    deserialise → AI detection → persist → metrics (DB + Prometheus) → cache invalidate → alerts
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.ai import detect_anomaly
from app.alert import trigger_anomaly_alert, trigger_error_rate_alert
from app.cache import invalidate_anomalies, invalidate_metrics
from app.config import settings
from app.metrics_exporter import (
    record_anomaly,
    record_cache_miss,
    record_log_ingested,
    record_processing_time,
)
from app.models import LogEntry
from app.schemas import KafkaLogMessage, LogLevel
from db.repository import LogRepository

logger = logging.getLogger(__name__)


def process_log(envelope: KafkaLogMessage, db: Session) -> LogEntry:
    """
    Full processing pipeline for one log message.
    Raises on hard failure (caller decides retry vs DLQ).
    """
    start_ts = time.time()
    log      = envelope.log

    # ── 1. AI anomaly detection ─────────────────────────────────────────────────
    anomaly = detect_anomaly(log)

    # ── 2. Persist to PostgreSQL ────────────────────────────────────────────────
    repo  = LogRepository(db)
    entry = LogEntry(
        service_name   = log.service_name,
        level          = log.level,
        message        = log.message,
        metadata_      = log.metadata,
        trace_id       = log.trace_id,
        span_id        = log.span_id,
        environment    = log.environment,
        host           = log.host,
        timestamp      = log.timestamp,
        retry_count    = envelope.retry_count,
        is_anomaly     = anomaly.is_anomaly,
        anomaly_score  = anomaly.score if anomaly.is_anomaly else None,
        anomaly_reason = anomaly.reason,
        processed_at   = datetime.now(timezone.utc),
    )
    saved = repo.create(entry)

    # ── 3. Update Postgres metrics bucket ───────────────────────────────────────
    try:
        repo.upsert_metric(log.service_name, log.level, anomaly.is_anomaly)
    except Exception as exc:
        logger.warning("Metrics upsert failed: %s", exc)

    # ── 4. Update Prometheus counters ───────────────────────────────────────────
    record_log_ingested(log.service_name, log.level)
    if anomaly.is_anomaly:
        record_anomaly(log.service_name, log.level)

    # ── 5. Invalidate Redis caches (stale after new data) ───────────────────────
    invalidate_metrics()
    if anomaly.is_anomaly:
        invalidate_anomalies()

    # ── 6. Alerting ─────────────────────────────────────────────────────────────
    if anomaly.is_anomaly and anomaly.score and anomaly.score >= 0.5:
        trigger_anomaly_alert(log, anomaly.reason or "", anomaly.score, db)

    if log.level in (LogLevel.ERROR, LogLevel.CRITICAL):
        error_count = repo.count_by_level_in_window(
            log.service_name, log.level,
            window_minutes=settings.ANOMALY_WINDOW_MINUTES,
        )
        if error_count >= settings.ANOMALY_ERROR_SPIKE_THRESHOLD:
            trigger_error_rate_alert(
                log.service_name, error_count,
                settings.ANOMALY_WINDOW_MINUTES, db,
            )

    # ── 7. Record processing latency ────────────────────────────────────────────
    record_processing_time(start_ts)

    logger.info(
        "Processed log | service=%s level=%s anomaly=%s score=%s id=%s",
        log.service_name, log.level,
        anomaly.is_anomaly, anomaly.score, saved.id,
    )
    return saved
