"""
app/retry.py — Retry and Dead Letter Queue (DLQ) management

Retry flow:
    process_message()
        → success                  → store in DB
        → failure, retries remain  → re-publish to logs.retry topic (with backoff)
        → failure, max retries hit → publish to logs.dlq + persist to DLQ table
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.config import settings
from app.ingestion import publish_log, publish_to_dlq
from app.models import DLQEntry
from app.schemas import LogIngest

logger = logging.getLogger(__name__)


def should_retry(retry_count: int) -> bool:
    """Return True if the message should be retried."""
    return retry_count < settings.MAX_RETRY_COUNT


def handle_retry(log: LogIngest, retry_count: int, error: Exception) -> None:
    """
    Re-publish the message to the retry topic with an incremented counter.
    Confluent Kafka doesn't natively support delay, so we rely on the consumer
    sleeping RETRY_DELAY_SECONDS before re-consuming.  A production system
    would use a scheduler or a topic-per-retry-tier (T1/T2/T3).
    """
    new_count = retry_count + 1
    logger.warning(
        "Retrying log | service=%s attempt=%d/%d error=%s",
        log.service_name, new_count, settings.MAX_RETRY_COUNT, error,
    )
    publish_log(log, retry_count=new_count)


def send_to_dlq(
    log: LogIngest,
    retry_count: int,
    error: Exception,
    original_topic: str,
    db: Session,
) -> None:
    """
    Permanently failed message: push to Kafka DLQ topic AND persist to DB.
    """
    error_msg = f"{type(error).__name__}: {error}"
    payload   = log.model_dump(mode="json")

    logger.error(
        "Sending to DLQ | service=%s retries=%d error=%s",
        log.service_name, retry_count, error_msg,
    )

    # 1. Kafka DLQ topic (for replay / stream consumers)
    publish_to_dlq(payload, error_msg, original_topic)

    # 2. Postgres DLQ table (for dashboard / inspection)
    dlq_entry = DLQEntry(
        original_topic  = original_topic,
        payload         = payload,
        error_message   = error_msg,
        retry_count     = retry_count,
        first_failed_at = datetime.now(timezone.utc),
        last_failed_at  = datetime.now(timezone.utc),
    )
    db.add(dlq_entry)
    db.commit()
    logger.info("DLQ entry persisted | id=%s", dlq_entry.id)
