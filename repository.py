"""
db/repository.py — All database operations in one place (Repository Pattern)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import LogEntry, DLQEntry, Alert, LogMetric


# ── Log Repository ──────────────────────────────────────────────────────────────

class LogRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, log: LogEntry) -> LogEntry:
        self.db.add(log)
        self.db.commit()
        self.db.refresh(log)
        return log

    def bulk_create(self, logs: list[LogEntry]) -> int:
        self.db.add_all(logs)
        self.db.commit()
        return len(logs)

    def get_by_id(self, log_id: str) -> Optional[LogEntry]:
        return self.db.query(LogEntry).filter(LogEntry.id == uuid.UUID(log_id)).first()

    def get_recent(
        self,
        service_name: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[LogEntry]:
        q = self.db.query(LogEntry).order_by(LogEntry.timestamp.desc())
        if service_name:
            q = q.filter(LogEntry.service_name == service_name)
        if level:
            q = q.filter(LogEntry.level == level)
        return q.limit(limit).offset(offset).all()

    def get_anomalies(self, since_minutes: int = 60, limit: int = 50) -> list[LogEntry]:
        since = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        return (
            self.db.query(LogEntry)
            .filter(LogEntry.is_anomaly.is_(True), LogEntry.timestamp >= since)
            .order_by(LogEntry.timestamp.desc())
            .limit(limit)
            .all()
        )

    def count_by_level_in_window(
        self, service_name: str, level: str, window_minutes: int = 5
    ) -> int:
        since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        result = self.db.execute(
            text(
                """
                SELECT COUNT(*) FROM logs
                WHERE service_name = :service AND level = :level
                  AND timestamp >= :since
                """
            ),
            {"service": service_name, "level": level, "since": since},
        )
        return result.scalar() or 0

    def upsert_metric(self, service_name: str, level: str, is_anomaly: bool) -> None:
        """Upsert a 1-minute bucket count for this service+level."""
        bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        self.db.execute(
            text(
                """
                INSERT INTO log_metrics (service_name, level, bucket, count, anomaly_count)
                VALUES (:service, :level, :bucket, 1, :anomaly)
                ON CONFLICT (service_name, level, bucket)
                DO UPDATE SET
                    count         = log_metrics.count + 1,
                    anomaly_count = log_metrics.anomaly_count + EXCLUDED.anomaly_count
                """
            ),
            {
                "service": service_name,
                "level": level,
                "bucket": bucket,
                "anomaly": 1 if is_anomaly else 0,
            },
        )
        self.db.commit()

    def get_metrics(self, since_minutes: int = 60) -> list[dict]:
        since = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        rows = self.db.execute(
            text(
                """
                SELECT service_name, level, bucket, count, anomaly_count
                FROM log_metrics
                WHERE bucket >= :since
                ORDER BY bucket DESC
                """
            ),
            {"since": since},
        ).fetchall()
        return [dict(r._mapping) for r in rows]


# ── DLQ Repository ─────────────────────────────────────────────────────────────

class DLQRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, entry: DLQEntry) -> DLQEntry:
        self.db.add(entry)
        self.db.commit()
        self.db.refresh(entry)
        return entry

    def get_unresolved(self, limit: int = 50) -> list[DLQEntry]:
        return (
            self.db.query(DLQEntry)
            .filter(DLQEntry.resolved.is_(False))
            .order_by(DLQEntry.last_failed_at.desc())
            .limit(limit)
            .all()
        )

    def resolve(self, entry_id: str) -> bool:
        entry = self.db.query(DLQEntry).filter(DLQEntry.id == uuid.UUID(entry_id)).first()
        if not entry:
            return False
        entry.resolved = True
        entry.resolved_at = datetime.now(timezone.utc)
        self.db.commit()
        return True


# ── Alert Repository ───────────────────────────────────────────────────────────

class AlertRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, alert: Alert) -> Alert:
        self.db.add(alert)
        self.db.commit()
        self.db.refresh(alert)
        return alert

    def get_active(self, limit: int = 20) -> list[Alert]:
        return (
            self.db.query(Alert)
            .filter(Alert.acknowledged.is_(False))
            .order_by(Alert.triggered_at.desc())
            .limit(limit)
            .all()
        )

    def acknowledge(self, alert_id: str) -> bool:
        alert = self.db.query(Alert).filter(Alert.id == uuid.UUID(alert_id)).first()
        if not alert:
            return False
        alert.acknowledged = True
        alert.acknowledged_at = datetime.now(timezone.utc)
        self.db.commit()
        return True
