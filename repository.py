"""
db/repository.py — All database operations in one place (Repository Pattern)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import case, func, text
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

    def count_anomalies_in_window(
        self,
        since_hours: int = 24,
        service_name: Optional[str] = None,
    ) -> int:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        q = self.db.query(LogEntry).filter(
            LogEntry.is_anomaly.is_(True),
            LogEntry.timestamp >= since,
        )
        if service_name:
            q = q.filter(LogEntry.service_name == service_name)
        return q.count()


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

    def count_unresolved(self) -> int:
        return self.db.query(DLQEntry).filter(DLQEntry.resolved.is_(False)).count()


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

    def _report_query(
        self,
        since_hours: int,
        include_acknowledged: bool,
        service_name: Optional[str],
    ):
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        q = self.db.query(Alert).filter(Alert.triggered_at >= since)
        if service_name:
            q = q.filter(Alert.service_name == service_name)
        if not include_acknowledged:
            q = q.filter(Alert.acknowledged.is_(False))
        return q

    def get_report_totals(
        self,
        since_hours: int = 24,
        include_acknowledged: bool = False,
        service_name: Optional[str] = None,
    ) -> dict[str, int]:
        q = self._report_query(since_hours, include_acknowledged, service_name)
        return {
            "total_alerts": q.count(),
            "active_alerts": q.filter(Alert.acknowledged.is_(False)).count(),
            "acknowledged_alerts": q.filter(Alert.acknowledged.is_(True)).count(),
            "critical_alerts": q.filter(Alert.severity == "CRITICAL").count(),
            "affected_services": (
                q.filter(Alert.service_name.is_not(None))
                .with_entities(func.count(func.distinct(Alert.service_name)))
                .scalar()
                or 0
            ),
        }

    def get_severity_breakdown(
        self,
        since_hours: int = 24,
        include_acknowledged: bool = False,
        service_name: Optional[str] = None,
    ) -> list[dict[str, int | str]]:
        q = self._report_query(since_hours, include_acknowledged, service_name)
        rows = (
            q.with_entities(
                Alert.severity.label("severity"),
                func.count(Alert.id).label("count"),
            )
            .group_by(Alert.severity)
            .order_by(func.count(Alert.id).desc(), Alert.severity.asc())
            .all()
        )
        return [{"severity": row.severity, "count": row.count} for row in rows]

    def get_type_breakdown(
        self,
        since_hours: int = 24,
        include_acknowledged: bool = False,
        service_name: Optional[str] = None,
    ) -> list[dict[str, int | str]]:
        q = self._report_query(since_hours, include_acknowledged, service_name)
        rows = (
            q.with_entities(
                Alert.alert_type.label("alert_type"),
                func.count(Alert.id).label("count"),
            )
            .group_by(Alert.alert_type)
            .order_by(func.count(Alert.id).desc(), Alert.alert_type.asc())
            .all()
        )
        return [{"alert_type": row.alert_type, "count": row.count} for row in rows]

    def get_service_breakdown(
        self,
        since_hours: int = 24,
        include_acknowledged: bool = False,
        service_name: Optional[str] = None,
        limit: int = 5,
    ) -> list[dict[str, int | str]]:
        q = self._report_query(since_hours, include_acknowledged, service_name)
        service_label = func.coalesce(Alert.service_name, "unknown")
        rows = (
            q.with_entities(
                service_label.label("service_name"),
                func.count(Alert.id).label("count"),
                func.sum(case((Alert.severity == "CRITICAL", 1), else_=0)).label("critical_count"),
            )
            .group_by(service_label)
            .order_by(func.count(Alert.id).desc(), service_label.asc())
            .limit(limit)
            .all()
        )
        return [
            {
                "service_name": row.service_name,
                "count": row.count,
                "critical_count": row.critical_count or 0,
            }
            for row in rows
        ]

    def get_hourly_trend(
        self,
        since_hours: int = 24,
        include_acknowledged: bool = False,
        service_name: Optional[str] = None,
    ) -> list[dict[str, int | datetime]]:
        q = self._report_query(since_hours, include_acknowledged, service_name)
        bucket = func.date_trunc("hour", Alert.triggered_at)
        rows = (
            q.with_entities(
                bucket.label("bucket"),
                func.count(Alert.id).label("total"),
                func.sum(case((Alert.acknowledged.is_(True), 1), else_=0)).label("acknowledged"),
            )
            .group_by(bucket)
            .order_by(bucket.asc())
            .all()
        )
        return [
            {
                "bucket": row.bucket,
                "total": row.total,
                "acknowledged": row.acknowledged or 0,
            }
            for row in rows
        ]

    def get_recent_for_report(
        self,
        since_hours: int = 24,
        include_acknowledged: bool = False,
        service_name: Optional[str] = None,
        limit: int = 10,
    ) -> list[Alert]:
        q = self._report_query(since_hours, include_acknowledged, service_name)
        return q.order_by(Alert.triggered_at.desc()).limit(limit).all()
