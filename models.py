"""
app/models.py — SQLAlchemy ORM models (maps to db/init.sql tables)
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer,
    String, Text, JSON, UUID, UniqueConstraint
)
from db.database import Base


def _now():
    return datetime.now(timezone.utc)


class LogEntry(Base):
    __tablename__ = "logs"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_name   = Column(String(100), nullable=False, index=True)
    level          = Column(String(20),  nullable=False, index=True)
    message        = Column(Text,        nullable=False)
    metadata_      = Column("metadata",  JSON, default=dict)
    trace_id       = Column(String(64))
    span_id        = Column(String(64))
    environment    = Column(String(50),  default="production")
    host           = Column(String(255))
    timestamp      = Column(DateTime(timezone=True), nullable=False, default=_now, index=True)
    received_at    = Column(DateTime(timezone=True), nullable=False, default=_now)
    processed_at   = Column(DateTime(timezone=True))
    retry_count    = Column(Integer,  nullable=False, default=0)
    is_anomaly     = Column(Boolean,  nullable=False, default=False)
    anomaly_score  = Column(Float)
    anomaly_reason = Column(Text)


class DLQEntry(Base):
    __tablename__ = "dead_letter_queue"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_topic  = Column(String(100), nullable=False)
    payload         = Column(JSON,        nullable=False)
    error_message   = Column(Text,        nullable=False)
    retry_count     = Column(Integer,     nullable=False, default=0)
    first_failed_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    last_failed_at  = Column(DateTime(timezone=True), nullable=False, default=_now)
    resolved        = Column(Boolean,     nullable=False, default=False)
    resolved_at     = Column(DateTime(timezone=True))


class Alert(Base):
    __tablename__ = "alerts"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    alert_type      = Column(String(50),  nullable=False)
    severity        = Column(String(20),  nullable=False)
    service_name    = Column(String(100))
    message         = Column(Text,        nullable=False)
    metadata_       = Column("metadata",  JSON, default=dict)
    triggered_at    = Column(DateTime(timezone=True), nullable=False, default=_now)
    acknowledged    = Column(Boolean,     nullable=False, default=False)
    acknowledged_at = Column(DateTime(timezone=True))


class LogMetric(Base):
    __tablename__ = "log_metrics"
    __table_args__ = (
        UniqueConstraint("service_name", "level", "bucket", name="uq_log_metrics_service_level_bucket"),
    )

    id            = Column(Integer, primary_key=True, autoincrement=True)
    service_name  = Column(String(100), nullable=False)
    level         = Column(String(20),  nullable=False)
    bucket        = Column(DateTime(timezone=True), nullable=False)
    count         = Column(Integer, nullable=False, default=0)
    anomaly_count = Column(Integer, nullable=False, default=0)
