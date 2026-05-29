"""
app/schemas.py — Pydantic v2 request / response schemas
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field, model_validator


# ── Enums ───────────────────────────────────────────────────────────────────────

class LogLevel(str, Enum):
    DEBUG    = "DEBUG"
    INFO     = "INFO"
    WARN     = "WARN"
    ERROR    = "ERROR"
    CRITICAL = "CRITICAL"


class AlertSeverity(str, Enum):
    LOW      = "LOW"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"


class AlertType(str, Enum):
    ANOMALY_SPIKE  = "ANOMALY_SPIKE"
    ERROR_RATE     = "ERROR_RATE"
    DLQ_OVERFLOW   = "DLQ_OVERFLOW"
    SERVICE_DOWN   = "SERVICE_DOWN"


# ── Inbound ─────────────────────────────────────────────────────────────────────

class LogIngest(BaseModel):
    """What a microservice POSTs to /logs."""
    service_name : str      = Field(..., min_length=1, max_length=100, examples=["payment-service"])
    level        : LogLevel = Field(..., examples=["ERROR"])
    message      : str      = Field(..., min_length=1, examples=["NullPointerException in checkout flow"])
    metadata     : dict[str, Any] = Field(default_factory=dict)
    trace_id     : Optional[str]  = Field(None, max_length=64)
    span_id      : Optional[str]  = Field(None, max_length=64)
    environment  : str            = Field(default="production", max_length=50)
    host         : Optional[str]  = Field(None, max_length=255)
    timestamp    : Optional[datetime] = None   # defaults to server time if omitted

    @model_validator(mode="after")
    def set_default_timestamp(self) -> "LogIngest":
        if self.timestamp is None:
            from datetime import timezone
            self.timestamp = datetime.now(timezone.utc)
        return self


class BulkLogIngest(BaseModel):
    """Batch ingest endpoint."""
    logs: list[LogIngest] = Field(..., min_length=1, max_length=500)


# ── Outbound ────────────────────────────────────────────────────────────────────

class LogResponse(BaseModel):
    id           : UUID
    service_name : str
    level        : str
    message      : str
    metadata     : dict[str, Any] = Field(validation_alias=AliasChoices("metadata_", "metadata"))
    trace_id     : Optional[str]
    environment  : str
    timestamp    : datetime
    is_anomaly   : bool
    anomaly_score: Optional[float]
    anomaly_reason: Optional[str]

    model_config = {"from_attributes": True}


class DLQEntryResponse(BaseModel):
    id             : UUID
    original_topic : str
    payload        : dict
    error_message  : str
    retry_count    : int
    first_failed_at: datetime
    last_failed_at : datetime
    resolved       : bool

    model_config = {"from_attributes": True}


class AlertResponse(BaseModel):
    id           : UUID
    alert_type   : str
    severity     : str
    service_name : Optional[str]
    message      : str
    triggered_at : datetime
    acknowledged : bool

    model_config = {"from_attributes": True}


class MetricPoint(BaseModel):
    service_name  : str
    level         : str
    bucket        : datetime
    count         : int
    anomaly_count : int


# ── Kafka message envelope ──────────────────────────────────────────────────────

class KafkaLogMessage(BaseModel):
    """Internal Kafka envelope — wraps LogIngest with retry tracking."""
    log        : LogIngest
    retry_count: int = 0
    producer_ts: datetime = Field(default_factory=lambda: __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    ))
