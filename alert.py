"""
app/alert.py — Alerting Service

Triggers alerts for:
  - Anomaly spikes
  - High error rates
  - DLQ overflow

Notification channels:
  - Database (always)
  - Slack webhook (if configured)
  - Email via SMTP (if configured)
"""
from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Alert
from app.schemas import AlertSeverity, AlertType, LogIngest

logger = logging.getLogger(__name__)

# Deduplicate: track the last time we fired each alert type+service
_last_alert: dict[str, datetime] = {}
_COOLDOWN_SECONDS = 60  # don't re-fire same alert within 1 minute


# ── Public trigger functions ────────────────────────────────────────────────────

def trigger_anomaly_alert(
    log: LogIngest,
    anomaly_reason: str,
    anomaly_score: float,
    db: Session,
) -> None:
    """Fire when AI detects an anomaly in a log entry."""
    severity = _score_to_severity(anomaly_score)
    message  = (
        f"[{log.service_name}] Anomaly detected in {log.level} log | "
        f"score={anomaly_score:.2f} reason={anomaly_reason}"
    )
    _fire_alert(
        alert_type   = AlertType.ANOMALY_SPIKE,
        severity     = severity,
        service_name = log.service_name,
        message      = message,
        metadata     = {
            "level"         : log.level,
            "anomaly_score" : anomaly_score,
            "anomaly_reason": anomaly_reason,
            "log_message"   : log.message[:200],
            "trace_id"      : log.trace_id,
        },
        db=db,
    )


def trigger_error_rate_alert(
    service_name: str,
    error_count : int,
    window_min  : int,
    db          : Session,
) -> None:
    """Fire when error rate crosses the configured threshold."""
    message = (
        f"[{service_name}] High error rate: {error_count} errors "
        f"in last {window_min}m"
    )
    _fire_alert(
        alert_type   = AlertType.ERROR_RATE,
        severity     = AlertSeverity.HIGH,
        service_name = service_name,
        message      = message,
        metadata     = {"error_count": error_count, "window_minutes": window_min},
        db=db,
    )


def trigger_dlq_alert(service_name: Optional[str], dlq_size: int, db: Session) -> None:
    """Fire when the DLQ grows too large."""
    message = f"DLQ overflow: {dlq_size} unresolved entries"
    if service_name:
        message = f"[{service_name}] {message}"
    _fire_alert(
        alert_type   = AlertType.DLQ_OVERFLOW,
        severity     = AlertSeverity.CRITICAL,
        service_name = service_name,
        message      = message,
        metadata     = {"dlq_size": dlq_size},
        db=db,
    )


# ── Internal ────────────────────────────────────────────────────────────────────

def _fire_alert(
    alert_type  : AlertType,
    severity    : AlertSeverity,
    service_name: Optional[str],
    message     : str,
    metadata    : dict,
    db          : Session,
) -> None:
    # ── Cooldown check ──────────────────────────────────────────────────────────
    dedup_key = f"{alert_type}:{service_name}"
    last      = _last_alert.get(dedup_key)
    now       = datetime.now(timezone.utc)
    if last and (now - last).total_seconds() < _COOLDOWN_SECONDS:
        logger.debug("Alert suppressed (cooldown) | key=%s", dedup_key)
        return
    _last_alert[dedup_key] = now

    logger.warning("ALERT [%s/%s] %s", severity, alert_type, message)

    # 1. Persist to DB
    alert = Alert(
        alert_type   = alert_type,
        severity     = severity,
        service_name = service_name,
        message      = message,
        metadata_    = metadata,
    )
    db.add(alert)
    db.commit()

    # 2. Slack
    if settings.SLACK_WEBHOOK_URL:
        send_slack_notification(
            title=f"{severity} alert | {alert_type}",
            message=message,
            metadata={"severity": str(severity), **metadata},
        )

    # 3. Email
    if settings.SMTP_HOST and settings.ALERT_EMAIL_TO:
        send_email_notification(
            subject=f"[{severity}] AI Log Monitor Alert: {alert_type}",
            body=f"Alert Type : {alert_type}\nSeverity   : {severity}\n\n{message}",
        )


def _score_to_severity(score: float) -> AlertSeverity:
    if score >= 0.9:
        return AlertSeverity.CRITICAL
    if score >= 0.7:
        return AlertSeverity.HIGH
    if score >= 0.5:
        return AlertSeverity.MEDIUM
    return AlertSeverity.LOW


def send_slack_notification(title: str, message: str, metadata: Optional[dict] = None) -> bool:
    if not settings.SLACK_WEBHOOK_URL:
        return False

    metadata = metadata or {}
    severity = str(metadata.get("severity", "INFO")).upper()
    emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "INFO": "🔵"}.get(
        severity,
        "⚪",
    )
    metadata_lines = []
    for key, value in metadata.items():
        if value in (None, "", [], {}):
            continue
        metadata_lines.append(f"• {key}: {value}")
    metadata_block = "\n" + "\n".join(metadata_lines) if metadata_lines else ""
    payload = {
        "text": f"{emoji} *{title}*\n>{message}{metadata_block}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{emoji} *{title}*\n>{message}{metadata_block}",
                },
            }
        ],
    }
    try:
        resp = httpx.post(settings.SLACK_WEBHOOK_URL, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info("Slack notification sent | status=%d", resp.status_code)
        return True
    except Exception as exc:
        logger.error("Slack notification failed: %s", exc)
        return False


def send_email_notification(subject: str, body: str) -> bool:
    if not (settings.SMTP_HOST and settings.ALERT_EMAIL_TO):
        return False

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = settings.SMTP_USER
    msg["To"]      = settings.ALERT_EMAIL_TO

    try:
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
            server.starttls()
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            server.sendmail(settings.SMTP_USER, settings.ALERT_EMAIL_TO, msg.as_string())
        logger.info("Email notification sent to %s", settings.ALERT_EMAIL_TO)
        return True
    except Exception as exc:
        logger.error("Email notification failed: %s", exc)
        return False
