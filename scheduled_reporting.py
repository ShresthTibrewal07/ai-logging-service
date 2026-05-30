"""
app/scheduled_reporting.py — Periodic alert report scheduler

Runs a configurable background loop inside the API service process.
It dispatches alert reports through the same notification channels used by the
on-demand reporting API and exposes lightweight runtime status.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import settings
from app.schemas import (
    AlertReportDispatchResponse,
    AlertReportSchedulerStatusResponse,
    AlertReportSendRequest,
    ReportChannel,
)
from db.database import SessionLocal
from reporting import dispatch_alert_report

logger = logging.getLogger(__name__)

_scheduler_task: Optional[asyncio.Task] = None
_scheduler_state: dict[str, object] = {
    "running": False,
    "next_run_at": None,
    "last_run_at": None,
    "last_success_at": None,
    "last_error": None,
    "last_delivered_channels": [],
    "last_skipped_channels": [],
}


def start_report_scheduler() -> None:
    global _scheduler_task

    if not settings.REPORT_SCHEDULER_ENABLED:
        logger.info("Scheduled alert reporting disabled")
        _scheduler_state["running"] = False
        _scheduler_state["next_run_at"] = None
        return

    if _scheduler_task and not _scheduler_task.done():
        return

    _scheduler_task = asyncio.create_task(_scheduler_loop(), name="alert-report-scheduler")
    logger.info(
        "Scheduled alert reporting enabled | interval=%ss channels=%s",
        settings.REPORT_SCHEDULE_SECONDS,
        settings.report_schedule_channels,
    )


async def stop_report_scheduler() -> None:
    global _scheduler_task

    if not _scheduler_task:
        _scheduler_state["running"] = False
        _scheduler_state["next_run_at"] = None
        return

    _scheduler_task.cancel()
    with suppress(asyncio.CancelledError):
        await _scheduler_task
    _scheduler_task = None
    _scheduler_state["running"] = False
    _scheduler_state["next_run_at"] = None


async def _scheduler_loop() -> None:
    _scheduler_state["running"] = True
    interval_seconds = max(settings.REPORT_SCHEDULE_SECONDS, 1)

    try:
        if settings.REPORT_SCHEDULE_RUN_ON_STARTUP:
            await _run_report_job()

        while True:
            _scheduler_state["next_run_at"] = datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
            await asyncio.sleep(interval_seconds)
            await _run_report_job()
    except asyncio.CancelledError:
        logger.info("Scheduled alert reporting stopped")
        raise
    finally:
        _scheduler_state["running"] = False


async def _run_report_job() -> None:
    _scheduler_state["last_run_at"] = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        result = dispatch_alert_report(db=db, payload=_build_payload_from_settings())
        _scheduler_state["last_success_at"] = result.generated_at
        _scheduler_state["last_error"] = None
        _scheduler_state["last_delivered_channels"] = result.delivered_channels
        _scheduler_state["last_skipped_channels"] = result.skipped_channels
        logger.info(
            "Scheduled alert report generated | delivered=%s skipped=%s subject=%s",
            [channel.value for channel in result.delivered_channels],
            [channel.value for channel in result.skipped_channels],
            result.subject,
        )
    except Exception as exc:
        _scheduler_state["last_error"] = str(exc)
        logger.exception("Scheduled alert report failed: %s", exc)
    finally:
        db.close()


def run_scheduled_report_job_once() -> AlertReportDispatchResponse:
    db = SessionLocal()
    try:
        result = dispatch_alert_report(db=db, payload=_build_payload_from_settings())
        _scheduler_state["last_run_at"] = result.generated_at
        _scheduler_state["last_success_at"] = result.generated_at
        _scheduler_state["last_error"] = None
        _scheduler_state["last_delivered_channels"] = result.delivered_channels
        _scheduler_state["last_skipped_channels"] = result.skipped_channels
        return result
    finally:
        db.close()


def get_scheduler_status() -> AlertReportSchedulerStatusResponse:
    return AlertReportSchedulerStatusResponse(
        enabled=settings.REPORT_SCHEDULER_ENABLED,
        running=bool(_scheduler_state["running"]),
        interval_seconds=settings.REPORT_SCHEDULE_SECONDS,
        run_on_startup=settings.REPORT_SCHEDULE_RUN_ON_STARTUP,
        since_hours=settings.REPORT_SCHEDULE_SINCE_HOURS,
        include_acknowledged=settings.REPORT_SCHEDULE_INCLUDE_ACKNOWLEDGED,
        recent_limit=settings.REPORT_SCHEDULE_RECENT_LIMIT,
        service_name=settings.report_schedule_service_name,
        subject=settings.report_schedule_subject,
        channels=_configured_channels(),
        next_run_at=_scheduler_state["next_run_at"],
        last_run_at=_scheduler_state["last_run_at"],
        last_success_at=_scheduler_state["last_success_at"],
        last_error=_scheduler_state["last_error"],
        last_delivered_channels=_scheduler_state["last_delivered_channels"],
        last_skipped_channels=_scheduler_state["last_skipped_channels"],
    )


def _build_payload_from_settings() -> AlertReportSendRequest:
    channels = _configured_channels()
    if not channels:
        raise ValueError("REPORT_SCHEDULE_CHANNELS must contain at least one of: EMAIL, SLACK")

    return AlertReportSendRequest(
        since_hours=settings.REPORT_SCHEDULE_SINCE_HOURS,
        include_acknowledged=settings.REPORT_SCHEDULE_INCLUDE_ACKNOWLEDGED,
        service_name=settings.report_schedule_service_name,
        recent_limit=settings.REPORT_SCHEDULE_RECENT_LIMIT,
        channels=channels,
        subject=settings.report_schedule_subject,
    )


def _configured_channels() -> list[ReportChannel]:
    channels: list[ReportChannel] = []
    invalid: list[str] = []

    for raw_channel in settings.report_schedule_channels:
        try:
            channels.append(ReportChannel(raw_channel))
        except ValueError:
            invalid.append(raw_channel)

    if invalid:
        raise ValueError(
            f"Invalid REPORT_SCHEDULE_CHANNELS values: {', '.join(invalid)}. Use EMAIL and/or SLACK."
        )
    return channels
