"""
app/reporting.py — Alert reporting service

Builds alert reports from persisted alerts, anomaly logs, and DLQ state.
Can also dispatch reports through the configured Slack and email channels.
"""
from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app.alert import send_email_notification, send_slack_notification
from app.schemas import (
    AlertReportDispatchResponse,
    AlertReportResponse,
    AlertReportSendRequest,
    AlertReportTotals,
    ReportChannel,
)
from db.repository import AlertRepository, DLQRepository, LogRepository


def build_alert_report(
    db: Session,
    since_hours: int = 24,
    include_acknowledged: bool = False,
    service_name: str | None = None,
    recent_limit: int = 10,
) -> AlertReportResponse:
    alert_repo = AlertRepository(db)
    log_repo = LogRepository(db)
    dlq_repo = DLQRepository(db)

    totals = alert_repo.get_report_totals(
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
    )
    totals["anomaly_logs"] = log_repo.count_anomalies_in_window(
        since_hours=since_hours,
        service_name=service_name,
    )
    totals["unresolved_dlq"] = dlq_repo.count_unresolved()

    severity_breakdown = alert_repo.get_severity_breakdown(
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
    )
    type_breakdown = alert_repo.get_type_breakdown(
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
    )
    service_breakdown = alert_repo.get_service_breakdown(
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
    )
    hourly_trend = alert_repo.get_hourly_trend(
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
    )
    recent_alerts = alert_repo.get_recent_for_report(
        since_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
        limit=recent_limit,
    )

    report_totals = AlertReportTotals(**totals)
    return AlertReportResponse(
        generated_at=datetime.now(timezone.utc),
        window_hours=since_hours,
        include_acknowledged=include_acknowledged,
        service_name=service_name,
        totals=report_totals,
        severity_breakdown=severity_breakdown,
        type_breakdown=type_breakdown,
        service_breakdown=service_breakdown,
        hourly_trend=hourly_trend,
        recent_alerts=recent_alerts,
        recommendations=_build_recommendations(report_totals, service_breakdown),
    )


def dispatch_alert_report(
    db: Session,
    payload: AlertReportSendRequest,
) -> AlertReportDispatchResponse:
    report = build_alert_report(
        db=db,
        since_hours=payload.since_hours,
        include_acknowledged=payload.include_acknowledged,
        service_name=payload.service_name,
        recent_limit=payload.recent_limit,
    )
    subject = payload.subject or _default_subject(report)
    delivered_channels: list[ReportChannel] = []
    skipped_channels: list[ReportChannel] = []

    for channel in payload.channels:
        if channel == ReportChannel.SLACK:
            sent = send_slack_notification(
                title=subject,
                message=_format_slack_summary(report),
                metadata={
                    "severity": _top_severity(report),
                    "total_alerts": report.totals.total_alerts,
                    "active_alerts": report.totals.active_alerts,
                    "critical_alerts": report.totals.critical_alerts,
                    "window_hours": report.window_hours,
                    "service": report.service_name or "all",
                },
            )
        elif channel == ReportChannel.EMAIL:
            sent = send_email_notification(subject=subject, body=_format_email_report(report))
        else:
            sent = False

        if sent:
            delivered_channels.append(channel)
        else:
            skipped_channels.append(channel)

    return AlertReportDispatchResponse(
        subject=subject,
        delivered_channels=delivered_channels,
        skipped_channels=skipped_channels,
        generated_at=report.generated_at,
        report=report,
    )


def _build_recommendations(
    totals: AlertReportTotals,
    service_breakdown: list[dict[str, int | str]],
) -> list[str]:
    recommendations: list[str] = []

    if totals.total_alerts == 0:
        return ["No alerts matched this window. Keep this report endpoint wired into your demo or a scheduled job."]

    if totals.critical_alerts > 0:
        recommendations.append("Prioritise CRITICAL alerts first and verify each incident has an owner or acknowledgment.")
    if totals.active_alerts > 0:
        recommendations.append("Review unacknowledged alerts to prevent repeated paging on issues that are already understood.")
    if totals.unresolved_dlq > 0:
        recommendations.append("Investigate unresolved DLQ entries because replay failures often sit behind persistent alert noise.")
    if totals.anomaly_logs > max(totals.total_alerts * 2, 10):
        recommendations.append("Anomaly volume is high relative to fired alerts. Tighten thresholds or add service-level suppression rules.")
    if service_breakdown:
        top_service = service_breakdown[0]
        if int(top_service["count"]) >= max(3, totals.total_alerts // 2):
            recommendations.append(
                f"{top_service['service_name']} is driving most alert activity. Start triage with that service before widening the investigation."
            )

    if not recommendations:
        recommendations.append("Alert distribution looks balanced. Use the recent alert list to confirm the latest incidents are acknowledged.")

    return recommendations


def _default_subject(report: AlertReportResponse) -> str:
    scope = report.service_name or "all services"
    return f"AI Log Alert Report | {scope} | last {report.window_hours}h"


def _top_severity(report: AlertReportResponse) -> str:
    if report.totals.critical_alerts > 0:
        return "CRITICAL"
    if any(row.count > 0 and row.severity == "HIGH" for row in report.severity_breakdown):
        return "HIGH"
    if any(row.count > 0 and row.severity == "MEDIUM" for row in report.severity_breakdown):
        return "MEDIUM"
    return "LOW"


def _format_slack_summary(report: AlertReportResponse) -> str:
    lines = [
        f"Window: last {report.window_hours}h",
        f"Scope: {report.service_name or 'all services'}",
        f"Alerts: {report.totals.total_alerts}",
        f"Active: {report.totals.active_alerts}",
        f"Critical: {report.totals.critical_alerts}",
        f"Anomalies: {report.totals.anomaly_logs}",
        f"Unresolved DLQ: {report.totals.unresolved_dlq}",
    ]
    if report.service_breakdown:
        top_service = report.service_breakdown[0]
        lines.append(f"Noisiest service: {top_service.service_name} ({top_service.count} alerts)")
    return "\n".join(lines)


def _format_email_report(report: AlertReportResponse) -> str:
    lines = [
        "AI Log Monitoring Alert Report",
        "",
        f"Generated at: {report.generated_at.isoformat()}",
        f"Window: last {report.window_hours} hours",
        f"Scope: {report.service_name or 'all services'}",
        f"Included acknowledged alerts: {report.include_acknowledged}",
        "",
        "Totals",
        f"- Alerts in scope: {report.totals.total_alerts}",
        f"- Active alerts: {report.totals.active_alerts}",
        f"- Acknowledged alerts: {report.totals.acknowledged_alerts}",
        f"- Critical alerts: {report.totals.critical_alerts}",
        f"- Affected services: {report.totals.affected_services}",
        f"- Anomaly logs: {report.totals.anomaly_logs}",
        f"- Unresolved DLQ entries: {report.totals.unresolved_dlq}",
        "",
        "Severity breakdown",
    ]
    lines.extend(
        f"- {row.severity}: {row.count}"
        for row in report.severity_breakdown
    )
    lines.append("")
    lines.append("Type breakdown")
    lines.extend(
        f"- {row.alert_type}: {row.count}"
        for row in report.type_breakdown
    )
    lines.append("")
    lines.append("Top services")
    lines.extend(
        f"- {row.service_name}: {row.count} alerts ({row.critical_count} critical)"
        for row in report.service_breakdown
    )
    lines.append("")
    lines.append("Recommendations")
    lines.extend(f"- {item}" for item in report.recommendations)

    if report.recent_alerts:
        lines.append("")
        lines.append("Recent alerts")
        lines.extend(
            f"- [{alert.severity}] {alert.alert_type} | {alert.service_name or 'unknown'} | {alert.message}"
            for alert in report.recent_alerts
        )

    return "\n".join(lines)
