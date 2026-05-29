"""
app/ai.py — AI Anomaly Detection Engine

Phase 1: Statistical anomaly detection
  - Error spike detection (count threshold in sliding window)
  - Z-score based outlier detection on error rate per service
  - Critical keyword detection in log messages

Phase 2 (optional): OpenAI integration for log summarisation
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import settings
from app.schemas import LogIngest, LogLevel

logger = logging.getLogger(__name__)


# ─── Data structures ────────────────────────────────────────────────────────────

@dataclass
class AnomalyResult:
    is_anomaly    : bool  = False
    score         : float = 0.0   # 0.0 – 1.0
    reason        : Optional[str] = None


# In-memory sliding window: service → deque of (timestamp, level)
# Each element is a datetime of an ERROR/CRITICAL log
_error_windows: dict[str, deque] = defaultdict(lambda: deque())

# Running stats per service for Z-score: list of per-minute error counts
_rate_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=60))  # last 60 minutes

# Patterns that are always suspicious
_CRITICAL_PATTERNS = [
    re.compile(r"out of memory",            re.IGNORECASE),
    re.compile(r"stack overflow",           re.IGNORECASE),
    re.compile(r"segmentation fault",       re.IGNORECASE),
    re.compile(r"connection refused",       re.IGNORECASE),
    re.compile(r"deadlock detected",        re.IGNORECASE),
    re.compile(r"disk( space)? (full|exhausted)", re.IGNORECASE),
    re.compile(r"database.*unavailable",    re.IGNORECASE),
    re.compile(r"kafka.*disconnect",        re.IGNORECASE),
    re.compile(r"(sql|database) error",     re.IGNORECASE),
    re.compile(r"payment.*fail",            re.IGNORECASE),
    re.compile(r"auth(entication)?.*fail",  re.IGNORECASE),
]


# ─── Main detection function ────────────────────────────────────────────────────

def detect_anomaly(log: LogIngest) -> AnomalyResult:
    """
    Run all detection strategies and return the highest-severity result.
    """
    reasons: list[str] = []
    max_score: float   = 0.0

    # 1. Critical keyword match
    kw_result = _keyword_detection(log)
    if kw_result.is_anomaly:
        reasons.append(kw_result.reason)  # type: ignore[arg-type]
        max_score = max(max_score, kw_result.score)

    # 2. Immediate CRITICAL-level flag
    if log.level == LogLevel.CRITICAL:
        reasons.append("CRITICAL level log")
        max_score = max(max_score, 0.85)

    # 3. Sliding-window error spike
    if log.level in (LogLevel.ERROR, LogLevel.CRITICAL):
        spike_result = _error_spike_detection(log)
        if spike_result.is_anomaly:
            reasons.append(spike_result.reason)  # type: ignore[arg-type]
            max_score = max(max_score, spike_result.score)

    # 4. Z-score based rate anomaly
    zscore_result = _zscore_detection(log)
    if zscore_result.is_anomaly:
        reasons.append(zscore_result.reason)  # type: ignore[arg-type]
        max_score = max(max_score, zscore_result.score)

    if reasons:
        return AnomalyResult(
            is_anomaly=True,
            score=round(min(max_score, 1.0), 4),
            reason="; ".join(reasons),
        )

    return AnomalyResult(is_anomaly=False, score=0.0)


# ─── Strategy implementations ───────────────────────────────────────────────────

def _keyword_detection(log: LogIngest) -> AnomalyResult:
    for pattern in _CRITICAL_PATTERNS:
        if pattern.search(log.message):
            return AnomalyResult(
                is_anomaly=True,
                score=0.80,
                reason=f"Critical keyword matched: '{pattern.pattern}'",
            )
    return AnomalyResult()


def _error_spike_detection(log: LogIngest) -> AnomalyResult:
    """
    Count errors in the last N minutes for this service.
    Flag if count exceeds threshold.
    """
    window  = _error_windows[log.service_name]
    now     = datetime.now(timezone.utc)
    cutoff  = now - timedelta(minutes=settings.ANOMALY_WINDOW_MINUTES)

    # Evict expired entries (left side of deque)
    while window and window[0] < cutoff:
        window.popleft()

    window.append(now)
    count = len(window)

    if count >= settings.ANOMALY_ERROR_SPIKE_THRESHOLD:
        score = min(0.5 + (count / settings.ANOMALY_ERROR_SPIKE_THRESHOLD) * 0.3, 1.0)
        return AnomalyResult(
            is_anomaly=True,
            score=round(score, 4),
            reason=(
                f"Error spike: {count} errors in last "
                f"{settings.ANOMALY_WINDOW_MINUTES}m "
                f"(threshold={settings.ANOMALY_ERROR_SPIKE_THRESHOLD})"
            ),
        )
    return AnomalyResult()


def _zscore_detection(log: LogIngest) -> AnomalyResult:
    """
    Maintain a rolling per-minute error count.
    Compute Z-score of the current minute vs historical distribution.
    """
    if log.level not in (LogLevel.ERROR, LogLevel.CRITICAL):
        return AnomalyResult()

    history = _rate_history[log.service_name]

    # Append 1 for current minute (simplification: each call = 1 event)
    history.append(1)

    if len(history) < 10:
        # Not enough history to compute meaningful Z-score
        return AnomalyResult()

    import statistics
    data   = list(history)
    mean   = statistics.mean(data)
    stdev  = statistics.stdev(data)

    if stdev == 0:
        return AnomalyResult()

    current = data[-1]
    zscore  = abs((current - mean) / stdev)

    if zscore >= settings.ANOMALY_ZSCORE_THRESHOLD:
        score = min(0.4 + (zscore / 10), 1.0)
        return AnomalyResult(
            is_anomaly=True,
            score=round(score, 4),
            reason=f"Z-score anomaly: {zscore:.2f} (threshold={settings.ANOMALY_ZSCORE_THRESHOLD})",
        )
    return AnomalyResult()


# ─── Phase 2: OpenAI summarisation (optional) ──────────────────────────────────

async def summarise_logs_openai(logs: list[LogIngest]) -> str:
    """
    Use OpenAI to produce a concise incident summary from a batch of logs.
    Only active when OPENAI_API_KEY is set.
    """
    if not settings.OPENAI_API_KEY:
        return "OpenAI integration not configured."

    try:
        import openai  # noqa: F401 (optional dependency)
        client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

        log_text = "\n".join(
            f"[{l.level}] {l.service_name}: {l.message}" for l in logs[:20]
        )
        prompt = (
            "You are a senior SRE analysing production logs. "
            "Given the following log entries, provide:\n"
            "1. A one-line incident summary\n"
            "2. Likely root cause\n"
            "3. Recommended immediate actions\n\n"
            f"Logs:\n{log_text}"
        )

        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
        )
        return resp.choices[0].message.content or ""

    except Exception as exc:  # noqa: BLE001
        logger.warning("OpenAI summarisation failed: %s", exc)
        return f"Summarisation unavailable: {exc}"
