"""
app/metrics_exporter.py — Prometheus Metrics Exporter (Phase 2)

Exposes a /metrics endpoint in Prometheus text format.
Grafana scrapes this every 15s.

Metrics exposed:
  log_ingested_total          counter  — total logs received by level + service
  log_anomalies_total         counter  — total anomalies detected
  log_processing_duration_ms  histogram — consumer processing time
  dlq_entries_total           gauge    — current unresolved DLQ size
  active_alerts_total         gauge    — current unacknowledged alerts
  kafka_consumer_lag          gauge    — estimated consumer lag (messages behind)
  redis_cache_hits_total      counter  — cache hits
  redis_cache_misses_total    counter  — cache misses
"""
from __future__ import annotations

import time
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── In-process metric stores ────────────────────────────────────────────────────

@dataclass
class _Counters:
    # label_key → count
    logs_ingested  : dict = field(default_factory=lambda: defaultdict(int))
    anomalies      : dict = field(default_factory=lambda: defaultdict(int))
    cache_hits     : int  = 0
    cache_misses   : int  = 0

@dataclass
class _Gauges:
    dlq_unresolved   : int   = 0
    active_alerts    : int   = 0
    consumer_lag     : int   = 0

@dataclass
class _Histogram:
    # processing duration buckets in ms
    buckets : list = field(default_factory=lambda: [5, 10, 25, 50, 100, 250, 500, 1000, 2500])
    counts  : dict = field(default_factory=dict)  # bucket_le → count
    sum_ms  : float = 0.0
    total   : int   = 0

    def __post_init__(self):
        self.counts = {le: 0 for le in self.buckets}
        self.counts["+Inf"] = 0

    def observe(self, ms: float):
        self.sum_ms += ms
        self.total  += 1
        for le in self.buckets:
            if ms <= le:
                self.counts[le] += 1
        self.counts["+Inf"] += 1


_counters  = _Counters()
_gauges    = _Gauges()
_histogram = _Histogram()


# ── Public recording functions (called by processor/consumer) ───────────────────

def record_log_ingested(service_name: str, level: str) -> None:
    key = f'{service_name}|{level}'
    _counters.logs_ingested[key] += 1


def record_anomaly(service_name: str, level: str) -> None:
    key = f'{service_name}|{level}'
    _counters.anomalies[key] += 1


def record_processing_time(start_ts: float) -> None:
    ms = (time.time() - start_ts) * 1000
    _histogram.observe(ms)


def record_cache_hit() -> None:
    _counters.cache_hits += 1


def record_cache_miss() -> None:
    _counters.cache_misses += 1


def set_dlq_size(n: int) -> None:
    _gauges.dlq_unresolved = n


def set_active_alerts(n: int) -> None:
    _gauges.active_alerts = n


def set_consumer_lag(n: int) -> None:
    _gauges.consumer_lag = n


# ── Prometheus text format renderer ────────────────────────────────────────────

def render_prometheus() -> str:
    """Render all metrics in Prometheus text exposition format."""
    lines: list[str] = []

    def comment(name: str, help_text: str, metric_type: str):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")

    # ── log_ingested_total ──────────────────────────────────────────────────────
    comment("log_ingested_total", "Total log messages ingested by service and level", "counter")
    for label_key, count in _counters.logs_ingested.items():
        service, level = label_key.split("|", 1)
        lines.append(
            f'log_ingested_total{{service="{service}",level="{level}"}} {count}'
        )

    # ── log_anomalies_total ─────────────────────────────────────────────────────
    comment("log_anomalies_total", "Total anomalies detected by service and level", "counter")
    for label_key, count in _counters.anomalies.items():
        service, level = label_key.split("|", 1)
        lines.append(
            f'log_anomalies_total{{service="{service}",level="{level}"}} {count}'
        )

    # ── log_processing_duration_ms ──────────────────────────────────────────────
    comment("log_processing_duration_ms", "Consumer log processing latency in milliseconds", "histogram")
    for le, cnt in _histogram.counts.items():
        lines.append(f'log_processing_duration_ms_bucket{{le="{le}"}} {cnt}')
    lines.append(f"log_processing_duration_ms_sum {_histogram.sum_ms:.2f}")
    lines.append(f"log_processing_duration_ms_count {_histogram.total}")

    # ── Gauges ──────────────────────────────────────────────────────────────────
    comment("dlq_entries_total", "Current number of unresolved DLQ entries", "gauge")
    lines.append(f"dlq_entries_total {_gauges.dlq_unresolved}")

    comment("active_alerts_total", "Current number of unacknowledged alerts", "gauge")
    lines.append(f"active_alerts_total {_gauges.active_alerts}")

    comment("kafka_consumer_lag", "Estimated Kafka consumer lag in messages", "gauge")
    lines.append(f"kafka_consumer_lag {_gauges.consumer_lag}")

    # ── Cache ───────────────────────────────────────────────────────────────────
    comment("redis_cache_hits_total", "Total Redis cache hits", "counter")
    lines.append(f"redis_cache_hits_total {_counters.cache_hits}")

    comment("redis_cache_misses_total", "Total Redis cache misses", "counter")
    lines.append(f"redis_cache_misses_total {_counters.cache_misses}")

    return "\n".join(lines) + "\n"
