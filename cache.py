"""
app/cache.py — Redis Cache Layer (Phase 2)

Wraps all Redis operations for the system.

Strategy:
  - Metrics summary    → cache 30s  (hot, read constantly by dashboard)
  - Anomaly list       → cache 15s  (near-real-time for on-call engineers)
  - Active alerts      → cache 10s  (must be fresh)
  - Service health     → cache 60s  (expensive aggregation query)
  - AI summaries       → cache 300s (OpenAI calls are slow + costly)

Keys use a flat namespace:  logs:<scope>:<params_hash>
"""
from __future__ import annotations

import hashlib
import json
import logging
from functools import wraps
from typing import Any, Callable, Optional

import redis

from app.config import settings

logger = logging.getLogger(__name__)

# ── Connection pool (singleton) ─────────────────────────────────────────────────
_pool: Optional[redis.ConnectionPool] = None


def _get_pool() -> redis.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=20,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _pool


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=_get_pool())


def redis_healthy() -> bool:
    try:
        return get_redis().ping()
    except Exception:
        return False


# ── TTL constants (seconds) ─────────────────────────────────────────────────────
TTL_METRICS    = 30
TTL_ANOMALIES  = 15
TTL_ALERTS     = 10
TTL_HEALTH     = 60
TTL_AI_SUMMARY = 300   # 5 minutes — OpenAI calls are expensive


# ── Generic get/set helpers ─────────────────────────────────────────────────────

def cache_get(key: str) -> Optional[Any]:
    """Return parsed JSON value or None on miss/error."""
    try:
        raw = get_redis().get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Redis GET failed key=%s: %s", key, exc)
        return None


def cache_set(key: str, value: Any, ttl: int) -> None:
    """Serialise value to JSON and store with TTL. Silent on error."""
    try:
        get_redis().setex(key, ttl, json.dumps(value, default=str))
    except Exception as exc:
        logger.warning("Redis SET failed key=%s: %s", key, exc)


def cache_delete(key: str) -> None:
    try:
        get_redis().delete(key)
    except Exception as exc:
        logger.warning("Redis DEL failed key=%s: %s", key, exc)


def cache_delete_pattern(pattern: str) -> int:
    """Delete all keys matching a glob pattern. Returns count deleted."""
    try:
        r = get_redis()
        keys = r.keys(pattern)
        if keys:
            return r.delete(*keys)
        return 0
    except Exception as exc:
        logger.warning("Redis DEL pattern=%s failed: %s", pattern, exc)
        return 0


# ── Key builders ────────────────────────────────────────────────────────────────

def _hash_params(**kwargs) -> str:
    """Create a short deterministic hash from keyword params."""
    raw = json.dumps(kwargs, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()[:8]


def key_metrics(since_minutes: int) -> str:
    return f"logs:metrics:{since_minutes}"


def key_anomalies(since_minutes: int, limit: int) -> str:
    return f"logs:anomalies:{since_minutes}:{limit}"


def key_alerts(limit: int) -> str:
    return f"logs:alerts:{limit}"


def key_service_health() -> str:
    return "logs:service_health"


def key_ai_summary(service_name: Optional[str], since_minutes: int) -> str:
    h = _hash_params(service=service_name, since=since_minutes)
    return f"logs:ai_summary:{h}"


def key_dlq_stats() -> str:
    return "logs:dlq_stats"


# ── Invalidation helpers (call after writes) ────────────────────────────────────

def invalidate_metrics() -> None:
    cache_delete_pattern("logs:metrics:*")


def invalidate_alerts() -> None:
    cache_delete_pattern("logs:alerts:*")


def invalidate_anomalies() -> None:
    cache_delete_pattern("logs:anomalies:*")


def invalidate_all() -> None:
    cache_delete_pattern("logs:*")
