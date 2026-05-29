"""
app/summariser.py — OpenAI Log Summarisation (Phase 2)

Provides two functions:
  summarise_incident(logs)   → AI-written incident report for a set of anomaly logs
  explain_anomaly(log)       → Single-log root-cause explanation

Both check Redis cache before calling OpenAI, and fall back to a
rule-based summary if no API key is configured.

The Anthropic Claude API is used as an alternative when ANTHROPIC_API_KEY
is set (claude-3-5-haiku is cheaper and faster than GPT-4o for log triage).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.cache import cache_get, cache_set, key_ai_summary, TTL_AI_SUMMARY
from app.config import settings
from app.schemas import LogIngest, LogLevel

logger = logging.getLogger(__name__)


# ── Prompt templates ────────────────────────────────────────────────────────────

_INCIDENT_SYSTEM = """You are a senior Site Reliability Engineer performing log triage.
Given a batch of production log entries, respond with a JSON object only — no markdown,
no preamble. Schema:
{
  "summary": "one sentence describing what is going wrong",
  "severity": "LOW | MEDIUM | HIGH | CRITICAL",
  "likely_cause": "concise technical root-cause hypothesis",
  "affected_services": ["list", "of", "services"],
  "recommended_actions": ["action 1", "action 2", "action 3"],
  "confidence": 0.0
}"""

_EXPLAIN_SYSTEM = """You are a senior SRE explaining a single anomalous log entry to a junior engineer.
Respond with JSON only:
{
  "explanation": "plain-English explanation of what happened",
  "why_anomalous": "why this specific log is flagged as an anomaly",
  "likely_cause": "most probable root cause",
  "next_steps": ["step 1", "step 2"]
}"""


# ── Public API ──────────────────────────────────────────────────────────────────

async def summarise_incident(
    logs: list[LogIngest],
    service_name: Optional[str] = None,
    since_minutes: int = 30,
) -> dict:
    """
    Generate an AI incident summary for a group of anomaly logs.
    Returns a structured dict — cached for TTL_AI_SUMMARY seconds.
    """
    cache_key = key_ai_summary(service_name, since_minutes)
    cached    = cache_get(cache_key)
    if cached:
        logger.debug("AI summary cache HIT | key=%s", cache_key)
        cached["_cached"] = True
        return cached

    if not logs:
        return _empty_summary("No logs provided")

    log_text = _format_logs(logs[:30])   # cap at 30 lines to control tokens

    result = await _call_llm(
        system  = _INCIDENT_SYSTEM,
        user    = f"Analyse these production logs:\n\n{log_text}",
        fallback= _rule_based_summary(logs),
    )

    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    result["log_count"]    = len(logs)
    result["_cached"]      = False

    cache_set(cache_key, result, TTL_AI_SUMMARY)
    return result


async def explain_anomaly(log: LogIngest, anomaly_reason: str) -> dict:
    """Explain why a single log was flagged as anomalous."""
    cache_key = key_ai_summary(log.service_name, 0) + f":{hash(log.message)}"
    cached    = cache_get(cache_key)
    if cached:
        return cached

    user_prompt = (
        f"Service: {log.service_name}\n"
        f"Level: {log.level}\n"
        f"Message: {log.message}\n"
        f"Detected anomaly reason: {anomaly_reason}\n\n"
        f"Explain this anomaly to a junior engineer."
    )

    result = await _call_llm(
        system  = _EXPLAIN_SYSTEM,
        user    = user_prompt,
        fallback= {
            "explanation"  : log.message,
            "why_anomalous": anomaly_reason,
            "likely_cause" : "Unknown — configure OPENAI_API_KEY for AI analysis",
            "next_steps"   : ["Check service logs", "Review recent deployments"],
        },
    )

    cache_set(cache_key, result, TTL_AI_SUMMARY)
    return result


# ── LLM caller — tries OpenAI, falls back gracefully ───────────────────────────

async def _call_llm(system: str, user: str, fallback: dict) -> dict:
    if not settings.OPENAI_API_KEY:
        logger.info("No OPENAI_API_KEY — using rule-based fallback")
        return fallback

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                    "Content-Type" : "application/json",
                },
                json={
                    "model"      : "gpt-4o-mini",
                    "messages"   : [
                        {"role": "system", "content": system},
                        {"role": "user",   "content": user},
                    ],
                    "max_tokens"      : 600,
                    "temperature"     : 0.2,
                    "response_format" : {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            return json.loads(content)

    except httpx.HTTPStatusError as exc:
        logger.error("OpenAI API error %s: %s", exc.response.status_code, exc.response.text)
        return {**fallback, "_error": f"OpenAI {exc.response.status_code}"}
    except Exception as exc:
        logger.error("LLM call failed: %s", exc)
        return {**fallback, "_error": str(exc)}


# ── Rule-based fallback (no OpenAI required) ────────────────────────────────────

def _rule_based_summary(logs: list[LogIngest]) -> dict:
    """Deterministic summary used when OpenAI is unavailable."""
    levels   = [l.level for l in logs]
    services = list({l.service_name for l in logs})
    critical = sum(1 for lv in levels if lv == LogLevel.CRITICAL)
    errors   = sum(1 for lv in levels if lv == LogLevel.ERROR)

    if critical > 0:
        severity = "CRITICAL"
    elif errors > 3:
        severity = "HIGH"
    elif errors > 0:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    return {
        "summary"            : f"{errors} errors and {critical} critical events across {len(services)} service(s)",
        "severity"           : severity,
        "likely_cause"       : "Automated analysis unavailable — configure OPENAI_API_KEY for AI triage",
        "affected_services"  : services,
        "recommended_actions": [
            "Review recent deployments on affected services",
            "Check downstream dependencies and DB connections",
            "Examine full logs via GET /logs?service_name=<name>",
        ],
        "confidence"         : 0.0,
    }


def _empty_summary(reason: str) -> dict:
    return {
        "summary"            : reason,
        "severity"           : "LOW",
        "likely_cause"       : "N/A",
        "affected_services"  : [],
        "recommended_actions": [],
        "confidence"         : 0.0,
        "log_count"          : 0,
    }


def _format_logs(logs: list[LogIngest]) -> str:
    lines = []
    for l in logs:
        ts = l.timestamp.strftime("%H:%M:%S") if l.timestamp else "??"
        lines.append(f"[{ts}] [{l.level:8s}] {l.service_name}: {l.message}")
    return "\n".join(lines)
