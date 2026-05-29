#!/usr/bin/env python3
"""
scripts/simulate_logs.py

Simulates 5 microservices sending logs to the ingestion API.
Includes normal traffic + injected anomalies to trigger AI detection.

Usage:
    python scripts/simulate_logs.py [--host http://localhost:8000] [--count 200]
"""
import argparse
import random
import time
from datetime import datetime, timezone

import httpx

# ── Config ──────────────────────────────────────────────────────────────────────
SERVICES = [
    "payment-service",
    "auth-service",
    "order-service",
    "notification-service",
    "inventory-service",
]

NORMAL_MESSAGES = {
    "DEBUG" : [
        "Cache hit for user session",
        "DB query executed in 12ms",
        "Request received: GET /api/v1/products",
        "Kafka message produced successfully",
    ],
    "INFO"  : [
        "User login successful",
        "Order #1042 created",
        "Payment processed: $49.99",
        "Email notification sent",
        "Inventory updated for SKU-8821",
    ],
    "WARN"  : [
        "Response time exceeded 500ms",
        "Retry attempt 1 for downstream call",
        "Cache miss — falling back to DB",
        "Rate limit approaching for client IP",
    ],
    "ERROR" : [
        "Failed to connect to downstream service",
        "Database query timeout after 30s",
        "Unhandled exception in request handler",
        "HTTP 503 from upstream payment gateway",
    ],
}

ANOMALY_MESSAGES = [
    "CRITICAL: Out of memory — heap exhausted",
    "CRITICAL: Database unavailable — connection pool empty",
    "CRITICAL: Deadlock detected in transaction processor",
    "ERROR: Stack overflow in recursive resolver",
    "ERROR: Payment gateway authentication failed — invalid credentials",
    "ERROR: Disk space exhausted — write operations failing",
]


def send_log(client: httpx.Client, host: str, payload: dict) -> bool:
    try:
        r = client.post(f"{host}/logs", json=payload, timeout=5)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"  ✗ Failed to send log: {e}")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",  default="http://localhost:8000")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--delay", type=float, default=0.05,
                        help="Seconds between logs (default 0.05)")
    args = parser.parse_args()

    print(f"🚀 Simulating {args.count} log events → {args.host}")
    print("   Services:", ", ".join(SERVICES))
    print()

    sent = failed = anomalies = 0

    with httpx.Client() as client:
        for i in range(args.count):
            service = random.choice(SERVICES)

            # Inject an anomaly every ~20 logs
            if i % 20 == 0 and i > 0:
                level   = "CRITICAL" if random.random() > 0.5 else "ERROR"
                message = random.choice(ANOMALY_MESSAGES)
                anomalies += 1
            else:
                level_weights = [0.10, 0.50, 0.20, 0.15, 0.05]
                level = random.choices(
                    ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
                    weights=level_weights,
                )[0]
                level_key = level if level != "CRITICAL" else "ERROR"
                message = random.choice(NORMAL_MESSAGES.get(level_key, NORMAL_MESSAGES["INFO"]))

            payload = {
                "service_name": service,
                "level"       : level,
                "message"     : message,
                "trace_id"    : f"trace-{random.randint(10000, 99999)}",
                "environment" : "production",
                "host"        : f"{service}-pod-{random.randint(1, 3)}",
                "metadata"    : {
                    "request_id": f"req-{i:06d}",
                    "user_id"   : random.randint(1000, 9999) if level != "DEBUG" else None,
                    "latency_ms": random.randint(10, 2000),
                },
            }

            ok = send_log(client, args.host, payload)
            if ok:
                sent += 1
                icon = "🔴" if level in ("ERROR", "CRITICAL") else "🟢"
                print(f"  {icon} [{i+1:3d}] {service:25s} {level:8s} {message[:60]}")
            else:
                failed += 1

            time.sleep(args.delay)

    print()
    print("─" * 60)
    print(f"✅ Sent    : {sent}")
    print(f"❌ Failed  : {failed}")
    print(f"🤖 Anomalies injected: {anomalies}")
    print()
    print("Check anomalies: GET /logs/anomalies")
    print("Check alerts   : GET /alerts")
    print("Check DLQ      : GET /dlq")


if __name__ == "__main__":
    main()
