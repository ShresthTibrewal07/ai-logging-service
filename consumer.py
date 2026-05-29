"""
app/consumer.py — Kafka Consumer (long-running worker process)

Subscribes to:
  - logs.raw    (new logs)
  - logs.retry  (messages being retried)

On failure:
  - retries up to MAX_RETRY_COUNT
  - moves to DLQ after exhausting retries

Run standalone:
    python -m app.consumer
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from app.config import settings
from app.ingestion import ensure_topics
from app.processor import process_log
from app.retry import handle_retry, send_to_dlq, should_retry
from app.schemas import KafkaLogMessage
from db.database import SessionLocal

logging.basicConfig(
    level    = getattr(logging, settings.LOG_LEVEL),
    format   = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt  = "%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("consumer")

# ── Graceful shutdown ───────────────────────────────────────────────────────────
_running = True

def _shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal received, draining...")
    _running = False

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# ── Consumer factory ────────────────────────────────────────────────────────────

def _make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers"        : settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id"                 : settings.KAFKA_CONSUMER_GROUP,
        "auto.offset.reset"        : "earliest",
        "enable.auto.commit"       : False,   # manual commit for at-least-once
        "max.poll.interval.ms"     : 300_000,
        "session.timeout.ms"       : 30_000,
        "heartbeat.interval.ms"    : 10_000,
    })


# ── Message handler ─────────────────────────────────────────────────────────────

def _handle_message(msg: Message) -> None:
    raw = msg.value()
    if raw is None:
        return

    db = SessionLocal()
    try:
        envelope  = KafkaLogMessage.model_validate_json(raw)
        log       = envelope.log
        retry_cnt = envelope.retry_count

        try:
            process_log(envelope, db)

        except Exception as processing_error:
            if should_retry(retry_cnt):
                # Sleep for backoff, then republish to retry topic
                time.sleep(settings.RETRY_DELAY_SECONDS)
                handle_retry(log, retry_cnt, processing_error)
            else:
                send_to_dlq(
                    log            = log,
                    retry_count    = retry_cnt,
                    error          = processing_error,
                    original_topic = msg.topic(),
                    db             = db,
                )

    except Exception as parse_error:
        # Unparseable message — send directly to DLQ (no retry)
        logger.error("Unparseable message, sending to DLQ: %s", parse_error)
        try:
            send_to_dlq(
                log            = _dummy_log(),
                retry_count    = settings.MAX_RETRY_COUNT,
                error          = parse_error,
                original_topic = msg.topic(),
                db             = db,
            )
        except Exception as dlq_err:
            logger.critical("DLQ write failed: %s", dlq_err)

    finally:
        db.close()


def _dummy_log():
    """Placeholder when we can't deserialise the actual log."""
    from app.schemas import LogIngest, LogLevel
    return LogIngest(
        service_name="unknown",
        level=LogLevel.ERROR,
        message="[UNPARSEABLE] Raw message could not be deserialised",
    )


# ── Main loop ───────────────────────────────────────────────────────────────────

def run() -> None:
    ensure_topics()
    consumer = _make_consumer()
    topics   = [settings.KAFKA_TOPIC_LOGS, settings.KAFKA_TOPIC_RETRY]
    consumer.subscribe(topics)
    logger.info("Consumer started | topics=%s", topics)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Partition EOF | topic=%s partition=%s",
                                 msg.topic(), msg.partition())
                else:
                    raise KafkaException(msg.error())
                continue

            _handle_message(msg)

            # Manual commit after successful handling
            consumer.commit(message=msg, asynchronous=False)

    except KafkaException as exc:
        logger.critical("Fatal Kafka error: %s", exc)
        sys.exit(1)

    finally:
        logger.info("Closing consumer...")
        consumer.close()


if __name__ == "__main__":
    run()
