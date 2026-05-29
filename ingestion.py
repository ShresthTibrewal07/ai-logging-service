"""
app/ingestion.py — Kafka Producer

Publishes log messages to the `logs.raw` topic.
Called by the FastAPI route; decouples HTTP handling from processing.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import settings
from app.schemas import KafkaLogMessage, LogIngest

logger = logging.getLogger(__name__)


# ── Producer singleton ─────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _get_producer() -> Producer:
    conf = {
        "bootstrap.servers"  : settings.KAFKA_BOOTSTRAP_SERVERS,
        "client.id"          : "log-ingestion-service",
        "acks"               : "all",          # wait for all ISR replicas
        "retries"            : 5,
        "retry.backoff.ms"   : 300,
        "compression.type"   : "snappy",
        "linger.ms"          : 5,              # small batching window
        "batch.size"         : 65536,
    }
    return Producer(conf)


@lru_cache(maxsize=1)
def _get_admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})


def _delivery_report(err, msg):
    """Callback fired after each produce()."""
    if err:
        logger.error(
            "Kafka delivery failed | topic=%s partition=%s error=%s",
            msg.topic(), msg.partition(), err,
        )
    else:
        logger.debug(
            "Kafka delivery OK | topic=%s partition=%s offset=%s",
            msg.topic(), msg.partition(), msg.offset(),
        )


def ensure_topics() -> None:
    """Create required Kafka topics if they do not already exist."""
    admin = _get_admin_client()
    futures = admin.create_topics([
        NewTopic(settings.KAFKA_TOPIC_LOGS, num_partitions=1, replication_factor=1),
        NewTopic(settings.KAFKA_TOPIC_RETRY, num_partitions=1, replication_factor=1),
        NewTopic(settings.KAFKA_TOPIC_DLQ, num_partitions=1, replication_factor=1),
    ])

    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Kafka topic ready | topic=%s", topic)
        except Exception as exc:
            error_text = str(exc)
            if "TOPIC_ALREADY_EXISTS" in error_text or "already exists" in error_text:
                continue
            raise


# ── Public API ─────────────────────────────────────────────────────────────────

def publish_log(log: LogIngest, retry_count: int = 0) -> None:
    """
    Serialise a LogIngest into a KafkaLogMessage envelope and publish it.
    Uses service_name as the partition key for ordering within a service.
    """
    envelope = KafkaLogMessage(log=log, retry_count=retry_count)
    payload  = envelope.model_dump_json().encode("utf-8")
    key      = log.service_name.encode("utf-8")

    topic = settings.KAFKA_TOPIC_LOGS if retry_count == 0 else settings.KAFKA_TOPIC_RETRY

    ensure_topics()
    producer = _get_producer()
    producer.produce(
        topic    = topic,
        key      = key,
        value    = payload,
        callback = _delivery_report,
    )
    # Non-blocking; flush in bulk for throughput. The producer buffers internally.
    producer.poll(0)


def publish_to_dlq(payload: dict, error_message: str, original_topic: str) -> None:
    """Push a permanently-failed message to the dead-letter topic."""
    dlq_payload = json.dumps({
        "original_topic": original_topic,
        "payload"       : payload,
        "error_message" : error_message,
    }).encode("utf-8")

    ensure_topics()
    producer = _get_producer()
    producer.produce(
        topic    = settings.KAFKA_TOPIC_DLQ,
        value    = dlq_payload,
        callback = _delivery_report,
    )
    producer.poll(0)


def flush_producer(timeout: float = 10.0) -> None:
    """Flush pending messages — call on app shutdown."""
    _get_producer().flush(timeout)
