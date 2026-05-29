"""
app/config.py — Centralised settings (Phase 1 + Phase 2)
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── App ────────────────────────────────────────────────────────────────────
    APP_ENV  : str = "development"
    LOG_LEVEL: str = "INFO"

    # ── Kafka ──────────────────────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_LOGS        : str = "logs.raw"
    KAFKA_TOPIC_RETRY       : str = "logs.retry"
    KAFKA_TOPIC_DLQ         : str = "logs.dlq"
    KAFKA_CONSUMER_GROUP    : str = "log-processor-group"

    # ── Database ───────────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql://loguser:logpassword@localhost:5432/logsdb"

    # ── Redis ──────────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379"

    # ── Retry / DLQ ────────────────────────────────────────────────────────────
    MAX_RETRY_COUNT    : int = 3
    RETRY_DELAY_SECONDS: int = 5

    # ── AI Anomaly Detection ───────────────────────────────────────────────────
    ANOMALY_WINDOW_MINUTES        : int   = 5
    ANOMALY_ERROR_SPIKE_THRESHOLD : int   = 10
    ANOMALY_ZSCORE_THRESHOLD      : float = 2.5

    # ── Alerting ───────────────────────────────────────────────────────────────
    SLACK_WEBHOOK_URL: str = ""
    SMTP_HOST        : str = ""
    SMTP_PORT        : int = 587
    SMTP_USER        : str = ""
    SMTP_PASSWORD    : str = ""
    ALERT_EMAIL_TO   : str = ""

    # ── Phase 2: OpenAI ────────────────────────────────────────────────────────
    OPENAI_API_KEY: str = ""

    # ── Phase 2: Prometheus ────────────────────────────────────────────────────
    METRICS_ENABLED: bool = True

    # ── Phase 2: DLQ Replay ────────────────────────────────────────────────────
    DLQ_REPLAY_BATCH_MAX: int = 50   # max entries per replay call


settings = Settings()
