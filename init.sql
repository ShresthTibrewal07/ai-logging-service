-- ─── Logs Table ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name    VARCHAR(100) NOT NULL,
    level           VARCHAR(20)  NOT NULL,  -- DEBUG, INFO, WARN, ERROR, CRITICAL
    message         TEXT         NOT NULL,
    metadata        JSONB        DEFAULT '{}',
    trace_id        VARCHAR(64),
    span_id         VARCHAR(64),
    environment     VARCHAR(50)  DEFAULT 'production',
    host            VARCHAR(255),
    timestamp       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    retry_count     INT          NOT NULL DEFAULT 0,
    is_anomaly      BOOLEAN      NOT NULL DEFAULT FALSE,
    anomaly_score   FLOAT,
    anomaly_reason  TEXT
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_logs_service_name   ON logs (service_name);
CREATE INDEX IF NOT EXISTS idx_logs_level          ON logs (level);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp      ON logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_is_anomaly     ON logs (is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_logs_trace_id       ON logs (trace_id) WHERE trace_id IS NOT NULL;

-- ─── DLQ (Dead Letter Queue) Table ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_topic  VARCHAR(100) NOT NULL,
    payload         JSONB        NOT NULL,
    error_message   TEXT         NOT NULL,
    retry_count     INT          NOT NULL DEFAULT 0,
    first_failed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_failed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved        BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ
);

-- ─── Alerts Table ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_type      VARCHAR(50)  NOT NULL,  -- ANOMALY_SPIKE, ERROR_RATE, DLQ_OVERFLOW
    severity        VARCHAR(20)  NOT NULL,  -- LOW, MEDIUM, HIGH, CRITICAL
    service_name    VARCHAR(100),
    message         TEXT         NOT NULL,
    metadata        JSONB        DEFAULT '{}',
    triggered_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    acknowledged    BOOLEAN      NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_triggered_at ON alerts (triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged  ON alerts (acknowledged) WHERE acknowledged = FALSE;

-- ─── Metrics Aggregation Table (for dashboards) ───────────────────────────────
CREATE TABLE IF NOT EXISTS log_metrics (
    id              SERIAL PRIMARY KEY,
    service_name    VARCHAR(100) NOT NULL,
    level           VARCHAR(20)  NOT NULL,
    bucket          TIMESTAMPTZ  NOT NULL,  -- 1-minute buckets
    count           INT          NOT NULL DEFAULT 0,
    anomaly_count   INT          NOT NULL DEFAULT 0,
    UNIQUE (service_name, level, bucket)
);

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON log_metrics (bucket DESC);
