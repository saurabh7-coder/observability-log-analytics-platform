-- Observability & Log Analytics Data Platform - ClickHouse schema
-- Apply with:  clickhouse-client --multiquery < clickhouse/schema.sql
-- (In Docker Compose this file is mounted into /docker-entrypoint-initdb.d/
--  and applied automatically on first start.)

CREATE DATABASE IF NOT EXISTS observability;

-- ---------------------------------------------------------------------------
-- 1. Raw logs: one row per log line.
--    MergeTree partitioned by (service, day) so per-service / per-day queries
--    prune partitions instead of scanning everything, ordered by
--    (service, timestamp) for fast time-range scans, with a 30-day TTL on
--    the raw data (aggregates below live longer / forever).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.raw_logs
(
    timestamp   DateTime64(3, 'UTC'),
    service     LowCardinality(String),
    level       LowCardinality(String),
    status_code UInt16,
    latency_ms  Float64,
    method      LowCardinality(String),
    endpoint    String,
    request_id  String,
    trace_id    String,
    message     String
)
ENGINE = MergeTree
PARTITION BY (service, toYYYYMMDD(timestamp))
ORDER BY (service, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- 2. Kafka ingestion: a Kafka-engine table reading JSONEachRow messages from
--    the `raw_logs` topic, plus a materialized view that streams them into
--    raw_logs. Kafka sits in front of ClickHouse so ingestion spikes are
--    buffered instead of overwhelming the analytical store (backpressure).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.raw_logs_kafka
(
    timestamp   String,
    service     String,
    level       String,
    status_code UInt16,
    latency_ms  Float64,
    method      String,
    endpoint    String,
    request_id  String,
    trace_id    String,
    message     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'raw_logs',
    kafka_group_name = 'clickhouse-raw-logs',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1048576;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.raw_logs_kafka_mv
TO observability.raw_logs
AS SELECT
    parseDateTimeBestEffort(timestamp) AS timestamp,
    service, level, status_code, latency_ms, method,
    endpoint, request_id, trace_id, message
FROM observability.raw_logs_kafka;

-- ---------------------------------------------------------------------------
-- 3. Per-service, per-minute rollup (the same shape pipeline/aggregate.py
--    produces as minute_metrics.csv for the file-based demo):
--    requests, errors, error rate %, p95 and average latency.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.mv_service_minute
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (service, minute)
AS SELECT
    toStartOfMinute(timestamp)              AS minute,
    service,
    count()                                 AS requests,
    countIf(status_code >= 500)             AS errors,
    round(100.0 * countIf(status_code >= 500) / count(), 3) AS error_rate,
    quantile(0.95)(latency_ms)              AS p95_latency_ms,
    avg(latency_ms)                         AS avg_latency_ms
FROM observability.raw_logs
GROUP BY minute, service;

-- Query helper view over the rollup (SummingMergeTree needs a final sum
-- when read directly; this view keeps dashboard queries simple).
CREATE VIEW IF NOT EXISTS observability.service_minute AS
SELECT
    minute,
    service,
    sum(requests)                                        AS requests,
    sum(errors)                                          AS errors,
    round(100.0 * sum(errors) / nullIf(sum(requests), 0), 3) AS error_rate,
    max(p95_latency_ms)                                  AS p95_latency_ms,
    avg(avg_latency_ms)                                  AS avg_latency_ms
FROM observability.mv_service_minute
GROUP BY minute, service;

-- ---------------------------------------------------------------------------
-- 4. Anomalies: written by the nightly Airflow anomaly-detection job
--    (pipeline/anomaly_detection.py). One row per flagged service-minute.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.anomalies
(
    detected_at   DateTime DEFAULT now(),
    timestamp     DateTime,
    service       LowCardinality(String),
    error_rate    Float64,
    baseline_mean Float64,
    baseline_std  Float64,
    z_score       Float64,
    requests      UInt32,
    errors        UInt32,
    p95_latency_ms Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (service, timestamp);

-- Example checks -------------------------------------------------------------
-- SELECT service, count() FROM observability.raw_logs GROUP BY service;
-- SELECT * FROM observability.service_minute WHERE service = 'checkout-service'
--   ORDER BY minute DESC LIMIT 30;
-- SELECT * FROM observability.anomalies ORDER BY z_score DESC LIMIT 15;
