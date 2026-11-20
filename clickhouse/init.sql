-- Initialize Analytical Database in ClickHouse
CREATE DATABASE IF NOT EXISTS analytics;

-- Analytical table with ReplacingMergeTree for idempotent upserts and out-of-order deduplication
CREATE TABLE IF NOT EXISTS analytics.enriched_orders
(
    order_id String,
    customer_id String,
    order_status LowCardinality(String),
    total_amount Decimal64(2),
    currency LowCardinality(String),
    items_count UInt32,
    order_created_at DateTime64(3),
    order_updated_at DateTime64(3),
    
    -- Enriched payment attributes
    payment_id Nullable(String),
    payment_method LowCardinality(Nullable(String)),
    payment_status LowCardinality(Nullable(String)),
    paid_amount Nullable(Decimal64(2)),
    payment_latency_ms Nullable(Int64),
    
    -- CDC and stream processing metadata
    cdc_op LowCardinality(String),             -- 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
    cdc_ts_ms Int64,                           -- Debezium event timestamp in milliseconds
    is_deleted UInt8 DEFAULT 0,                -- Soft-delete flag on CDC DELETE
    version UInt64,                            -- Monotonic version for ReplacingMergeTree (cdc_ts_ms)
    inserted_at DateTime64(3) DEFAULT now64(3) -- Analytical ingestion timestamp
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(order_created_at)
PRIMARY KEY order_id
ORDER BY (order_id, customer_id);

-- Dead letter table to capture malformed records, schema violations, or unprocessable events
CREATE TABLE IF NOT EXISTS analytics.dead_letter_events
(
    error_id UUID DEFAULT generateUUIDv4(),
    source_topic String,
    partition_id Int32,
    offset_id Int64,
    raw_payload String,
    error_reason String,
    stack_trace String,
    error_timestamp DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(error_timestamp)
ORDER BY (error_timestamp, error_id);
