import os
from dataclasses import dataclass

@dataclass
class Config:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    orders_topic: str = os.getenv("ORDERS_TOPIC", "cdc.public.orders")
    payments_topic: str = os.getenv("PAYMENTS_TOPIC", "cdc.public.payments")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "cdc.dead_letter_queue")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "cdc-stream-enrichment-group")
    
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "defaultpassword")
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB", "analytics")
    
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    flush_interval_ms: int = int(os.getenv("FLUSH_INTERVAL_MS", "1000"))
    dedup_ttl_seconds: int = int(os.getenv("DEDUP_TTL_SECONDS", "3600"))      # 1 hour TTL
    enrich_ttl_seconds: int = int(os.getenv("ENRICH_TTL_SECONDS", "7200"))    # 2 hour TTL

config = Config()
