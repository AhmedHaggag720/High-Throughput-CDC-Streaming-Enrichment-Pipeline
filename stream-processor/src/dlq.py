import json
import logging
from typing import Optional, Dict, Any
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

class DeadLetterQueue:
    """
    Routes malformed records, corrupt payloads, or schema validation errors
    to Redpanda dead-letter topic and ClickHouse dead_letter_events table.
    """
    def __init__(self, bootstrap_servers: str, dlq_topic: str, clickhouse_client=None):
        self.dlq_topic = dlq_topic
        self.clickhouse_client = clickhouse_client
        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                acks=1
            )
            logger.info(f"Initialized Kafka DLQ Producer targeting topic '{dlq_topic}'")
        except Exception as e:
            logger.warning(f"Could not initialize Kafka DLQ producer: {e}")

    def route(
        self,
        raw_message: Any,
        source_topic: str,
        partition: int,
        offset: int,
        reason: str,
        stack_trace: str = ""
    ):
        raw_str = raw_message if isinstance(raw_message, str) else str(raw_message)
        payload = {
            "source_topic": source_topic,
            "partition": partition,
            "offset": offset,
            "raw_payload": raw_str,
            "error_reason": reason,
            "stack_trace": stack_trace
        }
        
        # 1. Forward to Kafka DLQ Topic
        if self.producer:
            try:
                self.producer.send(self.dlq_topic, value=payload)
            except Exception as e:
                logger.error(f"Failed to publish to Kafka DLQ topic: {e}")

        # 2. Forward to ClickHouse DLQ table if client available
        if self.clickhouse_client:
            try:
                row = [source_topic, partition, offset, raw_str[:4000], reason[:1000], stack_trace[:2000]]
                self.clickhouse_client.insert(
                    "dead_letter_events",
                    [row],
                    column_names=["source_topic", "partition_id", "offset_id", "raw_payload", "error_reason", "stack_trace"]
                )
            except Exception as e:
                logger.error(f"Failed to insert into ClickHouse DLQ table: {e}")

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
            except Exception:
                pass
