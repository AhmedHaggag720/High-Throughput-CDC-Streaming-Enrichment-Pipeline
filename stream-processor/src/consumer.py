import json
import time
import logging
import traceback
from typing import Dict, Any, List
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from .config import Config
from .deduplicator import Deduplicator
from .enricher import StreamEnricher
from .dlq import DeadLetterQueue
from .sink_clickhouse import ClickHouseSink

logger = logging.getLogger(__name__)

class StreamingProcessor:
    """
    High-Throughput CDC Stream Processor:
    - Consumes orders and payments topics
    - Manual offset commits
    - Stateful deduplication with TTL
    - Streaming join and latency enrichment
    - Dead-letter queue routing
    - Idempotent ClickHouse flush
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.running = False
        
        # 1. State Store / Deduplicator
        self.dedup = Deduplicator(
            host=cfg.redis_host,
            port=cfg.redis_port,
            db=cfg.redis_db,
            ttl_seconds=cfg.dedup_ttl_seconds
        )
        
        # 2. Stateful Stream Enricher
        self.enricher = StreamEnricher(
            host=cfg.redis_host,
            port=cfg.redis_port,
            db=cfg.redis_db + 1,
            ttl_seconds=cfg.enrich_ttl_seconds
        )
        
        # 3. Analytical Sink
        self.sink = ClickHouseSink(
            host=cfg.clickhouse_host,
            port=cfg.clickhouse_port,
            user=cfg.clickhouse_user,
            password=cfg.clickhouse_password,
            database=cfg.clickhouse_db
        )
        
        # 4. Dead Letter Queue
        self.dlq = DeadLetterQueue(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            dlq_topic=cfg.dlq_topic,
            clickhouse_client=self.sink.client
        )
        
        # 5. Kafka Consumer with manual offset commits
        self.consumer = None
        self._init_consumer()
        
        # Metrics and buffer
        self.buffer: List[Dict[str, Any]] = []
        self.offsets_to_commit: Dict[TopicPartition, OffsetAndMetadata] = {}
        self.last_flush_time = time.time()
        self.total_processed = 0
        self.total_deduped = 0
        self.total_flushed = 0
        self.total_dlq = 0

    def _init_consumer(self, retries=15, delay=3):
        topics = [self.cfg.orders_topic, self.cfg.payments_topic]
        for attempt in range(1, retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=self.cfg.kafka_bootstrap_servers,
                    group_id=self.cfg.consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    max_poll_records=self.cfg.batch_size,
                    value_deserializer=None,  # raw bytes, parsed manually for DLQ safety
                    key_deserializer=None,
                    consumer_timeout_ms=500
                )
                logger.info(f"Connected to Kafka/Redpanda consumer group '{self.cfg.consumer_group}' on {topics}")
                return
            except Exception as e:
                logger.warning(f"Kafka consumer connection attempt {attempt}/{retries} failed: {e}")
                time.sleep(delay)
        raise RuntimeError("Failed to connect to Kafka/Redpanda broker")

    def run(self):
        self.running = True
        logger.info("Stream Processor started processing events...")
        
        try:
            while self.running:
                # Poll batch
                records_dict = self.consumer.poll(timeout_ms=500)
                
                if records_dict:
                    for tp, records in records_dict.items():
                        for record in records:
                            self._process_single_record(record, tp)
                
                # Check flush triggers (size-based or time-based)
                time_since_flush = (time.time() - self.last_flush_time) * 1000
                if len(self.buffer) >= self.cfg.batch_size or (self.buffer and time_since_flush >= self.cfg.flush_interval_ms):
                    self.flush()

        except Exception as e:
            logger.error(f"Fatal error in consumer run loop: {e}\n{traceback.format_exc()}")
        finally:
            self.shutdown()

    def _process_single_record(self, record, tp: TopicPartition):
        self.total_processed += 1
        raw_val = record.value
        
        # 1. Parse JSON safely
        try:
            if raw_val is None:
                # Tombstone record on delete if enabled
                return
            val_str = raw_val.decode("utf-8")
            event_obj = json.loads(val_str)
        except Exception as e:
            self.total_dlq += 1
            self.dlq.route(
                raw_message=raw_val,
                source_topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                reason=f"JSON Decode Error: {e}",
                stack_trace=traceback.format_exc()
            )
            # Track offset so we don't stall on poison pill
            self.offsets_to_commit[tp] = OffsetAndMetadata(record.offset + 1, None)
            return

        # 2. Debezium payload extraction
        # Supports both unflattened { "before": ..., "after": ..., "op": ... }
        # and flattened or envelope structures
        payload = event_obj.get("payload", event_obj) if isinstance(event_obj, dict) else {}
        if not isinstance(payload, dict):
            self.total_dlq += 1
            self.dlq.route(
                raw_message=val_str,
                source_topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                reason="Malformed Payload: payload is not a valid JSON object"
            )
            self.offsets_to_commit[tp] = OffsetAndMetadata(record.offset + 1, None)
            return

        op = payload.get("op", "c")
        cdc_ts_ms = payload.get("ts_ms", int(time.time() * 1000))
        after_data = payload.get("after")
        before_data = payload.get("before")
        row_data = after_data if after_data is not None else before_data

        if not row_data or not isinstance(row_data, dict):
            # Missing row data
            self.total_dlq += 1
            self.dlq.route(
                raw_message=val_str,
                source_topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                reason=f"Schema Violation: Missing 'after' or 'before' row data for op '{op}'"
            )
            self.offsets_to_commit[tp] = OffsetAndMetadata(record.offset + 1, None)
            return

        # 3. Stateful Deduplication
        # Unique Event ID based on entity ID + CDC timestamp + op
        is_order = "orders" in record.topic
        entity_id = row_data.get("order_id") if is_order else row_data.get("payment_id")
        
        if not entity_id:
            self.total_dlq += 1
            self.dlq.route(
                raw_message=val_str,
                source_topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                reason="Schema Violation: Missing primary entity ID (order_id or payment_id)"
            )
            self.offsets_to_commit[tp] = OffsetAndMetadata(record.offset + 1, None)
            return

        dedup_key = f"{record.topic}:{entity_id}:{cdc_ts_ms}:{op}"
        if self.dedup.is_duplicate(dedup_key):
            self.total_deduped += 1
            # Record offset for commit
            self.offsets_to_commit[tp] = OffsetAndMetadata(record.offset + 1, None)
            return

        # 4. Stream Enrichment & Join
        try:
            if is_order:
                enriched_order = self.enricher.process_order_event(row_data, op, cdc_ts_ms)
                if enriched_order:
                    self.buffer.append(enriched_order)
            else:
                enriched_order = self.enricher.process_payment_event(row_data, op, cdc_ts_ms)
                if enriched_order:
                    self.buffer.append(enriched_order)
        except Exception as e:
            self.total_dlq += 1
            self.dlq.route(
                raw_message=val_str,
                source_topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                reason=f"Enrichment Error: {e}",
                stack_trace=traceback.format_exc()
            )

        # Track offset to commit on batch flush
        self.offsets_to_commit[tp] = OffsetAndMetadata(record.offset + 1, None)

    def flush(self):
        """
        Atomically flushes buffered records to ClickHouse and commits Kafka offsets.
        """
        if not self.buffer and not self.offsets_to_commit:
            return

        if self.buffer:
            flushed_count = self.sink.flush_batch(self.buffer)
            self.total_flushed += flushed_count
            logger.info(
                f"Flushed {flushed_count} enriched records to ClickHouse. "
                f"(Total Flushed: {self.total_flushed}, Deduped: {self.total_deduped}, DLQ: {self.total_dlq})"
            )
            self.buffer.clear()

        # Commit offsets to Kafka ONLY after ClickHouse confirms write
        if self.offsets_to_commit:
            try:
                self.consumer.commit(offsets=self.offsets_to_commit)
                self.offsets_to_commit.clear()
            except Exception as e:
                logger.error(f"Failed to commit Kafka offsets: {e}")

        self.last_flush_time = time.time()

    def shutdown(self):
        logger.info("Shutting down Stream Processor...")
        self.running = False
        try:
            self.flush()
        except Exception as e:
            logger.error(f"Error during final flush: {e}")
            
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
        self.dlq.close()
        logger.info("Stream Processor shutdown completed.")
