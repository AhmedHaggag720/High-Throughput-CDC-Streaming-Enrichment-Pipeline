import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import clickhouse_connect

logger = logging.getLogger(__name__)

class ClickHouseSink:
    """
    Idempotent batch sink for ClickHouse.
    Flushes enriched records into analytics.enriched_orders with ReplacingMergeTree semantics.
    """
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = None
        self._connect()

    def _connect(self, retries=10, delay=3):
        for attempt in range(1, retries + 1):
            try:
                self.client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    database=self.database
                )
                logger.info(f"Connected to ClickHouse at {self.host}:{self.port}/{self.database}")
                return
            except Exception as e:
                logger.warning(f"ClickHouse connection attempt {attempt}/{retries} failed: {e}")
                time.sleep(delay)
        raise RuntimeError(f"Could not connect to ClickHouse at {self.host}:{self.port}")

    def _format_datetime(self, val) -> datetime:
        """Helper to convert int/float timestamp (ms or us) or ISO string to datetime."""
        if val is None:
            return datetime.utcnow()
        if isinstance(val, (int, float)):
            # If timestamp is in microseconds
            if val > 1e14:
                return datetime.utcfromtimestamp(val / 1e6)
            # If timestamp is in milliseconds
            if val > 1e11:
                return datetime.utcfromtimestamp(val / 1e3)
            # Seconds
            return datetime.utcfromtimestamp(val)
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                return datetime.utcnow()
        return datetime.utcnow()

    def flush_batch(self, batch: List[Dict[str, Any]]) -> int:
        """
        Atomically inserts a batch of enriched records into ClickHouse.
        Returns the number of rows inserted.
        """
        if not batch:
            return 0

        rows = []
        for r in batch:
            created_at = self._format_datetime(r.get("order_created_at"))
            updated_at = self._format_datetime(r.get("order_updated_at"))
            
            rows.append([
                str(r["order_id"]),
                str(r["customer_id"]),
                str(r["order_status"]),
                float(r["total_amount"]),
                str(r["currency"]),
                int(r["items_count"]),
                created_at,
                updated_at,
                str(r["payment_id"]) if r.get("payment_id") else None,
                str(r["payment_method"]) if r.get("payment_method") else None,
                str(r["payment_status"]) if r.get("payment_status") else None,
                float(r["paid_amount"]) if r.get("paid_amount") is not None else None,
                int(r["payment_latency_ms"]) if r.get("payment_latency_ms") is not None else None,
                str(r.get("cdc_op", "c")),
                int(r.get("cdc_ts_ms", 0)),
                int(r.get("is_deleted", 0)),
                int(r.get("version", int(time.time() * 1000))),
                datetime.utcnow()
            ])

        columns = [
            "order_id",
            "customer_id",
            "order_status",
            "total_amount",
            "currency",
            "items_count",
            "order_created_at",
            "order_updated_at",
            "payment_id",
            "payment_method",
            "payment_status",
            "paid_amount",
            "payment_latency_ms",
            "cdc_op",
            "cdc_ts_ms",
            "is_deleted",
            "version",
            "inserted_at"
        ]

        try:
            self.client.insert("enriched_orders", rows, column_names=columns)
            return len(rows)
        except Exception as e:
            logger.error(f"ClickHouse batch insert failed: {e}. Reconnecting...")
            self._connect()
            self.client.insert("enriched_orders", rows, column_names=columns)
            return len(rows)
