import time
import json
import logging
from typing import Dict, Any, Optional, Tuple
import redis

logger = logging.getLogger(__name__)

class StreamEnricher:
    """
    Stateful stream enrichment buffer.
    Maintains active orders and payments state with TTL to join out-of-order streaming events.
    """
    def __init__(self, host: str, port: int, db: int = 1, ttl_seconds: int = 7200):
        self.ttl_seconds = ttl_seconds
        self.redis_client: Optional[redis.Redis] = None
        self._order_state: Dict[str, Dict[str, Any]] = {}
        self._payment_state: Dict[str, Dict[str, Any]] = {}
        
        try:
            r = redis.Redis(host=host, port=port, db=db, socket_timeout=2, decode_responses=True)
            r.ping()
            self.redis_client = r
            logger.info(f"Connected to Redis state store for enrichment buffer at {host}:{port}/db={db}")
        except Exception as e:
            logger.warning(f"Could not connect to Redis for enrichment ({e}). Using in-memory enrichment state.")
            self.redis_client = None

    def process_order_event(self, order_data: Dict[str, Any], cdc_op: str, cdc_ts_ms: int) -> Dict[str, Any]:
        """
        Enriches an order event with payment state if available.
        """
        order_id = str(order_data.get("order_id"))
        
        # Check if matching payment arrived previously out-of-order
        payment = self._get_payment(order_id)
        
        # Save order into active state
        self._set_order(order_id, order_data)
        
        payment_latency_ms = None
        paid_amount = None
        payment_id = None
        payment_method = None
        payment_status = "UNPAID"
        
        if payment:
            payment_id = payment.get("payment_id")
            payment_method = payment.get("payment_method")
            payment_status = payment.get("payment_status", "PENDING")
            paid_amount = payment.get("amount")
            # Calculate payment latency if order_created_at and payment timestamps exist
            try:
                order_created = order_data.get("created_at")
                paid_at = payment.get("created_at")
                if order_created and paid_at:
                    # In milliseconds
                    payment_latency_ms = int(paid_at - order_created) if isinstance(paid_at, (int, float)) else 0
            except Exception:
                payment_latency_ms = 0

        # Construct enriched row for ClickHouse
        enriched = {
            "order_id": order_id,
            "customer_id": str(order_data.get("customer_id", "")),
            "order_status": str(order_data.get("order_status", "PENDING")),
            "total_amount": float(order_data.get("total_amount", 0.0)),
            "currency": str(order_data.get("currency", "USD")),
            "items_count": int(order_data.get("items_count", 1)),
            "order_created_at": order_data.get("created_at"),
            "order_updated_at": order_data.get("updated_at"),
            "payment_id": payment_id,
            "payment_method": payment_method,
            "payment_status": payment_status,
            "paid_amount": paid_amount,
            "payment_latency_ms": payment_latency_ms,
            "cdc_op": cdc_op,
            "cdc_ts_ms": cdc_ts_ms,
            "is_deleted": 1 if cdc_op == "d" else 0,
            "version": int(cdc_ts_ms) if cdc_ts_ms else int(time.time() * 1000)
        }
        return enriched

    def process_payment_event(self, payment_data: Dict[str, Any], cdc_op: str, cdc_ts_ms: int) -> Optional[Dict[str, Any]]:
        """
        Processes payment event. If order is already in state, emits an enriched order update.
        If order is not yet in state, buffers payment with TTL for future order arrival.
        """
        order_id = str(payment_data.get("order_id"))
        self._set_payment(order_id, payment_data)
        
        order = self._get_order(order_id)
        if not order:
            # Out-of-order: payment arrived before order CDC event. Buffered!
            logger.debug(f"Payment {payment_data.get('payment_id')} arrived before order {order_id}. Buffered in state.")
            return None
            
        # Order exists: emit updated enriched order row with payment details
        payment_latency_ms = None
        try:
            order_created = order.get("created_at")
            paid_at = payment_data.get("created_at")
            if order_created and paid_at and isinstance(order_created, (int, float)) and isinstance(paid_at, (int, float)):
                payment_latency_ms = int(paid_at - order_created)
        except Exception:
            payment_latency_ms = 0

        enriched = {
            "order_id": order_id,
            "customer_id": str(order.get("customer_id", "")),
            "order_status": str(order.get("order_status", "PENDING")),
            "total_amount": float(order.get("total_amount", 0.0)),
            "currency": str(order.get("currency", "USD")),
            "items_count": int(order.get("items_count", 1)),
            "order_created_at": order.get("created_at"),
            "order_updated_at": order.get("updated_at"),
            "payment_id": str(payment_data.get("payment_id", "")),
            "payment_method": str(payment_data.get("payment_method", "UNKNOWN")),
            "payment_status": str(payment_data.get("payment_status", "SUCCESS")),
            "paid_amount": float(payment_data.get("amount", 0.0)),
            "payment_latency_ms": payment_latency_ms,
            "cdc_op": cdc_op,
            "cdc_ts_ms": cdc_ts_ms,
            "is_deleted": 0,
            "version": int(cdc_ts_ms) if cdc_ts_ms else int(time.time() * 1000)
        }
        return enriched

    def _get_payment(self, order_id: str) -> Optional[Dict[str, Any]]:
        if self.redis_client:
            try:
                raw = self.redis_client.get(f"pay:{order_id}")
                return json.loads(raw) if raw else None
            except Exception as e:
                logger.warning(f"Redis get payment error: {e}")
        return self._payment_state.get(order_id)

    def _set_payment(self, order_id: str, data: Dict[str, Any]):
        if self.redis_client:
            try:
                self.redis_client.set(f"pay:{order_id}", json.dumps(data), ex=self.ttl_seconds)
                return
            except Exception as e:
                logger.warning(f"Redis set payment error: {e}")
        self._payment_state[order_id] = data

    def _get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        if self.redis_client:
            try:
                raw = self.redis_client.get(f"ord:{order_id}")
                return json.loads(raw) if raw else None
            except Exception as e:
                logger.warning(f"Redis get order error: {e}")
        return self._order_state.get(order_id)

    def _set_order(self, order_id: str, data: Dict[str, Any]):
        if self.redis_client:
            try:
                self.redis_client.set(f"ord:{order_id}", json.dumps(data), ex=self.ttl_seconds)
                return
            except Exception as e:
                logger.warning(f"Redis set order error: {e}")
        self._order_state[order_id] = data
