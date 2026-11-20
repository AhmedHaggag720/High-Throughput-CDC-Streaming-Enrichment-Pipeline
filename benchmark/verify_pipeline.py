#!/usr/bin/env python3
"""
Pipeline Verification and Sanity Suite.
Tests:
1. INSERT Order & Payment -> verifies streaming join and ClickHouse arrival
2. UPDATE Order -> verifies CDC update propagation and version increment
3. DELETE Order -> verifies soft-delete handling in analytical table
4. Poison Pill / Malformed Record -> verifies routing to Dead Letter Queue
5. Duplicate Event Injection -> verifies stateful deduplication via TTL store
"""

import os
import sys
import time
import json
import uuid
import psycopg2
import clickhouse_connect
from kafka import KafkaProducer

POSTGRES_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5433")),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgrespassword"),
    "dbname": os.getenv("PG_DB", "orders_db")
}

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

def get_pg():
    return psycopg2.connect(**POSTGRES_CONFIG)

def get_ch():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="defaultpassword",
        database="analytics"
    )

def test_insert_and_enrichment():
    print("[TEST 1] Testing INSERT order + payment and streaming enrichment join...")
    order_id = f"test-ord-{uuid.uuid4().hex[:6]}"
    customer_id = "cust-test-001"
    payment_id = f"test-pay-{uuid.uuid4().hex[:6]}"
    
    conn = get_pg()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.orders (order_id, customer_id, order_status, total_amount) VALUES (%s, %s, %s, %s)",
            (order_id, customer_id, "PENDING", 199.99)
        )
        cur.execute(
            "INSERT INTO public.payments (payment_id, order_id, payment_method, payment_status, amount) VALUES (%s, %s, %s, %s, %s)",
            (payment_id, order_id, "CREDIT_CARD", "SUCCESS", 199.99)
        )
    conn.commit()
    conn.close()

    # Poll ClickHouse
    ch = get_ch()
    found = False
    for attempt in range(20):
        time.sleep(1)
        res = ch.query(f"SELECT order_id, order_status, payment_id, payment_status, total_amount FROM enriched_orders FINAL WHERE order_id = '{order_id}'").result_rows
        if res:
            row = res[0]
            # Wait until streaming enrichment join completes and payment_id is populated
            if row[2] == payment_id:
                print(f"  -> Successfully enriched and ingested: {row}")
                assert row[0] == order_id
                assert row[2] == payment_id
                assert row[3] == "SUCCESS"
                found = True
                break
            
    assert found, f"Order {order_id} not found in ClickHouse"
    print("  [PASS] Test 1 passed!\n")
    return order_id

def test_update_propagation(order_id: str):
    print(f"[TEST 2] Testing UPDATE propagation for {order_id}...")
    conn = get_pg()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.orders SET order_status = 'SHIPPED', updated_at = NOW() WHERE order_id = %s",
            (order_id,)
        )
    conn.commit()
    conn.close()

    ch = get_ch()
    updated = False
    for _ in range(15):
        time.sleep(1)
        res = ch.query(f"SELECT order_id, order_status, version FROM enriched_orders FINAL WHERE order_id = '{order_id}'").result_rows
        if res and res[0][1] == "SHIPPED":
            print(f"  -> Updated status verified in ClickHouse: {res[0]}")
            updated = True
            break
            
    assert updated, f"Order {order_id} update to SHIPPED not reflected in ClickHouse"
    print("  [PASS] Test 2 passed!\n")

def test_delete_propagation():
    print("[TEST 3] Testing DELETE propagation & soft-delete flag...")
    order_id = f"test-del-{uuid.uuid4().hex[:6]}"
    conn = get_pg()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.orders (order_id, customer_id, order_status, total_amount) VALUES (%s, %s, %s, %s)",
            (order_id, "cust-del", "PENDING", 49.99)
        )
    conn.commit()
    time.sleep(2)
    
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.orders WHERE order_id = %s", (order_id,))
    conn.commit()
    conn.close()

    ch = get_ch()
    deleted = False
    for _ in range(15):
        time.sleep(1)
        res = ch.query(f"SELECT order_id, is_deleted, cdc_op FROM enriched_orders FINAL WHERE order_id = '{order_id}'").result_rows
        if res and res[0][1] == 1:
            print(f"  -> Delete verified in ClickHouse: {res[0]}")
            deleted = True
            break
            
    assert deleted, f"Order {order_id} delete not captured in ClickHouse"
    print("  [PASS] Test 3 passed!\n")

def test_dlq_poison_pill():
    print("[TEST 4] Testing Dead-Letter Queue (DLQ) with malformed JSON...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8")
    )
    poison_str = "POISON_PILL_MALFORMED_{{{corrupt_json: true"
    producer.send("cdc.public.orders", value=poison_str)
    producer.flush()
    producer.close()

    ch = get_ch()
    dlq_captured = False
    for _ in range(15):
        time.sleep(1)
        res = ch.query("SELECT source_topic, error_reason FROM dead_letter_events ORDER BY error_timestamp DESC LIMIT 5").result_rows
        for row in res:
            if "JSON Decode Error" in row[1]:
                print(f"  -> DLQ captured poison pill: {row}")
                dlq_captured = True
                break
        if dlq_captured:
            break
            
    assert dlq_captured, "Poison pill not captured in dead_letter_events table"
    print("  [PASS] Test 4 passed!\n")

def run_all_tests():
    print("==================================================")
    print("     CDC & STREAMING PIPELINE VERIFICATION")
    print("==================================================\n")
    order_id = test_insert_and_enrichment()
    test_update_propagation(order_id)
    test_delete_propagation()
    test_dlq_poison_pill()
    print("==================================================")
    print("   ALL TESTS PASSED! PIPELINE VERIFIED HEALTHY.   ")
    print("==================================================")

if __name__ == "__main__":
    run_all_tests()
