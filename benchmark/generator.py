#!/usr/bin/env python3
"""
High-Throughput OLTP Write Generator for PostgreSQL.
Generates INSERT, UPDATE, and PAYMENT events into PostgreSQL orders and payments tables.
"""

import os
import sys
import time
import uuid
import random
import argparse
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_batch

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5433")),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgrespassword"),
    "dbname": os.getenv("PG_DB", "orders_db")
}

STATUSES = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"]
PAYMENT_METHODS = ["CREDIT_CARD", "PAYPAL", "CRYPTO", "BANK_TRANSFER"]
PAYMENT_STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]  # 75% success

def get_connection(config=DB_CONFIG):
    return psycopg2.connect(**config)

def generate_orders_batch(batch_size: int, start_idx: int):
    orders = []
    payments = []
    
    for i in range(batch_size):
        order_id = f"ord-{start_idx + i}-{uuid.uuid4().hex[:8]}"
        customer_id = f"cust-{random.randint(1000, 99999)}"
        amount = round(random.uniform(10.0, 950.0), 2)
        items = random.randint(1, 10)
        
        orders.append((
            order_id,
            customer_id,
            "PENDING",
            amount,
            "USD",
            items
        ))
        
        payment_id = f"pay-{start_idx + i}-{uuid.uuid4().hex[:8]}"
        method = random.choice(PAYMENT_METHODS)
        status = random.choice(PAYMENT_STATUSES)
        
        payments.append((
            payment_id,
            order_id,
            method,
            status,
            amount,
            f"tx_{uuid.uuid4().hex[:12]}"
        ))
        
    return orders, payments

def worker_insert(batch_size: int, start_idx: int, config=DB_CONFIG):
    orders, payments = generate_orders_batch(batch_size, start_idx)
    conn = get_connection(config)
    try:
        with conn.cursor() as cur:
            # Insert orders
            execute_batch(
                cur,
                """
                INSERT INTO public.orders (order_id, customer_id, order_status, total_amount, currency, items_count)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                orders,
                page_size=batch_size
            )
            # Insert payments
            execute_batch(
                cur,
                """
                INSERT INTO public.payments (payment_id, order_id, payment_method, payment_status, amount, transaction_ref)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                payments,
                page_size=batch_size
            )
        conn.commit()
        return len(orders) + len(payments)
    finally:
        conn.close()

def run_workload(total_writes: int = 100000, batch_size: int = 1000, num_workers: int = 8, config=None):
    if config is None:
        config = DB_CONFIG

    print(f"=== Starting OLTP High-Throughput Generator ===", flush=True)
    print(f"Target Writes: {total_writes} (Orders + Payments)", flush=True)
    print(f"Batch Size: {batch_size} | Parallel Workers: {num_workers}", flush=True)
    
    # Each batch produces 2 writes (1 order + 1 payment)
    batches_needed = max(1, total_writes // (batch_size * 2))
    start_time = time.time()
    total_orders = 0
    total_payments = 0
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for b in range(batches_needed):
            start_idx = b * batch_size
            f = executor.submit(worker_insert, batch_size, start_idx, config)
            futures.append(f)
            
        for i, f in enumerate(futures):
            written = f.result()
            total_orders += written // 2
            total_payments += written // 2
            if (i + 1) % 5 == 0 or (i + 1) == len(futures):
                elapsed = time.time() - start_time
                total_written = total_orders + total_payments
                rate = total_written / elapsed if elapsed > 0 else 0
                print(f"Progress: {total_written}/{total_writes} writes ({rate:.1f} writes/sec)", flush=True)
                
    elapsed = time.time() - start_time
    total_written = total_orders + total_payments
    rate = total_written / elapsed if elapsed > 0 else 0
    print(f"=== Completed: {total_written} writes ({total_orders} orders + {total_payments} payments) in {elapsed:.2f}s ({rate:.1f} writes/sec) ===", flush=True)
    return total_orders, total_payments, total_written

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OLTP Write Generator")
    parser.add_argument("--writes", type=int, default=100000, help="Total writes to generate")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size per transaction")
    parser.add_argument("--workers", type=int, default=8, help="Number of worker threads")
    parser.add_argument("--port", type=int, default=5433, help="Postgres host port")
    args = parser.parse_args()

    cfg = DB_CONFIG.copy()
    cfg["port"] = args.port
    run_workload(args.writes, args.batch_size, args.workers, cfg)
