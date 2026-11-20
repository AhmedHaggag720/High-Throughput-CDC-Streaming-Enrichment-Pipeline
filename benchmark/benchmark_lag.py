#!/usr/bin/env python3
"""
Lag Recovery & Throughput Benchmark Suite.
Implements Requirement 4:
1. Pauses the consumer (default 5 minutes, configurable via --pause-seconds).
2. Generates 100,000 DB writes into PostgreSQL concurrently.
3. Monitors accumulated Redpanda lag.
4. Resumes consumer and measures:
   - Consumer lag catch-up rate (events/second)
   - Memory footprint (MB)
   - End-to-end freshness latency (ms)
"""

import os
import sys
import time
import argparse
import subprocess
import json
import clickhouse_connect
from tabulate import tabulate

from generator import run_workload, DB_CONFIG

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "cdc-stream-enrichment-group")
CONTAINER_NAME = os.getenv("CONSUMER_CONTAINER", "cdc-stream-processor")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

def get_clickhouse_stats():
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username="default",
            password="defaultpassword",
            database="analytics"
        )
        row = client.query("SELECT count(), max(version), max(inserted_at) FROM enriched_orders").result_rows[0]
        return {
            "total_records": row[0],
            "max_version": row[1],
            "last_inserted": str(row[2])
        }
    except Exception as e:
        return {"total_records": 0, "error": str(e)}

def get_latency_percentiles():
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username="default",
            password="defaultpassword",
            database="analytics"
        )
        # Latency = inserted_at (ms) - cdc_ts_ms
        query = """
        SELECT
            min(toUnixTimestamp64Milli(inserted_at) - cdc_ts_ms) AS min_lat,
            quantile(0.50)(toUnixTimestamp64Milli(inserted_at) - cdc_ts_ms) AS p50_lat,
            quantile(0.95)(toUnixTimestamp64Milli(inserted_at) - cdc_ts_ms) AS p95_lat,
            quantile(0.99)(toUnixTimestamp64Milli(inserted_at) - cdc_ts_ms) AS p99_lat,
            max(toUnixTimestamp64Milli(inserted_at) - cdc_ts_ms) AS max_lat
        FROM enriched_orders
        WHERE cdc_ts_ms > 0
        """
        rows = client.query(query).result_rows[0]
        return {
            "min_ms": rows[0],
            "p50_ms": rows[1],
            "p95_ms": rows[2],
            "p99_ms": rows[3],
            "max_ms": rows[4]
        }
    except Exception as e:
        return {"error": str(e)}

def get_container_memory_mb(container_name: str) -> float:
    try:
        cmd = ["docker", "stats", container_name, "--no-stream", "--format", "{{.MemUsage}}"]
        output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        # Format is typically "45.2MiB / 7.669GiB"
        mem_part = output.split("/")[0].strip()
        if "GiB" in mem_part:
            return float(mem_part.replace("GiB", "").strip()) * 1024
        elif "MiB" in mem_part:
            return float(mem_part.replace("MiB", "").strip())
        elif "kB" in mem_part:
            return float(mem_part.replace("kB", "").strip()) / 1024
    except Exception:
        pass
    return 0.0

def pause_consumer(container_name: str):
    print(f"\n[ACTION] Pausing stream processor container '{container_name}'...")
    subprocess.run(["docker", "pause", container_name], check=True)
    print("Stream processor successfully PAUSED.")

def resume_consumer(container_name: str):
    print(f"\n[ACTION] Resuming stream processor container '{container_name}'...")
    subprocess.run(["docker", "unpause", container_name], check=True)
    print("Stream processor successfully RESUMED.")

def get_kafka_lag():
    """Calculates total consumer lag across orders and payments topics using rpk."""
    try:
        cmd = ["docker", "exec", "cdc-redpanda", "rpk", "group", "describe", CONSUMER_GROUP]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            for line in res.stdout.splitlines():
                if "TOTAL-LAG" in line:
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        return int(parts[1])
    except Exception:
        pass
    return -1

def run_benchmark(pause_seconds: int = 300, total_writes: int = 100000, batch_size: int = 1000, workers: int = 8):
    print("=" * 70, flush=True)
    print("  HIGH-THROUGHPUT CDC & STREAMING LAG RECOVERY BENCHMARK", flush=True)
    print("=" * 70, flush=True)
    
    # 1. Initial State
    initial_stats = get_clickhouse_stats()
    initial_count = initial_stats.get("total_records", 0)
    print(f"Initial ClickHouse Records: {initial_count}", flush=True)

    # 2. Pause the Consumer
    pause_consumer(CONTAINER_NAME)
    
    # 3. Generate DB Writes
    print(f"\n[PHASE 1] Generating {total_writes} PostgreSQL writes while consumer is paused...", flush=True)
    t_gen_start = time.time()
    total_orders, total_payments, actual_writes = run_workload(total_writes=total_writes, batch_size=batch_size, num_workers=workers)
    t_gen_elapsed = time.time() - t_gen_start
    print(f"Generated {actual_writes} writes ({total_orders} orders) in {t_gen_elapsed:.2f}s", flush=True)

    # 4. Wait out the remaining pause duration (if any)
    elapsed_since_pause = time.time() - t_gen_start
    remaining_pause = max(0, pause_seconds - elapsed_since_pause)
    if remaining_pause > 0:
        print(f"\n[PHASE 2] Holding consumer in paused state for remaining {remaining_pause:.1f}s...", flush=True)
        while remaining_pause > 0:
            step = min(10, remaining_pause)
            time.sleep(step)
            remaining_pause -= step
            print(f"  Holding pause... {remaining_pause:.0f}s remaining.", flush=True)

    # 5. Check Accumulated Lag in Redpanda
    accumulated_lag = get_kafka_lag()
    print(f"\nAccumulated Consumer Lag before resume: {accumulated_lag} events", flush=True)

    # 6. Resume Consumer and Measure Catch-Up
    print("\n[PHASE 3] Resuming consumer & measuring recovery throughput...", flush=True)
    t_resume = time.time()
    resume_consumer(CONTAINER_NAME)
    
    # Poll until ClickHouse captures all writes or lag drops to zero
    expected_final_count = initial_count + total_orders
    peak_memory_mb = 0.0
    catchup_done = False
    
    while not catchup_done:
        time.sleep(1.0)
        curr_stats = get_clickhouse_stats()
        curr_count = curr_stats.get("total_records", 0)
        curr_lag = get_kafka_lag()
        mem_mb = get_container_memory_mb(CONTAINER_NAME)
        if mem_mb > peak_memory_mb:
            peak_memory_mb = mem_mb

        elapsed = time.time() - t_resume
        processed_since_resume = curr_count - initial_count
        rate = processed_since_resume / elapsed if elapsed > 0 else 0
        
        print(
            f"  [T+{elapsed:4.1f}s] ClickHouse Rows: {curr_count} (+{processed_since_resume}) | "
            f"Lag: {curr_lag} | Rate: {rate:6.1f} rows/s | Mem: {mem_mb:.1f} MB",
            flush=True
        )

        # Check completion condition: lag cleared or all expected records captured
        if curr_count >= expected_final_count or (curr_lag == 0 and processed_since_resume > 0 and elapsed > 5):
            catchup_done = True

    t_total_recovery = time.time() - t_resume
    total_processed = curr_count - initial_count
    effective_rate = total_processed / t_total_recovery if t_total_recovery > 0 else 0

    # 7. Collect Freshness Latency Percentiles
    latency = get_latency_percentiles()

    # 8. Print Benchmark Summary Table
    print("\n" + "=" * 70)
    print("                    BENCHMARK RESULTS REPORT")
    print("=" * 70)

    report_table = [
        ["Total Database Writes Generated", f"{total_writes:,}"],
        ["Consumer Pause Duration", f"{pause_seconds} seconds ({pause_seconds / 60:.1f} min)"],
        ["Enriched Entity Records Captured", f"{total_processed:,}"],
        ["Catch-up Duration", f"{t_total_recovery:.2f} seconds"],
        ["Consumer Catch-up Rate", f"{effective_rate:,.1f} events/sec"],
        ["Peak Consumer Memory Footprint", f"{peak_memory_mb:.2f} MB"],
        ["Min Freshness Latency", f"{latency.get('min_ms', 'N/A')} ms"],
        ["P50 (Median) Freshness Latency", f"{latency.get('p50_ms', 'N/A')} ms"],
        ["P95 Freshness Latency", f"{latency.get('p95_ms', 'N/A')} ms"],
        ["P99 Freshness Latency", f"{latency.get('p99_ms', 'N/A')} ms"],
        ["Max Freshness Latency", f"{latency.get('max_ms', 'N/A')} ms"],
    ]

    print(tabulate(report_table, headers=["Metric", "Value"], tablefmt="fancy_grid"))
    print("=" * 70 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lag Recovery & Catch-up Benchmark")
    parser.add_argument("--pause-seconds", type=int, default=300, help="Consumer pause time in seconds (default: 300 = 5 min)")
    parser.add_argument("--writes", type=int, default=100000, help="Number of writes (default: 100000)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size per transaction")
    parser.add_argument("--workers", type=int, default=8, help="Parallel worker threads")
    args = parser.parse_args()

    run_benchmark(
        pause_seconds=args.pause_seconds,
        total_writes=args.writes,
        batch_size=args.batch_size,
        workers=args.workers
    )
