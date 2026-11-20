#!/usr/bin/env python3
"""
Master Pipeline Orchestrator.
Commands:
  python run_pipeline.py up          -> Starts Docker Compose services & registers Debezium CDC connector
  python run_pipeline.py down        -> Shuts down containers
  python run_pipeline.py verify      -> Runs integration verification suite
  python run_pipeline.py benchmark   -> Runs 100k writes lag recovery benchmark
  python run_pipeline.py status      -> Checks status of all services
"""

import sys
import time
import subprocess
import requests

def run_cmd(cmd, check=True):
    print(f">> Executing: {cmd}")
    res = subprocess.run(cmd, shell=True, text=True)
    if check and res.returncode != 0:
        sys.exit(res.returncode)
    return res

def wait_for_service(name, check_fn, max_retries=30, delay=2):
    print(f"Waiting for {name} to be ready...")
    for i in range(1, max_retries + 1):
        try:
            if check_fn():
                print(f"  [OK] {name} is ready.")
                return True
        except Exception:
            pass
        time.sleep(delay)
    print(f"  [FAILED] {name} timed out.")
    return False

def check_debezium():
    r = requests.get("http://localhost:8083/connectors", timeout=2)
    return r.status_code == 200

def check_clickhouse():
    r = requests.get("http://localhost:8123/ping", timeout=2)
    return r.status_code == 200 and r.text.strip() == "Ok."

def up():
    print("=== Starting CDC & Streaming Enrichment Infrastructure ===")
    run_cmd("docker compose up -d --build")
    
    print("\nVerifying service readiness...")
    wait_for_service("Debezium REST API", check_debezium, max_retries=30, delay=3)
    wait_for_service("ClickHouse Server", check_clickhouse, max_retries=20, delay=2)
    
    print("\nRegistering PostgreSQL Debezium CDC Connector...")
    run_cmd("python debezium/register_connector.py")
    
    print("\n=== All Services Operational! ===")
    print("Services running:")
    print("  - PostgreSQL:       localhost:5433")
    print("  - Redpanda Broker:  localhost:9092")
    print("  - Redpanda Console: http://localhost:8080")
    print("  - Debezium Connect: http://localhost:8083")
    print("  - Redis:            localhost:6379")
    print("  - ClickHouse:       http://localhost:8123")
    print("  - Stream Processor: container 'cdc-stream-processor'")

def down():
    print("=== Stopping Infrastructure ===")
    run_cmd("docker compose down")

def status():
    print("=== Container Status ===")
    run_cmd("docker compose ps")

def verify():
    print("=== Running Verification Suite ===")
    run_cmd("python benchmark/verify_pipeline.py")

def benchmark():
    print("=== Running 100k Benchmark ===")
    run_cmd("python benchmark/benchmark_lag.py")

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "up"
    if action == "up":
        up()
    elif action == "down":
        down()
    elif action == "status":
        status()
    elif action == "verify":
        verify()
    elif action == "benchmark":
        benchmark()
    else:
        print(f"Unknown command: {action}. Use up, down, status, verify, or benchmark.")
