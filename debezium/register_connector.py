#!/usr/bin/env python3
"""
Registers the PostgreSQL CDC connector with Debezium Connect REST API.
Can be executed from host (port 8083) or container.
"""

import sys
import time
import requests
import json

DEBEZIUM_URL = "http://localhost:8083"
CONNECTOR_NAME = "postgres-cdc-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgrespassword",
        "database.dbname": "orders_db",
        "topic.prefix": "cdc",
        "table.include.list": "public.orders,public.payments",
        "publication.name": "cdc_publication",
        "publication.autocreate.mode": "filtered",
        "slot.name": "debezium_cdc_slot",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}

def wait_for_debezium(url, max_attempts=30, delay=2):
    print(f"Waiting for Debezium REST API at {url}...")
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(f"{url}/connectors", timeout=5)
            if resp.status_code == 200:
                print(f"Debezium is ready (attempt {attempt}).")
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(delay)
    return False

def register_connector(url):
    check_url = f"{url}/connectors/{CONNECTOR_NAME}"
    resp = requests.get(check_url)
    
    if resp.status_code == 200:
        print(f"Connector '{CONNECTOR_NAME}' already registered. Checking status...")
    else:
        print(f"Registering connector '{CONNECTOR_NAME}'...")
        post_url = f"{url}/connectors"
        resp = requests.post(
            post_url,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=json.dumps(CONNECTOR_CONFIG)
        )
        if resp.status_code not in (200, 201):
            print(f"Failed to register connector: {resp.status_code} - {resp.text}")
            return False
        print("Connector registration submitted successfully.")

    # Check status
    time.sleep(3)
    status_url = f"{url}/connectors/{CONNECTOR_NAME}/status"
    status_resp = requests.get(status_url)
    if status_resp.status_code == 200:
        status_data = status_resp.json()
        print(f"Connector Status: {json.dumps(status_data, indent=2)}")
        connector_state = status_data.get("connector", {}).get("state")
        tasks = status_data.get("tasks", [])
        all_running = connector_state == "RUNNING" and all(t.get("state") == "RUNNING" for t in tasks)
        if all_running:
            print("Connector and all tasks are RUNNING healthy!")
            return True
        else:
            print("Connector registered, waiting for tasks to become RUNNING...")
            return True
    else:
        print(f"Could not fetch status: {status_resp.status_code} - {status_resp.text}")
        return False

if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else DEBEZIUM_URL
    if not wait_for_debezium(url):
        print("Error: Debezium did not become ready in time.")
        sys.exit(1)
    success = register_connector(url)
    sys.exit(0 if success else 1)
