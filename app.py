import os
import json
import time
import boto3
import psycopg2
from psycopg2.extras import execute_values
from flask import Flask, jsonify

app = Flask(__name__)

# -----------------------------
# AWS / SQS
# -----------------------------
sqs = boto3.client("sqs")
QUEUE_URL = os.environ["SPOT_EVENTS_QUEUE_URL"]

# -----------------------------
# DB CONFIG
# -----------------------------
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "port": int(os.environ.get("DB_PORT", 5432)),
}

# -----------------------------
# DB Helper (SAFE)
# -----------------------------
def get_db_conn(retries=5, delay=2):
    for attempt in range(retries):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as e:
            if attempt == retries - 1:
                raise
            print(f"DB not ready, retrying in {delay}s...")
            time.sleep(delay)

# -----------------------------
# Health check
# -----------------------------
@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})

# -----------------------------
# SQS Worker Endpoint
# -----------------------------
@app.route("/worker/spot", methods=["POST"])
def process_spot_events():
    conn = None

    try:
        conn = get_db_conn()
        conn.autocommit = False

        with conn.cursor() as cur:
            resp = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
            )

            messages = resp.get("Messages", [])
            if not messages:
                return jsonify({"processed": 0})

            rows = []
            receipt_handles = []

            for m in messages:
                body = json.loads(m["Body"])
                payload = body.get("payload", {})
                rows.append((
                    payload.get("event_type"),
                    json.dumps(payload),
                ))
                receipt_handles.append(m["ReceiptHandle"])

            execute_values(
                cur,
                """
                INSERT INTO spot_events (event_type, payload)
                VALUES %s
                """,
                rows,
            )

        conn.commit()

        for rh in receipt_handles:
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=rh
            )

        return jsonify({"processed": len(rows)})

    except Exception as e:
        if conn:
            conn.rollback()
        print("ERROR:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()
