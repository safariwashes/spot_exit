import os
import json
import time
import boto3
import psycopg2
from flask import Flask, jsonify

# --------------------------------------------------
# Flask app
# --------------------------------------------------
app = Flask(__name__)

# --------------------------------------------------
# AWS / SQS
# --------------------------------------------------
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
QUEUE_URL = os.environ["SPOT_EVENTS_QUEUE_URL"]

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

# --------------------------------------------------
# Postgres config (Render DB)
# --------------------------------------------------
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "port": int(os.environ.get("DB_PORT", 5432)),
}

# --------------------------------------------------
# Safe DB connector (retry on startup)
# --------------------------------------------------
def get_db_conn(retries=5, delay=2):
    for i in range(retries):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as e:
            if i == retries - 1:
                raise
            print(f"[DB] Not ready yet, retrying in {delay}s...")
            time.sleep(delay)

# --------------------------------------------------
# Health check (Render + uptime monitors)
# --------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok"})

# --------------------------------------------------
# SQS Worker Endpoint
# --------------------------------------------------
@app.route("/worker/spot", methods=["POST"])
def process_spot_events():
    print("[WORKER] Polling SQS...")

    conn = None
    processed = 0

    try:
        # ---- Fetch messages ----
        resp = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )

        messages = resp.get("Messages", [])
        if not messages:
            return jsonify({"processed": 0})

        # ---- DB connection (ONLY here) ----
        conn = get_db_conn()
        conn.autocommit = False

        with conn.cursor() as cur:
            for msg in messages:
                body = json.loads(msg["Body"])
                payload = body.get("payload", {})

                # Minimal insert (expand later safely)
                cur.execute(
                    """
                    INSERT INTO spot_events (
                        event_type,
                        payload
                    )
                    VALUES (%s, %s)
                    """,
                    (
                        payload.get("event_type"),
                        json.dumps(payload),
                    )
                )

                # Delete message only AFTER successful insert
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

                processed += 1

        conn.commit()
        print(f"[WORKER] Processed {processed} messages")

        return jsonify({"processed": processed})

    except Exception as e:
        if conn:
            conn.rollback()
        print("[ERROR]", str(e))
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()
