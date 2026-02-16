import os
import json
import time
import boto3
import psycopg2
from datetime import datetime

sqs = boto3.client(
    "sqs",
    region_name="us-east-2",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

QUEUE_URL = os.environ["SPOT_QUEUE_URL"]

def get_db_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=5432,
    )

while True:
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=20,
        VisibilityTimeout=60,
    )

    messages = resp.get("Messages", [])

    if not messages:
        continue

    conn = get_db_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for msg in messages:
                body = json.loads(msg["Body"])
                payload = body["payload"]

                # Example insert
                cur.execute(
                    """
                    INSERT INTO spot_events_raw (payload, received_at)
                    VALUES (%s, %s)
                    """,
                    (json.dumps(payload), datetime.utcnow())
                )

                # delete ONLY after DB success
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"],
                )

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("ERROR:", e)

    finally:
        conn.close()
