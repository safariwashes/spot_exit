import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from flask import Flask, request

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
SPOT_SHARED_SECRET = os.getenv("SPOT_SHARED_SECRET")

if not DATABASE_URL or not SPOT_SHARED_SECRET:
    raise RuntimeError("Missing required environment variables")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok"}, 200

@app.route("/spot/exit", methods=["POST"])
def spot_exit():
    # ---- Auth ----
    if request.headers.get("Authorization") != f"Bearer {SPOT_SHARED_SECRET}":
        return {"error": "unauthorized"}, 401

    payload = request.get_json(silent=True) or {}
    camera_id = payload.get("camera_id")
    event_ts  = payload.get("timestamp")  # optional

    if not camera_id:
        return {"error": "camera_id missing"}, 400

    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # ---- Camera resolution ----
                cur.execute("""
                    SELECT tenant_id, location_id, camera_role
                    FROM spot_camera_map
                    WHERE camera_id = %s
                      AND active = true
                """, (camera_id,))
                cam = cur.fetchone()

                if not cam or cam["camera_role"] != "exit":
                    return {"error": "invalid camera"}, 400

                tenant_id   = cam["tenant_id"]
                location_id = cam["location_id"]
                camera_role = cam["camera_role"]

                # ---- Insert activity event ----
                cur.execute("""
                    INSERT INTO spot_camera_event (
                        camera_id, tenant_id, location_id, camera_role,
                        event_ts, raw_payload, status
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,'received')
                    RETURNING id;
                """, (
                    camera_id,
                    tenant_id,
                    location_id,
                    camera_role,
                    event_ts,
                    Json(payload)
                ))

                event_id = cur.fetchone()["id"]

                # ---- FIFO tunnel update ----
                cur.execute("""
                    WITH fifo AS (
                        SELECT bill, location, created_on
                        FROM tunnel
                        WHERE tenant_id   = %s
                          AND location_id = %s
                          AND load = true
                          AND exit = false
                        ORDER BY load_time ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE tunnel
                    SET exit = true,
                        exit_time = CURRENT_TIME
                    WHERE (bill, location, created_on) IN (
                        SELECT bill, location, created_on FROM fifo
                    )
                    RETURNING bill;
                """, (tenant_id, location_id))

                row = cur.fetchone()

                # ---- Update event outcome ----
                if row:
                    cur.execute("""
                        UPDATE spot_camera_event
                        SET matched_bill = %s,
                            status = 'matched'
                        WHERE id = %s;
                    """, (row["bill"], event_id))
                else:
                    cur.execute("""
                        UPDATE spot_camera_event
                        SET status = 'no_fifo'
                        WHERE id = %s;
                    """, (event_id,))

        return {"status": "ok"}, 200

    finally:
        conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
