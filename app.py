import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from flask import Flask, request

# -------------------------------------------------
# App setup
# -------------------------------------------------
app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# -------------------------------------------------
# DB helper
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL)

# -------------------------------------------------
# Health check
# -------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok"}, 200

# -------------------------------------------------
# Spot AI Exit Webhook
# -------------------------------------------------
@app.route("/spot/exit", methods=["POST"])
def spot_exit():
    # ---- Spot AI webhook auth (Option 1) ----
    spot_sig  = request.headers.get("Spot-Webhook-Signature")
    spot_meta = request.headers.get("Spot-Webhook-Meta")

    if not spot_sig or not spot_meta:
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

                # ---- Resolve camera → tenant / location / role ----
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

                # ---- Insert camera activity event ----
                cur.execute("""
                    INSERT INTO spot_camera_event (
                        camera_id,
                        tenant_id,
                        location_id,
                        camera_role,
                        event_ts,
                        raw_payload,
                        status
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

                # ---- FIFO tunnel exit update ----
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
                    SET ex
