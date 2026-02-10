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
# Spot AI Exit / Entry Webhook
# -------------------------------------------------
@app.route("/spot/exit", methods=["POST"])
def spot_exit():

    # ---- Spot webhook auth (presence check only) ----
    if not request.headers.get("Spot-Webhook-Signature") or not request.headers.get("Spot-Webhook-Meta"):
        return {"error": "unauthorized"}, 401

    payload = request.get_json(silent=True) or {}

    # ---- Extract camera_id from real Spot payload shapes ----
    camera_id = (
        payload.get("camera_id")
        or payload.get("cameraId")
        or payload.get("data", {}).get("camera", {}).get("id")
    )

    if camera_id is None:
        return {"error": "camera_id missing"}, 400

    camera_id = str(camera_id)
    event_ts = payload.get("timestamp")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # -------------------------------------------------
                # Camera resolution
                # -------------------------------------------------
                cur.execute(
                    """
                    SELECT tenant_id, location_id, camera_role
                    FROM spot_camera_map
                    WHERE camera_id = %s
                      AND active = true
                    """,
                    (camera_id,),
                )
                cam = cur.fetchone()

                tenant_id = cam["tenant_id"] if cam else None
                location_id = cam["location_id"] if cam else None
                camera_role = cam["camera_role"] if cam else None

                # -------------------------------------------------
                # ALWAYS insert audit row FIRST (non-negotiable)
                # -------------------------------------------------
                cur.execute(
                    """
                    INSERT INTO spot_camera_event (
                        camera_id,
                        tenant_id,
                        location_id,
                        camera_role,
                        event_ts,
                        raw_payload,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'received')
                    RETURNING id
                    """,
                    (
                        camera_id,
                        tenant_id,
                        location_id,
                        camera_role,
                        event_ts,
                        Json(payload),
                    ),
                )

                event_id = cur.fetchone()["id"]

                # -------------------------------------------------
                # Camera validation
                # -------------------------------------------------
                if not cam:
                    cur.execute(
                        """
                        UPDATE spot_camera_event
                        SET status = 'unknown_camera'
                        WHERE id = %s
                        """,
                        (event_id,),
                    )
                    return {"status": "ignored"}, 200

                if camera_role != "exit":
                    cur.execute(
                        """
                        UPDATE spot_camera_event
                        SET status = 'invalid_role'
                        WHERE id = %s
                        """,
                        (event_id,),
                    )
                    return {"status": "ignored"}, 200

                # -------------------------------------------------
                # FIFO tunnel exit attempt (best-effort)
                # -------------------------------------------------
                cur.execute(
                    """
                    WITH fifo AS (
                        SELECT bill, location, created_on
                        FROM tunnel
                        WHERE tenant_id = %s
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
                    RETURNING bill
                    """,
                    (tenant_id, location_id),
                )

                row = cur.fetchone()

                # -------------------------------------------------
                # Diagnose failure vs success
                # -------------------------------------------------
                if row:
                    cur.execute(
                        """
                        UPDATE spot_camera_event
                        SET matched_bill = %s,
                            status = 'matched'
                        WHERE id = %s
                        """,
                        (row["bill"], event_id),
                    )
                else:
                    # Diagnose WHY nothing matched
                    cur.execute(
                        """
                        SELECT load, exit
                        FROM tunnel
                        WHERE tenant_id = %s
                          AND location_id = %s
                        ORDER BY load_time ASC
                        LIMIT 1
                        """,
                        (tenant_id, location_id),
                    )
                    state = cur.fetchone()

                    if not state:
                        status = "no_tunnel_rows"
                    elif not state["load"]:
                        status = "load_not_ready"
                    elif state["exit"]:
                        status = "exit_already_true"
                    else:
                        status = "no_fifo"

                    cur.execute(
                        """
                        UPDATE spot_camera_event
                        SET status = %s
                        WHERE id = %s
                        """,
                        (status, event_id),
                    )

        return {"status": "ok"}, 200

    finally:
        conn.close()

# -------------------------------------------------
# Local dev
# -------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
