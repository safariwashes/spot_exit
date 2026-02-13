import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from flask import Flask, request

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# Sentinel values (must exist in DB)
UNKNOWN_TENANT_ID = "00000000-0000-0000-0000-000000000000"
UNKNOWN_LOCATION_ID = "00000000-0000-0000-0000-000000000000"
UNKNOWN_ROLE = "unknown"

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok"}, 200

@app.route("/spot/exit", methods=["POST"])
def spot_exit():

    payload = request.get_json(silent=True) or {}

    camera_id = (
        payload.get("camera_id")
        or payload.get("cameraId")
        or payload.get("data", {}).get("camera", {}).get("id")
    )
    camera_id = str(camera_id) if camera_id is not None else "UNKNOWN"
    event_ts = payload.get("timestamp")

    conn = get_conn()
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # Resolve camera mapping (never returns)
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

            tenant_id = cam["tenant_id"] if cam else UNKNOWN_TENANT_ID
            location_id = cam["location_id"] if cam else UNKNOWN_LOCATION_ID
            camera_role = cam["camera_role"] if cam else "exit"

            # ALWAYS INSERT — must satisfy NOT NULL + CHECK
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
                    Json({
                        "headers": dict(request.headers),
                        "payload": payload
                    }),
                ),
            )

            event_id = cur.fetchone()["id"]

            # Classify — NEVER introduce new statuses
            if not request.headers.get("Spot-Webhook-Signature") or not request.headers.get("Spot-Webhook-Meta"):
                cur.execute(
                    """
                    UPDATE spot_camera_event
                    SET status = 'unauthorized'
                    WHERE id = %s
                    """,
                    (event_id,),
                )
                return {"status": "logged"}, 200

            if not cam:
                cur.execute(
                    """
                    UPDATE spot_camera_event
                    SET status = 'unknown_camera'
                    WHERE id = %s
                    """,
                    (event_id,),
                )
                return {"status": "logged"}, 200

            if camera_role != "exit":
                cur.execute(
                    """
                    UPDATE spot_camera_event
                    SET status = 'invalid_role'
                    WHERE id = %s
                    """,
                    (event_id,),
                )
                return {"status": "logged"}, 200

            # EXIT role accepted — leave status as 'received'
            # Tunnel logic will update it later

    return {"status": "ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
