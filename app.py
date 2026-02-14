import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from flask import Flask, request

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

UNKNOWN_TENANT_ID = "00000000-0000-0000-0000-000000000000"
UNKNOWN_LOCATION_ID = "00000000-0000-0000-0000-000000000000"

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
    camera_id = str(camera_id) if camera_id else "UNKNOWN"
    event_ts = payload.get("timestamp")

    final_status = "received"

    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # Resolve camera mapping (safe)
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

                # ALWAYS INSERT
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
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
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
                        final_status,
                    ),
                )

                event_id = cur.fetchone()["id"]

                # Classify AFTER insert
                if not request.headers.get("Spot-Webhook-Signature") or not request.headers.get("Spot-Webhook-Meta"):
                    final_status = "unauthorized"
                elif not cam:
                    final_status = "unknown_camera"
                elif camera_role != "exit":
                    final_status = "invalid_role"

                # Update status once
                if final_status != "received":
                    cur.execute(
                        """
                        UPDATE spot_camera_event
                        SET status = %s
                        WHERE id = %s
                        """,
                        (final_status, event_id),
                    )

    finally:
        conn.close()

    return {"status": "ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
