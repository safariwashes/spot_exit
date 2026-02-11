import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from flask import Flask, request

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

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
    camera_id = str(camera_id) if camera_id is not None else None
    event_ts = payload.get("timestamp")

    conn = get_conn()
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # ALWAYS log first — no conditions, no auth, no validation
            cur.execute(
                """
                INSERT INTO spot_camera_event (
                    camera_id,
                    event_ts,
                    raw_payload,
                    status
                )
                VALUES (%s, %s, %s, 'received')
                RETURNING id
                """,
                (
                    camera_id,
                    event_ts,
                    Json({
                        "headers": dict(request.headers),
                        "payload": payload
                    }),
                ),
            )

            event_id = cur.fetchone()["id"]

            # Optional auth classification (does NOT drop rows)
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

    return {"status": "ok"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
