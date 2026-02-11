@app.route("/spot/exit", methods=["POST"])
def spot_exit():

    payload = request.get_json(silent=True) or {}

    # Extract camera_id best-effort (may be None)
    camera_id = (
        payload.get("camera_id")
        or payload.get("cameraId")
        or payload.get("data", {}).get("camera", {}).get("id")
    )
    camera_id = str(camera_id) if camera_id is not None else None

    event_ts = payload.get("timestamp")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # -------------------------------------------------
                # 🔴 ABSOLUTE FIRST STEP: LOG EVERYTHING
                # -------------------------------------------------
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

                # -------------------------------------------------
                # EVERYTHING BELOW THIS POINT IS OPTIONAL LOGIC
                # -------------------------------------------------

                # ---- Auth check (NO LONGER DROPS EVENTS) ----
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
