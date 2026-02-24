# loader_combined_worker.py
# Combined Loader Start + Loader End Worker (Render-safe)

import os
import time
import logging
from datetime import timedelta

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor


# =========================================================
# Logging
# =========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("loader_combined_worker")


# =========================================================
# Config
# =========================================================
DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

HEARTBEAT_SOURCE = "loader_combined_worker"


def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


# =========================================================
# SQL — START PHASE
# =========================================================

SELECT_START_ROWS = """
SELECT
    id,
    tenant_id,
    location_id,
    bill,
    pos_receipt,
    log_ts,
    (log_ts::date) AS created_on
FROM loader_controller_log
WHERE processed = false
  AND pos_receipt IS NOT NULL
ORDER BY log_ts
FOR UPDATE SKIP LOCKED
LIMIT %s;
"""

UPDATE_VEHICLE_WASH = """
UPDATE vehicle
SET status = 3,
    status_desc = 'Wash',
    bill_wshfy = %s
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s;
"""

UPDATE_SUPER_WASH = """
UPDATE super
SET status = 3,
    status_desc = 'Wash'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s;
"""

UPSERT_TUNNEL_LOAD = """
INSERT INTO tunnel (
    tenant_id, location_id, bill, created_on,
    load, load_time
)
VALUES (%s, %s, %s, %s, true, %s)
ON CONFLICT (bill, location, created_on)
DO UPDATE SET
    load = true,
    load_time = EXCLUDED.load_time;
"""

MARK_START_PROCESSED = """
UPDATE loader_controller_log
SET processed = true
WHERE id = %s;
"""


# =========================================================
# SQL — END PHASE
# =========================================================

SELECT_EXIT_ROWS = """
SELECT
    t.bill,
    t.location,
    t.created_on,
    t.tenant_id,
    t.location_id,
    t.load_time,
    l.length_sec
FROM tunnel t
JOIN tunnel_length l
  ON l.tenant_id = t.tenant_id
 AND l.location_id = t.location_id
WHERE t.exit = false
  AND t.load_time IS NOT NULL
FOR UPDATE SKIP LOCKED
LIMIT %s;
"""

UPDATE_VEHICLE_DRY = """
UPDATE vehicle
SET status = 4,
    status_desc = 'Dry & Shine'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s;
"""

UPDATE_SUPER_DRY = """
UPDATE super
SET status = 4,
    status_desc = 'Dry & Shine'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s;
"""

UPDATE_TUNNEL_EXIT = """
UPDATE tunnel
SET exit = true,
    exit_time = NOW()
WHERE bill = %s
  AND location = %s
  AND created_on = %s;
"""


# =========================================================
# SQL — HEARTBEAT
# =========================================================

INSERT_HEARTBEAT = """
INSERT INTO heartbeat (source, tenant_id, location_id)
VALUES (%s, %s, %s);
"""


# =========================================================
# Worker Logic
# =========================================================

def run_once(conn):
    processed = 0
    touched = set()
    cur = None

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # -------------------------
        # START PHASE
        # -------------------------
        cur.execute(SELECT_START_ROWS, (BATCH_SIZE,))
        start_rows = cur.fetchall()

        for r in start_rows:
            log.info("START bill=%s", r["bill"])

            cur.execute(
                UPDATE_VEHICLE_WASH,
                (r["pos_receipt"], r["tenant_id"], r["location_id"], r["created_on"], r["bill"]),
            )

            cur.execute(
                UPDATE_SUPER_WASH,
                (r["tenant_id"], r["location_id"], r["created_on"], r["bill"]),
            )

            cur.execute(
                UPSERT_TUNNEL_LOAD,
                (r["tenant_id"], r["location_id"], r["bill"], r["created_on"], r["log_ts"]),
            )

            cur.execute(MARK_START_PROCESSED, (r["id"],))

            processed += 1
            touched.add((r["tenant_id"], r["location_id"]))

        # -------------------------
        # END PHASE
        # -------------------------
        cur.execute("SELECT NOW() AS now;")
        now = cur.fetchone()["now"]

        cur.execute(SELECT_EXIT_ROWS, (BATCH_SIZE,))
        exit_rows = cur.fetchall()

        for r in exit_rows:
            due_time = r["load_time"] + timedelta(seconds=r["length_sec"])

            if now >= due_time:
                log.info("EXIT bill=%s", r["bill"])

                cur.execute(
                    UPDATE_VEHICLE_DRY,
                    (r["tenant_id"], r["location_id"], r["created_on"], r["bill"]),
                )

                cur.execute(
                    UPDATE_SUPER_DRY,
                    (r["tenant_id"], r["location_id"], r["created_on"], r["bill"]),
                )

                cur.execute(
                    UPDATE_TUNNEL_EXIT,
                    (r["bill"], r["location"], r["created_on"]),
                )

                processed += 1
                touched.add((r["tenant_id"], r["location_id"]))

        conn.commit()
        return processed, touched

    except OperationalError:
        raise

    except Exception:
        conn.rollback()
        raise

    finally:
        if cur:
            cur.close()


def write_heartbeat(conn, touched):
    try:
        cur = conn.cursor()
        if touched:
            for t, l in touched:
                cur.execute(INSERT_HEARTBEAT, (HEARTBEAT_SOURCE, t, l))
        else:
            cur.execute(INSERT_HEARTBEAT, (HEARTBEAT_SOURCE, None, None))
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()


# =========================================================
# Main Loop
# =========================================================

def main():
    log.info("Starting loader_combined_worker")

    conn = None

    while True:
        try:
            if conn is None or conn.closed:
                conn = get_conn()
                log.info("DB connected")

            processed, touched = run_once(conn)

            if processed:
                log.info("Processed %s actions", processed)

            write_heartbeat(conn, touched)
            time.sleep(POLL_SECONDS)

        except OperationalError as oe:
            log.warning("DB error: %s (reconnecting)", oe)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

        except Exception:
            log.exception("Worker error")
            time.sleep(2)


if __name__ == "__main__":
    main()