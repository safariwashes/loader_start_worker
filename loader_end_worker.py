# loader_end_worker.py
# Computes tunnel exit based on fixed travel time

import os
import time
import logging
from datetime import timedelta

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor


# ---------------------------------------------------
# Logging
# ---------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()  # DEBUG temporarily
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("loader_end_worker")


# ---------------------------------------------------
# ENV
# ---------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

HEARTBEAT_SOURCE = "loader_end_worker"


def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


# ---------------------------------------------------
# SQL
# ---------------------------------------------------

SELECT_TUNNEL_ROWS = """
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

UPDATE_VEHICLE_SQL = """
UPDATE vehicle
SET status = 4,
    status_desc = 'Dry & Shine'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s;
"""

UPDATE_SUPER_SQL = """
UPDATE super
SET status = 4,
    status_desc = 'Dry & Shine'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s;
"""

UPDATE_TUNNEL_EXIT_SQL = """
UPDATE tunnel
SET exit = true,
    exit_time = NOW()
WHERE bill = %s
  AND location = %s
  AND created_on = %s;
"""

INSERT_HEARTBEAT_SQL = """
INSERT INTO heartbeat (source, tenant_id, location_id)
VALUES (%s, %s, %s);
"""


# ---------------------------------------------------
# Worker Logic
# ---------------------------------------------------

def process_batch(conn):
    processed = 0
    touched = set()

    cur = conn.cursor(cursor_factory=RealDictCursor)

    log.debug("Selecting tunnel rows...")

    cur.execute(SELECT_TUNNEL_ROWS, (BATCH_SIZE,))
    rows = cur.fetchall()

    if not rows:
        log.debug("No eligible tunnel rows found.")
        cur.close()
        return 0, touched

    log.debug("Fetched %s tunnel rows", len(rows))

    cur.execute("SELECT NOW();")
    now = cur.fetchone()["now"]

    for r in rows:
        bill = r["bill"]
        location = r["location"]
        created_on = r["created_on"]
        tenant_id = r["tenant_id"]
        location_id = r["location_id"]
        load_time = r["load_time"]
        length_sec = r["length_sec"]

        exit_due_time = load_time + timedelta(seconds=length_sec)

        log.debug(
            "Evaluating bill=%s load_time=%s length=%s exit_due=%s now=%s",
            bill, load_time, length_sec, exit_due_time, now
        )

        if now >= exit_due_time:

            log.info("Exit triggered for bill=%s", bill)

            # 1️⃣ Update vehicle
            cur.execute(
                UPDATE_VEHICLE_SQL,
                (tenant_id, location_id, created_on, bill),
            )
            log.debug("Vehicle rows updated: %s", cur.rowcount)

            # 2️⃣ Update super
            cur.execute(
                UPDATE_SUPER_SQL,
                (tenant_id, location_id, created_on, bill),
            )
            log.debug("Super rows updated: %s", cur.rowcount)

            # 3️⃣ Update tunnel LAST
            cur.execute(
                UPDATE_TUNNEL_EXIT_SQL,
                (bill, location, created_on),
            )
            log.debug("Tunnel rows updated: %s", cur.rowcount)

            if cur.rowcount == 0:
                log.error(
                    "Tunnel UPDATE FAILED for bill=%s (no rows affected!)",
                    bill
                )

            processed += 1
            touched.add((tenant_id, location_id))

        else:
            log.debug(
                "Skipping bill=%s (exit not due yet)",
                bill
            )

    conn.commit()
    cur.close()

    return processed, touched


def write_heartbeat(conn, touched):
    try:
        cur = conn.cursor()
        if touched:
            for tenant_id, location_id in touched:
                cur.execute(
                    INSERT_HEARTBEAT_SQL,
                    (HEARTBEAT_SOURCE, tenant_id, location_id)
                )
        else:
            cur.execute(
                INSERT_HEARTBEAT_SQL,
                (HEARTBEAT_SOURCE, None, None)
            )
        conn.commit()
        cur.close()
        log.debug("Heartbeat written.")
    except Exception as e:
        log.debug("Heartbeat skipped: %s", e)


# ---------------------------------------------------
# Main Loop
# ---------------------------------------------------

def main():
    log.info("Starting loader_end_worker")

    conn = None

    while True:
        try:
            if conn is None or conn.closed:
                conn = get_conn()
                log.info("DB connected")

            processed, touched = process_batch(conn)

            if processed:
                log.info("Processed exits: %s", processed)

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

        except Exception as e:
            log.exception("Worker error: %s", e)
            time.sleep(2)


if __name__ == "__main__":
    main()