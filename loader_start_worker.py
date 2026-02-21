# loader_start_worker.py
# Render Background Worker
# Idempotent, duplicate-safe, multi-tenant safe

import os
import time
import logging
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
log = logging.getLogger("loader_start_worker")


# =========================================================
# ENV
# =========================================================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

HEARTBEAT_SOURCE = "loader_start_worker"


# =========================================================
# DB Connection
# =========================================================
def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


# =========================================================
# SQL
# =========================================================

# IMPORTANT: No date filter — process ALL unprocessed rows
SELECT_ROWS_SQL = """
SELECT
    rowid,
    tenant_id,
    location_id,
    bill,
    pos_receipt,
    log_ts,
    (log_ts::date) AS created_on
FROM loader_controller_log
WHERE processed = false
  AND pos_receipt IS NOT NULL
ORDER BY log_ts ASC
FOR UPDATE SKIP LOCKED
LIMIT %s
"""

UPDATE_VEHICLE_SQL = """
UPDATE vehicle
SET
    status = 3,
    status_desc = 'Wash',
    bill_wshfy = %s
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
RETURNING bill;
"""

UPDATE_SUPER_SQL = """
UPDATE super
SET
    status = 3,
    status_desc = 'Wash'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
"""

UPDATE_TUNNEL_SQL = """
UPDATE tunnel
SET
    load = true,
    load_time = %s
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
"""

MARK_PROCESSED_SQL = """
UPDATE loader_controller_log
SET processed = true
WHERE rowid = %s
"""

INSERT_HEARTBEAT_SQL = """
INSERT INTO heartbeat (source, tenant_id, location_id)
VALUES (%s, %s, %s)
"""


# =========================================================
# Worker Logic
# =========================================================
def process_batch(conn):
    processed_count = 0
    touched_pairs = set()

    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            cur.execute(SELECT_ROWS_SQL, (BATCH_SIZE,))
            rows = cur.fetchall()

            if not rows:
                return 0, touched_pairs

            for r in rows:
                rowid = r["rowid"]
                tenant_id = r["tenant_id"]
                location_id = r["location_id"]
                bill = r["bill"]
                pos_receipt = r["pos_receipt"]
                log_ts = r["log_ts"]
                created_on = r["created_on"]

                # -------------------------------------------------
                # Update vehicle (idempotent-safe)
                # -------------------------------------------------
                cur.execute(
                    UPDATE_VEHICLE_SQL,
                    (pos_receipt, tenant_id, location_id, created_on, bill),
                )
                vehicle_rows = cur.fetchall()

                if not vehicle_rows:
                    log.warning(
                        "No vehicle row found → tenant=%s location=%s bill=%s",
                        tenant_id, location_id, bill,
                    )

                # -------------------------------------------------
                # Update super (optional)
                # -------------------------------------------------
                cur.execute(
                    UPDATE_SUPER_SQL,
                    (tenant_id, location_id, created_on, bill),
                )

                # -------------------------------------------------
                # Update tunnel (optional)
                # -------------------------------------------------
                cur.execute(
                    UPDATE_TUNNEL_SQL,
                    (log_ts, tenant_id, location_id, created_on, bill),
                )

                # -------------------------------------------------
                # Mark processed (ALWAYS mark if we reached here)
                # -------------------------------------------------
                cur.execute(MARK_PROCESSED_SQL, (rowid,))
                if cur.rowcount == 1:
                    processed_count += 1
                    touched_pairs.add((tenant_id, location_id))
                else:
                    log.warning("Failed to mark processed rowid=%s", rowid)

    return processed_count, touched_pairs


# =========================================================
# Heartbeat
# =========================================================
def write_heartbeat(conn, touched_pairs):
    with conn:
        with conn.cursor() as cur:
            if touched_pairs:
                for tenant_id, location_id in touched_pairs:
                    cur.execute(
                        INSERT_HEARTBEAT_SQL,
                        (HEARTBEAT_SOURCE, tenant_id, location_id),
                    )
            else:
                # Worker alive but no rows
                cur.execute(
                    INSERT_HEARTBEAT_SQL,
                    (HEARTBEAT_SOURCE, None, None),
                )


# =========================================================
# Main Loop
# =========================================================
def main():
    log.info(
        "Starting worker: poll=%ss batch=%s source=%s",
        POLL_SECONDS, BATCH_SIZE, HEARTBEAT_SOURCE
    )

    conn = None

    while True:
        try:
            if conn is None or conn.closed:
                conn = get_conn()
                conn.autocommit = False
                log.info("DB connected")

            processed, touched = process_batch(conn)

            if processed:
                log.info("Processed rows: %s", processed)

            write_heartbeat(conn, touched)

            time.sleep(POLL_SECONDS)

        except OperationalError as oe:
            log.warning("DB connection error: %s (reconnecting)", oe)
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