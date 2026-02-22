# loader_start_worker.py (DEBUG)
# Render Background Worker with aggressive debug logging

import os
import time
import logging
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("loader_start_worker")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
HEARTBEAT_SOURCE = "loader_start_worker"

def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)

# ---------- SQL ----------
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

COUNT_ELIGIBLE_SQL = """
SELECT
    COUNT(*) AS eligible,
    MIN(log_ts) AS min_log_ts,
    MAX(log_ts) AS max_log_ts
FROM loader_controller_log
WHERE processed = false
  AND pos_receipt IS NOT NULL
"""

# Quick peek at locks (very lightweight)
COUNT_LOCKED_HINT_SQL = """
SELECT COUNT(*) AS locked_rows
FROM loader_controller_log
WHERE processed = false
  AND pos_receipt IS NOT NULL
  AND rowid IN (
      SELECT rowid
      FROM loader_controller_log
      WHERE processed = false AND pos_receipt IS NOT NULL
      FOR UPDATE SKIP LOCKED
      LIMIT 200
  );
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

# ---------- Debug helpers ----------
def log_db_identity(cur):
    cur.execute("SELECT current_database() db, current_user usr, inet_server_addr() host, inet_server_port() port;")
    row = cur.fetchone()
    log.info("DB identity: db=%s user=%s host=%s port=%s", row["db"], row["usr"], row["host"], row["port"])

    cur.execute("SHOW timezone;")
    tz = cur.fetchone()
    log.info("DB timezone: %s", list(tz.values())[0])

    cur.execute("SELECT CURRENT_DATE AS current_date, NOW() AS now;")
    t = cur.fetchone()
    log.info("DB time: current_date=%s now=%s", t["current_date"], t["now"])

def process_batch(conn):
    processed_count = 0
    touched_pairs = set()

    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # --- DEBUG: prove we are in the right DB and see eligible counts
            log_db_identity(cur)

            cur.execute(COUNT_ELIGIBLE_SQL)
            stats = cur.fetchone()
            log.info(
                "Eligible rows (processed=false & pos_receipt not null): %s (min_log_ts=%s max_log_ts=%s)",
                stats["eligible"], stats["min_log_ts"], stats["max_log_ts"]
            )

            cur.execute(SELECT_ROWS_SQL, (BATCH_SIZE,))
            rows = cur.fetchall()
            log.info("Fetched %s rows (batch_size=%s).", len(rows), BATCH_SIZE)

            if not rows:
                # This is the critical signal: either zero eligible, or all eligible rows are locked by another tx
                return 0, touched_pairs

            # DEBUG: show first few rowids
            sample = rows[:5]
            log.info("Sample rowids: %s", [r["rowid"] for r in sample])

            for idx, r in enumerate(rows, start=1):
                rowid = r["rowid"]
                tenant_id = r["tenant_id"]
                location_id = r["location_id"]
                bill = r["bill"]
                pos_receipt = r["pos_receipt"]
                log_ts = r["log_ts"]
                created_on = r["created_on"]

                log.info(
                    "[%s/%s] Processing rowid=%s bill=%s pos_receipt=%s created_on=%s log_ts=%s tenant=%s location=%s",
                    idx, len(rows), rowid, bill, pos_receipt, created_on, log_ts, tenant_id, location_id
                )

                # Vehicle
                cur.execute(
                    UPDATE_VEHICLE_SQL,
                    (pos_receipt, tenant_id, location_id, created_on, bill),
                )
                vehicle_rows = cur.fetchall()
                log.info("Vehicle update matched rows: %s", len(vehicle_rows))
                if not vehicle_rows:
                    log.warning("No vehicle row matched for bill=%s (still marking controller row processed).", bill)

                # Super
                cur.execute(UPDATE_SUPER_SQL, (tenant_id, location_id, created_on, bill))
                log.info("Super update rowcount: %s", cur.rowcount)

                # Tunnel
                cur.execute(UPDATE_TUNNEL_SQL, (log_ts, tenant_id, location_id, created_on, bill))
                log.info("Tunnel update rowcount: %s", cur.rowcount)

                # Mark processed
                cur.execute(MARK_PROCESSED_SQL, (rowid,))
                log.info("Mark processed rowcount: %s", cur.rowcount)

                if cur.rowcount == 1:
                    processed_count += 1
                    touched_pairs.add((tenant_id, location_id))

            log.info("Batch done. processed_count=%s touched_pairs=%s", processed_count, len(touched_pairs))

    return processed_count, touched_pairs

def write_heartbeat(conn, touched_pairs):
    with conn:
        with conn.cursor() as cur:
            if touched_pairs:
                for tenant_id, location_id in touched_pairs:
                    cur.execute(INSERT_HEARTBEAT_SQL, (HEARTBEAT_SOURCE, tenant_id, location_id))
            else:
                cur.execute(INSERT_HEARTBEAT_SQL, (HEARTBEAT_SOURCE, None, None))

def main():
    log.info("Starting worker: poll=%ss batch=%s source=%s", POLL_SECONDS, BATCH_SIZE, HEARTBEAT_SOURCE)

    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = get_conn()
                conn.autocommit = False
                log.info("DB connected")

            processed, touched = process_batch(conn)

            log.info("Loop summary: processed=%s touched_pairs=%s", processed, len(touched))
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