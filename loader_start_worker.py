# loader_start_worker.py
# Simple Render Worker (NO blueprint). Uses psycopg v3.
# Reads loader_controller_log processed=false & pos_receipt not null
# Updates: vehicle.status=3, super.status=3 (if exists), tunnel.load=true/load_time
# Then marks loader_controller_log.processed=true
# Writes heartbeat source=loader_start_worker

import os
import sys
import time
import logging
from datetime import datetime
import psycopg
from psycopg.rows import dict_row

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

SOURCE = "loader_start_worker"

logging.basicConfig(
    stream=sys.stdout,
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(SOURCE)

# ---------- SQL ----------
SELECT_ROWS_SQL = """
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
"""

UPDATE_SUPER_SQL = """
UPDATE super
SET status = 3,
    status_desc = 'Wash'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
"""

# NOTE: your tunnel.load_time column is "time without time zone".
# We'll store local DB time derived from log_ts.
UPDATE_TUNNEL_SQL = """
UPDATE tunnel
SET
  load = true,
  load_time = ( %s::timestamptz AT TIME ZONE current_setting('TIMEZONE') )::time
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
"""

MARK_PROCESSED_SQL = """
UPDATE loader_controller_log
SET processed = true
WHERE id = %s
"""

INSERT_HEARTBEAT_SQL = """
INSERT INTO heartbeat (source, tenant_id, location_id)
VALUES (%s, %s, %s)
"""

COUNT_ELIGIBLE_SQL = """
SELECT COUNT(*) AS eligible
FROM loader_controller_log
WHERE processed=false AND pos_receipt IS NOT NULL
"""

# ---------- helpers ----------
def connect():
    # psycopg v3
    return psycopg.connect(DATABASE_URL, connect_timeout=10)

def heartbeat(conn, touched_pairs):
    try:
        with conn.cursor() as cur:
            if touched_pairs:
                for tenant_id, location_id in touched_pairs:
                    cur.execute(INSERT_HEARTBEAT_SQL, (SOURCE, tenant_id, location_id))
            else:
                cur.execute(INSERT_HEARTBEAT_SQL, (SOURCE, None, None))
    except Exception as e:
        log.debug("Heartbeat skipped: %s", e)

def process_batch(conn):
    processed_count = 0
    touched_pairs = set()

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            # TEMP DEBUG: show eligible count occasionally
            cur.execute(COUNT_ELIGIBLE_SQL)
            eligible = cur.fetchone()["eligible"]
            log.debug("Eligible loader rows=%s", eligible)

            cur.execute(SELECT_ROWS_SQL, (BATCH_SIZE,))
            rows = cur.fetchall()

            if not rows:
                log.info("No eligible rows this cycle.")
                return 0, touched_pairs

            log.info("Fetched %s loader rows", len(rows))

            for r in rows:
                log_id = r["id"]
                tenant_id = r["tenant_id"]
                location_id = r["location_id"]
                bill = r["bill"]
                pos_receipt = r["pos_receipt"]
                log_ts = r["log_ts"]
                created_on = r["created_on"]

                log.info("Processing loader_log.id=%s bill=%s", log_id, bill)

                cur.execute(UPDATE_VEHICLE_SQL, (pos_receipt, tenant_id, location_id, created_on, bill))
                v_rc = cur.rowcount

                cur.execute(UPDATE_SUPER_SQL, (tenant_id, location_id, created_on, bill))
                s_rc = cur.rowcount

                cur.execute(UPDATE_TUNNEL_SQL, (log_ts, tenant_id, location_id, created_on, bill))
                t_rc = cur.rowcount

                cur.execute(MARK_PROCESSED_SQL, (log_id,))
                m_rc = cur.rowcount

                log.debug("Rows updated: vehicle=%s super=%s tunnel=%s marked=%s", v_rc, s_rc, t_rc, m_rc)

                if m_rc == 1:
                    processed_count += 1
                    touched_pairs.add((tenant_id, location_id))
                else:
                    log.warning("Could not mark processed for loader_log.id=%s", log_id)

    return processed_count, touched_pairs

def main():
    log.info("Starting %s poll=%ss batch=%s", SOURCE, POLL_SECONDS, BATCH_SIZE)

    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = connect()
                conn.autocommit = False
                log.info("DB connected")

            processed, touched = process_batch(conn)
            heartbeat(conn, touched)

            if processed:
                log.info("Processed loader rows=%s", processed)

            time.sleep(POLL_SECONDS)

        except psycopg.OperationalError as e:
            log.warning("DB operational error (reconnecting): %s", e)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

        except Exception as e:
            log.exception("Worker error: %s", e)
            # reset connection on any unknown exception
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

if __name__ == "__main__":
    main()