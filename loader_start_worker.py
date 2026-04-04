# loader_start_worker.py
# Hardened Render Worker (psycopg v3)
#
# What it does:
# - Reads loader_controller_log rows where processed=false and pos_receipt IS NOT NULL
# - Updates vehicle -> status=3 / status_desc='Wash' / bill_wshfy=pos_receipt
# - Updates super   -> status=3 / status_desc='Wash'   (if row exists)
# - Updates tunnel  -> load=true / load_time from log_ts local DB time
# - Marks source row processed ONLY when vehicle + tunnel are confirmed in desired state
# - Writes heartbeat every cycle
#
# Improvements vs original:
# - Safe processed logic (no blind processed=true)
# - Idempotent updates
# - Optional tenant/location filters
# - Per-cycle heartbeat in its own transaction
# - Better mismatch logging
# - Safer retry behavior
#
# Environment variables:
#   DATABASE_URL           required
#   LOG_LEVEL              default INFO
#   POLL_SECONDS           default 2
#   BATCH_SIZE             default 50
#   TENANT_ID              optional UUID filter
#   LOCATION_ID            optional UUID filter
#   REQUIRE_SUPER          default false; if true, require super row to be in desired state too

import os
import sys
import time
import logging
from typing import Optional, Tuple, Set

import psycopg
from psycopg.rows import dict_row

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
TENANT_ID = (os.getenv("TENANT_ID") or "").strip() or None
LOCATION_ID = (os.getenv("LOCATION_ID") or "").strip() or None
REQUIRE_SUPER = (os.getenv("REQUIRE_SUPER", "false").strip().lower() in {"1", "true", "yes", "y"})

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

SOURCE = "loader_start_worker"

logging.basicConfig(
    stream=sys.stdout,
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(SOURCE)

# -----------------------------
# SQL
# -----------------------------

SELECT_ROWS_SQL = """
SELECT
    id,
    tenant_id,
    location_id,
    location_code,
    bill,
    pos_receipt,
    log_ts,
    log_ts::date AS created_on
FROM loader_controller_log
WHERE processed = false
  AND pos_receipt IS NOT NULL
  AND (%s::uuid IS NULL OR tenant_id = %s::uuid)
  AND (%s::uuid IS NULL OR location_id = %s::uuid)
ORDER BY log_ts ASC, id ASC
FOR UPDATE SKIP LOCKED
LIMIT %s
"""

COUNT_ELIGIBLE_SQL = """
SELECT COUNT(*) AS eligible
FROM loader_controller_log
WHERE processed = false
  AND pos_receipt IS NOT NULL
  AND (%s::uuid IS NULL OR tenant_id = %s::uuid)
  AND (%s::uuid IS NULL OR location_id = %s::uuid)
"""

UPDATE_VEHICLE_SQL = """
UPDATE vehicle
SET
    status = '3',
    status_desc = 'Wash',
    bill_wshfy = %s
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
  AND (
        COALESCE(status, '0') <> '3'
        OR status_desc IS DISTINCT FROM 'Wash'
        OR bill_wshfy IS DISTINCT FROM %s
      )
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
  AND (
        COALESCE(status, 0) <> 3
        OR status_desc IS DISTINCT FROM 'Wash'
      )
"""

UPDATE_TUNNEL_SQL = """
UPDATE tunnel
SET
    load = true,
    load_time = (%s::timestamp AT TIME ZONE current_setting('TIMEZONE'))::time
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
  AND (
        COALESCE(load, false) = false
        OR load_time IS DISTINCT FROM (%s::timestamp AT TIME ZONE current_setting('TIMEZONE'))::time
      )
"""

CHECK_VEHICLE_OK_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM vehicle
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
      AND status = '3'
      AND status_desc = 'Wash'
      AND bill_wshfy = %s
) AS ok
"""

CHECK_SUPER_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM super
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
) AS exists_flag
"""

CHECK_SUPER_OK_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM super
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
      AND status = 3
      AND status_desc = 'Wash'
) AS ok
"""

CHECK_TUNNEL_OK_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM tunnel
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
      AND load = true
      AND load_time = (%s::timestamp AT TIME ZONE current_setting('TIMEZONE'))::time
) AS ok
"""

MARK_PROCESSED_SQL = """
UPDATE loader_controller_log
SET processed = true
WHERE id = %s
  AND processed = false
"""

INSERT_HEARTBEAT_SQL = """
INSERT INTO heartbeat (source, tenant_id, location_id)
VALUES (%s, %s, %s)
"""

# -----------------------------
# Helpers
# -----------------------------

def connect():
    return psycopg.connect(DATABASE_URL, connect_timeout=10)

def fetch_bool(cur, sql: str, params: tuple) -> bool:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return False
    # query aliases are ok / exists_flag depending on query
    return bool(row.get("ok", row.get("exists_flag", False)))

def write_heartbeat(conn, touched_pairs: Set[Tuple[Optional[str], Optional[str]]]) -> None:
    """
    Heartbeat in its own committed transaction.
    If rows were processed, write one heartbeat per tenant/location pair touched.
    Otherwise write one generic heartbeat, optionally scoped by configured env filters.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            if touched_pairs:
                for tenant_id, location_id in sorted(touched_pairs):
                    cur.execute(INSERT_HEARTBEAT_SQL, (SOURCE, tenant_id, location_id))
            else:
                cur.execute(INSERT_HEARTBEAT_SQL, (SOURCE, TENANT_ID, LOCATION_ID))

def process_batch(conn) -> Tuple[int, Set[Tuple[str, str]]]:
    processed_count = 0
    touched_pairs: Set[Tuple[str, str]] = set()

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            # light observability
            cur.execute(COUNT_ELIGIBLE_SQL, (TENANT_ID, TENANT_ID, LOCATION_ID, LOCATION_ID))
            eligible = cur.fetchone()["eligible"]
            log.debug(
                "Eligible rows=%s tenant_filter=%s location_filter=%s",
                eligible,
                TENANT_ID,
                LOCATION_ID,
            )

            cur.execute(
                SELECT_ROWS_SQL,
                (TENANT_ID, TENANT_ID, LOCATION_ID, LOCATION_ID, BATCH_SIZE),
            )
            rows = cur.fetchall()

            if not rows:
                log.info("No eligible rows this cycle.")
                return 0, touched_pairs

            log.info("Fetched %s loader rows", len(rows))

            for r in rows:
                log_id = r["id"]
                tenant_id = r["tenant_id"]
                location_id = r["location_id"]
                location_code = r["location_code"]
                bill = r["bill"]
                pos_receipt = r["pos_receipt"]
                log_ts = r["log_ts"]
                created_on = r["created_on"]

                log.info(
                    "Processing loader_log.id=%s tenant=%s location=%s/%s bill=%s pos_receipt=%s log_ts=%s",
                    log_id,
                    tenant_id,
                    location_code,
                    location_id,
                    bill,
                    pos_receipt,
                    log_ts,
                )

                # Idempotent updates
                cur.execute(
                    UPDATE_VEHICLE_SQL,
                    (pos_receipt, tenant_id, location_id, created_on, bill, pos_receipt),
                )
                v_rc = cur.rowcount

                cur.execute(
                    UPDATE_SUPER_SQL,
                    (tenant_id, location_id, created_on, bill),
                )
                s_rc = cur.rowcount

                cur.execute(
                    UPDATE_TUNNEL_SQL,
                    (log_ts, tenant_id, location_id, created_on, bill, log_ts),
                )
                t_rc = cur.rowcount

                # Confirm desired final state, not just rowcount
                vehicle_ok = fetch_bool(
                    cur,
                    CHECK_VEHICLE_OK_SQL,
                    (tenant_id, location_id, created_on, bill, pos_receipt),
                )

                super_exists = fetch_bool(
                    cur,
                    CHECK_SUPER_EXISTS_SQL,
                    (tenant_id, location_id, created_on, bill),
                )
                super_ok = fetch_bool(
                    cur,
                    CHECK_SUPER_OK_SQL,
                    (tenant_id, location_id, created_on, bill),
                ) if super_exists else (not REQUIRE_SUPER)

                tunnel_ok = fetch_bool(
                    cur,
                    CHECK_TUNNEL_OK_SQL,
                    (tenant_id, location_id, created_on, bill, log_ts),
                )

                # Rule:
                # - vehicle and tunnel must be confirmed
                # - super is optional unless REQUIRE_SUPER=true
                # - if super row exists, confirm it too
                final_super_ok = super_ok if super_exists else (not REQUIRE_SUPER)

                log.debug(
                    "Result loader_log.id=%s v_rc=%s s_rc=%s t_rc=%s vehicle_ok=%s super_exists=%s super_ok=%s tunnel_ok=%s",
                    log_id, v_rc, s_rc, t_rc, vehicle_ok, super_exists, final_super_ok, tunnel_ok
                )

                if vehicle_ok and tunnel_ok and final_super_ok:
                    cur.execute(MARK_PROCESSED_SQL, (log_id,))
                    m_rc = cur.rowcount
                    if m_rc == 1:
                        processed_count += 1
                        touched_pairs.add((str(tenant_id), str(location_id)))
                        log.info(
                            "Marked processed loader_log.id=%s bill=%s",
                            log_id,
                            bill,
                        )
                    else:
                        log.warning(
                            "Skipped mark processed because row was already updated elsewhere loader_log.id=%s bill=%s",
                            log_id,
                            bill,
                        )
                else:
                    reasons = []
                    if not vehicle_ok:
                        reasons.append("vehicle not in desired state")
                    if not tunnel_ok:
                        reasons.append("tunnel not in desired state")
                    if not final_super_ok:
                        reasons.append("super not in desired state")
                    log.warning(
                        "Leaving unprocessed loader_log.id=%s bill=%s tenant=%s location=%s reasons=%s",
                        log_id,
                        bill,
                        tenant_id,
                        location_id,
                        "; ".join(reasons),
                    )

    return processed_count, touched_pairs

def main():
    log.info(
        "Starting %s poll=%ss batch=%s tenant_filter=%s location_filter=%s require_super=%s",
        SOURCE,
        POLL_SECONDS,
        BATCH_SIZE,
        TENANT_ID,
        LOCATION_ID,
        REQUIRE_SUPER,
    )

    conn = None

    while True:
        try:
            if conn is None or conn.closed:
                conn = connect()
                conn.autocommit = False
                log.info("DB connected")

            processed, touched = process_batch(conn)

            # heartbeat committed separately
            write_heartbeat(conn, touched)

            if processed:
                log.info("Processed loader rows=%s", processed)

            time.sleep(POLL_SECONDS)

        except psycopg.OperationalError as e:
            log.warning("DB operational error (reconnecting): %s", e)
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

        except Exception as e:
            log.exception("Worker error: %s", e)
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

if __name__ == "__main__":
    main()