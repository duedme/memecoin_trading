"""
maintenance_worker.py
Ejecuta periódicamente: dead-letter, retention, y actualiza heartbeat.
"""
import os
import time
import signal
import json

import psycopg2

from sharedconfig import DBCONFIG, REALTIME_ENABLED, getlogger

log = getlogger("maintenance-worker")
PROCESS_NAME = "maintenance-worker"
STOP = False

DEAD_LETTER_INTERVAL = int(os.getenv("MAINT_DEAD_LETTER_INTERVAL", "300"))   # 5 min
RETENTION_INTERVAL = int(os.getenv("MAINT_RETENTION_INTERVAL", "3600"))       # 1 h
MAX_ATTEMPTS = int(os.getenv("MAINT_MAX_ATTEMPTS", "10"))


def _stop(signum, frame):
    global STOP
    STOP = True


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def _db():
    conn = psycopg2.connect(**DBCONFIG)
    conn.autocommit = True
    return conn


def heartbeat(conn, meta):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO process_heartbeat (process_name, last_seen_at, metadata, updated_at)
            VALUES (%s, NOW(), %s::jsonb, NOW())
            ON CONFLICT (process_name) DO UPDATE SET
                last_seen_at = EXCLUDED.last_seen_at,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
            """,
            (PROCESS_NAME, json.dumps(meta)),
        )


def run_dead_letter(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT reducer_queue_dead_letter(%s)", (MAX_ATTEMPTS,))
        n = cur.fetchone()[0]
    log.info("dead-letter: %s rows moved", n)
    return n


def run_retention(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM run_retention_cleanup()")
        rows = cur.fetchall()
    total = 0
    for tbl, n in rows:
        log.info("retention %s: %s", tbl, n)
        total += n
    return total


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. maintenance-worker no arranca.")
        return

    log.info("maintenance-worker arrancado. dl=%ss retention=%ss max_attempts=%s",
             DEAD_LETTER_INTERVAL, RETENTION_INTERVAL, MAX_ATTEMPTS)

    last_dl = 0
    last_rt = 0
    last_hb = 0
    stats = {"dl_moved_total": 0, "retention_total": 0, "cycles": 0}

    while not STOP:
        now = time.time()
        try:
            conn = _db()
            try:
                if now - last_dl >= DEAD_LETTER_INTERVAL:
                    stats["dl_moved_total"] += run_dead_letter(conn)
                    last_dl = now

                if now - last_rt >= RETENTION_INTERVAL:
                    stats["retention_total"] += run_retention(conn)
                    last_rt = now

                if now - last_hb >= 30:
                    heartbeat(conn, {"status": "running", **stats})
                    last_hb = now
            finally:
                conn.close()

            stats["cycles"] += 1
        except Exception as e:
            log.exception("loop error: %s", e)

        for _ in range(10):
            if STOP:
                break
            time.sleep(1)

    log.warning("maintenance-worker detenido. stats=%s", stats)


if __name__ == "__main__":
    main()