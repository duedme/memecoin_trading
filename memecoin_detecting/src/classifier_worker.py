"""
classifier_worker.py
Consume reducer_queue con event_type='classification_update' y clasifica
solo las wallets realmente afectadas. Con throttling para no reclasificar
la misma wallet más de una vez cada N segundos.
"""
import json
import time
import signal
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.extras

from shared_config import (
    DBCONFIG,
    REALTIME_ENABLED,
    REDUCER_BATCH_SIZE,
    REDUCER_WORKERS,
    getlogger,
)
from walletclassifier import classify_wallet

log = getlogger("classifier-worker")

PROCESS_NAME = "classifier-worker"
EVENT_TYPE = "classification_update"
THROTTLE_SECONDS = int(os.getenv("CLASSIFIER_THROTTLE_SECONDS", "120"))
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def db_connect():
    conn = psycopg2.connect(**DBCONFIG)
    conn.autocommit = False
    return conn


def heartbeat(conn, metadata):
    try:
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
                (PROCESS_NAME, json.dumps(metadata)),
            )
            conn.commit()
    except Exception as e:
        log.error("heartbeat falló: %s", e)
        conn.rollback()


def claim_batch(conn, limit):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH picked AS (
                SELECT id FROM reducer_queue
                 WHERE status = 'pending'
                   AND event_type = %s
                 ORDER BY priority DESC, created_at ASC
                 LIMIT %s
                 FOR UPDATE SKIP LOCKED
            )
            UPDATE reducer_queue r
               SET status = 'processing',
                   attempts = r.attempts + 1,
                   updated_at = NOW()
              FROM picked p
             WHERE r.id = p.id
         RETURNING r.id, r.walletaddress, r.signature
            """,
            (EVENT_TYPE, limit),
        )
        rows = cur.fetchall()
    conn.commit()
    return [dict(r) for r in rows]


def mark_queue(conn, ids, status, error=None):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE reducer_queue
               SET status = %s, last_error = %s,
                   processed_at = NOW(), updated_at = NOW()
             WHERE id = ANY(%s)
            """,
            (status, error, list(ids)),
        )
    conn.commit()


def is_throttled(conn, wallet: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_run_at FROM classifier_throttle
            WHERE walletaddress = %s
              AND last_run_at > NOW() - (%s || ' seconds')::interval
            """,
            (wallet, THROTTLE_SECONDS),
        )
        row = cur.fetchone()
        conn.commit()
    return row is not None


def process_group(events_for_wallet, stats):
    if not events_for_wallet:
        return

    wallet = events_for_wallet[0]["walletaddress"]
    ids = [e["id"] for e in events_for_wallet]

    conn = db_connect()
    try:
        if not wallet:
            mark_queue(conn, ids, "ignored", "missing wallet")
            stats["ignored"] += len(ids)
            return

        if is_throttled(conn, wallet):
            # Marcamos como done: otro evento ya clasificó recientemente.
            mark_queue(conn, ids, "done")
            stats["throttled"] += len(ids)
            return

        result = classify_wallet(conn, wallet)
        if result is None:
            mark_queue(conn, ids, "ignored", "no trades yet")
            stats["ignored"] += len(ids)
            return

        mark_queue(conn, ids, "done")
        stats["classified"] += len(ids)
        stats["wallets_classified"] += 1
    except Exception as e:
        log.exception("error clasificando %s: %s", wallet, e)
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            mark_queue(conn, ids, "error", str(e)[:500])
        except Exception:
            pass
        stats["errors"] += len(ids)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def group_by_wallet(events):
    groups = {}
    for ev in events:
        key = ev.get("walletaddress")
        groups.setdefault(key, []).append(ev)
    return list(groups.values())


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. classifier-worker no arranca.")
        return

    log.info(
        "classifier-worker iniciando. workers=%s batch=%s throttle=%ss",
        REDUCER_WORKERS, REDUCER_BATCH_SIZE, THROTTLE_SECONDS,
    )

    stats = {"classified": 0, "wallets_classified": 0,
             "throttled": 0, "ignored": 0, "errors": 0}
    last_hb = 0

    executor = ThreadPoolExecutor(max_workers=max(1, REDUCER_WORKERS))

    while not STOP:
        conn = db_connect()
        try:
            batch = claim_batch(conn, REDUCER_BATCH_SIZE)

            if not batch:
                conn.close()
                time.sleep(2)
                continue

            groups = group_by_wallet(batch)
            log.info("batch=%s wallets=%s", len(batch), len(groups))

            futures = [
                executor.submit(process_group, grp, stats)
                for grp in groups
            ]
            for f in futures:
                try:
                    f.result(timeout=30)
                except Exception as e:
                    log.error("worker error: %s", e)

            if time.time() - last_hb >= 15:
                heartbeat(conn, {"status": "running", **stats})
                last_hb = time.time()

        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(2)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    executor.shutdown(wait=True, cancel_futures=True)
    log.warning("classifier-worker detenido. stats=%s", stats)


if __name__ == "__main__":
    main()