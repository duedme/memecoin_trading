"""
tx_parser.py
Consume chain_events_staging -> descarga tx via RPC -> parser por source ->
escribe wallettransactions -> encola reducer_queue -> marca staging como parsed/ignored/error.
"""
import json
import time
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.extras
import requests

from shared_config import (
    DBCONFIG,
    RPC_HTTP_URL,
    REALTIME_ENABLED,
    PARSER_BATCH_SIZE,
    PARSER_POLL_SECONDS,
    PARSER_WORKERS,
    getlogger,
)
from parsers import get_parser, supported_sources

log = getlogger("tx-parser")

PROCESS_NAME = "tx-parser"
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Señal %s recibida, apagando tx-parser...", signum)


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def db_connect():
    conn = psycopg2.connect(**DBCONFIG)
    conn.autocommit = False
    return conn


def heartbeat(conn, metadata=None):
    metadata = metadata or {}
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


def fetch_known_mints(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT mintaddress FROM tokens WHERE status = 'active'")
        rows = cur.fetchall()
        conn.commit()
    return {r[0] for r in rows if r[0]}


def claim_batch(conn, limit: int) -> list:
    """
    Toma hasta `limit` eventos pending, los marca processing y los devuelve.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH picked AS (
                SELECT id FROM chain_events_staging
                WHERE status = 'pending'
                  AND source = ANY(%s)
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE chain_events_staging s
               SET status = 'processing',
                   attempts = s.attempts + 1,
                   updated_at = NOW()
              FROM picked p
             WHERE s.id = p.id
         RETURNING s.id, s.signature, s.source, s.program_id, s.program_name, s.slot
            """,
            (list(supported_sources()), limit),
        )
        rows = cur.fetchall()
    conn.commit()
    return rows


def mark_staging(conn, event_id: int, status: str, error: str = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE chain_events_staging
               SET status = %s,
                   parse_error = %s,
                   processed_at = NOW(),
                   updated_at = NOW()
             WHERE id = %s
            """,
            (status, error, event_id),
        )
    conn.commit()


def rpc_get_transaction(signature: str, timeout: int = 10):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed",
            },
        ],
    }
    r = requests.post(RPC_HTTP_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return data.get("result")


INSERT_TX = """
INSERT INTO wallettransactions
    (time, signature, walletaddress, mintaddress, side,
     amounttoken, amountsol, pricesol, slot, amm, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""

UPSERT_WALLET = """
INSERT INTO wallets (walletaddress, firstseen, lastseen)
VALUES (%s, NOW(), NOW())
ON CONFLICT (walletaddress) DO UPDATE SET lastseen = NOW()
"""

ENQUEUE_REDUCER = """
INSERT INTO reducer_queue
    (event_type, walletaddress, mintaddress, signature, priority, status, created_at)
VALUES (%s, %s, %s, %s, %s, 'pending', NOW())
ON CONFLICT (event_type, signature, walletaddress, mintaddress) DO NOTHING
"""


def persist_swap(conn, swap: dict):
    with conn.cursor() as cur:
        cur.execute(UPSERT_WALLET, (swap["walletaddress"],))
        cur.execute(INSERT_TX, (
            swap["time"],
            swap["signature"],
            swap["walletaddress"],
            swap["mintaddress"],
            swap["side"],
            swap["amounttoken"],
            swap["amountsol"],
            swap["pricesol"],
            swap.get("slot"),
            swap.get("amm"),
            swap.get("source"),
        ))
        # Encolar reducers
        cur.execute(ENQUEUE_REDUCER, (
            "position_update", swap["walletaddress"], swap["mintaddress"],
            swap["signature"], 10,
        ))
        cur.execute(ENQUEUE_REDUCER, (
            "wallet_pnl_update", swap["walletaddress"], None,
            swap["signature"], 8,
        ))
        cur.execute(ENQUEUE_REDUCER, (
            "token_trader_update", swap["walletaddress"], swap["mintaddress"],
            swap["signature"], 8,
        ))
        cur.execute(ENQUEUE_REDUCER, (
            "classification_update", swap["walletaddress"], None,
            swap["signature"], 3,
        ))


def process_event(event, known_mints, stats):
    conn = db_connect()
    event_id = event["id"]
    signature = event["signature"]
    source = event["source"]

    try:
        parser = get_parser(source)
        if not parser:
            mark_staging(conn, event_id, "ignored", f"no parser for source={source}")
            stats["ignored"] += 1
            return

        tx = rpc_get_transaction(signature)
        if tx is None:
            mark_staging(conn, event_id, "error", "tx not found or not finalized")
            stats["errors"] += 1
            return

        swaps = parser(tx, source, known_mints)

        if not swaps:
            mark_staging(conn, event_id, "ignored", "no swaps extracted or mint unknown")
            stats["ignored"] += 1
            return

        for swap in swaps:
            if not swap.get("signature"):
                swap["signature"] = signature
            persist_swap(conn, swap)

        mark_staging(conn, event_id, "parsed")
        stats["parsed"] += 1
        stats["swaps"] += len(swaps)

    except Exception as e:
        log.exception("error procesando event_id=%s sig=%s: %s", event_id, signature, e)
        try:
            mark_staging(conn, event_id, "error", str(e)[:500])
        except Exception:
            pass
        stats["errors"] += 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. tx-parser no arranca.")
        return

    log.info("tx-parser iniciando. workers=%s batch=%s poll=%ss sources=%s",
             PARSER_WORKERS, PARSER_BATCH_SIZE, PARSER_POLL_SECONDS,
             supported_sources())

    stats = {"parsed": 0, "ignored": 0, "errors": 0, "swaps": 0}
    last_hb = 0
    mints_refresh_ts = 0
    known_mints = set()

    executor = ThreadPoolExecutor(max_workers=max(1, PARSER_WORKERS))

    while not STOP:
        conn = db_connect()
        try:
            # Refresh known_mints cada 30s
            if time.time() - mints_refresh_ts > 30:
                known_mints = fetch_known_mints(conn)
                mints_refresh_ts = time.time()

            batch = claim_batch(conn, PARSER_BATCH_SIZE)

            if not batch:
                conn.close()
                time.sleep(PARSER_POLL_SECONDS)
                continue

            futures = [
                executor.submit(process_event, dict(ev), known_mints, stats)
                for ev in batch
            ]
            for f in futures:
                try:
                    f.result(timeout=30)
                except Exception as e:
                    log.error("worker error: %s", e)

            if time.time() - last_hb >= 15:
                heartbeat(conn, {
                    "status": "running",
                    "parsed": stats["parsed"],
                    "ignored": stats["ignored"],
                    "errors": stats["errors"],
                    "swaps": stats["swaps"],
                    "known_mints": len(known_mints),
                })
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
    log.warning("tx-parser detenido. stats=%s", stats)


if __name__ == "__main__":
    main()