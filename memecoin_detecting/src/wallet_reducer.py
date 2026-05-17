"""
wallet_reducer.py
Consume reducer_queue (event_type='wallet_pnl_update') y recalcula walletpnlcache
para cada walletaddress afectada a partir de walletpositions y wallettransactions.

Estrategia:
  - Advisory lock por wallet para evitar recálculos concurrentes.
  - Recalculo completo: es barato si indexamos bien y resistente a reorgs.
  - UPSERT con TODAS las métricas que lee el apiserver.py actual.
"""
import json
import time
import signal
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

log = getlogger("wallet-reducer")

PROCESS_NAME = "wallet-reducer"
EVENT_TYPE = "wallet_pnl_update"
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Señal %s recibida, apagando wallet-reducer...", signum)


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


def claim_batch(conn, limit: int) -> list:
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


def mark_queue(conn, event_ids, status, error=None):
    if not event_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE reducer_queue
               SET status = %s,
                   last_error = %s,
                   processed_at = NOW(),
                   updated_at = NOW()
             WHERE id = ANY(%s)
            """,
            (status, error, list(event_ids)),
        )
    conn.commit()


def requeue(conn, event_ids):
    if not event_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE reducer_queue
               SET status = 'pending',
                   updated_at = NOW()
             WHERE id = ANY(%s)
            """,
            (list(event_ids),),
        )
    conn.commit()


# ------------------------------------------------------------------
# Recálculo de PnL por wallet
# ------------------------------------------------------------------
RECOMPUTE_SQL = """
WITH
positions AS (
    SELECT
        walletaddress,
        mintaddress,
        amounttoken,
        realized_pnl_sol,
        unrealized_pnl_sol,
        total_bought_sol,
        total_sold_sol,
        status
    FROM walletpositions
    WHERE walletaddress = %(wallet)s
),
tx_agg AS (
    SELECT
        COUNT(*)                                         AS tradecount,
        COUNT(*) FILTER (WHERE side = 'buy')             AS buycount,
        COUNT(*) FILTER (WHERE side = 'sell')            AS sellcount,
        COUNT(DISTINCT mintaddress)                       AS tokenstraded,
        MIN(time)                                         AS firstactivity,
        MAX(time)                                         AS lastactivity
    FROM wallettransactions
    WHERE walletaddress = %(wallet)s
),
-- PnL "por trade" aproximado: para cada sell, usamos (pricesol sell - avg_buy) * amount
-- Aproximación: usamos por-mint realized/sell_count para best/worst "trade".
best_worst AS (
    SELECT
        COALESCE(MAX(realized_pnl_sol), 0)::double precision AS besttrade_sol,
        COALESCE(MIN(realized_pnl_sol), 0)::double precision AS worsttrade_sol
    FROM positions
    WHERE realized_pnl_sol IS NOT NULL
),
agg AS (
    SELECT
        COALESCE(SUM(p.realized_pnl_sol), 0)::double precision   AS realized_sol,
        COALESCE(SUM(p.unrealized_pnl_sol), 0)::double precision AS unrealized_sol,
        COALESCE(SUM(p.total_bought_sol), 0)::double precision   AS invested_sol,
        COALESCE(SUM(p.total_sold_sol), 0)::double precision     AS total_sold_sol,
        COUNT(*) FILTER (WHERE p.status = 'open')                AS openpositions,
        COUNT(*) FILTER (WHERE p.status = 'closed' AND p.realized_pnl_sol > 0) AS winning_closed,
        COUNT(*) FILTER (WHERE p.status = 'closed')              AS total_closed
    FROM positions p
)
INSERT INTO walletpnlcache (
    walletaddress,
    totalpnl_sol,
    realizedpnl_sol,
    unrealizedpnl_sol,
    totalpnlusd,
    realizedpnlusd,
    unrealizedpnlusd,
    invested_sol,
    realized_sol,
    tradecount,
    buycount,
    sellcount,
    tokenstraded,
    openpositions,
    winrate,
    roipct,
    besttrade_sol,
    worsttrade_sol,
    firstactivity,
    lastactivity,
    source,
    lastupdated
)
SELECT
    %(wallet)s,
    (a.realized_sol + a.unrealized_sol),
    a.realized_sol,
    a.unrealized_sol,
    0::double precision,                              -- USD: Fase 6 con price feed
    0::double precision,
    0::double precision,
    a.invested_sol,
    a.total_sold_sol,
    COALESCE(t.tradecount, 0),
    COALESCE(t.buycount, 0),
    COALESCE(t.sellcount, 0),
    COALESCE(t.tokenstraded, 0),
    a.openpositions,
    CASE WHEN a.total_closed > 0
         THEN (a.winning_closed::double precision / a.total_closed) * 100.0
         ELSE 0 END,
    CASE WHEN a.invested_sol > 0
         THEN ((a.realized_sol + a.unrealized_sol) / a.invested_sol) * 100.0
         ELSE 0 END,
    bw.besttrade_sol,
    bw.worsttrade_sol,
    t.firstactivity,
    t.lastactivity,
    'local',
    NOW()
FROM agg a, tx_agg t, best_worst bw
ON CONFLICT (walletaddress) DO UPDATE SET
    totalpnl_sol       = EXCLUDED.totalpnl_sol,
    realizedpnl_sol    = EXCLUDED.realizedpnl_sol,
    unrealizedpnl_sol  = EXCLUDED.unrealizedpnl_sol,
    invested_sol       = EXCLUDED.invested_sol,
    realized_sol       = EXCLUDED.realized_sol,
    tradecount         = EXCLUDED.tradecount,
    buycount           = EXCLUDED.buycount,
    sellcount          = EXCLUDED.sellcount,
    tokenstraded       = EXCLUDED.tokenstraded,
    openpositions      = EXCLUDED.openpositions,
    winrate            = EXCLUDED.winrate,
    roipct             = EXCLUDED.roipct,
    besttrade_sol      = EXCLUDED.besttrade_sol,
    worsttrade_sol     = EXCLUDED.worsttrade_sol,
    firstactivity      = COALESCE(walletpnlcache.firstactivity, EXCLUDED.firstactivity),
    lastactivity       = EXCLUDED.lastactivity,
    source             = 'local',
    lastupdated        = NOW();
"""


def recompute_wallet(conn, wallet: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
            (f"wallet::{wallet}",),
        )
        got = cur.fetchone()[0]
        if not got:
            conn.rollback()
            return False

        cur.execute(RECOMPUTE_SQL, {"wallet": wallet})
    conn.commit()
    return True


# ------------------------------------------------------------------
# Workers
# ------------------------------------------------------------------
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

        ok = recompute_wallet(conn, wallet)
        if ok:
            mark_queue(conn, ids, "done")
            stats["processed"] += len(ids)
            stats["wallets_updated"] += 1
        else:
            requeue(conn, ids)
            stats["deferred"] += len(ids)
    except Exception as e:
        log.exception("error recomputando wallet %s: %s", wallet, e)
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
        log.warning("REALTIME_ENABLED=false. wallet-reducer no arranca.")
        return

    log.info(
        "wallet-reducer iniciando. workers=%s batch=%s",
        REDUCER_WORKERS, REDUCER_BATCH_SIZE,
    )

    stats = {"processed": 0, "wallets_updated": 0,
             "ignored": 0, "deferred": 0, "errors": 0}
    last_hb = 0

    executor = ThreadPoolExecutor(max_workers=max(1, REDUCER_WORKERS))

    while not STOP:
        conn = db_connect()
        try:
            batch = claim_batch(conn, REDUCER_BATCH_SIZE)

            if not batch:
                conn.close()
                time.sleep(1)
                continue

            groups = group_by_wallet(batch)
            log.info("batch=%s wallets=%s", len(batch), len(groups))

            futures = [
                executor.submit(process_group, grp, stats)
                for grp in groups
            ]
            for f in futures:
                try:
                    f.result(timeout=60)
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
    log.warning("wallet-reducer detenido. stats=%s", stats)


if __name__ == "__main__":
    main()