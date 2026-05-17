"""
token_trader_reducer.py
Consume reducer_queue (event_type='token_trader_update') y reconstruye
el ranking de top traders por mint en tokentoptraderscache, además del
agregado en tokeninvestorstatscache.

Estrategia:
  - Advisory lock por mint.
  - Recálculo full del ranking por mint (DELETE + INSERT dentro de transacción).
  - Límite configurable de top-N traders por token (default 100).
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

log = getlogger("token-trader-reducer")

PROCESS_NAME = "token-trader-reducer"
EVENT_TYPE = "token_trader_update"
TOP_N_TRADERS_PER_TOKEN = int(os.getenv("TOKEN_TOP_N_TRADERS", "100"))
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Señal %s recibida, apagando token-trader-reducer...", signum)


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
         RETURNING r.id, r.walletaddress, r.mintaddress, r.signature
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
# Recálculo de top traders por mint
# ------------------------------------------------------------------
DELETE_OLD_RANKING = """
DELETE FROM tokentoptraderscache
 WHERE mintaddress = %(mint)s;
"""

INSERT_RANKING = """
WITH base AS (
    SELECT
        wp.walletaddress,
        wp.mintaddress,
        wp.amounttoken             AS current_balance_token,
        wp.total_bought_sol,
        wp.total_sold_sol,
        wp.avg_buy_price_sol,
        wp.realized_pnl_sol,
        wp.unrealized_pnl_sol,
        COALESCE(wp.realized_pnl_sol, 0) + COALESCE(wp.unrealized_pnl_sol, 0) AS totalpnl_sol,
        wp.last_tx_at
    FROM walletpositions wp
    WHERE wp.mintaddress = %(mint)s
      AND (wp.total_bought_sol > 0 OR wp.total_sold_sol > 0)
),
tx_count AS (
    SELECT walletaddress, COUNT(*) AS tradecount
    FROM wallettransactions
    WHERE mintaddress = %(mint)s
    GROUP BY walletaddress
),
ranked AS (
    SELECT
        b.*,
        COALESCE(t.tradecount, 0) AS tradecount,
        (b.total_bought_sol + b.total_sold_sol) AS volumesol,
        ROW_NUMBER() OVER (
            ORDER BY b.totalpnl_sol DESC NULLS LAST,
                     (b.total_bought_sol + b.total_sold_sol) DESC
        ) AS rnk
    FROM base b
    LEFT JOIN tx_count t ON t.walletaddress = b.walletaddress
)
INSERT INTO tokentoptraderscache (
    mintaddress,
    tokenid,
    walletaddress,
    rank,
    totalpnl,
    totalpnl_sol,
    volumeusd,
    volumesol,
    tradecount,
    current_balance_token,
    avg_buy_price_sol,
    lastactivity,
    lastupdated,
    source
)
SELECT
    r.mintaddress,
    (SELECT id FROM tokens WHERE mintaddress = r.mintaddress LIMIT 1),
    r.walletaddress,
    r.rnk,
    r.totalpnl_sol,
    r.totalpnl_sol,
    0::double precision,
    r.volumesol,
    r.tradecount,
    r.current_balance_token,
    r.avg_buy_price_sol,
    r.last_tx_at,
    NOW(),
    'local'
FROM ranked r
WHERE r.rnk <= %(top_n)s;
"""

UPSERT_STATS = """
WITH holders AS (
    SELECT ttc.walletaddress,
           wc.behavior,
           wc.investortype,
           wc.investorscore,
           ttc.volumesol,
           ttc.totalpnl_sol
    FROM tokentoptraderscache ttc
    LEFT JOIN walletclassifications wc
           ON wc.walletaddress = ttc.walletaddress
    WHERE ttc.mintaddress = %(mint)s
)
INSERT INTO tokeninvestorstatscache (
    mintaddress,
    totalinvestors,
    elitecount,
    profitablecount,
    regularcount,
    casualcount,
    losingcount,
    humanscount,
    botscount,
    avgscore,
    totalvolume_sol,
    totalpnl_sol,
    lastupdated
)
SELECT
    %(mint)s,
    COUNT(*),
    COUNT(*) FILTER (WHERE investortype = 'elite'),
    COUNT(*) FILTER (WHERE investortype = 'profitable'),
    COUNT(*) FILTER (WHERE investortype = 'regular'),
    COUNT(*) FILTER (WHERE investortype = 'casual'),
    COUNT(*) FILTER (WHERE investortype = 'losing'),
    COUNT(*) FILTER (WHERE behavior = 'human'),
    COUNT(*) FILTER (WHERE behavior = 'bot'),
    COALESCE(AVG(investorscore::double precision), 0),
    COALESCE(SUM(volumesol), 0),
    COALESCE(SUM(totalpnl_sol), 0),
    NOW()
FROM holders
ON CONFLICT (mintaddress) DO UPDATE SET
    totalinvestors  = EXCLUDED.totalinvestors,
    elitecount      = EXCLUDED.elitecount,
    profitablecount = EXCLUDED.profitablecount,
    regularcount    = EXCLUDED.regularcount,
    casualcount     = EXCLUDED.casualcount,
    losingcount     = EXCLUDED.losingcount,
    humanscount     = EXCLUDED.humanscount,
    botscount       = EXCLUDED.botscount,
    avgscore        = EXCLUDED.avgscore,
    totalvolume_sol = EXCLUDED.totalvolume_sol,
    totalpnl_sol    = EXCLUDED.totalpnl_sol,
    lastupdated     = NOW();
"""


def recompute_token_ranking(conn, mint: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
            (f"token::{mint}",),
        )
        got = cur.fetchone()[0]
        if not got:
            conn.rollback()
            return False

        cur.execute(DELETE_OLD_RANKING, {"mint": mint})
        cur.execute(INSERT_RANKING, {"mint": mint, "top_n": TOP_N_TRADERS_PER_TOKEN})
        cur.execute(UPSERT_STATS, {"mint": mint})
    conn.commit()
    return True


# ------------------------------------------------------------------
# Workers
# ------------------------------------------------------------------
def process_group(events_for_mint, stats):
    if not events_for_mint:
        return

    mint = events_for_mint[0]["mintaddress"]
    ids = [e["id"] for e in events_for_mint]

    conn = db_connect()
    try:
        if not mint:
            mark_queue(conn, ids, "ignored", "missing mint")
            stats["ignored"] += len(ids)
            return

        ok = recompute_token_ranking(conn, mint)
        if ok:
            mark_queue(conn, ids, "done")
            stats["processed"] += len(ids)
            stats["tokens_updated"] += 1
        else:
            requeue(conn, ids)
            stats["deferred"] += len(ids)
    except Exception as e:
        log.exception("error recomputando token %s: %s", mint, e)
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


def group_by_mint(events):
    groups = {}
    for ev in events:
        key = ev.get("mintaddress")
        groups.setdefault(key, []).append(ev)
    return list(groups.values())


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. token-trader-reducer no arranca.")
        return

    log.info(
        "token-trader-reducer iniciando. workers=%s batch=%s top_n=%s",
        REDUCER_WORKERS, REDUCER_BATCH_SIZE, TOP_N_TRADERS_PER_TOKEN,
    )

    stats = {"processed": 0, "tokens_updated": 0,
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

            groups = group_by_mint(batch)
            log.info("batch=%s mints=%s", len(batch), len(groups))

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
    log.warning("token-trader-reducer detenido. stats=%s", stats)


if __name__ == "__main__":
    main()