"""
position_reducer.py
Consume reducer_queue (event_type='position_update') y recalcula walletpositions
desde wallettransactions para cada (walletaddress, mintaddress) afectado.

Estrategia:
  - Lock optimista por (wallet, mint) via pg_try_advisory_xact_lock.
  - Recalculo full desde wallettransactions (seguro ante reorgs y reintentos).
  - UPSERT en walletpositions.
"""
import json
import time
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from shared_config import (
    DBCONFIG,
    REALTIME_ENABLED,
    REDUCER_BATCH_SIZE,
    REDUCER_WORKERS,
    getlogger,
)

log = getlogger("position-reducer")

PROCESS_NAME = "position-reducer"
EVENT_TYPE = "position_update"
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Señal %s recibida, apagando position-reducer...", signum)


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
    """
    Toma eventos position_update pending, los marca 'processing' y los devuelve.
    Deduplica por (walletaddress, mintaddress) dentro del batch para no recalcular N veces lo mismo.
    """
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


def _advisory_key(wallet: str, mint: str) -> int:
    """
    Genera un int64 estable para usar en pg_try_advisory_xact_lock.
    Usamos hashtext(wallet||mint) en SQL para no colisionar con Python hash.
    """
    return None  # se calcula server-side (abajo)


RECOMPUTE_SQL = """
WITH
buys AS (
    SELECT
        COALESCE(SUM(amounttoken), 0)::double precision AS total_bought_token,
        COALESCE(SUM(amountsol), 0)::double precision   AS total_bought_sol,
        COUNT(*)                                         AS buy_count,
        MIN(time)                                        AS first_buy_at,
        MAX(time)                                        AS last_buy_at
    FROM wallettransactions
    WHERE walletaddress = %(wallet)s
      AND mintaddress = %(mint)s
      AND side = 'buy'
),
sells AS (
    SELECT
        COALESCE(SUM(amounttoken), 0)::double precision AS total_sold_token,
        COALESCE(SUM(amountsol), 0)::double precision   AS total_sold_sol,
        COUNT(*)                                         AS sell_count,
        MAX(time)                                        AS last_sell_at
    FROM wallettransactions
    WHERE walletaddress = %(wallet)s
      AND mintaddress = %(mint)s
      AND side = 'sell'
),
last_tx AS (
    SELECT MAX(time) AS last_tx_at
    FROM wallettransactions
    WHERE walletaddress = %(wallet)s
      AND mintaddress = %(mint)s
),
agg AS (
    SELECT
        b.total_bought_token,
        b.total_bought_sol,
        b.buy_count,
        b.first_buy_at,
        b.last_buy_at,
        s.total_sold_token,
        s.total_sold_sol,
        s.sell_count,
        s.last_sell_at,
        l.last_tx_at,
        (b.total_bought_token - s.total_sold_token) AS current_amount,
        CASE WHEN b.total_bought_token > 0
             THEN b.total_bought_sol / b.total_bought_token
             ELSE 0 END AS avg_buy_price_sol,
        CASE WHEN s.total_sold_token > 0
             THEN s.total_sold_sol / s.total_sold_token
             ELSE 0 END AS avg_sell_price_sol,
        -- realized PnL FIFO-simplificado: (avg_sell - avg_buy) * cantidad vendida
        CASE
          WHEN s.total_sold_token > 0 AND b.total_bought_token > 0 THEN
            s.total_sold_sol
            - (s.total_sold_token * (b.total_bought_sol / b.total_bought_token))
          ELSE 0
        END AS realized_pnl_sol
    FROM buys b, sells s, last_tx l
)
INSERT INTO walletpositions (
    walletaddress,
    mintaddress,
    amounttoken,
    investedsol,
    realizedsol,
    total_bought_token,
    total_sold_token,
    total_bought_sol,
    total_sold_sol,
    avg_buy_price_sol,
    avg_sell_price_sol,
    realized_pnl_sol,
    unrealized_pnl_sol,
    total_pnl_sol,
    status,
    first_buy_at,
    last_buy_at,
    last_sell_at,
    last_tx_at,
    lastupdate,
    updated_at
)
SELECT
    %(wallet)s,
    %(mint)s,
    GREATEST(a.current_amount, 0),
    a.total_bought_sol,
    a.total_sold_sol,
    a.total_bought_token,
    a.total_sold_token,
    a.total_bought_sol,
    a.total_sold_sol,
    a.avg_buy_price_sol,
    a.avg_sell_price_sol,
    a.realized_pnl_sol,
    0::double precision,                      -- unrealized: se actualizará cuando haya price feed
    a.realized_pnl_sol,                       -- total = realized hasta tener unrealized
    CASE
      WHEN a.current_amount <= 0 AND (a.buy_count + a.sell_count) > 0 THEN 'closed'
      WHEN (a.buy_count + a.sell_count) = 0 THEN 'empty'
      ELSE 'open'
    END,
    a.first_buy_at,
    a.last_buy_at,
    a.last_sell_at,
    a.last_tx_at,
    NOW(),
    NOW()
FROM agg a
ON CONFLICT (walletaddress, mintaddress) DO UPDATE SET
    amounttoken         = EXCLUDED.amounttoken,
    investedsol         = EXCLUDED.investedsol,
    realizedsol         = EXCLUDED.realizedsol,
    total_bought_token  = EXCLUDED.total_bought_token,
    total_sold_token    = EXCLUDED.total_sold_token,
    total_bought_sol    = EXCLUDED.total_bought_sol,
    total_sold_sol      = EXCLUDED.total_sold_sol,
    avg_buy_price_sol   = EXCLUDED.avg_buy_price_sol,
    avg_sell_price_sol  = EXCLUDED.avg_sell_price_sol,
    realized_pnl_sol    = EXCLUDED.realized_pnl_sol,
    unrealized_pnl_sol  = EXCLUDED.unrealized_pnl_sol,
    total_pnl_sol       = EXCLUDED.total_pnl_sol,
    status              = EXCLUDED.status,
    first_buy_at        = COALESCE(walletpositions.first_buy_at, EXCLUDED.first_buy_at),
    last_buy_at         = EXCLUDED.last_buy_at,
    last_sell_at        = EXCLUDED.last_sell_at,
    last_tx_at          = EXCLUDED.last_tx_at,
    lastupdate          = NOW(),
    updated_at          = NOW();
"""


def recompute_position(conn, wallet: str, mint: str) -> bool:
    """
    Recalcula walletpositions(wallet, mint) tomando un advisory lock
    para evitar conflictos concurrentes sobre la misma posición.
    """
    with conn.cursor() as cur:
        # advisory lock por (wallet||mint)
        cur.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
            (f"{wallet}::{mint}",),
        )
        got = cur.fetchone()[0]
        if not got:
            # Otro worker ya está recalculando esta posición; dejamos el evento
            # en 'processing' y se reintentará después (o el otro ya lo cubre).
            conn.rollback()
            return False

        cur.execute(RECOMPUTE_SQL, {"wallet": wallet, "mint": mint})
    conn.commit()
    return True


def process_event(event_rows_for_pair, stats):
    """
    event_rows_for_pair: lista de eventos para una misma (wallet, mint).
    Hacemos UN recálculo y cerramos todos esos events.
    """
    if not event_rows_for_pair:
        return

    wallet = event_rows_for_pair[0]["walletaddress"]
    mint = event_rows_for_pair[0]["mintaddress"]
    ids = [e["id"] for e in event_rows_for_pair]

    conn = db_connect()
    try:
        if not wallet or not mint:
            mark_queue(conn, ids, "ignored", "missing wallet or mint")
            stats["ignored"] += len(ids)
            return

        ok = recompute_position(conn, wallet, mint)
        if ok:
            mark_queue(conn, ids, "done")
            stats["processed"] += len(ids)
            stats["positions_updated"] += 1
        else:
            # Deferir: volvemos a pending para reintentar
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE reducer_queue
                       SET status = 'pending',
                           updated_at = NOW()
                     WHERE id = ANY(%s)
                    """,
                    (ids,),
                )
            conn.commit()
            stats["deferred"] += len(ids)
    except Exception as e:
        log.exception("error recomputando %s/%s: %s", wallet, mint, e)
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


def group_by_pair(events):
    """Agrupa eventos por (wallet, mint) para recalcular una sola vez por par."""
    groups = {}
    for ev in events:
        key = (ev.get("walletaddress"), ev.get("mintaddress"))
        groups.setdefault(key, []).append(ev)
    return list(groups.values())


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. position-reducer no arranca.")
        return

    log.info(
        "position-reducer iniciando. workers=%s batch=%s",
        REDUCER_WORKERS, REDUCER_BATCH_SIZE,
    )

    stats = {"processed": 0, "positions_updated": 0,
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

            groups = group_by_pair(batch)
            log.info("batch=%s pairs=%s", len(batch), len(groups))

            futures = [
                executor.submit(process_event, grp, stats)
                for grp in groups
            ]
            for f in futures:
                try:
                    f.result(timeout=60)
                except Exception as e:
                    log.error("worker error: %s", e)

            if time.time() - last_hb >= 15:
                heartbeat(conn, {
                    "status": "running",
                    **stats,
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
    log.warning("position-reducer detenido. stats=%s", stats)


if __name__ == "__main__":
    main()