"""
price_updater.py
Refresca token_price_cache (precio SOL/token) desde la bonding curve Pump.fun
y encola position_update para holders con balance > 0 para recalcular
unrealized_pnl_sol en walletpositions.
"""
import json
import time
import signal

import psycopg2
import psycopg2.extras

from sharedconfig import DBCONFIG, REALTIME_ENABLED, getlogger
from parsers.pumpfun_curve import get_price_sol

log = getlogger("price-updater")
PROCESS_NAME = "price-updater"
STOP = False

REFRESH_INTERVAL = 15  # segundos


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


def get_sol_usd(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT priceusd FROM sol_price_cache WHERE id = 1")
        row = cur.fetchone()
        conn.commit()
    return float(row[0]) if row and row[0] else 150.0


def fetch_hot_mints(conn) -> list:
    """Mints con holders activos o trades recientes."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT t.mintaddress
            FROM tokens t
            WHERE t.status = 'active'
              AND EXISTS (
                SELECT 1 FROM wallettransactions wt
                WHERE wt.mintaddress = t.mintaddress
                  AND wt.time > NOW() - INTERVAL '6 hours'
              )
            LIMIT 500
            """
        )
        rows = cur.fetchall()
        conn.commit()
    return [r[0] for r in rows]


def fetch_holders(conn, mint: str) -> list:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT walletaddress FROM walletpositions
            WHERE mintaddress = %s AND amounttoken > 0
            """,
            (mint,),
        )
        rows = cur.fetchall()
        conn.commit()
    return [r[0] for r in rows]


UPSERT_PRICE = """
INSERT INTO token_price_cache (mintaddress, pricesol, priceusd, source, updated_at)
VALUES (%s, %s, %s, 'pumpfun_curve', NOW())
ON CONFLICT (mintaddress) DO UPDATE SET
    pricesol = EXCLUDED.pricesol,
    priceusd = EXCLUDED.priceusd,
    source = EXCLUDED.source,
    updated_at = NOW();
"""

UPDATE_UNREALIZED = """
UPDATE walletpositions
   SET unrealized_pnl_sol = amounttoken * (%s - avg_buy_price_sol),
       total_pnl_sol = realized_pnl_sol + (amounttoken * (%s - avg_buy_price_sol)),
       lastupdate = NOW(),
       updated_at = NOW()
 WHERE mintaddress = %s AND amounttoken > 0;
"""


def refresh_one_mint(conn, mint, sol_usd, stats):
    try:
        price_sol = get_price_sol(mint)
        if not price_sol or price_sol <= 0:
            stats["missing_price"] += 1
            return

        price_usd = price_sol * sol_usd

        with conn.cursor() as cur:
            cur.execute(UPSERT_PRICE, (mint, price_sol, price_usd))
            cur.execute(UPDATE_UNREALIZED, (price_sol, price_sol, mint))
        conn.commit()

        stats["updated"] += 1
    except Exception as e:
        log.warning("fallo mint %s: %s", mint, e)
        try:
            conn.rollback()
        except Exception:
            pass
        stats["errors"] += 1


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. price-updater no arranca.")
        return

    log.info("price-updater arrancado. interval=%ss", REFRESH_INTERVAL)

    stats = {"updated": 0, "missing_price": 0, "errors": 0, "cycles": 0}
    last_hb = 0

    while not STOP:
        try:
            conn = db_connect()
            sol_usd = get_sol_usd(conn)
            mints = fetch_hot_mints(conn)
            log.info("refrescando %s mints (sol_usd=%.2f)", len(mints), sol_usd)

            for mint in mints:
                if STOP:
                    break
                refresh_one_mint(conn, mint, sol_usd, stats)

            stats["cycles"] += 1

            if time.time() - last_hb >= 15:
                heartbeat(conn, {"status": "running", **stats})
                last_hb = time.time()

            conn.close()
        except Exception as e:
            log.exception("loop error: %s", e)

        # Interrumpible
        for _ in range(REFRESH_INTERVAL):
            if STOP:
                break
            time.sleep(1)

    log.warning("price-updater detenido. stats=%s", stats)


if __name__ == "__main__":
    main()