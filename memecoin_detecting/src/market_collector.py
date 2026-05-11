"""
market_collector.py
Reemplazo del metricscollector original. Refresca tokenmarketcache por tiers:
  - hot:     mints con actividad en últimos 5 min  -> cada MARKET_HOT_INTERVAL s
  - active:  mints con actividad en últimas 6 h    -> cada MARKET_ACTIVE_INTERVAL s
  - monitor: mints con actividad en últimas 72 h   -> cada MARKET_MONITOR_INTERVAL s

Usa las funciones SQL helpers y el precio desde token_price_cache.
"""
import json
import time
import signal
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.extras

from sharedconfig import (
    DBCONFIG,
    REALTIME_ENABLED,
    MARKET_HOT_INTERVAL,
    MARKET_ACTIVE_INTERVAL,
    MARKET_MONITOR_INTERVAL,
    getlogger,
)

log = getlogger("market-collector")
PROCESS_NAME = "market-collector"
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Señal %s recibida, apagando market-collector...", signum)


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


TIER_QUERIES = {
    "hot":     "AND EXISTS (SELECT 1 FROM wallettransactions wt WHERE wt.mintaddress = t.mintaddress AND wt.time > NOW() - INTERVAL '5 minutes')",
    "active":  "AND EXISTS (SELECT 1 FROM wallettransactions wt WHERE wt.mintaddress = t.mintaddress AND wt.time > NOW() - INTERVAL '6 hours')",
    "monitor": "AND EXISTS (SELECT 1 FROM wallettransactions wt WHERE wt.mintaddress = t.mintaddress AND wt.time > NOW() - INTERVAL '72 hours')",
}


def fetch_mints_for_tier(conn, tier: str) -> list:
    clause = TIER_QUERIES[tier]
    sql = f"""
        SELECT t.id AS tokenid, t.mintaddress
        FROM tokens t
        WHERE t.status = 'active'
          {clause}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        conn.commit()
    return rows


REFRESH_ONE_MINT_SQL = """
WITH
prices AS (
    SELECT pricesol, priceusd, liquiditysol, marketcapusd
    FROM token_price_cache
    WHERE mintaddress = %(mint)s
),
p24 AS (
    SELECT pricesol AS price
    FROM wallettransactions
    WHERE mintaddress = %(mint)s
      AND time > NOW() - INTERVAL '24 hours'
    ORDER BY time ASC LIMIT 1
),
p6 AS (
    SELECT pricesol AS price
    FROM wallettransactions
    WHERE mintaddress = %(mint)s
      AND time > NOW() - INTERVAL '6 hours'
    ORDER BY time ASC LIMIT 1
),
p1 AS (
    SELECT pricesol AS price
    FROM wallettransactions
    WHERE mintaddress = %(mint)s
      AND time > NOW() - INTERVAL '1 hour'
    ORDER BY time ASC LIMIT 1
),
p5 AS (
    SELECT pricesol AS price
    FROM wallettransactions
    WHERE mintaddress = %(mint)s
      AND time > NOW() - INTERVAL '5 minutes'
    ORDER BY time ASC LIMIT 1
),
cur AS (
    SELECT pricesol AS price
    FROM wallettransactions
    WHERE mintaddress = %(mint)s
    ORDER BY time DESC LIMIT 1
)
INSERT INTO tokenmarketcache (
    tokenid, mintaddress,
    pricesol, priceusd,
    liquidity, liquiditysol,
    marketcap, marketcapusd,
    volume5m, volume1h, volume6h, volume24h,
    change5m, change1h, change6h, change24h,
    txns24h, makers24h,
    tier,
    source, lastupdated
)
SELECT
    %(tokenid)s,
    %(mint)s,
    COALESCE((SELECT pricesol FROM prices), (SELECT price FROM cur), 0),
    COALESCE((SELECT priceusd FROM prices), 0),
    COALESCE((SELECT liquiditysol FROM prices), 0),
    COALESCE((SELECT liquiditysol FROM prices), 0),
    COALESCE((SELECT marketcapusd FROM prices), 0),
    COALESCE((SELECT marketcapusd FROM prices), 0),
    token_volume_sol(%(mint)s, INTERVAL '5 minutes'),
    token_volume_sol(%(mint)s, INTERVAL '1 hour'),
    token_volume_sol(%(mint)s, INTERVAL '6 hours'),
    token_volume_sol(%(mint)s, INTERVAL '24 hours'),
    CASE WHEN (SELECT price FROM p5) > 0
         THEN (((SELECT price FROM cur) - (SELECT price FROM p5)) / (SELECT price FROM p5)) * 100
         ELSE 0 END,
    CASE WHEN (SELECT price FROM p1) > 0
         THEN (((SELECT price FROM cur) - (SELECT price FROM p1)) / (SELECT price FROM p1)) * 100
         ELSE 0 END,
    CASE WHEN (SELECT price FROM p6) > 0
         THEN (((SELECT price FROM cur) - (SELECT price FROM p6)) / (SELECT price FROM p6)) * 100
         ELSE 0 END,
    CASE WHEN (SELECT price FROM p24) > 0
         THEN (((SELECT price FROM cur) - (SELECT price FROM p24)) / (SELECT price FROM p24)) * 100
         ELSE 0 END,
    token_txns_count(%(mint)s, INTERVAL '24 hours'),
    token_makers_count(%(mint)s, INTERVAL '24 hours'),
    %(tier)s,
    'local',
    NOW()
ON CONFLICT (tokenid) DO UPDATE SET
    mintaddress  = EXCLUDED.mintaddress,
    pricesol     = EXCLUDED.pricesol,
    priceusd     = EXCLUDED.priceusd,
    liquidity    = EXCLUDED.liquidity,
    liquiditysol = EXCLUDED.liquiditysol,
    marketcap    = EXCLUDED.marketcap,
    marketcapusd = EXCLUDED.marketcapusd,
    volume5m     = EXCLUDED.volume5m,
    volume1h     = EXCLUDED.volume1h,
    volume6h     = EXCLUDED.volume6h,
    volume24h    = EXCLUDED.volume24h,
    change5m     = EXCLUDED.change5m,
    change1h     = EXCLUDED.change1h,
    change6h     = EXCLUDED.change6h,
    change24h    = EXCLUDED.change24h,
    txns24h      = EXCLUDED.txns24h,
    makers24h    = EXCLUDED.makers24h,
    tier         = EXCLUDED.tier,
    source       = 'local',
    lastupdated  = NOW();
"""


def refresh_mint(mint_row, tier, stats):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(REFRESH_ONE_MINT_SQL, {
                "tokenid": mint_row[0],
                "mint": mint_row[1],
                "tier": tier,
            })
        conn.commit()
        stats["refreshed"] += 1
    except Exception as e:
        log.error("error refresh mint %s: %s", mint_row[1], e)
        try:
            conn.rollback()
        except Exception:
            pass
        stats["errors"] += 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_tier(tier, stats):
    conn = db_connect()
    try:
        mints = fetch_mints_for_tier(conn, tier)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not mints:
        return

    log.info("tier=%s mints=%s", tier, len(mints))

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(refresh_mint, m, tier, stats) for m in mints]
        for f in futures:
            try:
                f.result(timeout=30)
            except Exception as e:
                log.error("refresh worker error: %s", e)


def main():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. market-collector no arranca.")
        return

    log.info(
        "market-collector arrancado. hot=%ss active=%ss monitor=%ss",
        MARKET_HOT_INTERVAL, MARKET_ACTIVE_INTERVAL, MARKET_MONITOR_INTERVAL,
    )

    stats = {"refreshed": 0, "errors": 0, "cycles_hot": 0,
             "cycles_active": 0, "cycles_monitor": 0}

    last_hot = 0
    last_active = 0
    last_monitor = 0
    last_hb = 0

    while not STOP:
        now = time.time()
        try:
            if now - last_hot >= MARKET_HOT_INTERVAL:
                run_tier("hot", stats)
                stats["cycles_hot"] += 1
                last_hot = now

            if now - last_active >= MARKET_ACTIVE_INTERVAL:
                run_tier("active", stats)
                stats["cycles_active"] += 1
                last_active = now

            if now - last_monitor >= MARKET_MONITOR_INTERVAL:
                run_tier("monitor", stats)
                stats["cycles_monitor"] += 1
                last_monitor = now

            if now - last_hb >= 15:
                conn = db_connect()
                try:
                    heartbeat(conn, {"status": "running", **stats})
                finally:
                    conn.close()
                last_hb = now

        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(2)

        time.sleep(1)

    log.warning("market-collector detenido. stats=%s", stats)


if __name__ == "__main__":
    main()