import time
import psycopg2
from datetime import datetime, timezone
from shared_config import DB_CONFIG, METRICS_POLL_INTERVAL, get_logger

log = get_logger("metrics_collector")

SELECT_ACTIVE_TOKENS = """
    SELECT mint_address, pool_address
      FROM tokens
     WHERE status = 'active'
     ORDER BY detected_at DESC
     LIMIT 500
"""

INSERT_METRICS = """
    INSERT INTO token_metrics
        (time, mint_address, price_usd, price_sol, liquidity_sol, market_cap_usd,
         volume_5m, volume_1h, volume_6h, volume_24h,
         change_5m, change_1h, change_6h, change_24h,
         txns_24h, makers_24h)
    VALUES (%s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s)
"""

def compute_metrics_for_pool(pool_address: str) -> dict:
    # Aquí va tu lectura real del pool (Raydium/PumpFun) desde el nodo local.
    # Devolvemos None si todavía no hay datos.
    return {}

def run_once():
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        cur.execute(SELECT_ACTIVE_TOKENS)
        rows = cur.fetchall()
        for mint, pool in rows:
            m = compute_metrics_for_pool(pool) or {}
            cur.execute(
                INSERT_METRICS,
                (
                    now, mint,
                    m.get("price_usd"), m.get("price_sol"),
                    m.get("liquidity_sol"), m.get("market_cap_usd"),
                    m.get("volume_5m"), m.get("volume_1h"),
                    m.get("volume_6h"), m.get("volume_24h"),
                    m.get("change_5m"), m.get("change_1h"),
                    m.get("change_6h"), m.get("change_24h"),
                    m.get("txns_24h"), m.get("makers_24h"),
                ),
            )
        log.info("Metrics tick: %d tokens procesados.", len(rows))

def main():
    log.info("Metrics collector arrancando (local-only)...")
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("Error en metrics_collector: %s", e)
        time.sleep(METRICS_POLL_INTERVAL)

if __name__ == "__main__":
    main()