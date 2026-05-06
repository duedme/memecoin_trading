import time
import psycopg2
from shared_config import DB_CONFIG, DETECTOR_POLL_INTERVAL, get_logger
from rpc_helpers import get_slot

log = get_logger("detector")

# Programas AMM comunes en Solana para futuro filtrado fino.
AMM_PROGRAMS = {
    "raydium_v4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "pump_fun":   "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
}

def ensure_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn

def upsert_token(conn, mint: str, pool: str = None, amm: str = None,
                 symbol: str = None, name: str = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tokens (mint_address, symbol, name, pool_address, amm,
                                detected_at, created_at, status)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), 'active')
            ON CONFLICT (mint_address) DO UPDATE
               SET pool_address = COALESCE(EXCLUDED.pool_address, tokens.pool_address),
                   amm          = COALESCE(EXCLUDED.amm,          tokens.amm),
                   symbol       = COALESCE(EXCLUDED.symbol,       tokens.symbol),
                   name         = COALESCE(EXCLUDED.name,         tokens.name)
            """,
            (mint, symbol, name, pool, amm),
        )

def scan_once(conn):
    slot = get_slot()
    if slot is None:
        log.warning("No se pudo leer slot, reintento...")
        return 0
    # Placeholder: conecta aquí el escaneo real (logsSubscribe / programSubscribe,
    # getSignaturesForAddress por AMM, etc.). Mantenemos detector operativo.
    log.info("Detector OK. slot=%s", slot)
    return 0

def main():
    log.info("Detector arrancando (local-only)...")
    while True:
        try:
            with ensure_db() as conn:
                scan_once(conn)
        except Exception as e:
            log.exception("Error en detector: %s", e)
        time.sleep(DETECTOR_POLL_INTERVAL)

if __name__ == "__main__":
    main()