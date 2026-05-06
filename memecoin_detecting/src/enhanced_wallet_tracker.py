import time
import psycopg2
from datetime import datetime, timezone
from shared_config import (
    DB_CONFIG, TRACKER_POLL_INTERVAL, TRACKER_MAX_WALLETS,
    TRACKER_TX_LIMIT, get_logger,
)
from rpc_helpers import get_signatures_for_address, get_transaction

log = get_logger("wallet_tracker")

SELECT_TRACKED = """
    SELECT wallet_id, wallet_address, last_signature
      FROM tracked_wallets
     WHERE is_enabled = TRUE
     ORDER BY priority DESC, last_checked ASC
     LIMIT %s
"""

UPDATE_TRACKED_CURSOR = """
    UPDATE tracked_wallets
       SET last_signature = %s,
           last_checked   = NOW()
     WHERE wallet_id = %s
"""

UPSERT_WALLET = """
    INSERT INTO wallets (wallet_address, first_seen, last_seen)
    VALUES (%s, NOW(), NOW())
    ON CONFLICT (wallet_address) DO UPDATE
       SET last_seen = NOW()
    RETURNING id
"""

INSERT_TX = """
    INSERT INTO wallet_transactions
        (time, signature, wallet_address, mint_address,
         side, amount_token, amount_sol, price_sol)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

UPSERT_POSITION = """
    INSERT INTO wallet_positions
        (wallet_address, mint_address, amount_token, invested_sol, realized_sol, last_update)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (wallet_address, mint_address) DO UPDATE
       SET amount_token = wallet_positions.amount_token + EXCLUDED.amount_token,
           invested_sol = wallet_positions.invested_sol + EXCLUDED.invested_sol,
           realized_sol = wallet_positions.realized_sol + EXCLUDED.realized_sol,
           last_update  = NOW()
"""

def parse_swap(tx: dict):
    """
    Devuelve lista de dicts con: mint, side, amount_token, amount_sol, price_sol.
    Stub — conéctalo a tu parser real de Raydium/PumpFun.
    """
    return []

def process_wallet(conn, wallet_id: int, wallet_address: str, last_signature: str):
    sigs = get_signatures_for_address(wallet_address, limit=TRACKER_TX_LIMIT)
    if not sigs:
        return last_signature

    new_cursor = sigs[0]["signature"]
    stop_at = last_signature

    to_process = []
    for s in sigs:
        if stop_at and s["signature"] == stop_at:
            break
        to_process.append(s)

    with conn.cursor() as cur:
        for s in reversed(to_process):
            sig = s["signature"]
            tx  = get_transaction(sig)
            if not tx:
                continue
            block_time = tx.get("blockTime")
            ts = datetime.fromtimestamp(block_time, tz=timezone.utc) \
                 if block_time else datetime.now(timezone.utc)

            for swap in parse_swap(tx):
                cur.execute(UPSERT_WALLET, (wallet_address,))
                _ = cur.fetchone()

                amount_token = swap["amount_token"]
                amount_sol   = swap["amount_sol"]
                side         = swap["side"]

                if side == "buy":
                    amt_tok = +amount_token
                    inv_sol = +amount_sol
                    rea_sol = 0.0
                else:
                    amt_tok = -amount_token
                    inv_sol = 0.0
                    rea_sol = +amount_sol

                cur.execute(
                    INSERT_TX,
                    (ts, sig, wallet_address, swap["mint"], side,
                     amount_token, amount_sol, swap["price_sol"]),
                )
                cur.execute(
                    UPSERT_POSITION,
                    (wallet_address, swap["mint"], amt_tok, inv_sol, rea_sol),
                )

    return new_cursor

def run_once():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        cur.execute(SELECT_TRACKED, (TRACKER_MAX_WALLETS,))
        tracked = cur.fetchall()

    log.info("Tracker tick: %d wallets", len(tracked))
    for wallet_id, addr, last_sig in tracked:
        try:
            new_cursor = process_wallet(conn, wallet_id, addr, last_sig)
            if new_cursor and new_cursor != last_sig:
                with conn.cursor() as cur:
                    cur.execute(UPDATE_TRACKED_CURSOR, (new_cursor, wallet_id))
        except Exception as e:
            log.exception("Error procesando wallet %s: %s", addr, e)

def main():
    log.info("Wallet tracker arrancando (local-only)...")
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("Error en wallet_tracker: %s", e)
        time.sleep(TRACKER_POLL_INTERVAL)

if __name__ == "__main__":
    main()