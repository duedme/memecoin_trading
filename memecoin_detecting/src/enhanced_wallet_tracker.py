#!/usr/bin/env python3
"""
enhanced_wallet_tracker.py — Tracker 100% local.

- Lee wallets desde `tracked_wallets` (o `wallets` is_active=TRUE).
- Polling periódico a getSignaturesForAddress (RPC_HTTP_URL) con cursor
  almacenado en DB.
- Para cada nueva firma, getTransaction → parsea balances de token →
  deriva buy/sell en SOL → inserta wallet_transactions.
- Depende de la función SQL processtransaction() para actualizar
  wallet_positions y stats agregados.
"""

import logging
import os
import time
from typing import Optional, List, Dict, Any

import psycopg2
import psycopg2.extras

from shared_config import DB_CONFIG
from rpc_helpers import get_signatures_for_address, get_transaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - wallet-tracker - %(levelname)s - %(message)s",
)
logger = logging.getLogger("wallet-tracker")

POLL_INTERVAL        = int(os.getenv("TRACKER_POLL_INTERVAL", "20"))   # segundos
SIG_BATCH            = int(os.getenv("TRACKER_SIG_BATCH",     "50"))
MAX_WALLETS_PER_PASS = int(os.getenv("TRACKER_MAX_WALLETS",   "500"))


def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def list_wallets(conn) -> List[Dict[str, Any]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Prioriza tracked_wallets; si la tabla está vacía, cae a wallets activos
    cur.execute("""
        SELECT wallet_id, wallet_address, last_signature
        FROM tracked_wallets
        WHERE is_enabled = TRUE
        ORDER BY last_checked NULLS FIRST
        LIMIT %s
    """, (MAX_WALLETS_PER_PASS,))
    rows = cur.fetchall()
    if not rows:
        cur.execute("""
            SELECT wallet_id, wallet_address, NULL::text AS last_signature
            FROM wallets
            WHERE is_active = TRUE
            ORDER BY last_seen DESC NULLS LAST
            LIMIT %s
        """, (MAX_WALLETS_PER_PASS,))
        rows = cur.fetchall()
    cur.close()
    return rows


def update_cursor(conn, wallet_id: int, signature: str):
    cur = conn.cursor()
    cur.execute("""
        UPDATE tracked_wallets
           SET last_signature = %s,
               last_checked   = NOW()
         WHERE wallet_id = %s
    """, (signature, wallet_id))
    conn.commit()
    cur.close()


def parse_swap(tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Heurística mínima para clasificar una tx como buy/sell de un SPL contra SOL.
    Devuelve dict con mint, tx_type, token_amount, sol_amount, price.
    """
    try:
        meta = tx.get("meta") or {}
        if meta.get("err") is not None:
            return None
        pre_sol  = meta.get("preBalances")  or []
        post_sol = meta.get("postBalances") or []
        pre_tok  = meta.get("preTokenBalances")  or []
        post_tok = meta.get("postTokenBalances") or []
        if not pre_sol or not post_sol:
            return None

        # Delta SOL cuenta 0 (fee payer) en lamports → SOL
        sol_delta = (post_sol[0] - pre_sol[0]) / 1_000_000_000

        # Delta token por mint (agregado)
        token_deltas: Dict[str, float] = {}
        pre_map = {(b["accountIndex"], b["mint"]):
                   float(b["uiTokenAmount"].get("uiAmount") or 0) for b in pre_tok}
        for b in post_tok:
            key = (b["accountIndex"], b["mint"])
            post_amt = float(b["uiTokenAmount"].get("uiAmount") or 0)
            delta = post_amt - pre_map.get(key, 0.0)
            if delta != 0:
                token_deltas[b["mint"]] = token_deltas.get(b["mint"], 0.0) + delta

        if not token_deltas:
            return None

        # Elige el mint con mayor |delta|
        mint = max(token_deltas, key=lambda k: abs(token_deltas[k]))
        tok_delta = token_deltas[mint]

        if tok_delta > 0 and sol_delta < 0:
            tx_type = "buy"
            sol_amount = abs(sol_delta)
            token_amount = tok_delta
        elif tok_delta < 0 and sol_delta > 0:
            tx_type = "sell"
            sol_amount = abs(sol_delta)
            token_amount = abs(tok_delta)
        else:
            return None

        price = (sol_amount / token_amount) if token_amount > 0 else 0
        return {
            "mint":         mint,
            "tx_type":      tx_type,
            "token_amount": token_amount,
            "sol_amount":   sol_amount,
            "price":        price,
        }
    except Exception:
        return None


def persist_tx(conn, wallet_id: int, sig: str, block_time: Optional[int],
               parsed: Dict[str, Any]):
    cur = conn.cursor()
    # Asegura token_id
    cur.execute("""
        INSERT INTO tokens (mint_address, first_seen, is_active)
        VALUES (%s, NOW(), TRUE)
        ON CONFLICT (mint_address) DO NOTHING
    """, (parsed["mint"],))
    cur.execute("SELECT token_id FROM tokens WHERE mint_address = %s",
                (parsed["mint"],))
    token_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO wallet_transactions (
            wallet_id, token_id, signature, tx_type,
            token_amount, sol_amount, price, time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
        ON CONFLICT (signature) DO NOTHING
    """, (
        wallet_id, token_id, sig, parsed["tx_type"],
        parsed["token_amount"], parsed["sol_amount"], parsed["price"],
        block_time or int(time.time()),
    ))

    # Actualización transaccional de posiciones / stats
    try:
        cur.execute("SELECT process_transaction(%s)", (sig,))
    except Exception as e:
        logger.debug(f"process_transaction falló para {sig}: {e}")

    conn.commit()
    cur.close()


def process_wallet(conn, wallet: Dict[str, Any]):
    addr = wallet["wallet_address"]
    until = wallet.get("last_signature")
    sigs = get_signatures_for_address(addr, limit=SIG_BATCH, until=until)
    if not sigs:
        return
    # Procesar de más viejas a más nuevas, para que el cursor avance correcto
    sigs = list(reversed(sigs))
    latest_sig = None
    for s in sigs:
        sig = s.get("signature")
        if not sig:
            continue
        tx = get_transaction(sig)
        if not tx:
            continue
        parsed = parse_swap(tx)
        if parsed:
            persist_tx(conn, wallet["wallet_id"], sig,
                       s.get("blockTime"), parsed)
        latest_sig = sig
    if latest_sig:
        update_cursor(conn, wallet["wallet_id"], latest_sig)


def main_loop():
    conn = db_connect()
    logger.info("Tracker iniciado (100% local)")
    while True:
        try:
            wallets = list_wallets(conn)
            logger.info(f"Procesando {len(wallets)} wallets")
            for w in wallets:
                try:
                    process_wallet(conn, w)
                except Exception as e:
                    logger.warning(f"Wallet {w['wallet_address']}: {e}")
        except Exception as e:
            logger.error(f"Error en pase: {e}")
            try:
                conn.rollback()
            except Exception:
                conn = db_connect()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main_loop()