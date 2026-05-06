#!/usr/bin/env python3
"""
metrics_collector.py — Métricas de tokens 100% local.

- Para cada token en `tokens` is_active=TRUE:
    1. Lee su supply on-chain (getTokenSupply).
    2. Si tiene pool Raydium/Orca asociado (tabla `token_pools`), lee
       reservas del pool vía getMultipleAccounts.
    3. Deriva price (SOL), liquidity (SOL+token*price), volume_24h aproximado
       a partir de wallet_transactions recientes.
- Inserta en tokenmetrics (hypertable Timescale).
"""

import logging
import os
import time
from typing import Optional, List, Dict, Any

import psycopg2
import psycopg2.extras

from shared_config import DB_CONFIG
from rpc_helpers import (
    get_token_supply,
    get_token_account_balance,
    get_multiple_accounts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - metrics - %(levelname)s - %(message)s",
)
logger = logging.getLogger("metrics")

BATCH_SIZE    = int(os.getenv("METRICS_BATCH_SIZE",    "50"))
LOOP_INTERVAL = int(os.getenv("METRICS_LOOP_INTERVAL", "60"))   # segundos
SOL_MINT      = "So11111111111111111111111111111111111111112"


def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def list_tokens(conn) -> List[Dict[str, Any]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT t.token_id, t.mint_address,
               tp.pool_address, tp.base_vault, tp.quote_vault, tp.base_mint
        FROM tokens t
        LEFT JOIN token_pools tp ON tp.token_id = t.token_id
        WHERE t.is_active = TRUE
        ORDER BY t.last_updated NULLS FIRST
        LIMIT %s
    """, (BATCH_SIZE,))
    rows = cur.fetchall()
    cur.close()
    return rows


def compute_price_and_liquidity(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    price_sol   = sol_reserve / token_reserve
    liquidity   = sol_reserve * 2  (aprox en pools 50/50)
    """
    base_vault  = row.get("base_vault")
    quote_vault = row.get("quote_vault")
    base_mint   = row.get("base_mint")
    if not (base_vault and quote_vault):
        return {"price": None, "liquidity": None}

    try:
        b = get_token_account_balance(base_vault)
        q = get_token_account_balance(quote_vault)
        if not b or not q:
            return {"price": None, "liquidity": None}

        base_amt  = float(b.get("uiAmount")  or 0)
        quote_amt = float(q.get("uiAmount")  or 0)

        # Detectar cuál vault es SOL (WSOL)
        if base_mint == SOL_MINT:
            sol_amt, tok_amt = base_amt, quote_amt
        else:
            sol_amt, tok_amt = quote_amt, base_amt

        if tok_amt <= 0 or sol_amt <= 0:
            return {"price": None, "liquidity": None}

        price = sol_amt / tok_amt
        liquidity = sol_amt * 2
        return {"price": price, "liquidity": liquidity}
    except Exception as e:
        logger.debug(f"price calc fallo: {e}")
        return {"price": None, "liquidity": None}


def compute_volume_24h(conn, token_id: int) -> float:
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(sol_amount), 0)
        FROM wallet_transactions
        WHERE token_id = %s
          AND time >= NOW() - INTERVAL '24 hours'
    """, (token_id,))
    v = cur.fetchone()[0]
    cur.close()
    return float(v or 0)


def insert_metric(conn, token_id: int, price, liquidity, volume24h, supply):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tokenmetrics (
            time, token_id, price, liquidity, volume_24h, supply
        ) VALUES (NOW(), %s, %s, %s, %s, %s)
    """, (token_id, price, liquidity, volume24h, supply))
    cur.execute("""
        UPDATE tokens
           SET last_updated = NOW()
         WHERE token_id = %s
    """, (token_id,))
    conn.commit()
    cur.close()


def main_loop():
    conn = db_connect()
    logger.info("Metrics collector iniciado (100% local)")
    while True:
        start = time.time()
        try:
            tokens = list_tokens(conn)
            logger.info(f"Procesando {len(tokens)} tokens")
            ok = 0
            for t in tokens:
                try:
                    pl = compute_price_and_liquidity(t)
                    supply_info = get_token_supply(t["mint_address"])
                    supply = float(supply_info.get("uiAmount") or 0) if supply_info else None
                    vol = compute_volume_24h(conn, t["token_id"])
                    insert_metric(
                        conn, t["token_id"],
                        pl["price"], pl["liquidity"], vol, supply,
                    )
                    if pl["price"] is not None:
                        ok += 1
                except Exception as e:
                    logger.debug(f"metric token {t['mint_address']}: {e}")
            logger.info(f"Precios calculados {ok}/{len(tokens)} tokens")
        except Exception as e:
            logger.error(f"Pase fallido: {e}")
            try:
                conn.rollback()
            except Exception:
                conn = db_connect()
        elapsed = time.time() - start
        time.sleep(max(0, LOOP_INTERVAL - elapsed))


if __name__ == "__main__":
    main_loop()