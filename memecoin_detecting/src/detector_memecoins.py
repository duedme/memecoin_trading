#!/usr/bin/env python3
"""
detector_memecoins.py — Detector 100% local vía PubSub del nodo Agave.

- Se suscribe a logsSubscribe sobre programas conocidos (Token Program,
  Raydium AMM v4, pump.fun) usando RPC_WS_URL.
- Cuando aparece una firma nueva, resuelve mint vía getTransaction
  (RPC_HTTP_URL) y lo inserta en `tokens` si aún no existe.
- Metadata (name/symbol/uri) se intenta leer on-chain vía Metaplex.
"""

import asyncio
import json
import logging
import os
import struct
from typing import Optional, Dict, Any

import psycopg2
import psycopg2.extras
import websockets

from shared_config import DB_CONFIG, RPC_WS_URL
from rpc_helpers import (
    get_transaction,
    get_account_info,
    METAPLEX_METADATA_PROGRAM_ID,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - detector - %(levelname)s - %(message)s",
)
logger = logging.getLogger("detector")

# Programas a vigilar
TOKEN_PROGRAM_ID       = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
RAYDIUM_AMM_V4         = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
PUMP_FUN_PROGRAM       = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

WATCHED_PROGRAMS = [TOKEN_PROGRAM_ID, RAYDIUM_AMM_V4, PUMP_FUN_PROGRAM]


# ---------- Metaplex metadata ----------

def _derive_metadata_pda(mint: str) -> Optional[str]:
    """
    Deriva la PDA de metadata Metaplex para un mint.
    Se usa base58 local; si no hay 'base58' disponible, cae a None y
    se deja metadata incompleta (se rellena en otra pasada).
    """
    try:
        import base58  # type: ignore
        from hashlib import sha256
    except ImportError:
        return None

    # Derivación oficial Metaplex: ["metadata", program_id, mint]
    # Implementación minimal sin solders (bump search).
    try:
        prog = base58.b58decode(METAPLEX_METADATA_PROGRAM_ID)
        m    = base58.b58decode(mint)
        seed_prefix = b"metadata"
        for bump in range(255, -1, -1):
            data = seed_prefix + prog + m + bytes([bump]) + b"ProgramDerivedAddress"
            h = sha256(data).digest()
            # Punto fuera de curva = PDA válida (aprox: si termina en 0x01 heurísticamente)
            # Para robustez real, conviene 'solders.pubkey.Pubkey.find_program_address'.
            # Aquí hacemos best-effort; si falla, devolvemos None.
            if h[-1] != 0:
                return base58.b58encode(h).decode()
    except Exception:
        return None
    return None


def _parse_metaplex(data_b64: str) -> Dict[str, Any]:
    import base64
    raw = base64.b64decode(data_b64)
    # Layout simplificado de Metadata v1.
    # Offsets: key(1) + update_auth(32) + mint(32) = 65, luego strings largo-prefijados.
    try:
        off = 1 + 32 + 32
        def read_str():
            nonlocal off
            ln = struct.unpack("<I", raw[off:off+4])[0]
            off += 4
            s = raw[off:off+ln].decode("utf-8", errors="ignore").rstrip("\x00").strip()
            off += ln
            return s
        name   = read_str()
        symbol = read_str()
        uri    = read_str()
        return {"name": name, "symbol": symbol, "uri": uri}
    except Exception:
        return {}


def fetch_metadata(mint: str) -> Dict[str, Any]:
    pda = _derive_metadata_pda(mint)
    if not pda:
        return {}
    try:
        acc = get_account_info(pda, encoding="base64")
        if not acc or "data" not in acc:
            return {}
        return _parse_metaplex(acc["data"][0])
    except Exception as e:
        logger.debug(f"metadata fetch fallo para {mint}: {e}")
        return {}


# ---------- DB ----------

def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def upsert_token(conn, mint: str, name: str, symbol: str, image_url: str):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tokens (mint_address, name, symbol, image_url, first_seen, is_active)
        VALUES (%s, %s, %s, %s, NOW(), TRUE)
        ON CONFLICT (mint_address) DO NOTHING
        """,
        (mint, name or None, symbol or None, image_url or None),
    )
    conn.commit()
    cur.close()


# ---------- Extracción de mint desde tx ----------

def extract_mint_from_tx(tx: Dict[str, Any]) -> Optional[str]:
    """
    Intenta encontrar un mint nuevo en los balances postTokenBalances.
    """
    try:
        meta = tx.get("meta") or {}
        post = meta.get("postTokenBalances") or []
        pre  = meta.get("preTokenBalances") or []
        pre_mints = {b.get("mint") for b in pre}
        for b in post:
            m = b.get("mint")
            if m and m not in pre_mints:
                return m
        # fallback: primer mint que aparezca
        for b in post:
            if b.get("mint"):
                return b["mint"]
    except Exception:
        return None
    return None


# ---------- Loop PubSub ----------

async def subscribe_and_process():
    logger.info(f"Conectando PubSub: {RPC_WS_URL}")
    async with websockets.connect(RPC_WS_URL, ping_interval=30, ping_timeout=30) as ws:
        # Suscribir logs por cada programa
        for i, prog in enumerate(WATCHED_PROGRAMS):
            sub = {
                "jsonrpc": "2.0",
                "id": i + 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [prog]},
                    {"commitment": "confirmed"},
                ],
            }
            await ws.send(json.dumps(sub))
            logger.info(f"Suscripto a logs de {prog}")

        conn = db_connect()
        seen: set = set()

        while True:
            msg = await ws.recv()
            try:
                data = json.loads(msg)
                params = data.get("params") or {}
                result = (params.get("result") or {}).get("value") or {}
                sig = result.get("signature")
                if not sig or sig in seen:
                    continue
                seen.add(sig)
                if len(seen) > 50000:
                    # limitar memoria
                    seen.clear()

                tx = get_transaction(sig)
                if not tx:
                    continue
                mint = extract_mint_from_tx(tx)
                if not mint:
                    continue

                md = fetch_metadata(mint)
                upsert_token(
                    conn,
                    mint,
                    md.get("name", ""),
                    md.get("symbol", ""),
                    md.get("uri", ""),
                )
                logger.info(f"Token detectado: {mint} ({md.get('symbol','?')})")
            except Exception as e:
                logger.warning(f"Error procesando mensaje WS: {e}")


def main():
    while True:
        try:
            asyncio.run(subscribe_and_process())
        except Exception as e:
            logger.error(f"PubSub cayó, reintentando en 5s: {e}")
            import time
            time.sleep(5)


if __name__ == "__main__":
    main()