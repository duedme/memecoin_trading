#!/usr/bin/env python3
"""
detector_memecoins.py - Detector de memecoins via polling RPC v4

MODO BACKUP — Webhooks son el canal primario de detección.
Este script corre cada 60s como respaldo.

Cambios v4:
  - RPC: HELIUS_RPC_URL como primario (antes LOCAL_RPC_URL)
  - Fallback: LOCAL_RPC_URL si existe, sino solo Helius
  - Stagger entre AMMs: 1s (antes 0.5s) para no saturar rate limit
"""

import psycopg2
import time
from datetime import datetime
import logging
from typing import List, Dict, Optional, Set
from dotenv import load_dotenv

load_dotenv()

from shared_config import (
    DB_CONFIG, AMM_PROGRAMS, AMM_ADDRESSES,
    LOCAL_RPC_URL, HELIUS_RPC_URL, KNOWN_TOKEN_BLACKLIST
)
from rpc_helpers import SolanaRPC

# ── LOGGING ──
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh = logging.FileHandler("detector_memecoins.log")
fh.setFormatter(formatter)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)
logger.propagate = False

POLL_INTERVAL = 60
AMM_STAGGER = 1.0  # segundos entre AMMs (antes 0.5, subido para rate limit)


class MemeDetector:
    """
    Detector de memecoins via polling RPC — MODO BACKUP.
    Escanea 12 AMMs cada 60s buscando nuevos tokens.
    Webhooks detectan en real-time; este es el respaldo.
    """

    def __init__(self, db_config: Dict, rpc_url: str):
        self.db_config = db_config
        self.rpc = SolanaRPC(rpc_url)
        self.conn = None
        self.last_signatures: Dict[str, Optional[str]] = {
            addr: None for addr in AMM_ADDRESSES
        }
        self.seen_tokens: Set[str] = set()
        self.stats = {
            "cycles": 0, "tokens_detected": 0,
            "amm_breakdown": {name: 0 for name in AMM_PROGRAMS.values()},
            "errors": 0,
        }

    def connect_db(self):
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"DB conectada: {self.db_config['database']}@{self.db_config['host']}")
        except Exception as e:
            logger.error(f"Error DB: {e}")
            raise

    def safe_rollback(self):
        try:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
        except Exception:
            pass

    def extract_tokens_from_tx(self, tx: Dict, amm_address: str,
                                signature: str) -> List[Dict]:
        tokens = []
        try:
            meta = tx.get("meta", {})
            blocktime = tx.get("blockTime")
            post_balances = meta.get("postTokenBalances", [])
            seen_in_tx = set()
            for balance in post_balances:
                mint = balance.get("mint")
                if not mint or mint in seen_in_tx:
                    continue
                if mint in KNOWN_TOKEN_BLACKLIST:
                    continue
                if mint in self.seen_tokens:
                    continue
                seen_in_tx.add(mint)
                self.seen_tokens.add(mint)
                token_amount = balance.get("uiTokenAmount", {})
                amm_name = AMM_PROGRAMS.get(amm_address, "Unknown")
                tokens.append({
                    "mint_address": mint,
                    "decimals": token_amount.get("decimals", 9),
                    "amm": amm_name,
                    "created_at": datetime.fromtimestamp(blocktime) if blocktime else datetime.now(),
                    "signature": signature,
                })
            return tokens
        except Exception as e:
            logger.debug(f"Error extrayendo tokens: {e}")
            return []

    def save_token(self, token: Dict) -> Optional[int]:
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """INSERT INTO tokens
                    (mint_address, decimals, amm, created_at, detected_at,
                     creation_signature, status, retention_category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mint_address) DO NOTHING
                RETURNING token_id""",
                (token["mint_address"], token["decimals"], token["amm"],
                 token["created_at"], datetime.now(), token["signature"],
                 "active", "shortterm"),
            )
            result = cursor.fetchone()
            self.conn.commit()
            cursor.close()
            if result:
                token_id = result[0]
                self.stats["tokens_detected"] += 1
                self.stats["amm_breakdown"][token["amm"]] += 1
                logger.info(
                    f"✅ Token nuevo: {token['mint_address'][:16]}... "
                    f"{token['amm']} ID={token_id}"
                )
                return token_id
            return None
        except Exception as e:
            logger.debug(f"Error guardando token: {e}")
            self.safe_rollback()
            return None

    def scan_amm(self, amm_address: str):
        try:
            last_sig = self.last_signatures.get(amm_address)
            sigs_data = self.rpc.get_signatures_for_address(
                amm_address, limit=20, before=last_sig
            )
            if not sigs_data:
                return

            new_count = 0
            for sig_data in sigs_data:
                sig = sig_data.get("signature")
                if not sig:
                    continue
                if sig == last_sig:
                    break
                if sig_data.get("err") is not None:
                    continue
                tx = self.rpc.get_transaction(sig)
                if not tx:
                    continue
                tokens = self.extract_tokens_from_tx(tx, amm_address, sig)
                for token in tokens:
                    if self.save_token(token):
                        new_count += 1

            if sigs_data:
                self.last_signatures[amm_address] = sigs_data[0].get("signature")
            if new_count > 0:
                amm_name = AMM_PROGRAMS.get(amm_address, "Unknown")
                logger.info(f"  {amm_name}: {new_count} tokens nuevos detectados")
        except Exception as e:
            logger.debug(f"Error escaneando {amm_address[:8]}: {e}")
            self.stats["errors"] += 1
            self.safe_rollback()

    def log_stats(self):
        logger.info(
            f"Stats: Ciclos={self.stats['cycles']} "
            f"Tokens={self.stats['tokens_detected']} "
            f"Errores={self.stats['errors']}"
        )
        for amm_name, count in self.stats["amm_breakdown"].items():
            if count > 0:
                logger.info(f"  {amm_name}: {count}")

    def run(self):
        logger.info("=" * 70)
        logger.info("DETECTOR MEMECOINS v4 — MODO BACKUP (Helius RPC)")
        logger.info(f"Intervalo: {POLL_INTERVAL}s (webhooks son primarios)")
        logger.info(f"AMMs: {len(AMM_PROGRAMS)}")
        logger.info(f"RPC: {self.rpc.rpc_url[:60]}...")
        logger.info(f"Stagger entre AMMs: {AMM_STAGGER}s")
        logger.info("=" * 70)

        self.connect_db()
        try:
            while True:
                try:
                    for amm_address in AMM_ADDRESSES:
                        self.scan_amm(amm_address)
                        time.sleep(AMM_STAGGER)

                    self.stats["cycles"] += 1
                    if self.stats["cycles"] % 10 == 0:
                        self.log_stats()

                    time.sleep(POLL_INTERVAL)

                except psycopg2.OperationalError:
                    logger.warning("DB connection lost, reconectando...")
                    self.connect_db()
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error en loop principal: {e}")
                    self.stats["errors"] += 1
                    self.safe_rollback()
                    time.sleep(10)
        finally:
            if self.conn and not self.conn.closed:
                self.conn.close()
            logger.info("Detector detenido.")


if __name__ == "__main__":
    # Prioridad: Helius (siempre actualizado) > Local (puede estar atrasado)
    rpc_url = HELIUS_RPC_URL if HELIUS_RPC_URL and "api-key" in HELIUS_RPC_URL else LOCAL_RPC_URL
    detector = MemeDetector(db_config=DB_CONFIG, rpc_url=rpc_url)
    detector.run()
