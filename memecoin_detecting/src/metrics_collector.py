#!/usr/bin/env python3
"""
metrics_collector.py - Recolector de métricas v5

CAMBIOS v5 (vs v4):
- CALCULA PRECIO ON-CHAIN: lee vault + bonding curve → price_sol
- Soporta pump.fun bonding curves (lamports = SOL reserves)
- 2 batch calls extra por ciclo (vaults + pools) — costo mínimo
- parse_metric ahora usa price_data con precio real
"""

import os
import asyncio
import aiohttp
import json
import time
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

from shared_config import (
    DB_CONFIG, AMM_PROGRAMS, LOCAL_RPC_URL,
    HELIUS_RPC_URL, KNOWN_TOKEN_BLACKLIST
)

# ── LOGGING ──
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh = logging.FileHandler("metrics_collector.log")
fh.setFormatter(formatter)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)
logger.propagate = False

# ── CONSTANTES ──
TIERS = {
    "hot":     {"max_age_hours": 1,   "interval_seconds": 15,  "collect_holders": False},
    "active":  {"max_age_hours": 24,  "interval_seconds": 60,  "collect_holders": False},
    "monitor": {"max_age_hours": 168, "interval_seconds": 600, "collect_holders": True},
}
HOLDER_REFRESH_INTERVAL = 300

# ── RPC ENDPOINTS ──
RPC_ENDPOINTS = []
if LOCAL_RPC_URL:
    RPC_ENDPOINTS.append({"url": LOCAL_RPC_URL, "name": "Local"})
if HELIUS_RPC_URL and "api-key" in HELIUS_RPC_URL:
    RPC_ENDPOINTS.append({"url": HELIUS_RPC_URL, "name": "Helius"})
for env_key, name in [
    ("ALCHEMY_RPC_URL", "Alchemy"),
    ("QUICKNODE_RPC_URL", "QuickNode"),
    ("CHAINSTACK_RPC_URL", "Chainstack"),
]:
    url = os.getenv(env_key, "")
    if url and "" not in url and "tuapi" not in url.lower():
        RPC_ENDPOINTS.append({"url": url, "name": name})

MAX_ACCOUNTS_PER_BATCH = 100
MAX_CONCURRENT_RPC = 30
MAX_429_RETRIES = 3

# pump.fun bonding curve program ID
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


class MetricsCollector:
    def __init__(self):
        self.conn = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_RPC)
        self.tier_last_run = {tier: 0.0 for tier in TIERS}
        self.holder_cache: Dict[int, dict] = {}
        self.rpc_failures = defaultdict(int)
        self.rpc_latencies = defaultdict(list)
        self.stats = {
            "cycles": 0, "metrics_saved": 0,
            "rpc_calls_made": 0, "rpc_calls_saved": 0,
            "rpc_429s": 0, "errors": 0,
            "started_at": datetime.now().isoformat(),
        }

    # ── BD ──────────────────────────────────────────────────────────────

    def connect_db(self):
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
            self.conn = psycopg2.connect(**DB_CONFIG)
            logger.info(f"DB conectada: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
        except Exception as e:
            logger.error(f"Error DB: {e}")
            raise

    def safe_rollback(self):
        try:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
        except Exception:
            pass

    def get_active_tokens(self) -> Dict[str, List[dict]]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT token_id, mint_address, detected_at, amm, name, symbol
                FROM tokens
                WHERE status = 'active'
                  AND detected_at > NOW() - INTERVAL '7 days'
                ORDER BY detected_at DESC
            """)
            rows = cursor.fetchall()
            cursor.close()

            categorized = {"hot": [], "active": [], "monitor": []}
            now = datetime.now()
            for row in rows:
                age_hours = (now - row[2]).total_seconds() / 3600 if row[2] else 999
                token = {
                    "token_id": row[0], "mint_address": row[1],
                    "detected_at": row[2], "amm": row[3],
                    "name": row[4], "symbol": row[5],
                    "age_hours": age_hours,
                }
                if age_hours < TIERS["hot"]["max_age_hours"]:
                    categorized["hot"].append(token)
                elif age_hours < TIERS["active"]["max_age_hours"]:
                    categorized["active"].append(token)
                else:
                    categorized["monitor"].append(token)

            total = sum(len(v) for v in categorized.values())
            logger.info(
                f"Tokens: {len(categorized['hot'])} hot, "
                f"{len(categorized['active'])} active, "
                f"{len(categorized['monitor'])} monitor ({total} total)"
            )
            return categorized
        except Exception as e:
            logger.error(f"Error obteniendo tokens: {e}")
            self.safe_rollback()
            return {"hot": [], "active": [], "monitor": []}

    # ── RPC ─────────────────────────────────────────────────────────────

    async def rpc_call(self, method: str, params: list,
                       endpoint_idx: int = 0, retry_429: int = 0) -> Optional[dict]:
        if endpoint_idx >= len(RPC_ENDPOINTS):
            return None

        async with self.semaphore:
            ep = RPC_ENDPOINTS[endpoint_idx]
            payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            try:
                t0 = time.time()
                async with self.session.post(
                    ep["url"], json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 429:
                        self.stats["rpc_429s"] += 1
                        if retry_429 < MAX_429_RETRIES:
                            wait = float(resp.headers.get("Retry-After", 1.0))
                            logger.debug(f"429 de {ep['name']}, retry en {wait}s")
                            await asyncio.sleep(wait)
                            return await self.rpc_call(
                                method, params, endpoint_idx, retry_429 + 1
                            )
                        logger.warning(f"429 persistente de {ep['name']}, fallback")
                        return await self.rpc_call(method, params, endpoint_idx + 1)

                    if resp.status == 200:
                        data = await resp.json()
                        self.rpc_latencies[ep["name"]].append(time.time() - t0)
                        self.stats["rpc_calls_made"] += 1
                        if "result" in data:
                            return data["result"]
                        if "error" in data:
                            logger.debug(f"RPC error {ep['name']}: {data['error']}")

                    return await self.rpc_call(method, params, endpoint_idx + 1)
            except Exception as e:
                logger.debug(f"RPC exception {ep['name']}: {e}")
                self.rpc_failures[ep["name"]] += 1
                return await self.rpc_call(method, params, endpoint_idx + 1)

    async def get_multiple_accounts(self, addresses: List[str]) -> List[Optional[dict]]:
        """Batch getMultipleAccounts con jsonParsed (para token accounts y mints)."""
        if not addresses:
            return []
        all_results = []
        for i in range(0, len(addresses), MAX_ACCOUNTS_PER_BATCH):
            batch = addresses[i:i + MAX_ACCOUNTS_PER_BATCH]
            self.stats["rpc_calls_saved"] += len(batch) - 1
            result = await self.rpc_call(
                "getMultipleAccounts",
                [batch, {"encoding": "jsonParsed"}],
            )
            if result and "value" in result:
                all_results.extend(result["value"])
            else:
                all_results.extend([None] * len(batch))
        return all_results

    async def get_multiple_accounts_raw(self, addresses: List[str]) -> List[Optional[dict]]:
        """Batch getMultipleAccounts con base64 (para bonding curves / pools)."""
        if not addresses:
            return []
        all_results = []
        for i in range(0, len(addresses), MAX_ACCOUNTS_PER_BATCH):
            batch = addresses[i:i + MAX_ACCOUNTS_PER_BATCH]
            self.stats["rpc_calls_saved"] += len(batch) - 1
            result = await self.rpc_call(
                "getMultipleAccounts",
                [batch, {"encoding": "base64"}],
            )
            if result and "value" in result:
                all_results.extend(result["value"])
            else:
                all_results.extend([None] * len(batch))
        return all_results

    async def get_token_largest_accounts(self, mint: str) -> Optional[list]:
        result = await self.rpc_call("getTokenLargestAccounts", [mint])
        return result.get("value") if result and "value" in result else None

    # ── PRECIO ON-CHAIN ────────────────────────────────────────────────

    async def calculate_prices(
        self,
        tokens: List[dict],
        holder_data: Dict[int, Optional[list]],
        mint_accounts: List[Optional[dict]],
    ) -> Dict[str, dict]:
        """
        Lee vaults y bonding curves para calcular precio on-chain.

        Flujo:
          holders[0].address  = vault del pool (tiene los tokens)
          getMultipleAccounts(vaults)  → parsed.info.owner = bonding curve PDA
          getMultipleAccounts(curves)  → lamports = SOL reserves

          price_sol = (lamports / 1e9) / tokens_en_pool
        """
        price_data: Dict[str, dict] = {}

        # ── A: extraer vault addresses y token amounts ──
        vault_addresses: List[str] = []
        vault_index_map: Dict[str, List[int]] = {}
        vault_info: Dict[int, dict] = {}

        for i, token in enumerate(tokens):
            holders = holder_data.get(token["token_id"])
            if not holders or len(holders) == 0:
                continue

            vault_addr = holders[0].get("address")
            if not vault_addr:
                continue

            ui_amount = holders[0].get("uiAmount")
            if ui_amount and isinstance(ui_amount, (int, float)) and float(ui_amount) > 0:
                tokens_in_pool = float(ui_amount)
            else:
                raw = float(holders[0].get("amount", 0))
                decimals = 9
                mint_acc = mint_accounts[i] if i < len(mint_accounts) else None
                if mint_acc and isinstance(mint_acc, dict):
                    decimals = int(
                        mint_acc.get("data", {}).get("parsed", {})
                        .get("info", {}).get("decimals", 9)
                    )
                tokens_in_pool = raw / (10 ** decimals) if decimals > 0 else raw

            if tokens_in_pool <= 0:
                continue

            vault_info[i] = {
                "vault_addr": vault_addr,
                "tokens_in_pool": tokens_in_pool,
                "mint": token["mint_address"],
            }
            if vault_addr not in vault_index_map:
                vault_addresses.append(vault_addr)
                vault_index_map[vault_addr] = []
            vault_index_map[vault_addr].append(i)

        if not vault_addresses:
            logger.info("  Precios: 0 vaults encontrados")
            return price_data

        # ── B: leer vaults → obtener owner (bonding curve PDA) ──
        vault_accounts = await self.get_multiple_accounts(vault_addresses)

        pool_addresses: List[str] = []
        pool_index_map: Dict[str, List[int]] = {}

        for j, vault_acc in enumerate(vault_accounts):
            if not vault_acc or not isinstance(vault_acc, dict):
                continue
            vault_addr = vault_addresses[j]
            parsed_info = (
                vault_acc.get("data", {}).get("parsed", {}).get("info", {})
            )
            pool_addr = parsed_info.get("owner")
            if not pool_addr:
                continue

            if pool_addr not in pool_index_map:
                pool_addresses.append(pool_addr)
                pool_index_map[pool_addr] = []
            for idx in vault_index_map.get(vault_addr, []):
                pool_index_map[pool_addr].append(idx)

        if not pool_addresses:
            logger.info("  Precios: 0 pool addresses encontrados")
            return price_data

        # ── C: leer bonding curves → lamports = SOL reserves ──
        pool_accounts = await self.get_multiple_accounts_raw(pool_addresses)

        prices_found = 0
        for k, pool_acc in enumerate(pool_accounts):
            if not pool_acc or not isinstance(pool_acc, dict):
                continue

            pool_addr = pool_addresses[k]
            lamports = pool_acc.get("lamports", 0)
            account_owner = pool_acc.get("owner", "")

            # Solo pump.fun: el SOL vive en los lamports del bonding curve PDA
            if account_owner != PUMP_FUN_PROGRAM:
                continue

            sol_reserves = lamports / 1e9
            if sol_reserves <= 0:
                continue

            for idx in pool_index_map.get(pool_addr, []):
                info = vault_info.get(idx)
                if not info or info["tokens_in_pool"] <= 0:
                    continue

                price_sol = sol_reserves / info["tokens_in_pool"]
                price_data[info["mint"]] = {
                    "price_sol": price_sol,
                    "liquidity_sol": sol_reserves * 2,
                    "tokens_in_pool": info["tokens_in_pool"],
                }
                prices_found += 1

        logger.info(f"  Precios calculados: {prices_found}/{len(tokens)} tokens")
        return price_data

    # ── RECOLECCIÓN POR TIER ───────────────────────────────────────────

    async def collect_tier_metrics(self, tokens: List[dict], tier_name: str):
        if not tokens:
            return

        logger.info(f"[{tier_name.upper()}] Recolectando {len(tokens)} tokens...")
        mint_addresses = [t["mint_address"] for t in tokens]

        # Paso 1: Batch getMultipleAccounts (incluye supply en jsonParsed)
        mint_accounts = await self.get_multiple_accounts(mint_addresses)

        # Paso 2: Holders (con cache de 5 min)
        holder_data: Dict[int, Optional[list]] = {}
        now = time.time()
        tokens_needing_holders = []
        for token in tokens:
            tid = token["token_id"]
            cached = self.holder_cache.get(tid)
            if cached and (now - cached["ts"]) < HOLDER_REFRESH_INTERVAL:
                holder_data[tid] = cached["data"]
            else:
                tokens_needing_holders.append(token)

        if tokens_needing_holders:
            h_tasks = [
                self.get_token_largest_accounts(t["mint_address"])
                for t in tokens_needing_holders
            ]
            h_results = await asyncio.gather(*h_tasks, return_exceptions=True)
            for token, res in zip(tokens_needing_holders, h_results):
                tid = token["token_id"]
                if isinstance(res, Exception) or res is None:
                    holder_data[tid] = self.holder_cache.get(tid, {}).get("data")
                else:
                    holder_data[tid] = res
                    self.holder_cache[tid] = {"data": res, "ts": now}

        # Paso 3: Calcular precios on-chain (2 batch calls extra)
        price_data = await self.calculate_prices(tokens, holder_data, mint_accounts)

        # Paso 4: Parse y armar métricas
        metrics_to_save = []
        for i, token in enumerate(tokens):
            try:
                mint_acc = mint_accounts[i] if i < len(mint_accounts) else None
                holders = holder_data.get(token["token_id"])
                metric = self.parse_metric(token, mint_acc, holders, price_data)
                if metric:
                    metrics_to_save.append(metric)
            except Exception as e:
                logger.debug(f"Parse error {token['mint_address'][:12]}: {e}")
                self.stats["errors"] += 1

        # Paso 5: Bulk INSERT
        if metrics_to_save:
            self.save_metrics_batch(metrics_to_save)
            logger.info(f"  [{tier_name.upper()}] {len(metrics_to_save)} métricas guardadas")

    # ── PARSE: RPC → registro de métrica ───────────────────────────────

    def parse_metric(self, token: dict, mintaccount: Optional[dict],
                     holders: Optional[list],
                     price_data: Dict[str, dict] = None) -> Optional[dict]:
        try:
            # Supply desde getMultipleAccounts (jsonParsed)
            total_supply = 0.0
            decimals = 9
            if mintaccount and isinstance(mintaccount, dict):
                parsed_info = (
                    mintaccount.get("data", {}).get("parsed", {}).get("info", {})
                )
                raw_supply = parsed_info.get("supply", "0")
                decimals = int(parsed_info.get("decimals", 9))
                if raw_supply and decimals > 0:
                    total_supply = int(raw_supply) / (10 ** decimals)
                elif raw_supply:
                    total_supply = float(raw_supply)

            # Holder concentration
            holder_count = 0
            if holders and isinstance(holders, list):
                holder_count = len(holders)

            # Precio on-chain
            pd = (price_data or {}).get(token["mint_address"], {})
            price_sol = pd.get("price_sol", 0.0)
            liquidity_sol = pd.get("liquidity_sol", 0.0)
            market_cap = price_sol * total_supply if total_supply > 0 else 0.0

            return {
                "token_id": token["token_id"],
                "time": datetime.now(),
                "price": price_sol,
                "market_cap": market_cap,
                "fdv": market_cap,
                "liquidity": liquidity_sol,
                "volume_10m": 0.0,
                "swap_count": 0,
                "holders_count": holder_count,
            }
        except Exception as e:
            logger.debug(f"parse_metric error: {e}")
            return None

    # ── BD: Bulk INSERT ────────────────────────────────────────────────

    def save_metrics_batch(self, metrics: List[dict]):
        try:
            cursor = self.conn.cursor()
            values = [
                (m["token_id"], m["time"], m["price"], m["market_cap"],
                 m["fdv"], m["liquidity"], m["volume_10m"],
                 m["swap_count"], m["holders_count"])
                for m in metrics
            ]
            execute_values(
                cursor,
                """INSERT INTO token_metrics
                   (token_id, time, price, market_cap, fdv,
                    liquidity, volume_10m, swap_count, holders_count)
                VALUES %s
                ON CONFLICT (token_id, time) DO NOTHING""",
                values,
            )
            self.conn.commit()
            cursor.close()
            self.stats["metrics_saved"] += len(metrics)
        except Exception as e:
            logger.error(f"Error batch save: {e}")
            self.safe_rollback()
            self.stats["errors"] += 1

    # ── STATS ──────────────────────────────────────────────────────────

    def log_stats(self):
        made = self.stats["rpc_calls_made"]
        saved = self.stats["rpc_calls_saved"]
        total_theoretical = made + saved
        pct = (saved / total_theoretical * 100) if total_theoretical else 0
        logger.info(
            f"Stats: ciclos={self.stats['cycles']} "
            f"métricas={self.stats['metrics_saved']} "
            f"RPC {made} calls ({saved} ahorrados, {pct:.0f}% reducción) "
            f"429s={self.stats['rpc_429s']} errores={self.stats['errors']}"
        )
        for name in set(list(self.rpc_failures) + list(self.rpc_latencies)):
            lats = self.rpc_latencies.get(name, [-100])
            avg = sum(lats) / len(lats) if lats else 0
            fails = self.rpc_failures.get(name, 0)
            logger.info(f"  {name}: avg={avg:.3f}s, fails={fails}")

    # ── MAIN LOOP ──────────────────────────────────────────────────────

    async def run(self):
        logger.info("=" * 70)
        logger.info("METRICS COLLECTOR v5 — precio on-chain + semáforo + 429 retry")
        logger.info("=" * 70)
        logger.info(f"DB: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
        logger.info(f"RPC endpoints: {len(RPC_ENDPOINTS)}")
        for ep in RPC_ENDPOINTS:
            logger.info(f"  {ep['name']}: {ep['url'][:60]}...")
        logger.info(
            f"Tiers: hot={TIERS['hot']['interval_seconds']}s, "
            f"active={TIERS['active']['interval_seconds']}s, "
            f"monitor={TIERS['monitor']['interval_seconds']}s"
        )
        logger.info(f"Semáforo: {MAX_CONCURRENT_RPC} concurrent (rate limit: 50 req/s)")
        logger.info("=" * 70)

        self.connect_db()
        self.session = aiohttp.ClientSession()

        try:
            while True:
                try:
                    categorized = self.get_active_tokens()
                    now = time.time()
                    tasks = []

                    for tier_name, tier_cfg in TIERS.items():
                        elapsed = now - self.tier_last_run[tier_name]
                        if elapsed >= tier_cfg["interval_seconds"]:
                            tokens = categorized.get(tier_name, [])
                            if tokens:
                                tasks.append(
                                    self.collect_tier_metrics(tokens, tier_name)
                                )
                            self.tier_last_run[tier_name] = now

                    if tasks:
                        await asyncio.gather(*tasks)

                    self.stats["cycles"] += 1
                    if self.stats["cycles"] % 20 == 0:
                        self.log_stats()

                    await asyncio.sleep(5)

                except psycopg2.OperationalError:
                    logger.warning("DB connection lost, reconectando...")
                    self.connect_db()
                    await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"Error en loop principal: {e}")
                    self.stats["errors"] += 1
                    self.safe_rollback()
                    await asyncio.sleep(10)
        finally:
            if self.session:
                await self.session.close()
            if self.conn and not self.conn.closed:
                self.conn.close()
            logger.info("Metrics collector detenido.")


def main():
    collector = MetricsCollector()
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
