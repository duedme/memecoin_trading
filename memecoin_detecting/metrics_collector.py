#!/usr/bin/env python3
"""
metrics_collector.py - Recopila métricas de tokens cada 10 segundos
Versión HÍBRIDA: localhost para volumen, RPCs externos para pools/precios
Round-robin ponderado con backoff entre proveedores externos
"""

import psycopg2
from psycopg2.extras import execute_values
import asyncio
import aiohttp
import time
import random
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Any
from collections import defaultdict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('/home/rebelforce/scripts/memecoindetecting/metrics_collector.log')
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False

# ============================================================
# CONFIGURACIÓN DE PROVEEDORES RPC
# ============================================================

# Nodo local - SOLO para getSignaturesForAddress y getTransaction
LOCAL_RPC_URL = "http://127.0.0.1:7211"

# Proveedores externos - Para getTokenLargestAccounts, getAccountInfo, getMultipleAccounts
EXTERNAL_RPC_PROVIDERS = [
    {
        "name": "Helius",
        "url": "https://mainnet.helius-rpc.com/?api-key=TU_API_KEY_HELIUS",
        "weight": 2,
        "rate_limit": 10,  # req/s
        "has_das": True,  # Soporta DAS API para holders
    },
    {
        "name": "Alchemy",
        "url": "https://solana-mainnet.g.alchemy.com/v2/TU_API_KEY_ALCHEMY",
        "weight": 3,
        "rate_limit": 25,
        "has_das": False,
    },
    {
        "name": "QuickNode",
        "url": "https://YOUR_ENDPOINT.solana-mainnet.quiknode.pro/TU_API_KEY/",
        "weight": 2,
        "rate_limit": 15,
        "has_das": False,
    },
    {
        "name": "Chainstack",
        "url": "https://solana-mainnet.core.chainstack.com/TU_API_KEY",
        "weight": 2,
        "rate_limit": 10,
        "has_das": False,
    },
]

# Los 12 AMM Program IDs para verificación
AMM_PROGRAM_IDS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "Pump.fun",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium AMM",
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj": "Raydium LaunchLab",
    "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X": "FluxBeam",
    "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o": "HeavenDEX",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "Meteora DYN2",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora DYN",
    "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN": "Meteora DBC",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG": "Moonit",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca",
}

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "memecoins_db",
    "user": "postgres",
    "password": "12345"
}



class HybridAsyncRPC:
    """
    Cliente RPC híbrido asíncrono:
    - getSignaturesForAddress, getTransaction → localhost
    - getTokenLargestAccounts, getAccountInfo, getMultipleAccounts, getTokenAccounts → externos (round-robin)
    """
    
    def __init__(self, local_url: str, external_providers: List[dict]):
        self.local_url = local_url
        self.external_providers = external_providers
        self.request_id = 0
        
        self.weighted_providers = []
        for provider in external_providers:
            self.weighted_providers.extend([provider] * provider['weight'])
        
        self.current_idx = 0
        
        self.backoff = {p['name']: 0 for p in external_providers}
        self.backoff_until = {p['name']: 0 for p in external_providers}
        
        self.stats = {
            'local': {'success': 0, 'failures': 0},
            **{p['name']: {'success': 0, 'failures': 0, 'rate_limited': 0} for p in external_providers}
        }
        
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def _get_next_external(self) -> dict:
        """Obtiene el siguiente proveedor externo (weighted round-robin con backoff)"""
        now = time.time()
        available = [p for p in self.weighted_providers if self.backoff_until[p['name']] <= now]
        
        if not available:
            min_provider = min(self.external_providers, key=lambda x: self.backoff_until[x['name']])
            wait_time = self.backoff_until[min_provider['name']] - now
            if wait_time > 0:
                logger.warning(f"Todos los RPCs externos en backoff. Esperando {wait_time:.1f}s...")
                time.sleep(wait_time)
            return min_provider
        
        provider = available[self.current_idx % len(available)]
        self.current_idx += 1
        return provider
    
    def _apply_backoff(self, provider_name: str):
        """Aplica exponential backoff a un proveedor"""
        current = self.backoff[provider_name]
        if current == 0:
            self.backoff[provider_name] = 2
        else:
            self.backoff[provider_name] = min(current * 2, 60)
        self.backoff_until[provider_name] = time.time() + self.backoff[provider_name]
    
    async def call(self, method: str, params: list = None, timeout: int = 10) -> Optional[dict]:
        """
        Enruta la llamada según el método:
        - getSignaturesForAddress, getTransaction → local
        - getTokenLargestAccounts, getAccountInfo, getMultipleAccounts, getTokenAccounts → externo
        """
        if params is None:
            params = []
        
        self.request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params
        }
        
        if method in ["getSignaturesForAddress", "getTransaction", "getHealth", "getSlot"]:
            return await self._call_local(payload, timeout)
        else:
            return await self._call_external(payload, timeout)
    
    async def _call_local(self, payload: dict, timeout: int) -> Optional[dict]:
        """Llamada al nodo local"""
        try:
            async with self.session.post(self.local_url, json=payload, timeout=timeout) as response:
                if response.status == 200:
                    result = await response.json()
                    if 'error' not in result:
                        self.stats['local']['success'] += 1
                        return result.get('result')
                self.stats['local']['failures'] += 1
                return None
        except Exception as e:
            logger.debug(f"Error en local RPC: {e}")
            self.stats['local']['failures'] += 1
            return None
    
    async def _call_external(self, payload: dict, timeout: int) -> Optional[dict]:
        """Llamada a RPCs externos con round-robin y retry"""
        tried_providers = []
        
        for attempt in range(3):
            provider = self._get_next_external()
            
            if provider['name'] in tried_providers:
                continue
            tried_providers.append(provider['name'])
            
            result, error = await self._try_external(provider, payload, timeout)
            
            if error is None:
                self.stats[provider['name']]['success'] += 1
                self.backoff[provider['name']] = 0
                self.backoff_until[provider['name']] = 0
                return result
            elif error == 'rate_limited':
                self.stats[provider['name']]['rate_limited'] += 1
                self._apply_backoff(provider['name'])
                logger.debug(f"{provider['name']} rate limited. Backoff: {self.backoff[provider['name']]}s")
                continue
            else:
                self.stats[provider['name']]['failures'] += 1
                continue
        
        return None
    
    async def _try_external(self, provider: dict, payload: dict, timeout: int):
        """Intenta una llamada a un proveedor externo"""
        try:
            async with self.session.post(provider['url'], json=payload, timeout=timeout) as response:
                if response.status == 429:
                    return None, 'rate_limited'
                
                if response.status == 200:
                    result = await response.json()
                    if 'error' in result:
                        error_msg = str(result['error'].get('message', ''))
                        if 'could not find' in error_msg.lower() or 'not found' in error_msg.lower():
                            return None, 'not_found'
                        elif '429' in error_msg or 'rate limit' in error_msg.lower():
                            return None, 'rate_limited'
                        else:
                            return None, 'error'
                    return result.get('result'), None
                
                return None, 'http_error'
        except asyncio.TimeoutError:
            return None, 'timeout'
        except Exception as e:
            logger.debug(f"Error en {provider['name']}: {e}")
            return None, 'error'
    
    async def get_holders_count(self, mint_address: str) -> int:
        """
        Obtiene el número de holders usando Helius DAS API.
        Intenta solo con proveedores que tengan has_das=True.
        """
        helius_providers = [p for p in self.external_providers if p.get('has_das', False)]
        
        if not helius_providers:
            logger.warning("No hay proveedores con DAS API configurados")
            return 0
        
        provider = helius_providers[0]
        
        self.request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": "getTokenAccounts",
            "params": {
                "mint": mint_address,
                "limit": 1,
                "showZeroBalance": False
            }
        }
        
        try:
            async with self.session.post(provider['url'], json=payload, timeout=10) as response:
                if response.status == 200:
                    result = await response.json()
                    if 'result' in result and 'total' in result['result']:
                        return result['result']['total']
        except Exception as e:
            logger.debug(f"Error obteniendo holders para {mint_address[:8]}...: {e}")
        
        return 0
    
    def print_stats(self):
        """Imprime estadísticas de uso"""
        logger.info("=" * 70)
        logger.info("RPC STATISTICS")
        logger.info("=" * 70)
        
        local_total = self.stats['local']['success'] + self.stats['local']['failures']
        if local_total > 0:
            success_rate = (self.stats['local']['success'] / local_total) * 100
            logger.info(f"  {'Local':15} - Success: {success_rate:5.1f}% | Errors: {self.stats['local']['failures']:3d}")
        
        for name in [p['name'] for p in self.external_providers]:
            stats = self.stats[name]
            total = stats['success'] + stats['failures'] + stats['rate_limited']
            if total > 0:
                success_rate = (stats['success'] / total) * 100
                logger.info(
                    f"  {name:15} - Success: {success_rate:5.1f}% | "
                    f"429s: {stats['rate_limited']:3d} | Errors: {stats['failures']:3d}"
                )
        logger.info("=" * 70)



class MetricsCollectorFusion:
    """
    Versión fusionada que combina lo mejor de ambas versiones:
    1. getMultipleAccounts batch para pools (del original)
    2. Cálculo de volumen vía nodo local (de la nueva)
    3. Async con semaphore (de la nueva)
    4. mark_token_dead (del original)
    5. Enrutamiento híbrido (de la nueva)
    6. Holders count vía Helius DAS (nuevo)
    7. Liquidity desde lamports del pool (nuevo)
    """
    
    def __init__(self):
        self.conn = None
        self.rpc = None
        self.active_tokens = []
        self.semaphore = asyncio.Semaphore(20)  # Máximo 20 tareas concurrentes
        
        self.metrics_collected = 0
        self.tokens_not_found = 0
        self.batch_calls = 0
        self.start_time = datetime.now()
    
    def connect_db(self):
        """Conecta a PostgreSQL"""
        self.conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ Conectado a PostgreSQL")
    
    def load_active_tokens(self, hours: int = 1):
        """Carga tokens activos, priorizando los que tienen pool cacheado"""
        cursor = self.conn.cursor()
        query = """
        SELECT token_id, mint_address, amm, name, symbol, decimals, total_supply, pool_address
        FROM tokens
        WHERE detected_at > NOW() - INTERVAL '%s hours'
        AND status = 'active'
        ORDER BY 
            CASE WHEN pool_address IS NOT NULL THEN 0 ELSE 1 END,
            detected_at DESC
        """
        cursor.execute(query, (hours,))
        tokens = cursor.fetchall()
        
        self.active_tokens = [
            {
                'token_id': row[0],
                'mint_address': row[1],
                'amm': row[2],
                'name': row[3],
                'symbol': row[4],
                'decimals': row[5] or 9,
                'total_supply': row[6] or 0,
                'pool_address': row[7]
            }
            for row in tokens
        ]
        
        with_pool = sum(1 for t in self.active_tokens if t.get('pool_address'))
        without_pool = len(self.active_tokens) - with_pool
        
        logger.info(
            f"✓ Cargados {len(self.active_tokens)} tokens activos "
            f"({with_pool} con pool, {without_pool} sin pool)"
        )
        cursor.close()
    
    async def fetch_pools_batch(self, tokens: List[dict]) -> Dict[str, dict]:
        """
        Fetcha información de múltiples pools usando getMultipleAccounts.
        OPTIMIZACIÓN CLAVE del original.
        """
        pool_addresses = list(set(
            token['pool_address']
            for token in tokens
            if token.get('pool_address')
        ))
        
        if not pool_addresses:
            return {}
        
        result = await self.rpc.call('getMultipleAccounts', [
            pool_addresses,
            {"encoding": "jsonParsed"}
        ])
        
        if not result:
            return {}
        
        pools_data = {}
        for i, account_data in enumerate(result.get('value', [])):
            if account_data is None:
                continue
            
            pool_address = pool_addresses[i]
            pools_data[pool_address] = {
                'lamports': account_data.get('lamports', 0),
                'owner': account_data.get('owner')
            }
        
        self.batch_calls += 1
        return pools_data
    
    async def calculate_volume(self, token: dict) -> Dict[str, float]:
        """
        Calcula volumen usando nodo local (getSignaturesForAddress + getTransaction).
        FUNCIONALIDAD NUEVA de la versión híbrida.
        """
        mint = token['mint_address']
        now = int(time.time())
        
        signatures_result = await self.rpc.call('getSignaturesForAddress', [
            mint,
            {"limit": 100}
        ])
        
        if not signatures_result:
            return {'volume_10m': 0.0, 'swap_count': 0}
        
        volume_10m = 0.0
        swap_count = 0
        cutoff_10m = now - 600
        
        for sig_info in signatures_result:
            block_time = sig_info.get('blockTime', 0)
            if block_time < cutoff_10m:
                break
            
            signature = sig_info['signature']
            tx_result = await self.rpc.call('getTransaction', [
                signature,
                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
            ])
            
            if not tx_result or not tx_result.get('meta'):
                continue
            
            meta = tx_result['meta']
            pre_balances = meta.get('preBalances', [])
            post_balances = meta.get('postBalances', [])
            
            for i, (pre, post) in enumerate(zip(pre_balances, post_balances)):
                if pre != post:
                    sol_change = abs(post - pre) / 1_000_000_000
                    if sol_change > 0.001:  # Mínimo 0.001 SOL
                        volume_10m += sol_change
                        swap_count += 1
                        break
        
        return {'volume_10m': volume_10m, 'swap_count': swap_count}
    
    async def get_price_and_liquidity(self, token: dict, pools_data: Dict[str, dict]) -> tuple:
        """
        Calcula precio Y liquidity desde los datos del pool.
        Liquidity es NUEVO - antes estaba en 0.
        """
        pool_address = token.get('pool_address')
        if not pool_address or pool_address not in pools_data:
            return None, 0.0
        
        result = await self.rpc.call('getTokenLargestAccounts', [token['mint_address']])
        if not result:
            return None, 0.0
        
        accounts = result.get('value', [])
        if not accounts:
            return None, 0.0
        
        largest = accounts[0]
        token_amount_raw = int(largest['amount'])
        token_decimals = largest['decimals']
        token_balance = token_amount_raw / (10 ** token_decimals)
        
        sol_balance = pools_data[pool_address]['lamports'] / 1_000_000_000
        
        if token_balance <= 0:
            return None, 0.0
        
        price = sol_balance / token_balance
        
        liquidity = sol_balance * 2
        
        return price, liquidity
    
    async def collect_token_metrics(self, token: dict) -> Optional[dict]:
        """Recopila todas las métricas de un token (async con semaphore)"""
        async with self.semaphore:
            try:
                if token.get('pool_address'):
                    return token  # Se procesa en batch después
                
                pool, price, liquidity, error = await self.find_pool_and_price(token)
                
                if error == 'dead':
                    self.mark_token_dead(token['token_id'], token['mint_address'])
                    return None
                
                if price and price > 0:
                    volume_data = await self.calculate_volume(token)
                    
                    holders = await self.rpc.get_holders_count(token['mint_address'])
                    
                    total_supply = token['total_supply'] or 0
                    decimals = token['decimals'] or 9
                    supply_normalized = total_supply / (10 ** decimals) if total_supply > 0 else 0
                    market_cap = price * supply_normalized
                    
                    return {
                        'time': datetime.now(),
                        'token_id': token['token_id'],
                        'price': price,
                        'liquidity': liquidity,
                        'market_cap': market_cap,
                        'fdv': market_cap,
                        'holders_count': holders,
                        'volume_10m': volume_data['volume_10m'],
                        'swap_count': volume_data['swap_count'],
                        'pool_address': pool
                    }
                
                return None
                
            except Exception as e:
                logger.error(f"Error procesando {token['mint_address'][:16]}...: {e}")
                return None
    
    async def find_pool_and_price(self, token: dict) -> tuple:
        """Encuentra pool para token sin cache (versión async)"""
        mint_address = token['mint_address']
        
        result = await self.rpc.call('getTokenLargestAccounts', [mint_address])
        if not result:
            return None, None, 0.0, 'not_found'
        
        accounts = result.get('value', [])
        if not accounts:
            return None, None, 0.0, 'dead'
        
        for largest in accounts[:3]:
            largest_token_account = largest['address']
            token_amount_raw = int(largest['amount'])
            token_decimals = largest['decimals']
            
            acc_result = await self.rpc.call('getAccountInfo', [
                largest_token_account,
                {"encoding": "jsonParsed"}
            ])
            
            if not acc_result:
                continue
            
            account_data = acc_result.get('value')
            if not account_data:
                continue
            
            parsed = account_data.get('data', {}).get('parsed', {})
            pool_candidate = parsed.get('info', {}).get('owner')
            
            if not pool_candidate:
                continue
            
            pool_result = await self.rpc.call('getAccountInfo', [
                pool_candidate,
                {"encoding": "jsonParsed"}
            ])
            
            if not pool_result:
                continue
            
            pool_data = pool_result.get('value')
            if not pool_data:
                continue
            
            pool_owner = pool_data.get('owner')
            if pool_owner in AMM_PROGRAM_IDS:
                lamports = pool_data.get('lamports', 0)
                sol_balance = lamports / 1_000_000_000
                token_balance = token_amount_raw / (10 ** token_decimals)
                
                if token_balance > 0:
                    price = sol_balance / token_balance
                    liquidity = sol_balance * 2
                    self.cache_pool_address(token['token_id'], pool_candidate)
                    return pool_candidate, price, liquidity, None
        
        return None, None, 0.0, 'no_pool'
    
    def cache_pool_address(self, token_id: int, pool_address: str):
        """Cachea pool_address en BD"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE tokens SET pool_address = %s WHERE token_id = %s",
                (pool_address, token_id)
            )
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Error cacheando pool: {e}")
            self.conn.rollback()
    
    def mark_token_dead(self, token_id: int, mint_address: str):
        """Marca token como dead (del original)"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE tokens SET status = 'dead' WHERE token_id = %s",
                (token_id,)
            )
            self.conn.commit()
            cursor.close()
            logger.info(f"✗ Token {mint_address[:16]}... → dead")
            self.tokens_not_found += 1
        except Exception as e:
            logger.error(f"Error marcando token dead: {e}")
            self.conn.rollback()
    
    async def process_tokens_with_pools(self, tokens: List[dict]) -> List[dict]:
        """Procesa tokens que ya tienen pool cacheado (BATCH)"""
        metrics = []
        
        pools_data = await self.fetch_pools_batch(tokens)
        
        if not pools_data:
            return metrics
        
        for token in tokens:
            try:
                price, liquidity = await self.get_price_and_liquidity(token, pools_data)
                
                if price and price > 0:
                    volume_data = await self.calculate_volume(token)
                    
                    holders = await self.rpc.get_holders_count(token['mint_address'])
                    
                    total_supply = token['total_supply'] or 0
                    decimals = token['decimals'] or 9
                    supply_normalized = total_supply / (10 ** decimals) if total_supply > 0 else 0
                    market_cap = price * supply_normalized
                    
                    metrics.append({
                        'time': datetime.now(),
                        'token_id': token['token_id'],
                        'price': price,
                        'liquidity': liquidity,
                        'market_cap': market_cap,
                        'fdv': market_cap,
                        'holders_count': holders,
                        'volume_10m': volume_data['volume_10m'],
                        'swap_count': volume_data['swap_count'],
                        'pool_address': token['pool_address']
                    })
            except Exception as e:
                logger.error(f"Error procesando token con pool {token['mint_address'][:16]}...: {e}")
                continue
        
        return metrics
    
    async def run_collection_cycle(self):
        """Ejecuta un ciclo de recopilación (async)"""
        logger.info(f"Iniciando ciclo para {len(self.active_tokens)} tokens...")
        
        tokens_with_pool = [t for t in self.active_tokens if t.get('pool_address')]
        tokens_without_pool = [t for t in self.active_tokens if not t.get('pool_address')][:5]  # Límite de 5
        
        all_metrics = []
        
        if tokens_with_pool:
            logger.info(f"Procesando {len(tokens_with_pool)} tokens con pool (batch)...")
            metrics_batch = await self.process_tokens_with_pools(tokens_with_pool)
            all_metrics.extend(metrics_batch)
        
        if tokens_without_pool:
            logger.info(f"Descubriendo pools para {len(tokens_without_pool)} tokens...")
            tasks = [self.collect_token_metrics(token) for token in tokens_without_pool]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict):
                    all_metrics.append(result)
        
        if all_metrics:
            self.save_metrics(all_metrics)
        
        logger.info(f"✓ Ciclo completado: {len(all_metrics)} métricas recopiladas")
        return len(all_metrics)
    
    def save_metrics(self, metrics_batch: List[dict]):
        """Guarda métricas en BD"""
        if not metrics_batch:
            return
        
        try:
            cursor = self.conn.cursor()
            values = [
                (
                    m['time'],
                    m['token_id'],
                    m['price'],
                    m['liquidity'],
                    m['market_cap'],
                    m['fdv'],
                    m['holders_count'],
                    m.get('volume_10m', 0.0),
                    m.get('swap_count', 0),
                    m['pool_address']
                )
                for m in metrics_batch
            ]
            
            query = """
            INSERT INTO token_metrics
            (time, token_id, price, liquidity, market_cap, fdv, holders_count, volume_10m, swap_count, pool_address)
            VALUES %s
            ON CONFLICT (time, token_id) DO UPDATE SET
                price = EXCLUDED.price,
                liquidity = EXCLUDED.liquidity,
                market_cap = EXCLUDED.market_cap,
                holders_count = EXCLUDED.holders_count,
                volume_10m = EXCLUDED.volume_10m,
                swap_count = EXCLUDED.swap_count,
                pool_address = EXCLUDED.pool_address
            """
            execute_values(cursor, query, values)
            self.conn.commit()
            cursor.close()
            
            self.metrics_collected += len(metrics_batch)
            logger.info(f"✓ Guardadas {len(metrics_batch)} métricas en BD")
            
        except Exception as e:
            logger.error(f"Error guardando métricas: {e}")
            self.conn.rollback()
    
    def print_stats(self):
        """Imprime estadísticas"""
        uptime = datetime.now() - self.start_time
        logger.info("=" * 70)
        logger.info("METRICS COLLECTOR FUSION - ESTADÍSTICAS")
        logger.info("=" * 70)
        logger.info(f"Tiempo activo: {uptime}")
        logger.info(f"Tokens activos: {len(self.active_tokens)}")
        logger.info(f"Métricas recopiladas: {self.metrics_collected}")
        logger.info(f"Batch calls (getMultipleAccounts): {self.batch_calls}")
        logger.info(f"Tokens marcados dead: {self.tokens_not_found}")
        
        if self.rpc:
            self.rpc.print_stats()
    
    async def run(self, reload_interval_minutes: int = 5):
        """Bucle principal (async)"""
        logger.info("=" * 70)
        logger.info("METRICS COLLECTOR FUSION - INICIANDO")
        logger.info("=" * 70)
        logger.info("Características:")
        logger.info("  ✓ getMultipleAccounts batch (pools)")
        logger.info("  ✓ Cálculo de volumen vía nodo local")
        logger.info("  ✓ Holders count vía Helius DAS API")
        logger.info("  ✓ Liquidity desde lamports del pool")
        logger.info("  ✓ Async con semaphore (20 tareas paralelas)")
        logger.info("  ✓ Enrutamiento híbrido (local + externos)")
        logger.info("  ✓ Mark dead tokens automático")
        logger.info("=" * 70)
        logger.info("Proveedores RPC:")
        logger.info(f"  Local: {LOCAL_RPC_URL}")
        for provider in EXTERNAL_RPC_PROVIDERS:
            das = " [DAS]" if provider.get('has_das') else ""
            logger.info(f"  {provider['name']:15} (weight: {provider['weight']}, {provider['rate_limit']} req/s){das}")
        logger.info("=" * 70)
        
        self.connect_db()
        self.load_active_tokens(hours=1)
        
        async with HybridAsyncRPC(LOCAL_RPC_URL, EXTERNAL_RPC_PROVIDERS) as rpc:
            self.rpc = rpc
            
            last_reload = datetime.now()
            cycle_count = 0
            
            try:
                while True:
                    cycle_start = time.time()
                    
                    if datetime.now() - last_reload > timedelta(minutes=reload_interval_minutes):
                        logger.info("Recargando lista de tokens activos...")
                        self.load_active_tokens(hours=1)
                        last_reload = datetime.now()
                    
                    await self.run_collection_cycle()
                    cycle_count += 1
                    
                    if cycle_count % 5 == 0:
                        self.print_stats()
                    
                    elapsed = time.time() - cycle_start
                    wait_time = max(0, 10 - elapsed)
                    logger.info(f"Ciclo {cycle_count} completado en {elapsed:.1f}s. Esperando {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                    
            except KeyboardInterrupt:
                logger.info("\nDeteniendo...")
                self.print_stats()
            finally:
                if self.conn:
                    self.conn.close()
                    logger.info("Conexión DB cerrada")



if __name__ == "__main__":
    collector = MetricsCollectorFusion()
    asyncio.run(collector.run(reload_interval_minutes=5))