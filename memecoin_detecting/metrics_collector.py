#!/usr/bin/env python3
"""
metrics_collector.py - Recopila métricas de tokens cada 10 segundos
Versión con asyncio para procesar múltiples tokens en paralelo + cálculo de volumen
"""

import psycopg2
from psycopg2.extras import execute_values
import asyncio
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
from rpc_helpers import SolanaRPC, AsyncSolanaRPC

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/rebelforce/scripts/memecoin_detecting/metrics_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

RENT_EXEMPT_MINIMUM = 0.002  # SOL mínimo de renta


class MetricsCollector:
    """Recopila métricas de tokens activos cada 10 segundos con asyncio"""
    
    def __init__(self, db_config: Dict, rpc_url: str = "http://127.0.0.1:7211"):
        self.db_config = db_config
        self.rpc_url = rpc_url
        self.rpc = SolanaRPC(rpc_url)  # Cliente síncrono para operaciones simples
        self.conn = None
        self.active_tokens = []
        
        # Estadísticas
        self.metrics_collected = 0
        self.errors_count = 0
        self.start_time = datetime.now()
    
    def connect_db(self):
        """Conecta a PostgreSQL"""
        try:
            if self.conn:
                self.conn.close()
            
            self.conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"]
            )
            logger.info("Conectado a PostgreSQL")
        except Exception as e:
            logger.error(f"Error conectando a PostgreSQL: {e}")
            raise
    
    def load_active_tokens(self, hours: int = 1):
        """
        Carga tokens detectados en las últimas N horas
        
        Args:
            hours: Número de horas hacia atrás (default: 1 hora para no saturar)
        """
        try:
            cursor = self.conn.cursor()
            query = """
                SELECT 
                    token_id, mint_address, amm, name, symbol, 
                    decimals, total_supply, pool_address
                FROM tokens 
                WHERE detected_at > NOW() - INTERVAL '%s hours'
                AND status = 'active'
                ORDER BY detected_at DESC
            """
            cursor.execute(query, (hours,))
            tokens = cursor.fetchall()
            
            self.active_tokens = [
                {
                    "token_id": row[0],
                    "mint_address": row[1],
                    "amm": row[2],
                    "name": row[3],
                    "symbol": row[4],
                    "decimals": row[5] or 9,
                    "total_supply": row[6] or 0,
                    "pool_address": row[7]  # Puede ser None si no se ha encontrado aún
                }
                for row in tokens
            ]
            
            logger.info(f"Cargados {len(self.active_tokens)} tokens activos de las últimas {hours}h")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error cargando tokens activos: {e}")
            self.active_tokens = []
    
    async def find_pool_and_price_async(self, rpc: AsyncSolanaRPC, mint_address: str) -> tuple:
        """
        Encuentra pool address Y calcula precio en una sola operación (versión async)
        
        CORRECCIÓN: No busca "result" dos veces, usa directamente lo que retorna rpc.call()
        
        Returns:
            (pool_address, price_in_sol) o (None, None)
        """
        try:
            # Paso 1: getTokenLargestAccounts para encontrar el holder más grande
            result = await rpc.get_token_largest_accounts(mint_address)
            
            # CORRECCIÓN: result ya ES el contenido de "result", no tiene key "result"
            if result is None:
                logger.warning(f"Token {mint_address[:16]}... no encontrado en blockchain (posiblemente cerrado)")
                return None, None
            
            # CORRECCIÓN: Acceso directo a "value"
            accounts = result.get("value", [])
            
            if not accounts:
                logger.warning(f"Sin holders para {mint_address[:16]}...")
                return None, None
            
            # Paso 2: Verificar las 3 cuentas más grandes para encontrar un pool
            for i, acc in enumerate(accounts[:3]):
                account_address = acc.get("address")
                token_amount_raw = int(acc.get("amount", 0))
                token_decimals = acc.get("decimals", 9)
                
                if not account_address:
                    continue
                
                logger.debug(f"  [{i+1}] Verificando cuenta {account_address[:16]}... con {token_amount_raw} tokens")
                
                # Obtener el owner de esta token account
                acc_result = await rpc.get_account_info(account_address, encoding="jsonParsed")
                
                # CORRECCIÓN: acc_result ya ES el contenido, no tiene key "result"
                if acc_result is None:
                    continue
                
                acc_data = acc_result.get("value")
                if not acc_data:
                    continue
                
                parsed = acc_data.get("data", {}).get("parsed", {})
                pool_candidate = parsed.get("info", {}).get("owner")
                
                if not pool_candidate:
                    continue
                
                # Paso 3: Verificar que el owner del pool sea un AMM conocido
                try:
                    pool_result = await rpc.get_account_info(pool_candidate, encoding="jsonParsed")
                    
                    # CORRECCIÓN: pool_result ya ES el contenido
                    if pool_result is None:
                        continue
                    
                    pool_data = pool_result.get("value")
                except Exception as e:
                    logger.error(f"Error en getAccountInfo (pool): {e}")
                    continue
                
                if not pool_data:
                    continue
                
                pool_owner = pool_data.get("owner")
                
                # Verificar si es un pool de AMM conocido
                if pool_owner in AMM_PROGRAM_IDS:
                    amm_name = AMM_PROGRAM_IDS[pool_owner]
                    
                    # Obtener SOL del pool
                    sol_lamports = pool_data.get("lamports", 0)
                    sol_balance = sol_lamports / 1_000_000_000
                    sol_for_price = max(sol_balance - RENT_EXEMPT_MINIMUM, 0)
                    
                    # Calcular precio
                    token_balance = token_amount_raw / (10 ** token_decimals)
                    
                    if token_balance > 0 and sol_for_price > 0:
                        price_in_sol = sol_for_price / token_balance
                    else:
                        price_in_sol = 0
                    
                    logger.info(
                        f"✓ {mint_address[:16]}... | Pool: {pool_candidate[:16]}... ({amm_name}) | "
                        f"SOL: {sol_balance:.6f} | Tokens: {token_balance:,.0f} | "
                        f"Precio: {price_in_sol:.12f} SOL"
                    )
                    
                    # Guardar pool en BD para no buscarlo de nuevo
                    self.save_pool_to_db(mint_address, pool_candidate)
                    
                    return pool_candidate, price_in_sol
            
            # Ninguna de las 3 cuentas más grandes era un pool
            logger.warning(
                f"✗ No se encontró pool para {mint_address[:16]}... "
                f"(las 3 cuentas más grandes no pertenecen a ningún AMM)"
            )
            return None, None
            
        except Exception as e:
            logger.error(f"Error en find_pool_and_price_async: {e}")
            return None, None
    
    async def get_price_from_known_pool_async(
        self, 
        rpc: AsyncSolanaRPC, 
        pool_address: str, 
        mint_address: str
    ) -> float:
        """
        Si ya conocemos el pool, solo necesitamos 2 llamadas para el precio (versión async)
        
        CORRECCIÓN: No busca "result" dos veces
        """
        try:
            # Obtener balance de SOL del pool
            pool_result = await rpc.get_account_info(pool_address)
            
            # CORRECCIÓN: pool_result ya ES el contenido
            if pool_result is None:
                return 0
            
            pool_data = pool_result.get("value")
            if not pool_data:
                return 0
            
            sol_lamports = pool_data.get("lamports", 0)
            sol_balance = sol_lamports / 1_000_000_000
            sol_for_price = max(sol_balance - RENT_EXEMPT_MINIMUM, 0)
            
            # Obtener balance de tokens en el pool
            result = await rpc.get_token_largest_accounts(mint_address)
            
            # CORRECCIÓN: result ya ES el contenido
            if result is None:
                return 0
            
            accounts = result.get("value", [])
            
            # Buscar la token account que pertenece a este pool
            for acc in accounts[:5]:
                acc_result = await rpc.get_account_info(acc["address"], encoding="jsonParsed")
                
                # CORRECCIÓN: acc_result ya ES el contenido
                if acc_result is None:
                    continue
                
                acc_data = acc_result.get("value")
                if not acc_data:
                    continue
                
                parsed = acc_data.get("data", {}).get("parsed", {})
                owner = parsed.get("info", {}).get("owner")
                
                if owner == pool_address:
                    token_amount = int(acc["amount"])
                    decimals = acc["decimals"]
                    token_balance = token_amount / (10 ** decimals)
                    
                    if token_balance > 0 and sol_for_price > 0:
                        return sol_for_price / token_balance
                    
                    return 0
            
            return 0
            
        except Exception as e:
            logger.error(f"Error obteniendo precio de pool conocido: {e}")
            return 0
    
    def save_pool_to_db(self, mint_address: str, pool_address: str):
        """Guarda el pool en la BD para no buscarlo de nuevo"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE tokens SET pool_address = %s WHERE mint_address = %s",
                (pool_address, mint_address)
            )
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Error guardando pool en BD: {e}")
            self.conn.rollback()
    
    async def calculate_volume_async(
        self, 
        rpc: AsyncSolanaRPC, 
        pool_address: str, 
        time_window_seconds: int = 600
    ) -> Dict[str, float]:
        """
        Calcula el volumen de trading en un pool durante una ventana de tiempo
        
        Args:
            rpc: Cliente RPC asíncrono
            pool_address: Dirección del pool
            time_window_seconds: Ventana de tiempo en segundos (default: 10 minutos)
            
        Returns:
            {"volume_sol": float, "swap_count": int}
        """
        try:
            # Obtener firmas de transacciones recientes del pool
            signatures_result = await rpc.call(
                "getSignaturesForAddress",
                [pool_address, {"limit": 100}]  # Últimas 100 transacciones
            )
            
            if signatures_result is None:
                return {"volume_sol": 0, "swap_count": 0}
            
            now = int(time.time())
            cutoff_time = now - time_window_seconds
            
            volume_sol = 0
            swap_count = 0
            
            # Filtrar solo transacciones dentro de la ventana de tiempo
            for sig_info in signatures_result:
                blocktime = sig_info.get("blockTime")
                if blocktime is None or blocktime < cutoff_time:
                    continue
                
                # Obtener detalles de la transacción
                tx = await rpc.call(
                    "getTransaction",
                    [
                        sig_info["signature"],
                        {
                            "encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                )
                
                if tx is None or "meta" not in tx:
                    continue
                
                # Analizar los cambios de balance SOL (lamports)
                meta = tx["meta"]
                pre_balances = meta.get("preBalances", [])
                post_balances = meta.get("postBalances", [])
                
                # Calcular el monto total de SOL que cambió de manos
                for i in range(min(len(pre_balances), len(post_balances))):
                    diff = abs(post_balances[i] - pre_balances[i])
                    if diff > 0:
                        sol_amount = diff / 1_000_000_000
                        volume_sol += sol_amount
                        swap_count += 1
                        break  # Solo contar una vez por transacción
            
            return {
                "volume_sol": volume_sol,
                "swap_count": swap_count
            }
            
        except Exception as e:
            logger.error(f"Error calculando volumen: {e}")
            return {"volume_sol": 0, "swap_count": 0}
    
    async def collect_metrics_for_token_async(
        self, 
        rpc: AsyncSolanaRPC, 
        token: Dict
    ) -> Optional[Dict]:
        """
        Recopila todas las métricas para un token (versión async)
        """
        try:
            mint_address = token["mint_address"]
            pool_address = token.get("pool_address")
            
            # Si no tenemos pool, buscarlo
            if not pool_address:
                pool_address, price_in_sol = await self.find_pool_and_price_async(rpc, mint_address)
                
                if not pool_address or not price_in_sol or price_in_sol == 0:
                    # No se pudo obtener precio - puede ser token cerrado
                    return None
            else:
                # Ya tenemos pool, solo obtener precio
                price_in_sol = await self.get_price_from_known_pool_async(rpc, pool_address, mint_address)
            
            # Calcular market cap y FDV
            total_supply = float(token.get("total_supply", 0))
            decimals = token.get("decimals", 9)
            supply = total_supply / (10 ** decimals)
            market_cap = price_in_sol * supply
            fdv = market_cap  # Para tokens sin quema, FDV = Market Cap
            
            # Calcular volumen en ventanas de tiempo
            volume_data = await self.calculate_volume_async(rpc, pool_address, time_window_seconds=600)  # 10 minutos
            volume_10min = volume_data["volume_sol"]
            
            return {
                "time": datetime.now(),
                "token_id": token["token_id"],
                "price": price_in_sol,
                "liquidity": 0,  # TODO: Calcular desde pool reserves
                "volume_10s": 0,  # Necesita tracking continuo más granular
                "volume_10m": volume_10min,
                "volume_1h": 0,  # TODO: Expandir ventana a 1 hora
                "volume_24h": 0,  # TODO: Expandir ventana a 24 horas
                "market_cap": market_cap,
                "fdv": fdv,
                "holders_count": 0,  # TODO: Implementar count_token_holders
                "transactions_count": volume_data["swap_count"],
                "pool_address": pool_address
            }
            
        except Exception as e:
            logger.error(f"Error recopilando métricas para token {token['token_id']}: {e}")
            self.errors_count += 1
            return None
    
    def save_metrics(self, metrics_batch: List[Dict]):
        """Guarda un lote de métricas en la BD"""
        try:
            if not metrics_batch:
                return
            
            cursor = self.conn.cursor()
            
            # Preparar datos para inserción masiva
            values = [
                (
                    m["time"],
                    m["token_id"],
                    m["price"],
                    m["liquidity"],
                    m["volume_10s"],
                    m.get("volume_10m", 0),
                    m.get("volume_1h", 0),
                    m.get("volume_24h", 0),
                    m["market_cap"],
                    m["fdv"],
                    m["holders_count"],
                    m.get("transactions_count", 0),
                    m["pool_address"]
                )
                for m in metrics_batch
            ]
            
            query = """
                INSERT INTO token_metrics (
                    time, token_id, price, liquidity, 
                    volume_10s, volume_10m, volume_1h, volume_24h,
                    market_cap, fdv, holders_count, transactions_count, pool_address
                )
                VALUES %s
                ON CONFLICT (time, token_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    liquidity = EXCLUDED.liquidity,
                    volume_10s = EXCLUDED.volume_10s,
                    volume_10m = EXCLUDED.volume_10m,
                    holders_count = EXCLUDED.holders_count
            """
            
            execute_values(cursor, query, values)
            self.conn.commit()
            cursor.close()
            
            self.metrics_collected += len(metrics_batch)
            logger.info(f"✓ Guardadas {len(metrics_batch)} métricas")
            
        except Exception as e:
            logger.error(f"Error guardando métricas: {e}")
            self.conn.rollback()
            self.errors_count += 1
    
    async def run_collection_cycle_async(self):
        """
        Ejecuta un ciclo de recopilación de métricas usando asyncio para paralelizar
        """
        try:
            logger.info(f"Iniciando ciclo de recopilación para {len(self.active_tokens)} tokens")
            
            metrics_batch = []
            
            # Usar AsyncSolanaRPC con context manager
            async with AsyncSolanaRPC(self.rpc_url, max_concurrent=20) as rpc:
                # Crear tareas para todos los tokens (procesamiento en paralelo)
                tasks = [
                    self.collect_metrics_for_token_async(rpc, token)
                    for token in self.active_tokens
                ]
                
                # Ejecutar todas las tareas en paralelo
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Filtrar resultados exitosos
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Excepción en tarea: {result}")
                        self.errors_count += 1
                    elif result is not None:
                        metrics_batch.append(result)
            
            # Guardar todas las métricas
            if metrics_batch:
                self.save_metrics(metrics_batch)
            
            return len(metrics_batch)
            
        except Exception as e:
            logger.error(f"Error en ciclo de recopilación: {e}")
            return 0
    
    def print_stats(self):
        """Imprime estadísticas del collector"""
        uptime = datetime.now() - self.start_time
        logger.info("=" * 60)
        logger.info("ESTADÍSTICAS DEL METRICS COLLECTOR")
        logger.info("=" * 60)
        logger.info(f"Tiempo activo: {uptime}")
        logger.info(f"Tokens activos monitoreados: {len(self.active_tokens)}")
        logger.info(f"Métricas recopiladas: {self.metrics_collected}")
        logger.info(f"Errores: {self.errors_count}")
        
        if self.metrics_collected > 0:
            success_rate = (1 - self.errors_count / max(self.metrics_collected, 1)) * 100
            logger.info(f"Tasa de éxito: {success_rate:.2f}%")
        
        logger.info("=" * 60)
    
    def run(self, reload_interval_minutes: int = 10):
        """
        Bucle principal del collector
        
        Args:
            reload_interval_minutes: Cada cuántos minutos recargar la lista de tokens activos
        """
        logger.info("Iniciando MetricsCollector con asyncio...")
        
        self.connect_db()
        self.load_active_tokens(hours=1)  # Solo última hora
        
        last_reload = datetime.now()
        cycle_count = 0
        
        try:
            while True:
                cycle_start = time.time()
                
                # Recargar lista de tokens cada N minutos
                if datetime.now() - last_reload > timedelta(minutes=reload_interval_minutes):
                    logger.info("Recargando lista de tokens activos...")
                    self.load_active_tokens(hours=1)
                    last_reload = datetime.now()
                
                # Ejecutar ciclo de recopilación (ahora con asyncio)
                metrics_count = asyncio.run(self.run_collection_cycle_async())
                cycle_count += 1
                
                # Imprimir stats cada 10 ciclos
                if cycle_count % 10 == 0:
                    self.print_stats()
                
                # Calcular tiempo de espera
                elapsed = time.time() - cycle_start
                wait_time = max(0, 10 - elapsed)  # 10 segundos entre ciclos
                
                logger.info(f"Ciclo completado en {elapsed:.2f}s. Esperando {wait_time:.2f}s...")
                time.sleep(wait_time)
                
        except KeyboardInterrupt:
            logger.info("Deteniendo MetricsCollector...")
            self.print_stats()
        except Exception as e:
            logger.error(f"Error fatal en MetricsCollector: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Conexión a BD cerrada")


if __name__ == "__main__":
    # Configuración de la base de datos
    DB_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "database": "memecoins_db",
        "user": "postgres",
        "password": "12345"
    }
    
    # Configuración del RPC
    RPC_URL = "http://127.0.0.1:7211"
    
    # Crear y ejecutar collector
    collector = MetricsCollector(DB_CONFIG, RPC_URL)
    collector.run(reload_interval_minutes=10)
