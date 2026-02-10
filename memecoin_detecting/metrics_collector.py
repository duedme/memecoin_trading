#!/usr/bin/env python3
"""
metrics_collector.py
Recopila métricas de tokens cada 10 segundos usando el método de getTokenLargestAccounts
"""

import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
from rpc_helpers import SolanaRPC

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/rebelforce/scripts/memecoin_detecting/metrics_collector.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Los 12 AMM Program IDs (para verificación)
AMM_PROGRAM_IDS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "Pump.fun",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium AMM",
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj": "Raydium LaunchLab",
    "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X": "FluxBeam",
    "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o": "HeavenDEX",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo":  "Meteora DLMM",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG":  "Meteora DYN2",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora DYN",
    "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN":  "Meteora DBC",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG":  "Moonit",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc":  "Orca",
}

RENT_EXEMPT_MINIMUM = 0.002  # SOL mínimo de renta


class MetricsCollector:
    """Recopila métricas de tokens activos cada 10 segundos"""
    
    def __init__(self, db_config: Dict, rpc_url: str = "http://127.0.0.1:7211"):
        self.db_config = db_config
        self.rpc = SolanaRPC(rpc_url)
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
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
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
                    token_id,
                    mint_address,
                    amm,
                    name,
                    symbol,
                    decimals,
                    total_supply
                FROM tokens
                WHERE detected_at >= NOW() - INTERVAL '%s hours'
                    AND status = 'active'
                ORDER BY detected_at DESC
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
                    'total_supply': row[6] or 0
                }
                for row in tokens
            ]
            
            logger.info(f"Cargados {len(self.active_tokens)} tokens activos de las últimas {hours}h")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error cargando tokens activos: {e}")
            self.active_tokens = []
    
    def find_pool_and_price(self, mint_address: str):
        """
        Encuentra pool address Y calcula precio en una sola operación.
        Usa 3 llamadas RPC ligeras + verificación.
        Guarda el pool en BD para no buscarlo de nuevo.
        
        Retorna: (pool_address, price_in_sol) o (None, None)
        """
        
        # ============================================
        # PRIMERO: ¿Ya lo tenemos guardado en la BD?
        # ============================================
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT pool_address FROM tokens WHERE mint_address = %s",
                (mint_address,)
            )
            result = cursor.fetchone()
            cursor.close()
            
            cached_pool = result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"Error leyendo pool de BD: {e}")
            cached_pool = None
        
        # Si ya tenemos el pool, solo necesitamos el precio
        if cached_pool:
            price = self._get_price_from_known_pool(cached_pool, mint_address)
            return cached_pool, price
        
        # ============================================
        # PASO 1: ¿Quién tiene más tokens de esta moneda?
        # ============================================
        try:
            result = self.rpc.call("getTokenLargestAccounts", [mint_address])
            if not result or "result" not in result:
                logger.warning(f"⚠️ Token {mint_address[:16]}... no encontrado en blockchain (posiblemente cerrado)")
                return None, None
            accounts = result.get("result", {}).get("value", [])
        except Exception as e:
            logger.error(f"Error en getTokenLargestAccounts: {e}")
            return None, None
        
        if not accounts:
            logger.warning(f"❌ Sin holders para {mint_address[:16]}...")
            return None, None
        
        # Intentar con las primeras cuentas (por si la primera es un whale)
        for i, largest in enumerate(accounts[:3]):
            largest_token_account = largest["address"]
            token_amount_raw = int(largest["amount"])
            token_decimals = largest["decimals"]
            
            # ============================================
            # PASO 2: ¿Quién controla esa token account?
            # ============================================
            try:
                result = self.rpc.call("getAccountInfo", [
                    largest_token_account,
                    {"encoding": "jsonParsed"}
                ])
                account_data = result.get("result", {}).get("value", {})
            except Exception as e:
                logger.error(f"Error en getAccountInfo (token account): {e}")
                continue
            
            if not account_data:
                continue
            
            parsed = account_data.get("data", {}).get("parsed", {})
            pool_candidate = parsed.get("info", {}).get("owner")
            
            if not pool_candidate:
                continue
            
            # ============================================
            # PASO 3: ¿El candidato pertenece a un AMM?
            # ============================================
            try:
                result = self.rpc.call("getAccountInfo", [
                    pool_candidate,
                    {"encoding": "jsonParsed"}
                ])
                pool_data = result.get("result", {}).get("value", {})
            except Exception as e:
                logger.error(f"Error en getAccountInfo (pool): {e}")
                continue
            
            if not pool_data:
                continue
            
            pool_owner = pool_data.get("owner")
            
            # ============================================
            # VERIFICACIÓN: ¿Es un AMM conocido?
            # ============================================
            if pool_owner in AMM_PROGRAM_IDS:
                amm_name = AMM_PROGRAM_IDS[pool_owner]
                
                # Calcular precio
                sol_lamports = pool_data.get("lamports", 0)
                sol_balance = sol_lamports / 1_000_000_000
                sol_for_price = max(sol_balance - RENT_EXEMPT_MINIMUM, 0)
                
                token_balance = token_amount_raw / (10 ** token_decimals)
                
                if token_balance > 0 and sol_for_price > 0:
                    price_in_sol = sol_for_price / token_balance
                else:
                    price_in_sol = 0
                
                logger.info(
                    f"✅ {mint_address[:16]}... | "
                    f"Pool: {pool_candidate[:16]}... ({amm_name}) | "
                    f"SOL: {sol_balance:.6f} | "
                    f"Tokens: {token_balance:,.0f} | "
                    f"Precio: {price_in_sol:.12f} SOL"
                )
                
                # Guardar pool en BD para la próxima vez
                self._save_pool_to_db(mint_address, pool_candidate)
                
                return pool_candidate, price_in_sol
            
            else:
                # No es un AMM, probablemente un whale
                logger.debug(
                    f"⚠️  Cuenta #{i} ({pool_candidate[:16]}...) "
                    f"no es AMM (owner: {pool_owner[:16]}...), "
                    f"intentando siguiente..."
                )
                continue
        
        # Ninguna de las 3 cuentas más grandes era un pool
        logger.warning(
            f"❌ No se encontró pool para {mint_address[:16]}... "
            f"(las 3 cuentas más grandes no pertenecen a ningún AMM)"
        )
        return None, None

    def _get_price_from_known_pool(self, pool_address: str, mint_address: str) -> float:
        """
        Si ya conocemos el pool, solo necesitamos 2 llamadas para el precio.
        """
        try:
            # Obtener SOL del pool
            result = self.rpc.call("getAccountInfo", [
                pool_address,
                {"encoding": "jsonParsed"}
            ])
            pool_data = result.get("result", {}).get("value", {})
            
            if not pool_data:
                return 0
            
            sol_lamports = pool_data.get("lamports", 0)
            sol_balance = sol_lamports / 1_000_000_000
            sol_for_price = max(sol_balance - RENT_EXEMPT_MINIMUM, 0)
            
            # Obtener tokens en el pool
            result = self.rpc.call("getTokenLargestAccounts", [mint_address])
            accounts = result.get("result", {}).get("value", [])
            
            if not accounts:
                return 0
            
            # Buscar la token account que pertenece a este pool
            for acc in accounts[:5]:
                acc_result = self.rpc.call("getAccountInfo", [
                    acc["address"],
                    {"encoding": "jsonParsed"}
                ])
                acc_data = acc_result.get("result", {}).get("value", {})
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
            
        except Exception as e:
            logger.error(f"Error obteniendo precio de pool conocido: {e}")
            return 0

    def _save_pool_to_db(self, mint_address: str, pool_address: str):
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
    
    def collect_metrics_for_token(self, token: Dict) -> Optional[Dict]:
        """Recopila todas las métricas para un token"""
        try:
            mint_address = token['mint_address']
            
            # Obtener pool y precio en una sola llamada
            pool_address, price_in_sol = self.find_pool_and_price(mint_address)
            
            if not pool_address or not price_in_sol or price_in_sol <= 0:
                return None
            
            # Calcular market cap y FDV
            total_supply = float(token.get('total_supply', 0))
            decimals = token.get('decimals', 9)
            supply = total_supply / (10 ** decimals)
            market_cap = price_in_sol * supply
            fdv = market_cap
            
            return {
                'time': datetime.now(),
                'token_id': token['token_id'],
                'price': price_in_sol,
                'liquidity': 0,  # TODO: Calcular desde pool
                'volume_10s': 0,  # TODO: Desde transacciones monitoreadas
                'volume_1m': 0,
                'volume_5m': 0,
                'volume_1h': 0,
                'volume_24h': 0,
                'market_cap': market_cap,
                'fdv': fdv,
                'holders_count': 0,  # TODO: Implementar count_token_holders
                'transactions_count': 0,
                'pool_address': pool_address
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
                    m['time'],
                    m['token_id'],
                    m['price'],
                    m['liquidity'],
                    m['volume_10s'],
                    m.get('volume_1m', 0),
                    m.get('volume_5m', 0),
                    m.get('volume_1h', 0),
                    m.get('volume_24h', 0),
                    m['market_cap'],
                    m['fdv'],
                    m['holders_count'],
                    m.get('transactions_count', 0),
                    m['pool_address']
                )
                for m in metrics_batch
            ]
            
            query = """
                INSERT INTO token_metrics (
                    time, token_id, price, liquidity,
                    volume_10s, volume_1m, volume_5m, volume_1h, volume_24h,
                    market_cap, fdv, holders_count, transactions_count,
                    pool_address
                ) VALUES %s
                ON CONFLICT (time, token_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    liquidity = EXCLUDED.liquidity,
                    volume_10s = EXCLUDED.volume_10s,
                    holders_count = EXCLUDED.holders_count
            """
            
            execute_values(cursor, query, values)
            self.conn.commit()
            cursor.close()
            
            self.metrics_collected += len(metrics_batch)
            logger.info(f"✅ Guardadas {len(metrics_batch)} métricas")
            
        except Exception as e:
            logger.error(f"Error guardando métricas: {e}")
            self.conn.rollback()
            self.errors_count += 1
    
    def run_collection_cycle(self):
        """Ejecuta un ciclo de recopilación de métricas"""
        try:
            logger.info(f"Iniciando ciclo de recopilación para {len(self.active_tokens)} tokens")
            
            metrics_batch = []
            
            for i, token in enumerate(self.active_tokens):
                try:
                    metrics = self.collect_metrics_for_token(token)
                    if metrics:
                        metrics_batch.append(metrics)
                    
                    # Mostrar progreso cada 10 tokens
                    if (i + 1) % 10 == 0:
                        logger.info(f"Progreso: {i + 1}/{len(self.active_tokens)} tokens procesados")
                    
                except Exception as e:
                    logger.error(f"Error procesando token {token['token_id']}: {e}")
                    continue
            
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
        logger.info("📊 ESTADÍSTICAS DEL METRICS COLLECTOR")
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
        logger.info("🚀 Iniciando MetricsCollector...")
        
        self.connect_db()
        self.load_active_tokens(hours=1)  # Solo última hora
        
        last_reload = datetime.now()
        cycle_count = 0
        
        try:
            while True:
                cycle_start = time.time()
                
                # Recargar tokens si es necesario
                if datetime.now() - last_reload >= timedelta(minutes=reload_interval_minutes):
                    logger.info("Recargando lista de tokens activos...")
                    self.load_active_tokens(hours=1)
                    last_reload = datetime.now()
                
                # Ejecutar ciclo de recopilación
                metrics_count = self.run_collection_cycle()
                
                cycle_count += 1
                
                # Mostrar stats cada 10 ciclos
                if cycle_count % 10 == 0:
                    self.print_stats()
                
                # Calcular tiempo de espera
                elapsed = time.time() - cycle_start
                wait_time = max(0, 10 - elapsed)  # 10 segundos entre ciclos
                
                logger.info(f"Ciclo completado en {elapsed:.2f}s. Esperando {wait_time:.2f}s...")
                time.sleep(wait_time)
                
        except KeyboardInterrupt:
            logger.info("\n⚠️  Deteniendo MetricsCollector...")
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
