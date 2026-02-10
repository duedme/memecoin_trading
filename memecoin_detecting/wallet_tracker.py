#!/usr/bin/env python3
"""
wallet_tracker.py
Rastrea todas las transacciones de wallets que invierten en memecoins
Calcula ganancias/pérdidas y maneja órdenes parciales
"""

import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Set
from rpc_helpers import SolanaRPC, parse_swap_transaction, batch_process_transactions
from collections import defaultdict

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/rebelforce/scripts/memecoin_detecting/wallet_tracker.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class WalletTracker:
    """
    Rastrea wallets que invierten en memecoins
    - Registra todas sus transacciones (compra/venta)
    - Calcula ganancias/pérdidas
    - Maneja órdenes parciales
    """
    
    def __init__(self, db_config: Dict, rpc_url: str = "http://127.0.0.1:7211"):
        self.db_config = db_config
        self.rpc = SolanaRPC(rpc_url)
        self.conn = None
        
        # Wallets y tokens monitoreados
        self.tracked_wallets: Set[str] = set()
        self.monitored_tokens: Set[str] = set()  # Mint addresses de tokens activos
        self.discovered_wallets: Set[str] = set()  # Wallets descubiertos automáticamente
        
        # Cache de últimas firmas procesadas (para evitar duplicados)
        self.processed_signatures: Set[str] = set()
        self.max_cache_size = 10000
        
        # Cache de token_id por mint_address
        self.token_id_cache: Dict[str, int] = {}
        
        # Estadísticas
        self.transactions_processed = 0
        self.wallets_discovered = 0
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
    
    def load_tracked_wallets(self):
        """Carga wallets que queremos rastrear específicamente"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT wallet_address FROM tracked_wallets WHERE is_active = TRUE"
            )
            
            wallets = cursor.fetchall()
            self.tracked_wallets = {row[0] for row in wallets}
            
            logger.info(f"Cargados {len(self.tracked_wallets)} wallets rastreados")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error cargando wallets rastreados: {e}")
            self.tracked_wallets = set()
    
    def load_monitored_tokens(self, hours: int = 24):
        """
        Carga tokens activos para monitorear transacciones
        
        Args:
            hours: Tokens detectados en las últimas N horas
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT token_id, mint_address
                FROM tokens
                WHERE detected_at >= NOW() - INTERVAL '%s hours'
                    AND status = 'active'
            """, (hours,))
            
            tokens = cursor.fetchall()
            
            self.monitored_tokens = {row[1] for row in tokens}
            self.token_id_cache = {row[1]: row[0] for row in tokens}
            
            logger.info(f"Monitoreando {len(self.monitored_tokens)} tokens activos")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error cargando tokens monitoreados: {e}")
            self.monitored_tokens = set()
    
    def get_token_id(self, mint_address: str) -> Optional[int]:
        """Obtiene token_id desde mint_address (con cache)"""
        if mint_address in self.token_id_cache:
            return self.token_id_cache[mint_address]
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT token_id FROM tokens WHERE mint_address = %s",
                (mint_address,)
            )
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                token_id = result[0]
                self.token_id_cache[mint_address] = token_id
                return token_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error obteniendo token_id: {e}")
            return None
    
    def scan_token_transactions(self, mint_address: str, limit: int = 50) -> List[Dict]:
        """
        Escanea transacciones recientes de un token
        
        Args:
            mint_address: Dirección del token
            limit: Número de transacciones a obtener
        
        Returns:
            Lista de transacciones parseadas
        """
        try:
            # Para obtener transacciones de un token, necesitamos monitorear
            # el pool address o program address asociado
            
            # ESTRATEGIA: Monitorear los pools de los AMMs principales
            # Ya que no podemos monitorear un mint directamente, monitoreamos
            # los pools donde ese token tiene actividad
            
            # Por ahora, esta función es un placeholder
            # En producción, necesitarías:
            # 1. Obtener pool address del token
            # 2. Monitorear transacciones del pool
            # 3. Filtrar por el mint específico
            
            logger.debug(f"Escaneando transacciones para {mint_address}")
            return []
            
        except Exception as e:
            logger.error(f"Error escaneando transacciones: {e}")
            return []
    
    def scan_wallet_transactions(self, wallet_address: str, limit: int = 50) -> List[Dict]:
        """
        Escanea transacciones recientes de un wallet
        
        Args:
            wallet_address: Dirección del wallet
            limit: Número de transacciones a obtener
        
        Returns:
            Lista de transacciones parseadas
        """
        try:
            # Obtener firmas de transacciones del wallet
            signatures_data = self.rpc.get_signatures_for_address(
                wallet_address,
                limit=limit
            )
            
            if not signatures_data:
                return []
            
            # Filtrar solo transacciones nuevas (no procesadas)
            new_signatures = []
            for sig_data in signatures_data:
                sig = sig_data.get('signature')
                if sig and sig not in self.processed_signatures:
                    new_signatures.append(sig)
            
            if not new_signatures:
                return []
            
            logger.info(f"Procesando {len(new_signatures)} transacciones nuevas de {wallet_address[:8]}...")
            
            # Obtener y parsear transacciones en paralelo
            transactions = batch_process_transactions(self.rpc, new_signatures, max_workers=3)
            
            # Filtrar solo transacciones de tokens monitoreados
            relevant_txs = []
            for tx in transactions:
                if tx:
                    # Verificar si alguno de los tokens involucrados está en nuestro monitoneo
                    if tx['token_in'] in self.monitored_tokens or tx['token_out'] in self.monitored_tokens:
                        relevant_txs.append(tx)
                        # Marcar firma como procesada
                        self.processed_signatures.add(tx['signature'])
            
            # Limpiar cache de firmas si crece mucho
            if len(self.processed_signatures) > self.max_cache_size:
                # Mantener solo las más recientes
                self.processed_signatures = set(list(self.processed_signatures)[-self.max_cache_size//2:])
            
            return relevant_txs
            
        except Exception as e:
            logger.error(f"Error escaneando wallet {wallet_address}: {e}")
            return []
    
    def detect_partial_fills(self, transactions: List[Dict]) -> List[Dict]:
        """
        Detecta órdenes que se completaron en múltiples transacciones (parciales)
        
        Criterios para detectar parciales:
        1. Misma wallet + mismo token
        2. Mismo tipo (buy/sell)
        3. En un periodo corto de tiempo (ej: <5 minutos)
        """
        try:
            if not transactions:
                return transactions
            
            # Agrupar por wallet + token + tipo + ventana de tiempo
            groups = defaultdict(list)
            
            for tx in transactions:
                # Determinar el token de la memecoin (no SOL/USDC)
                memecoin_mint = None
                if tx['token_out'] in self.monitored_tokens:
                    memecoin_mint = tx['token_out']
                elif tx['token_in'] in self.monitored_tokens:
                    memecoin_mint = tx['token_in']
                
                if not memecoin_mint:
                    continue
                
                # Clave de agrupación
                time_window = tx['block_time'] // 300  # Ventanas de 5 minutos
                key = (tx['wallet'], memecoin_mint, tx['type'], time_window)
                
                groups[key].append(tx)
            
            # Marcar transacciones parciales
            for key, group_txs in groups.items():
                if len(group_txs) > 1:
                    # Generar order_id único
                    order_id = f"{key[0][:8]}_{key[1][:8]}_{key[2]}_{key[3]}"
                    
                    # Marcar cada tx como parcial
                    for i, tx in enumerate(group_txs, 1):
                        tx['is_partial'] = True
                        tx['order_id'] = order_id
                        tx['partial_fill_index'] = i
                    
                    logger.info(f"Detectadas {len(group_txs)} transacciones parciales para orden {order_id}")
            
            return transactions
            
        except Exception as e:
            logger.error(f"Error detectando parciales: {e}")
            return transactions
    
    def process_transaction(self, tx: Dict):
        """
        Procesa una transacción y la registra en la BD
        
        Actualiza:
        - wallet_transactions
        - wallet_positions
        - wallets (estadísticas)
        """
        try:
            # Determinar wallet, token y tipo
            wallet_address = tx['wallet']
            memecoin_mint = None
            sol_amount = 0
            token_amount = 0
            tx_type = tx['type']
            
            # Identificar el token de memecoin y las cantidades
            if tx['token_out'] in self.monitored_tokens:
                # Comprando memecoin con SOL/USDC
                memecoin_mint = tx['token_out']
                token_amount = tx['amount_out']
                sol_amount = tx['amount_in']
                tx_type = 'buy'
            elif tx['token_in'] in self.monitored_tokens:
                # Vendiendo memecoin por SOL/USDC
                memecoin_mint = tx['token_in']
                token_amount = tx['amount_in']
                sol_amount = tx['amount_out']
                tx_type = 'sell'
            
            if not memecoin_mint:
                logger.warning(f"No se pudo identificar memecoin en TX {tx['signature']}")
                return
            
            # Calcular precio
            price = sol_amount / token_amount if token_amount > 0 else 0
            
            # Obtener token_id
            token_id = self.get_token_id(memecoin_mint)
            if not token_id:
                logger.warning(f"Token no encontrado en BD: {memecoin_mint}")
                return
            
            # Usar la función SQL para procesar la transacción
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT process_transaction(
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                wallet_address,
                memecoin_mint,
                tx['signature'],
                tx_type,
                token_amount,
                sol_amount,
                price,
                datetime.fromtimestamp(tx['block_time']),
                0,  # fee (por ahora 0, se puede calcular desde meta)
                tx.get('is_partial', False),
                tx.get('order_id')
            ))
            
            self.conn.commit()
            cursor.close()
            
            self.transactions_processed += 1
            
            # Descubrir nuevo wallet si no lo conocíamos
            if wallet_address not in self.discovered_wallets:
                self.discovered_wallets.add(wallet_address)
                self.wallets_discovered += 1
                logger.info(f"🆕 Wallet descubierto: {wallet_address[:16]}...")
            
        except Exception as e:
            logger.error(f"Error procesando transacción {tx.get('signature', 'unknown')}: {e}")
            self.conn.rollback()
            self.errors_count += 1
    
    def track_wallet_batch(self, wallet_addresses: List[str]):
        """
        Rastrea un lote de wallets
        
        Args:
            wallet_addresses: Lista de direcciones de wallet
        """
        try:
            for wallet in wallet_addresses:
                # Obtener transacciones recientes
                transactions = self.scan_wallet_transactions(wallet, limit=20)
                
                if not transactions:
                    continue
                
                # Detectar órdenes parciales
                transactions = self.detect_partial_fills(transactions)
                
                # Procesar cada transacción
                for tx in transactions:
                    self.process_transaction(tx)
                
                logger.info(f"✅ {wallet[:16]}... : {len(transactions)} transacciones procesadas")
                
                # Small delay para no sobrecargar el RPC
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error rastreando lote de wallets: {e}")
    
    def auto_discover_wallets_from_token(self, mint_address: str, limit: int = 10):
        """
        Descubre wallets activos desde un token específico
        
        Esta función busca las últimas transacciones del token y extrae
        los wallets que participaron
        """
        try:
            # Necesitaríamos monitorear el pool address del token
            # Por ahora, este es un placeholder
            
            logger.debug(f"Auto-descubriendo wallets desde token {mint_address[:8]}...")
            
            # TODO: Implementar lógica de descubrimiento
            # 1. Obtener pool address del token
            # 2. Obtener transacciones recientes del pool
            # 3. Extraer wallets únicos
            # 4. Agregar a self.discovered_wallets
            
        except Exception as e:
            logger.error(f"Error en auto-descubrimiento: {e}")
    
    def run_tracking_cycle(self):
        """Ejecuta un ciclo de tracking"""
        try:
            logger.info("Iniciando ciclo de tracking...")
            
            # Combinar wallets rastreados + descubiertos
            all_wallets = list(self.tracked_wallets | self.discovered_wallets)
            
            if not all_wallets:
                logger.warning("No hay wallets para rastrear")
                return 0
            
            logger.info(f"Rastreando {len(all_wallets)} wallets...")
            
            # Procesar en lotes
            batch_size = 10
            transactions_count = 0
            
            for i in range(0, len(all_wallets), batch_size):
                batch = all_wallets[i:i+batch_size]
                initial_count = self.transactions_processed
                
                self.track_wallet_batch(batch)
                
                batch_txs = self.transactions_processed - initial_count
                transactions_count += batch_txs
                
                logger.info(f"Lote {i//batch_size + 1}: {batch_txs} transacciones")
            
            return transactions_count
            
        except Exception as e:
            logger.error(f"Error en ciclo de tracking: {e}")
            return 0
    
    def print_stats(self):
        """Imprime estadísticas del tracker"""
        uptime = datetime.now() - self.start_time
        
        logger.info("=" * 70)
        logger.info("📊 ESTADÍSTICAS DEL WALLET TRACKER")
        logger.info("=" * 70)
        logger.info(f"Tiempo activo: {uptime}")
        logger.info(f"Wallets rastreados manualmente: {len(self.tracked_wallets)}")
        logger.info(f"Wallets descubiertos automáticamente: {len(self.discovered_wallets)}")
        logger.info(f"Total wallets monitoreados: {len(self.tracked_wallets | self.discovered_wallets)}")
        logger.info(f"Tokens monitoreados: {len(self.monitored_tokens)}")
        logger.info(f"Transacciones procesadas: {self.transactions_processed}")
        logger.info(f"Errores: {self.errors_count}")
        
        if self.transactions_processed > 0:
            success_rate = (1 - self.errors_count / max(self.transactions_processed, 1)) * 100
            logger.info(f"Tasa de éxito: {success_rate:.2f}%")
        
        logger.info("=" * 70)
    
    def add_wallet_to_track(self, wallet_address: str, label: str = "", reason: str = ""):
        """
        Agrega un wallet a la lista de tracking
        
        Args:
            wallet_address: Dirección del wallet
            label: Etiqueta/nombre del wallet
            reason: Razón por la que se rastrea
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO tracked_wallets (wallet_address, label, reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (wallet_address) DO UPDATE
                SET is_active = TRUE, label = EXCLUDED.label
            """, (wallet_address, label, reason))
            
            self.conn.commit()
            cursor.close()
            
            self.tracked_wallets.add(wallet_address)
            logger.info(f"✅ Wallet agregado al tracking: {wallet_address} ({label})")
            
        except Exception as e:
            logger.error(f"Error agregando wallet: {e}")
            self.conn.rollback()
    
    def run(
        self, 
        reload_interval_minutes: int = 10,
        cycle_interval_seconds: int = 30
    ):
        """
        Bucle principal del tracker
        
        Args:
            reload_interval_minutes: Cada cuántos minutos recargar listas
            cycle_interval_seconds: Intervalo entre ciclos de tracking (recomendado: 30s)
        """
        logger.info("🚀 Iniciando WalletTracker...")
        
        self.connect_db()
        self.load_tracked_wallets()
        self.load_monitored_tokens()
        
        last_reload = datetime.now()
        cycle_count = 0
        
        try:
            while True:
                cycle_start = time.time()
                
                # Recargar listas si es necesario
                if datetime.now() - last_reload >= timedelta(minutes=reload_interval_minutes):
                    logger.info("Recargando listas de wallets y tokens...")
                    self.load_tracked_wallets()
                    self.load_monitored_tokens()
                    last_reload = datetime.now()
                
                # Ejecutar ciclo de tracking
                txs_count = self.run_tracking_cycle()
                
                cycle_count += 1
                
                # Mostrar stats cada 10 ciclos
                if cycle_count % 10 == 0:
                    self.print_stats()
                
                # Calcular tiempo de espera
                elapsed = time.time() - cycle_start
                wait_time = max(0, cycle_interval_seconds - elapsed)
                
                logger.info(f"Ciclo {cycle_count} completado en {elapsed:.2f}s. Esperando {wait_time:.2f}s...")
                time.sleep(wait_time)
                
        except KeyboardInterrupt:
            logger.info("\n⚠️  Deteniendo WalletTracker...")
            self.print_stats()
        except Exception as e:
            logger.error(f"Error fatal en WalletTracker: {e}")
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
    
    # Crear y ejecutar tracker
    tracker = WalletTracker(DB_CONFIG, RPC_URL)
    
    # Opcional: Agregar wallets específicos para rastrear
    # tracker.add_wallet_to_track("WALLET_ADDRESS_HERE", "Whale #1", "Top trader")
    
    # Ejecutar
    tracker.run(
        reload_interval_minutes=10,
        cycle_interval_seconds=30  # Cada 30 segundos revisa los wallets
    )
