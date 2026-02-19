#!/usr/bin/env python3

"""
enhanced_wallet_tracker.py
SEGUIMIENTO COMPLETO de wallets - Rastrea TODAS sus transacciones con memecoins
v2: Auto-descubrimiento de wallets desde tokens recientes
"""

import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Set
from rpc_helpers import SolanaRPC, parse_swap_transaction, batch_process_transactions
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/rebelforce/scripts/memecoin_detecting/enhanced_wallet_tracker.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class EnhancedWalletTracker:
    """
    SEGUIMIENTO COMPLETO de wallets
    Diferencias con WalletTracker normal:
    1. Rastrea TODAS las transacciones del wallet (no solo memecoins monitoreadas)
    2. Auto-descubre nuevas memecoins cuando un wallet las compra
    3. Sigue la actividad completa del wallet en la blockchain
    4. Detecta patrones: si compra memecoin A, ¿también compra B, C, D?
    5. NUEVO: Auto-descubre wallets activos desde tokens recientes
    """

    def __init__(self, db_config: Dict, rpc_url: str = "http://127.0.0.1:7211"):
        self.db_config = db_config
        self.rpc = SolanaRPC(rpc_url)
        self.conn = None

        # Wallets rastreados
        self.tracked_wallets: Set[str] = set()
        self.discovered_wallets: Set[str] = set()

        # NUEVO: Todos los tokens que hemos visto (no solo los últimos 24h)
        self.all_known_tokens: Dict[str, int] = {}  # mint_address -> token_id

        # Cache de firmas procesadas
        self.processed_signatures: Set[str] = set()
        self.max_cache_size = 10000

        # NUEVO: Control de descubrimiento
        self.last_discovery_time = None
        self.discovery_interval_minutes = 5  # Descubrir wallets cada 5 min
        self.max_discovered_wallets = 100  # Máximo wallets auto-descubiertos activos
        self.min_trades_to_track = 2  # Mínimo trades para considerar un wallet interesante

        # Program IDs de AMMs conocidos (para detectar swaps)
        self.amm_program_ids = {
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # Pump.fun
            "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # PumpSwap
            "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium AMM
            "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",  # Raydium LaunchLab
            "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",  # FluxBeam
            "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o",  # HeavenDEX
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",  # Meteora DLMM
            "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",  # Meteora DYN2
            "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",  # Meteora DYN
            "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",  # Meteora DBC
            "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",  # Moonit
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  # Orca
        }

        # Estadísticas
        self.transactions_processed = 0
        self.wallets_discovered = 0
        self.new_tokens_discovered = 0
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

    def load_all_known_tokens(self):
        """
        NUEVO: Carga TODOS los tokens conocidos (no solo últimas 24h)
        Esto permite detectar cuando un wallet compra tokens viejos
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT token_id, mint_address
                FROM tokens
                WHERE status = 'active'
            """)
            tokens = cursor.fetchall()
            self.all_known_tokens = {row[1]: row[0] for row in tokens}
            logger.info(f"Cargados {len(self.all_known_tokens)} tokens conocidos (histórico completo)")
            cursor.close()
        except Exception as e:
            logger.error(f"Error cargando tokens: {e}")
            self.all_known_tokens = {}

    def load_tracked_wallets(self):
        """Carga wallets rastreados manualmente"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT wallet_address FROM tracked_wallets WHERE is_active = TRUE"
            )
            wallets = cursor.fetchall()
            self.tracked_wallets = {row[0] for row in wallets}
            logger.info(f"Cargados {len(self.tracked_wallets)} wallets rastreados manualmente")
            cursor.close()
        except Exception as e:
            logger.error(f"Error cargando wallets rastreados: {e}")
            self.tracked_wallets = set()

    def load_discovered_wallets(self):
        """Carga wallets descubiertos automáticamente (que ya tienen actividad)"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT DISTINCT wallet_address
                FROM wallets
                WHERE last_seen >= NOW() - INTERVAL '7 days'
                  AND is_active = TRUE
            """)
            wallets = cursor.fetchall()
            self.discovered_wallets = {row[0] for row in wallets}
            logger.info(f"Cargados {len(self.discovered_wallets)} wallets descubiertos (activos últimos 7 días)")
            cursor.close()
        except Exception as e:
            logger.error(f"Error cargando wallets descubiertos: {e}")
            self.discovered_wallets = set()

    # =========================================================
    # NUEVO: Auto-descubrimiento de wallets desde tokens
    # =========================================================

    def discover_wallets_from_recent_tokens(self):
        """
        NUEVO: Descubre wallets activos escaneando transacciones de tokens recientes.

        Flujo:
        1. Obtiene tokens detectados en las últimas 2 horas
        2. Para cada token, escanea sus transacciones recientes
        3. Extrae los wallets que participaron
        4. Filtra: solo wallets con >= min_trades_to_track transacciones
        5. Agrega los mejores a discovered_wallets
        """
        try:
            cursor = self.conn.cursor()

            # 1. Tokens recientes (últimas 2 horas, máx 20 tokens)
            cursor.execute("""
                SELECT mint_address, token_id
                FROM tokens
                WHERE detected_at >= NOW() - INTERVAL '2 hours'
                  AND status = 'active'
                ORDER BY detected_at DESC
                LIMIT 40
            """)
            recent_tokens = cursor.fetchall()

            if not recent_tokens:
                logger.info("🔍 No hay tokens recientes para descubrir wallets")
                cursor.close()
                return 0

            logger.info(f"🔍 Buscando wallets activos en {len(recent_tokens)} tokens recientes...")

            # Wallets ya conocidos (no re-descubrir)
            all_known_wallets = self.tracked_wallets | self.discovered_wallets

            new_wallets_found = {}  # wallet_address -> {trades: N, tokens: set()}

            # 2. Escanear transacciones de cada token
            for mint_address, token_id in recent_tokens:
                try:
                    # Obtener firmas recientes del token
                    signatures_data = self.rpc.get_signatures_for_address(
                        mint_address,
                        limit=20
                    )

                    if not signatures_data:
                        continue

                    # Descargar y parsear transacciones
                    for sig_data in signatures_data[:20]:  # Máx 20 por token
                        sig = sig_data.get('signature')
                        if not sig:
                            continue

                        tx = self.rpc.get_transaction(sig)
                        if not tx:
                            continue

                        parsed = parse_swap_transaction(tx)
                        if not parsed or not parsed.get('wallet'):
                            continue

                        wallet = parsed['wallet']

                        # Ignorar wallets ya conocidos
                        if wallet in all_known_wallets:
                            continue

                        # Acumular info del wallet
                        if wallet not in new_wallets_found:
                            new_wallets_found[wallet] = {'trades': 0, 'tokens': set(), 'volume_sol': 0.0}

                        new_wallets_found[wallet]['trades'] += 1
                        new_wallets_found[wallet]['tokens'].add(mint_address)

                        # Estimar volumen
                        sol_addresses = {
                            "So11111111111111111111111111111111111111112",
                            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        }
                        if parsed.get('token_in') in sol_addresses:
                            new_wallets_found[wallet]['volume_sol'] += parsed.get('amount_in', 0)
                        elif parsed.get('token_out') in sol_addresses:
                            new_wallets_found[wallet]['volume_sol'] += parsed.get('amount_out', 0)

                    time.sleep(0.1)  # No sobrecargar RPC

                except Exception as e:
                    logger.debug(f"Error escaneando token {mint_address[:8]}: {e}")
                    continue

            cursor.close()

            # 3. Filtrar wallets interesantes
            interesting_wallets = {
                addr: info for addr, info in new_wallets_found.items()
                if info['trades'] >= self.min_trades_to_track
            }

            # 4. Limitar cantidad (top por volumen)
            slots_available = self.max_discovered_wallets - len(self.discovered_wallets - self.tracked_wallets)
            if slots_available <= 0:
                logger.info(f"🔍 Ya tenemos {len(self.discovered_wallets)} wallets descubiertos (máximo: {self.max_discovered_wallets})")
                return 0

            sorted_wallets = sorted(
                interesting_wallets.items(),
                key=lambda x: x[1]['volume_sol'],
                reverse=True
            )[:slots_available]

            # 5. Agregar a discovered_wallets
            added = 0
            for wallet_addr, info in sorted_wallets:
                self.discovered_wallets.add(wallet_addr)
                self.wallets_discovered += 1
                added += 1
                logger.info(
                    f"🆕 Wallet descubierto: {wallet_addr[:12]}... "
                    f"({info['trades']} trades, {len(info['tokens'])} tokens, "
                    f"{info['volume_sol']:.2f} SOL)"
                )

            if added > 0:
                logger.info(f"✅ {added} nuevos wallets agregados al tracking (total: {len(self.discovered_wallets)})")
            else:
                logger.info(f"🔍 No se encontraron wallets nuevos que cumplan los criterios")

            return added

        except Exception as e:
            logger.error(f"Error en descubrimiento de wallets: {e}")
            return 0

    def prune_inactive_wallets(self):
        """
        NUEVO: Elimina wallets descubiertos que llevan >24h sin actividad.
        Mantiene el pool limpio y enfocado en wallets activos.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT wallet_address
                FROM wallets
                WHERE last_seen < NOW() - INTERVAL '24 hours'
                  AND wallet_address NOT IN (
                      SELECT wallet_address FROM tracked_wallets WHERE is_active = TRUE
                  )
            """)
            inactive = {row[0] for row in cursor.fetchall()}
            cursor.close()

            pruned = self.discovered_wallets & inactive
            if pruned:
                self.discovered_wallets -= pruned
                logger.info(f"🧹 {len(pruned)} wallets inactivos removidos del tracking")

            return len(pruned)
        except Exception as e:
            logger.error(f"Error en poda de wallets: {e}")
            return 0

    # =========================================================
    # Funciones existentes (sin cambios)
    # =========================================================

    def is_memecoin_transaction(self, tx: Dict) -> bool:
        """Determina si una transacción es un swap de memecoin"""
        try:
            if not tx:
                return False
            if not tx.get('token_in') or not tx.get('token_out'):
                return False
            if tx.get('program_id') not in self.amm_program_ids:
                return False

            non_memecoins = {
                "So11111111111111111111111111111111111111112",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",
            }

            token_in = tx['token_in']
            token_out = tx['token_out']
            is_memecoin = (token_in not in non_memecoins) or (token_out not in non_memecoins)
            return is_memecoin

        except Exception as e:
            logger.error(f"Error verificando memecoin transaction: {e}")
            return False

    def get_or_create_token(self, mint_address: str, tx: Dict) -> Optional[int]:
        """Obtiene token_id o lo crea si no existe"""
        try:
            if mint_address in self.all_known_tokens:
                return self.all_known_tokens[mint_address]

            cursor = self.conn.cursor()

            try:
                cursor.execute("""
                    INSERT INTO tokens (
                        mint_address, amm, created_at, detected_at,
                        creation_signature, status, retention_category
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mint_address) DO NOTHING
                    RETURNING token_id
                """, (
                    mint_address,
                    "auto-discovered",
                    datetime.fromtimestamp(tx.get('blocktime', time.time())),
                    datetime.now(),
                    tx.get('signature', ''),
                    'active',
                    'short_term'
                ))
                result = cursor.fetchone()

                if result:
                    token_id = result[0]
                    self.conn.commit()
                    self.all_known_tokens[mint_address] = token_id
                    self.new_tokens_discovered += 1
                    logger.info(f"🆕 Token nuevo agregado: {mint_address[:16]}... (ID: {token_id})")
                    cursor.close()
                    return token_id
                else:
                    cursor.execute(
                        "SELECT token_id FROM tokens WHERE mint_address = %s",
                        (mint_address,)
                    )
                    existing = cursor.fetchone()
                    if existing:
                        token_id = existing[0]
                        self.all_known_tokens[mint_address] = token_id
                        cursor.close()
                        return token_id
                    else:
                        logger.error(f"Token {mint_address} no encontrado después de ON CONFLICT")
                        cursor.close()
                        return None

            except psycopg2.IntegrityError as e:
                self.conn.rollback()
                cursor.execute(
                    "SELECT token_id FROM tokens WHERE mint_address = %s",
                    (mint_address,)
                )
                existing = cursor.fetchone()
                if existing:
                    token_id = existing[0]
                    self.all_known_tokens[mint_address] = token_id
                    cursor.close()
                    return token_id
                else:
                    cursor.close()
                    return None

        except Exception as e:
            logger.error(f"Error obteniendo/creando token {mint_address}: {e}")
            self.conn.rollback()
            return None

    def scan_wallet_all_transactions(self, wallet_address: str, limit: int = 100) -> List[Dict]:
        """Escanea TODAS las transacciones del wallet"""
        try:
            signatures_data = self.rpc.get_signatures_for_address(
                wallet_address, limit=limit
            )
            if not signatures_data:
                return []

            new_signatures = []
            for sig_data in signatures_data:
                sig = sig_data.get('signature')
                if sig and sig not in self.processed_signatures:
                    new_signatures.append(sig)

            if not new_signatures:
                return []

            logger.info(f"📡 Escaneando {len(new_signatures)} transacciones de {wallet_address[:8]}...")

            full_transactions = []
            for sig in new_signatures:
                tx = self.rpc.get_transaction(sig)
                if tx:
                    full_transactions.append(tx)

            transactions = batch_process_transactions(full_transactions)

            memecoin_txs = []
            for tx in transactions:
                if self.is_memecoin_transaction(tx):
                    memecoin_txs.append(tx)
                    self.processed_signatures.add(tx['signature'])

            if len(self.processed_signatures) > self.max_cache_size:
                self.processed_signatures = set(list(self.processed_signatures)[-self.max_cache_size//2:])

            if memecoin_txs:
                logger.info(f"✅ {wallet_address[:8]}... : {len(memecoin_txs)} transacciones de memecoins encontradas")

            return memecoin_txs

        except Exception as e:
            logger.error(f"Error escaneando wallet {wallet_address}: {e}")
            return []

    def detect_partial_fills(self, transactions: List[Dict]) -> List[Dict]:
        """Detecta órdenes parciales"""
        try:
            if not transactions:
                return transactions

            groups = defaultdict(list)
            for tx in transactions:
                memecoin_mint = None
                if tx['token_out'] not in ["So11111111111111111111111111111111111111112"]:
                    memecoin_mint = tx['token_out']
                elif tx['token_in'] not in ["So11111111111111111111111111111111111111112"]:
                    memecoin_mint = tx['token_in']

                if not memecoin_mint:
                    continue

                time_window = tx['blocktime'] // 300
                key = (tx['wallet'], memecoin_mint, tx['type'], time_window)
                groups[key].append(tx)

            for key, group_txs in groups.items():
                if len(group_txs) > 1:
                    order_id = f"{key[0][:8]}_{key[1][:8]}_{key[2]}_{key[3]}"
                    for i, tx in enumerate(group_txs, 1):
                        tx['is_partial'] = True
                        tx['order_id'] = order_id
                        tx['partial_fill_index'] = i
                    logger.info(f"✂️ Detectadas {len(group_txs)} transacciones parciales para orden {order_id}")

            return transactions

        except Exception as e:
            logger.error(f"Error detectando parciales: {e}")
            return transactions

    def process_transaction(self, tx: Dict):
        """Procesa transacción con auto-descubrimiento de tokens"""
        try:
            wallet_address = tx['wallet']

            memecoin_mint = None
            sol_amount = 0
            token_amount = 0
            tx_type = tx['type']

            sol_addresses = {
                "So11111111111111111111111111111111111111112",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            }

            if tx['token_out'] not in sol_addresses:
                memecoin_mint = tx['token_out']
                token_amount = tx['amount_out']
                sol_amount = tx['amount_in']
                tx_type = 'buy'
            elif tx['token_in'] not in sol_addresses:
                memecoin_mint = tx['token_in']
                token_amount = tx['amount_in']
                sol_amount = tx['amount_out']
                tx_type = 'sell'

            if not memecoin_mint:
                return

            token_id = self.get_or_create_token(memecoin_mint, tx)
            if not token_id:
                return

            price = sol_amount / token_amount if token_amount > 0 else 0

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
                datetime.fromtimestamp(tx['blocktime']),
                0,
                tx.get('is_partial', False),
                tx.get('order_id')
            ))
            self.conn.commit()
            cursor.close()
            self.transactions_processed += 1

            if wallet_address not in self.discovered_wallets:
                self.discovered_wallets.add(wallet_address)
                self.wallets_discovered += 1
                logger.info(f"🆕 Wallet descubierto: {wallet_address[:16]}...")

        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
            self.conn.rollback()
            self.errors_count += 1

    def track_wallet_batch(self, wallet_addresses: List[str]):
        """Rastrea un lote de wallets (TODAS sus transacciones)"""
        try:
            for wallet in wallet_addresses:
                transactions = self.scan_wallet_all_transactions(wallet, limit=50)
                if not transactions:
                    continue
                transactions = self.detect_partial_fills(transactions)
                for tx in transactions:
                    self.process_transaction(tx)
                time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error rastreando lote: {e}")

    def run_tracking_cycle(self):
        """Ejecuta un ciclo de tracking completo"""
        try:
            all_wallets = list(self.tracked_wallets | self.discovered_wallets)
            if not all_wallets:
                logger.warning("No hay wallets para rastrear")
                return 0

            logger.info(f"🔍 Rastreando {len(all_wallets)} wallets (TODAS sus transacciones)...")

            batch_size = 10
            transactions_count = 0
            for i in range(0, len(all_wallets), batch_size):
                batch = all_wallets[i:i+batch_size]
                initial_count = self.transactions_processed
                self.track_wallet_batch(batch)
                batch_txs = self.transactions_processed - initial_count
                transactions_count += batch_txs
                if batch_txs > 0:
                    logger.info(f"Lote {i//batch_size + 1}: {batch_txs} transacciones procesadas")

            return transactions_count

        except Exception as e:
            logger.error(f"Error en ciclo: {e}")
            return 0

    def print_stats(self):
        """Imprime estadísticas"""
        uptime = datetime.now() - self.start_time
        logger.info("=" * 70)
        logger.info("📊 ENHANCED WALLET TRACKER - ESTADÍSTICAS")
        logger.info("=" * 70)
        logger.info(f"Tiempo activo: {uptime}")
        logger.info(f"Wallets rastreados manualmente: {len(self.tracked_wallets)}")
        logger.info(f"Wallets descubiertos: {len(self.discovered_wallets)}")
        logger.info(f"Total wallets: {len(self.tracked_wallets | self.discovered_wallets)}")
        logger.info(f"Tokens conocidos: {len(self.all_known_tokens)}")
        logger.info(f"Tokens nuevos descubiertos: {self.new_tokens_discovered}")
        logger.info(f"Transacciones procesadas: {self.transactions_processed}")
        logger.info(f"Errores: {self.errors_count}")
        logger.info("=" * 70)

    def add_wallet_to_track(self, wallet_address: str, label: str = "", reason: str = ""):
        """Agrega wallet al tracking"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO tracked_wallets (wallet_address, label, reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (wallet_address) DO UPDATE
                SET is_active = TRUE
            """, (wallet_address, label, reason))
            self.conn.commit()
            cursor.close()
            self.tracked_wallets.add(wallet_address)
            logger.info(f"✅ Wallet agregado: {wallet_address} ({label})")
        except Exception as e:
            logger.error(f"Error agregando wallet: {e}")

    def run(self, reload_interval_minutes: int = 30, cycle_interval_seconds: int = 60):
        """
        Bucle principal
        MEJORADO: Incluye auto-descubrimiento periódico de wallets
        """
        logger.info("🚀 Iniciando ENHANCED WalletTracker (seguimiento completo)...")
        self.connect_db()
        self.load_all_known_tokens()
        self.load_tracked_wallets()
        self.load_discovered_wallets()

        last_reload = datetime.now()
        self.last_discovery_time = datetime.now()
        cycle_count = 0

        try:
            while True:
                cycle_start = time.time()

                # Recargar listas periódicamente
                if datetime.now() - last_reload >= timedelta(minutes=reload_interval_minutes):
                    logger.info("♻️ Recargando listas...")
                    self.load_all_known_tokens()
                    self.load_tracked_wallets()
                    self.load_discovered_wallets()
                    last_reload = datetime.now()

                # NUEVO: Auto-descubrir wallets cada N minutos
                if datetime.now() - self.last_discovery_time >= timedelta(minutes=self.discovery_interval_minutes):
                    logger.info("🔍 Iniciando auto-descubrimiento de wallets...")
                    new_found = self.discover_wallets_from_recent_tokens()
                    pruned = self.prune_inactive_wallets()
                    self.last_discovery_time = datetime.now()
                    if new_found > 0 or pruned > 0:
                        logger.info(f"📊 Descubrimiento: +{new_found} nuevos, -{pruned} inactivos")

                # Ejecutar ciclo de tracking
                txs_count = self.run_tracking_cycle()
                cycle_count += 1

                # Stats cada 10 ciclos
                if cycle_count % 10 == 0:
                    self.print_stats()

                # Esperar
                elapsed = time.time() - cycle_start
                wait_time = max(0, cycle_interval_seconds - elapsed)
                logger.info(f"Ciclo {cycle_count} completado en {elapsed:.2f}s. Esperando {wait_time:.2f}s...")
                time.sleep(wait_time)

        except KeyboardInterrupt:
            logger.info("\n⚠️  Deteniendo...")
            self.print_stats()
        except Exception as e:
            logger.error(f"Error fatal: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


if __name__ == "__main__":
    DB_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "database": "memecoins_db",
        "user": "postgres",
        "password": "12345"
    }

    RPC_URL = "http://127.0.0.1:7211"
    tracker = EnhancedWalletTracker(DB_CONFIG, RPC_URL)
    tracker.run(
        reload_interval_minutes=30,
        cycle_interval_seconds=60  # 1 minuto entre ciclos
    )
