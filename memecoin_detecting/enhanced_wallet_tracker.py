#!/usr/bin/env python3
"""
enhanced_wallet_tracker.py
SEGUIMIENTO COMPLETO de wallets - Rastrea TODAS sus transacciones con memecoins
v5.1 - FIXES CRÍTICOS:
  - _safe_rollback() en TODOS los except para evitar envenenamiento de conexión
  - Rollback preventivo entre cada load_*() en run()
  - Fix del bug de logger duplicado (handlers.clear())
"""

import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Set
from rpc_helpers import SolanaRPC, parse_swap_transaction, batch_process_transactions
from collections import defaultdict

# ─────────────────────────────────────────────────────────────
# Configuración de logging (FIX: handlers.clear())
# ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers.clear()  # ← FIX: Prevenir handlers duplicados
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('/home/rebelforce/scripts/memecoin_detecting/enhanced_wallet_tracker.log')
fh.setFormatter(formatter)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)
logger.propagate = False

class EnhancedWalletTracker:
    """
    SEGUIMIENTO COMPLETO de wallets

    Diferencias con WalletTracker normal:
    1. Rastrea TODAS las transacciones del wallet (no solo memecoins monitoreadas)
    2. Auto-descubre nuevas memecoins cuando un wallet las compra
    3. Sigue la actividad completa del wallet en la blockchain
    4. Detecta patrones: si compra memecoin A, también compra B, C, D?
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
        self.discovery_interval_minutes = 3  # Descubrir wallets cada 3 min
        self.max_discovered_wallets = 1000   # Máximo wallets auto-descubiertos activos
        self.min_trades_to_track = 2         # Mínimo trades para considerar un wallet interesante

        # Program IDs de AMMs conocidos para detectar swaps
        self.amm_program_ids = {
            '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',   # Pump.fun
            'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',   # PumpSwap
            'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',  # Raydium AMM
            'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj',  # Raydium LaunchLab
            'FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X',  # FluxBeam
            'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o',  # HeavenDEX
            'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',   # Meteora DLMM
            'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG',   # Meteora DYN2
            'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',  # Meteora DYN
            'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN',   # Meteora DBC
            'MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG',   # Moonit
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',   # Orca
        }

        # Estadísticas
        self.transactions_processed = 0
        self.wallets_discovered = 0
        self.new_tokens_discovered = 0
        self.errors_count = 0
        self.start_time = datetime.now()

    # ═══════════════════════════════════════════════════════════
    # FIX CRÍTICO v5.1: Safe rollback para prevenir envenenamiento
    # ═══════════════════════════════════════════════════════════
    def _safe_rollback(self):
        """
        Hace rollback seguro de la transacción actual.
        Previene el estado "transaction is aborted" que mata todas
        las queries posteriores.
        """
        try:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
        except Exception:
            pass

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

    # ═══════════════════════════════════════════════════════════
    # NUEVO v5: Anti-duplicados al reiniciar
    # FIX v5.1: Agregar rollback en except
    # ═══════════════════════════════════════════════════════════
    def load_processed_signatures(self):
        """
        Carga signatures ya procesados de la BD para evitar
        duplicados al reiniciar el servicio.
        Sin esto, el primer ciclo re-procesa txs que ya están en la BD.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT signature
                FROM wallet_transactions
                ORDER BY time DESC
                LIMIT %s
            """, (self.max_cache_size,))
            rows = cursor.fetchall()
            self.processed_signatures = {row[0] for row in rows}
            cursor.close()
            logger.info(f"📋 Cargados {len(self.processed_signatures)} signatures previos (anti-duplicados)")
        except Exception as e:
            logger.error(f"Error cargando processed_signatures: {e}")
            self._safe_rollback()  # ← FIX v5.1
            self.processed_signatures = set()

    def load_all_known_tokens(self):
        """NUEVO: Carga TODOS los tokens conocidos (no solo últimas 24h)"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT token_id, mint_address FROM tokens WHERE status = 'active'
            """)
            tokens = cursor.fetchall()
            self.all_known_tokens = {row[1]: row[0] for row in tokens}
            logger.info(f"Cargados {len(self.all_known_tokens)} tokens conocidos (histórico completo)")
            cursor.close()
        except Exception as e:
            logger.error(f"Error cargando tokens: {e}")
            self._safe_rollback()  # ← FIX v5.1
            self.all_known_tokens = {}

    def load_tracked_wallets(self):
        """Carga wallets rastreados manualmente"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT wallet_address FROM tracked_wallets WHERE is_active = TRUE")
            wallets = cursor.fetchall()
            self.tracked_wallets = {row[0] for row in wallets}
            logger.info(f"Cargados {len(self.tracked_wallets)} wallets rastreados manualmente")
            cursor.close()
        except Exception as e:
            logger.error(f"Error cargando wallets rastreados: {e}")
            self._safe_rollback()  # ← FIX v5.1
            self.tracked_wallets = set()

    def load_discovered_wallets(self):
        """Carga wallets descubiertos automáticamente que ya tienen actividad"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT DISTINCT wallet_address FROM wallets
                WHERE last_seen > NOW() - INTERVAL '7 days'
                AND is_active = TRUE
            """)
            wallets = cursor.fetchall()
            self.discovered_wallets = {row[0] for row in wallets}
            logger.info(f"Cargados {len(self.discovered_wallets)} wallets descubiertos activos (últimos 7 días)")
            cursor.close()
        except Exception as e:
            logger.error(f"Error cargando wallets descubiertos: {e}")
            self._safe_rollback()  # ← FIX v5.1
            self.discovered_wallets = set()

    def discover_wallets_from_recent_tokens(self):
        """
        NUEVO: Descubre wallets activos escaneando transacciones de tokens recientes.
        Flujo:
        1. Obtiene tokens detectados en las últimas 2 horas
        2. Para cada token, escanea sus transacciones recientes
        3. Extrae los wallets que participaron
        4. Filtra solo wallets con min_trades_to_track+ transacciones
        5. Agrega los mejores a discovered_wallets
        """
        try:
            cursor = self.conn.cursor()

            # 1. Tokens recientes (últimas 2 horas, máx 80 tokens)
            cursor.execute("""
                SELECT mint_address, token_id FROM tokens
                WHERE detected_at > NOW() - INTERVAL '2 hours'
                AND status = 'active'
                ORDER BY detected_at DESC
                LIMIT 80
            """)
            recent_tokens = cursor.fetchall()

            if not recent_tokens:
                logger.info("No hay tokens recientes para descubrir wallets")
                cursor.close()
                return 0

            logger.info(f"🔍 Buscando wallets activos en {len(recent_tokens)} tokens recientes...")

            # Wallets ya conocidos (no re-descubrir)
            all_known_wallets = self.tracked_wallets | self.discovered_wallets
            new_wallets_found = {}  # wallet_address -> {trades: N, tokens: set}

            for mint_address, token_id in recent_tokens:
                try:
                    # 2. Escanear transacciones de cada token
                    signatures_data = self.rpc.get_signatures_for_address(mint_address, limit=50)
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
                            'So11111111111111111111111111111111111111112',
                            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
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
                logger.info(f"Ya tenemos {len(self.discovered_wallets)} wallets descubiertos (máximo {self.max_discovered_wallets})")
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
                    f"{info['trades']} trades, {len(info['tokens'])} tokens, "
                    f"{info['volume_sol']:.2f} SOL"
                )

            if added > 0:
                logger.info(f"✅ {added} nuevos wallets agregados al tracking (total {len(self.discovered_wallets)})")
            else:
                logger.info(f"No se encontraron wallets nuevos que cumplan los criterios")

            return added

        except Exception as e:
            logger.error(f"Error en descubrimiento de wallets: {e}")
            self._safe_rollback()  # ← FIX v5.1
            return 0

    def prune_inactive_wallets(self):
        """
        NUEVO: Elimina wallets descubiertos que llevan >24h sin actividad.
        Mantiene el pool limpio y enfocado en wallets activos.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT wallet_address FROM wallets
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
            self._safe_rollback()  # ← FIX v5.1
            return 0

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
                'So11111111111111111111111111111111111111112',
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs',
            }
            token_in = tx['token_in']
            token_out = tx['token_out']
            is_memecoin = (token_in not in non_memecoins) or (token_out not in non_memecoins)
            return is_memecoin
        except Exception as e:
            logger.error(f"Error verificando memecoin transaction: {e}")
            return False

    def get_or_create_token(self, mint_address: str, tx: Dict) -> Optional[int]:
        """Obtiene token_id existente o crea nuevo token"""
        try:
            # Primero buscar en cache
            if mint_address in self.all_known_tokens:
                return self.all_known_tokens[mint_address]

            cursor = self.conn.cursor()
            cursor.execute("SELECT token_id FROM tokens WHERE mint_address = %s", (mint_address,))
            row = cursor.fetchone()

            if row:
                token_id = row[0]
                self.all_known_tokens[mint_address] = token_id
                cursor.close()
                return token_id

            # Crear nuevo token
            program_id = tx.get('program_id', 'unknown')
            pool_address = tx.get('pool_address')
            block_time = tx.get('blocktime', 0)
            detected_at = datetime.fromtimestamp(block_time) if block_time > 0 else datetime.now()

            cursor.execute("""
                INSERT INTO tokens (mint_address, program_id, pool_address, detected_at, status)
                VALUES (%s, %s, %s, %s, 'active')
                ON CONFLICT (mint_address) DO UPDATE SET status = 'active'
                RETURNING token_id
            """, (mint_address, program_id, pool_address, detected_at))
            token_id = cursor.fetchone()[0]
            self.conn.commit()
            cursor.close()

            self.all_known_tokens[mint_address] = token_id
            self.new_tokens_discovered += 1
            logger.info(f"🆕 Nuevo token descubierto: {mint_address[:12]}... (ID: {token_id})")

            return token_id

        except Exception as e:
            logger.error(f"Error obteniendo/creando token: {e}")
            self._safe_rollback()  # ← FIX v5.1
            return None

    def detect_partial_fills(self, transactions: List[Dict]) -> List[Dict]:
        """Detecta órdenes parciales"""
        try:
            if not transactions:
                return transactions

            groups = defaultdict(list)
            for tx in transactions:
                memecoin_mint = None
                if tx['token_out'] not in ('So11111111111111111111111111111111111111112',):
                    memecoin_mint = tx['token_out']
                elif tx['token_in'] not in ('So11111111111111111111111111111111111111112',):
                    memecoin_mint = tx['token_in']

                if not memecoin_mint:
                    continue

                # Clave de agrupación
                time_window = tx['blocktime'] // 300  # Ventanas de 5 minutos
                key = (tx['wallet'], memecoin_mint, tx['type'], time_window)
                groups[key].append(tx)

            # Marcar transacciones parciales
            for key, group_txs in groups.items():
                if len(group_txs) > 1:
                    order_id = f"{key[0][:8]}{key[1][:8]}{key[2]}{key[3]}"
                    for i, tx in enumerate(group_txs, 1):
                        tx['is_partial'] = True
                        tx['order_id'] = order_id
                        tx['partial_fill_index'] = i
                    logger.info(f"🧩 Detectadas {len(group_txs)} transacciones parciales para orden {order_id}")

            return transactions

        except Exception as e:
            logger.error(f"Error detectando parciales: {e}")
            return transactions

    # ═══════════════════════════════════════════════════════════
    # v4 FIX: process_transaction confía en tx['type']
    # v5.1 FIX: Agregar rollback en except
    # ═══════════════════════════════════════════════════════════
    def process_transaction(self, tx: Dict):
        """
        Procesa una transacción de swap.
        Ahora confía en tx['type'] de parse_swap (ya corregido).
        """
        try:
            signature = tx['signature']

            # Skip duplicados
            if signature in self.processed_signatures:
                return

            # Skip si no tiene type válido
            tx_type = tx.get('type')
            if not tx_type or tx_type not in ['buy', 'sell']:
                return

            wallet = tx['wallet']
            token_in = tx.get('token_in')
            token_out = tx.get('token_out')
            amount_in = tx.get('amount_in', 0)
            amount_out = tx.get('amount_out', 0)

            # Determinar memecoin_mint según tx_type
            sol_addresses = {
                'So11111111111111111111111111111111111111112',  # WSOL
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  # USDC
            }

            if tx_type == 'buy':
                # BUY: wallet recibió tokens (token_in), dió SOL (token_out)
                memecoin_mint = token_in
                token_amount = amount_in
                sol_amount = amount_out
            else:  # sell
                # SELL: wallet recibió SOL (token_in), dió tokens (token_out)
                memecoin_mint = token_out
                token_amount = amount_out
                sol_amount = amount_in

            # Validaciones
            if memecoin_mint in sol_addresses:
                return

            if token_amount <= 0:
                return

            if tx_type == 'buy' and sol_amount <= 0:
                return

            # Calcular precio
            if token_amount > 0:
                price = sol_amount / token_amount
            else:
                price = 0.0

            # Obtener o crear token
            token_id = self.get_or_create_token(memecoin_mint, tx)
            if not token_id:
                return

            block_time = tx.get('blocktime', 0)
            if block_time > 0:
                transaction_time = datetime.fromtimestamp(block_time)
            else:
                transaction_time = datetime.now()

            # Llamar función SQL para procesar la transacción
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT process_transaction(
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                wallet,
                memecoin_mint,
                signature,
                tx_type,
                token_amount,
                sol_amount,
                price,
                transaction_time
            ))
            self.conn.commit()
            cursor.close()

            self.processed_signatures.add(signature)
            self.transactions_processed += 1

            logger.info(
                f"{'🟢' if tx_type == 'buy' else '🔴'} {tx_type.upper()} {wallet[:12]}... "
                f"{token_amount:.2f} tokens @ {price:.8f} SOL "
                f"(Total: {sol_amount:.4f} SOL)"
            )

        except Exception as e:
            logger.error(f"Error procesando transacción {tx.get('signature', 'unknown')[:8]}: {e}")
            self._safe_rollback()  # ← FIX v5.1

    def track_wallet_batch(self, wallets: List[str], limit: int = 50):
        """Rastrea un lote de wallets"""
        try:
            for wallet in wallets:
                try:
                    # Obtener transacciones recientes
                    signatures_data = self.rpc.get_signatures_for_address(wallet, limit=limit)
                    if not signatures_data:
                        continue

                    # Filtrar solo signatures no procesados
                    new_sigs = [
                        sig_data for sig_data in signatures_data
                        if sig_data.get('signature') not in self.processed_signatures
                    ]

                    if not new_sigs:
                        continue

                    # Descargar transacciones en batch
                    signatures = [sig_data['signature'] for sig_data in new_sigs]
                    transactions = batch_process_transactions(self.rpc, signatures, parse_swap_transaction)

                    # Filtrar solo transacciones de memecoin
                    memecoin_txs = [tx for tx in transactions if self.is_memecoin_transaction(tx)]

                    if not memecoin_txs:
                        continue

                    # Detectar parciales
                    memecoin_txs = self.detect_partial_fills(memecoin_txs)

                    # Procesar cada transacción
                    for tx in memecoin_txs:
                        self.process_transaction(tx)

                    logger.info(f"📊 Wallet {wallet[:12]}... procesó {len(memecoin_txs)} transacciones de memecoin")

                except Exception as e:
                    logger.error(f"Error rastreando wallet {wallet[:12]}...: {e}")
                    self.errors_count += 1
                    continue

                time.sleep(0.05)  # Rate limiting suave

        except Exception as e:
            logger.error(f"Error rastreando lote: {e}")
            self._safe_rollback()  # ← FIX v5.1

    def run_tracking_cycle(self) -> int:
        """Ejecuta un ciclo de tracking"""
        try:
            # Combinar wallets rastreados y descubiertos
            all_wallets = list(self.tracked_wallets | self.discovered_wallets)

            if not all_wallets:
                logger.warning("No hay wallets para rastrear")
                return 0

            logger.info(f"📊 Rastreando {len(all_wallets)} wallets ({len(self.tracked_wallets)} manuales, {len(self.discovered_wallets)} descubiertos)...")

            # Procesar en lotes
            batch_size = 10
            for i in range(0, len(all_wallets), batch_size):
                batch = all_wallets[i:i+batch_size]
                self.track_wallet_batch(batch)

            return len(all_wallets)

        except Exception as e:
            logger.error(f"Error en ciclo: {e}")
            self._safe_rollback()  # ← FIX v5.1
            return 0

    def print_stats(self):
        """Imprime estadísticas"""
        uptime = datetime.now() - self.start_time
        logger.info("═" * 70)
        logger.info("📊 ESTADÍSTICAS")
        logger.info(f"Uptime: {uptime}")
        logger.info(f"Transacciones procesadas: {self.transactions_processed}")
        logger.info(f"Wallets descubiertos: {self.wallets_discovered}")
        logger.info(f"Tokens nuevos: {self.new_tokens_discovered}")
        logger.info(f"Errores: {self.errors_count}")
        logger.info(f"Wallets activos: {len(self.tracked_wallets | self.discovered_wallets)}")
        logger.info("═" * 70)

    # ═══════════════════════════════════════════════════════════
    # FIX v5.1: Rollback preventivo entre cada load_*()
    # ═══════════════════════════════════════════════════════════
    def run(self):
        """Loop principal"""
        try:
            self.connect_db()
            
            # FIX v5.1: Rollback preventivo entre cada carga para aislar fallos
            self.load_processed_signatures()
            self._safe_rollback()  # ← Limpiar transacción
            
            self.load_all_known_tokens()
            self._safe_rollback()  # ← Limpiar transacción
            
            self.load_tracked_wallets()
            self._safe_rollback()  # ← Limpiar transacción
            
            self.load_discovered_wallets()
            self._safe_rollback()  # ← Limpiar transacción

            cycle_count = 0
            while True:
                cycle_count += 1
                start_time = time.time()

                # Auto-descubrimiento cada N minutos
                should_discover = (
                    self.last_discovery_time is None or
                    (datetime.now() - self.last_discovery_time).total_seconds() > self.discovery_interval_minutes * 60
                )

                if should_discover:
                    logger.info("🔍 Iniciando auto-descubrimiento de wallets...")
                    new_wallets = self.discover_wallets_from_recent_tokens()
                    self.prune_inactive_wallets()
                    self.last_discovery_time = datetime.now()

                # Ejecutar ciclo de tracking
                tracked = self.run_tracking_cycle()

                # Estadísticas cada 10 ciclos
                if cycle_count % 10 == 0:
                    self.print_stats()

                elapsed = time.time() - start_time
                sleep_time = max(0, 60 - elapsed)  # Un ciclo cada 60 segundos
                logger.info(f"Ciclo {cycle_count} completado en {elapsed:.2f}s. Esperando {sleep_time:.2f}s...")
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Detenido por el usuario")
        except Exception as e:
            logger.error(f"Error fatal: {e}")
            self._safe_rollback()  # ← FIX v5.1
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Conexión cerrada")

if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'memecoins_db',
        'user': 'rebelforce',
        'password': 'Lol123123!'
    }

    tracker = EnhancedWalletTracker(db_config)
    tracker.run()
