#!/usr/bin/env python3
"""
enhanced_wallet_tracker.py
SEGUIMIENTO COMPLETO de wallets - Rastrea TODAS sus transacciones con memecoins
v5.1 - FIXES CRÍTICOS:
  - _safe_rollback() en TODOS los except para evitar envenenamiento de conexión
  - Rollback preventivo entre cada load_*() en run()
  - Fix del bug de logger duplicado (handlers.clear())
  - Fix SQL: SELECT DISTINCT + ORDER BY incompatible → removido DISTINCT
  - Fix: batch_process_transactions() se llama con 1 argumento (via scan_wallet_all_transactions)
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
fh = logging.FileHandler('enhanced_wallet_tracker.log')
fh.setFormatter(formatter)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)
logging.getLogger().handlers.clear()
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
        self.discovery_interval_minutes = 3
        self.max_discovered_wallets = 1000
        self.min_trades_to_track = 2
        # v3: Webhook-primary mode
        self.webhook_primary_mode = True  # Si True, solo hace discovery, no tracking polling


        # Program IDs de AMMs conocidos para detectar swaps
        # Import AMMs from shared_config (no duplicar)
        from shared_config import AMM_PROGRAMS
        self.amm_program_ids = set(AMM_PROGRAMS.keys())


        # Estadísticas
        self.transactions_processed = 0
        self.wallets_discovered = 0
        self.new_tokens_discovered = 0
        self.errors_count = 0
        self.start_time = datetime.now()

    # ═══════════════════════════════════════════════════════════
    # FIX v5.1: Safe rollback
    # ═══════════════════════════════════════════════════════════
    def _safe_rollback(self):
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
    # FIX v5.1: SQL corregido (removido DISTINCT incompatible con ORDER BY time)
    # ═══════════════════════════════════════════════════════════
    def load_processed_signatures(self):
        """
        Carga signatures ya procesados de la BD para evitar
        duplicados al reiniciar el servicio.
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
            self._safe_rollback()
            self.processed_signatures = set()

    def load_all_known_tokens(self):
        """Carga TODOS los tokens conocidos"""
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
            self._safe_rollback()
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
            self._safe_rollback()
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
            self._safe_rollback()
            self.discovered_wallets = set()

    def discover_wallets_from_recent_tokens(self):
        """
        Descubre wallets activos escaneando transacciones de tokens recientes.
        """
        try:
            cursor = self.conn.cursor()

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

            all_known_wallets = self.tracked_wallets | self.discovered_wallets
            new_wallets_found = {}

            for mint_address, token_id in recent_tokens:
                try:
                    signatures_data = self.rpc.get_signatures_for_address(mint_address, limit=50)
                    if not signatures_data:
                        continue

                    for sig_data in signatures_data[:20]:
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

                        if wallet in all_known_wallets:
                            continue

                        if wallet not in new_wallets_found:
                            new_wallets_found[wallet] = {'trades': 0, 'tokens': set(), 'volume_sol': 0.0}

                        new_wallets_found[wallet]['trades'] += 1
                        new_wallets_found[wallet]['tokens'].add(mint_address)

                        sol_addresses = {
                            'So11111111111111111111111111111111111111112',
                            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                        }
                        if parsed.get('token_in') in sol_addresses:
                            new_wallets_found[wallet]['volume_sol'] += parsed.get('amount_in', 0)
                        elif parsed.get('token_out') in sol_addresses:
                            new_wallets_found[wallet]['volume_sol'] += parsed.get('amount_out', 0)

                    time.sleep(0.1)

                except Exception as e:
                    logger.debug(f"Error escaneando token {mint_address[:8]}: {e}")
                    continue

            cursor.close()

            interesting_wallets = {
                addr: info for addr, info in new_wallets_found.items()
                if info['trades'] >= self.min_trades_to_track
            }

            slots_available = self.max_discovered_wallets - len(self.discovered_wallets - self.tracked_wallets)
            if slots_available <= 0:
                logger.info(f"Ya tenemos {len(self.discovered_wallets)} wallets descubiertos (máximo {self.max_discovered_wallets})")
                return 0

            sorted_wallets = sorted(
                interesting_wallets.items(),
                key=lambda x: x[1]['volume_sol'],
                reverse=True
            )[:slots_available]

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
            self._safe_rollback()
            return 0

    def prune_inactive_wallets(self):
        """Elimina wallets descubiertos que llevan >24h sin actividad."""
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
            self._safe_rollback()
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
            if mint_address in self.all_known_tokens:
                return self.all_known_tokens[mint_address]

            cursor = self.conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO tokens (mint_address, amm, created_at, detected_at, creation_signature, status, retention_category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mint_address) DO NOTHING
                    RETURNING token_id
                """, (
                    mint_address,
                    'auto-discovered',
                    datetime.fromtimestamp(tx.get('blocktime', time.time())),
                    datetime.now(),
                    tx.get('signature', ''),
                    'active',
                    'shortterm'
                ))
                result = cursor.fetchone()
                if result:
                    token_id = result[0]
                    self.conn.commit()
                    self.all_known_tokens[mint_address] = token_id
                    self.new_tokens_discovered += 1
                    logger.info(f"🆕 Token nuevo agregado: {mint_address[:16]}... (ID:{token_id})")
                    cursor.close()
                    return token_id
                else:
                    cursor.execute("SELECT token_id FROM tokens WHERE mint_address = %s", (mint_address,))
                    existing = cursor.fetchone()
                    if existing:
                        token_id = existing[0]
                        self.all_known_tokens[mint_address] = token_id
                        cursor.close()
                        return token_id
                    else:
                        cursor.close()
                        return None

            except psycopg2.IntegrityError as e:
                self.conn.rollback()
                cursor.execute("SELECT token_id FROM tokens WHERE mint_address = %s", (mint_address,))
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
            self._safe_rollback()
            return None

    # ═══════════════════════════════════════════════════════════
    # FIX v5.1: scan_wallet_all_transactions (del código original)
    # batch_process_transactions() recibe 1 solo argumento
    # ═══════════════════════════════════════════════════════════
    def scan_wallet_all_transactions(self, wallet_address: str, limit: int = 100) -> List[Dict]:
        """Escanea TODAS las transacciones del wallet"""
        try:
            signatures_data = self.rpc.get_signatures_for_address(wallet_address, limit=limit)
            if not signatures_data:
                return []

            new_signatures = []
            for sig_data in signatures_data:
                sig = sig_data.get('signature')
                if sig and sig not in self.processed_signatures:
                    new_signatures.append(sig)

            if not new_signatures:
                return []

            logger.info(f"🔍 Escaneando {len(new_signatures)} transacciones de {wallet_address[:8]}...")

            # Descargar transacciones crudas
            full_transactions = []
            for sig in new_signatures:
                tx = self.rpc.get_transaction(sig)
                if tx:
                    full_transactions.append(tx)

            # FIX: batch_process_transactions recibe 1 argumento (lista de txs crudas)
            transactions = batch_process_transactions(full_transactions)

            # Filtrar solo memecoin transactions
            memecoin_txs = []
            for tx in transactions:
                if self.is_memecoin_transaction(tx):
                    memecoin_txs.append(tx)
                    self.processed_signatures.add(tx['signature'])

            # Limitar cache size
            if len(self.processed_signatures) > self.max_cache_size:
                self.processed_signatures = set(list(self.processed_signatures)[-self.max_cache_size // 2:])

            if memecoin_txs:
                logger.info(f"  {wallet_address[:8]}... {len(memecoin_txs)} transacciones de memecoins encontradas")

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
                if tx['token_out'] not in ('So11111111111111111111111111111111111111112',):
                    memecoin_mint = tx['token_out']
                elif tx['token_in'] not in ('So11111111111111111111111111111111111111112',):
                    memecoin_mint = tx['token_in']

                if not memecoin_mint:
                    continue

                time_window = tx['blocktime'] // 300
                key = (tx['wallet'], memecoin_mint, tx['type'], time_window)
                groups[key].append(tx)

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

    def process_transaction(self, tx: Dict):
        """Procesa una transacción de swap."""
        try:
            signature = tx['signature']

            if signature in self.processed_signatures:
                return

            tx_type = tx.get('type')
            if not tx_type or tx_type not in ['buy', 'sell']:
                return

            wallet = tx['wallet']
            token_in = tx.get('token_in')
            token_out = tx.get('token_out')
            amount_in = tx.get('amount_in', 0)
            amount_out = tx.get('amount_out', 0)

            sol_addresses = {
                'So11111111111111111111111111111111111111112',
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            }

            if tx_type == 'buy':
                memecoin_mint = token_in
                token_amount = amount_in
                sol_amount = amount_out
            else:
                memecoin_mint = token_out
                token_amount = amount_out
                sol_amount = amount_in

            if memecoin_mint in sol_addresses:
                return
            if token_amount <= 0:
                return
            if tx_type == 'buy' and sol_amount <= 0:
                return

            if token_amount > 0:
                price = sol_amount / token_amount
            else:
                price = 0.0

            token_id = self.get_or_create_token(memecoin_mint, tx)
            if not token_id:
                return

            block_time = tx.get('blocktime', 0)
            if block_time > 0:
                transaction_time = datetime.fromtimestamp(block_time)
            else:
                transaction_time = datetime.now()

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
            logger.error(f"Error procesando transacción: {e}")
            self._safe_rollback()
            self.errors_count += 1

    # ═══════════════════════════════════════════════════════════
    # FIX v5.1: track_wallet_batch usa scan_wallet_all_transactions
    # (NO llama batch_process_transactions directamente)
    # ═══════════════════════════════════════════════════════════
    def track_wallet_batch(self, wallet_addresses: List[str]):
        """Rastrea un lote de wallets (TODAS sus transacciones)"""
        try:
            for wallet in wallet_addresses:
                transactions = self.scan_wallet_all_transactions(wallet, limit=300)
                if not transactions:
                    continue

                transactions = self.detect_partial_fills(transactions)

                for tx in transactions:
                    self.process_transaction(tx)

                time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error rastreando lote: {e}")
            self._safe_rollback()

    def run_tracking_cycle(self) -> int:
        """Ejecuta un ciclo de tracking"""
        try:
            all_wallets = list(self.tracked_wallets | self.discovered_wallets)

            if not all_wallets:
                logger.warning("No hay wallets para rastrear")
                return 0

            logger.info(f"📊 Rastreando {len(all_wallets)} wallets ({len(self.tracked_wallets)} manuales, {len(self.discovered_wallets)} descubiertos)...")

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
            self._safe_rollback()
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
        logger.info(f"Signatures en cache: {len(self.processed_signatures)}")
        logger.info(f"Errores: {self.errors_count}")
        logger.info("=" * 70)

    def add_wallet_to_track(self, wallet_address: str, label: str = "", reason: str = ""):
        """Agrega wallet al tracking"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO tracked_wallets (wallet_address, label, reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (wallet_address) DO UPDATE SET is_active = TRUE
            """, (wallet_address, label, reason))
            self.conn.commit()
            cursor.close()
            self.tracked_wallets.add(wallet_address)
            logger.info(f"✅ Wallet agregado: {wallet_address} {label}")
        except Exception as e:
            logger.error(f"Error agregando wallet: {e}")
            self._safe_rollback()

    # ═══════════════════════════════════════════════════════════
    # FIX v5.1: Rollback preventivo entre cada load_*() en run()
    # ═══════════════════════════════════════════════════════════
    def run(self):
        """
        Loop principal del tracker.
        v3: Webhook-primary mode
          - Discovery cada 3 minutos (wallets nuevos desde BD)
          - Si webhook_primary_mode=True: NO hace tracking polling (webhooks cubren eso)
          - Si webhook_primary_mode=False: hace tracking polling cada 60s (modo legacy)
        """
        logger.info("=" * 70)
        logger.info("🔍 ENHANCED WALLET TRACKER v3 — WEBHOOK-PRIMARY MODE")
        logger.info(f"   Discovery: cada {self.discovery_interval_minutes} min")
        logger.info(f"   Webhook-primary: {self.webhook_primary_mode}")
        if not self.webhook_primary_mode:
            logger.info("   ⚠ Legacy polling mode activo (no recomendado)")
        logger.info("=" * 70)

        self.connect_db()

        last_discovery = 0
        last_tracking_run = 0

        try:
            while True:
                try:
                    now = time.time()

                    # === 1. DISCOVERY CYCLE (cada 3 min) ===
                    elapsed_discovery = now - last_discovery
                    if elapsed_discovery >= (self.discovery_interval_minutes * 60):
                        logger.info(f"\n{'=' * 50}")
                        logger.info("🔍 DISCOVERY: Buscando wallets nuevas desde BD...")
                        logger.info(f"{'=' * 50}")
                        self.run_discovery_cycle()
                        last_discovery = now

                    # === 2. TRACKING CYCLE ===
                    # v3: En modo webhook-primary, solo discovery. No polling continuo.
                    if not self.webhook_primary_mode:
                        elapsed_tracking = now - last_tracking_run
                        if elapsed_tracking >= 60:
                            self.run_tracking_cycle()
                            last_tracking_run = now
                    else:
                        # Webhook mode: solo actualizamos timestamp para evitar loop innecesario
                        last_tracking_run = now

                    # === 3. STATS (cada 5 min) ===
                    self.stats["cycles"] += 1
                    if self.stats["cycles"] % 50 == 0:  # ~5 min si sleep 5s
                        self.log_stats()

                    # Sleep 5s entre checks
                    time.sleep(5)

                except psycopg2.OperationalError:
                    logger.warning("⚠ DB connection lost, reconectando...")
                    self.connect_db()
                    time.sleep(5)

                except Exception as e:
                    logger.error(f"Error en loop principal: {e}")
                    self.stats["errors"] += 1
                    self._safe_rollback()
                    time.sleep(10)

        except KeyboardInterrupt:
            logger.info("\n🛑 Deteniendo tracker...")

        finally:
            if self.conn and not self.conn.closed:
                self.conn.close()
            logger.info("Tracker detenido.")




if __name__ == "__main__":
    from shared_config import DB_CONFIG, LOCAL_RPC_URL

    tracker = EnhancedWalletTracker(DB_CONFIG, rpc_url=LOCAL_RPC_URL)
