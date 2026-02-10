#!/usr/bin/env python3
"""
detector_memecoins.py - Sistema completo de detección de memecoins
Monitorea 12 AMMs en Solana usando nodo RPC local + PostgreSQL/TimescaleDB

Uso:
    python detector_memecoins.py

Requisitos:
    pip install psycopg2-binary requests

Autor: Proyecto Memecoins Solana
Fecha: Febrero 2026
"""

import requests
import json
import time
import logging
import threading
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from collections import defaultdict

# ========================================
# CONFIGURACIÓN
# ========================================
RPC_URL = "http://127.0.0.1:7211"
POLLING_INTERVAL = 1.5  # segundos entre consultas por AMM

# Configuración de PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "memecoins_db",
    "user": "postgres",
    "password": "12345"
}

# Programas AMM a monitorear (12 AMMs completos)
AMMS = {
    "pump_fun": {
        "address": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        "name": "Pump.fun",
        "instructions": ["InitializeMint2"],
        "log_patterns": ["Program log: Instruction: InitializeMint2"]
    },
    "pumpswap": {
        "address": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        "name": "PumpSwap",
        "instructions": ["CreatePool"],
        "log_patterns": ["Program log: Instruction: CreatePool"]
    },
    "raydium_amm": {
        "address": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "name": "Raydium AMM",
        "instructions": ["CreatePool"],
        "log_patterns": ["Program log: Instruction: CreatePool"]
    },
    "raydium_launchlab": {
        "address": "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
        "name": "Raydium LaunchLab",
        "instructions": ["InitializeV2"],
        "log_patterns": ["Program log: Instruction: InitializeV2"]
    },
    "fluxbeam": {
        "address": "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",
        "name": "FluxBeam",
        "instructions": ["InitializeMint2"],
        "log_patterns": ["Program log: Instruction: InitializeMint2"]
    },
    "heavendex": {
        "address": "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o",
        "name": "HeavenDEX",
        "instructions": ["CreateStandardLiquidityPool"],
        "log_patterns": ["Program log: Instruction: CreateStandardLiquidityPool"]
    },
    "meteora_dlmm": {
        "address": "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        "name": "Meteora DLMM",
        "instructions": ["InitializeLbPair2", "InitializePool", "InitializeMint2", "CreateIdempotent"],
        "log_patterns": [
            "Program log: Instruction: InitializeLbPair2",
            "Program log: Instruction: InitializePool",
            "Program log: Instruction: InitializeMint2",
            "Program log: CreateIdempotent"
        ]
    },
    "meteora_dyn2": {
        "address": "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
        "name": "Meteora DYN2",
        "instructions": ["InitializePool"],
        "log_patterns": ["Program log: Instruction: InitializePool"]
    },
    "meteora_dyn": {
        "address": "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
        "name": "Meteora DYN",
        "instructions": ["InitializeMint2"],
        "log_patterns": ["Program log: Instruction: InitializeMint2"]
    },
    "meteora_dbc": {
        "address": "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",
        "name": "Meteora DBC",
        "instructions": ["InitializeVirtualPoolWithSplToken"],
        "log_patterns": ["Program log: Instruction: InitializeVirtualPoolWithSplToken"]
    },
    "moonit": {
        "address": "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",
        "name": "Moonit",
        "instructions": ["TokenMint"],
        "log_patterns": ["Program log: Instruction: TokenMint"]
    },
    "orca": {
        "address": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        "name": "Orca",
        "instructions": ["InitializePoolV2"],
        "log_patterns": ["Program log: Instruction: InitializePoolV2"]
    }
}

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('detector_memecoins.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========================================
# CLASE DE BASE DE DATOS
# ========================================

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        """Conecta a PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.config)
            logger.info("✅ Conexión a PostgreSQL establecida")
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
            raise

    def ensure_connection(self):
        """Asegura que la conexión esté activa"""
        try:
            self.conn.isolation_level
        except:
            logger.warning("🔄 Reconectando a PostgreSQL...")
            self.connect()

    def save_token(self, token_data):
        """Guarda un token nuevo en la base de datos"""
        self.ensure_connection()
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tokens (
                        mint_address, name, symbol, total_supply, decimals,
                        uri, image_url, amm, created_at, creation_signature,
                        creation_instruction
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mint_address) DO NOTHING
                    RETURNING token_id
                """, (
                    token_data.get('mint_address'),
                    token_data.get('name'),
                    token_data.get('symbol'),
                    token_data.get('total_supply'),
                    token_data.get('decimals'),
                    token_data.get('uri'),
                    token_data.get('image_url'),
                    token_data.get('amm'),
                    token_data.get('created_at'),
                    token_data.get('signature'),
                    token_data.get('instruction')
                ))
                result = cur.fetchone()
                self.conn.commit()

                if result:
                    token_id = result[0]
                    logger.info(f"💾 Token guardado: {token_data.get('symbol')} (ID: {token_id})")
                    return token_id
                else:
                    # Token ya existía
                    cur.execute(
                        "SELECT token_id FROM tokens WHERE mint_address = %s",
                        (token_data.get('mint_address'),)
                    )
                    existing = cur.fetchone()  # ← LÍNEA CORREGIDA (espacios, no tab)
                    return existing[0] if existing else None

        except Exception as e:
            logger.error(f"❌ Error guardando token: {e}")
            self.conn.rollback()
            return None

    def get_token_count(self):
        """Obtiene el número total de tokens detectados"""
        self.ensure_connection()
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM tokens")
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Error obteniendo conteo: {e}")
            return 0

    def close(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 Conexión a PostgreSQL cerrada")

# ========================================
# FUNCIONES RPC
# ========================================

def rpc_call(method, params=None):
    """Llamada RPC genérica al nodo local"""
    if params is None:
        params = []
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }
    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)
        return response.json()
    except requests.exceptions.ConnectionError:
        return {"error": "No se puede conectar al nodo RPC"}
    except Exception as e:
        return {"error": str(e)}

def get_signatures(program_address, limit=20, before=None):
    """Obtiene firmas de transacciones recientes"""
    config = {"limit": limit, "commitment": "confirmed"}
    if before:
        config["before"] = before

    params = [program_address, config]
    return rpc_call("getSignaturesForAddress", params)

def get_transaction(signature):
    """Obtiene detalles completos de una transacción"""
    params = [
        signature,
        {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed"
        }
    ]
    return rpc_call("getTransaction", params)

def get_account_info(address):
    """Obtiene información de una cuenta"""
    params = [address, {"encoding": "jsonParsed"}]
    return rpc_call("getAccountInfo", params)

def get_token_metadata(mint_address):
    """Obtiene metadata de un token SPL desde el mint address"""
    result = get_account_info(mint_address)

    metadata = {
        "name": None,
        "symbol": None,
        "uri": None,
        "image": None
    }

    if "error" in result or "result" not in result:
        return metadata

    account_data = result.get("result", {})
    if not account_data or account_data.get("value") is None:
        return metadata

    try:
        value = account_data["value"]
        data = value.get("data", {})

        if isinstance(data, dict) and "parsed" in data:
            parsed = data["parsed"]
            if "info" in parsed:
                info = parsed["info"]
                extensions = info.get("extensions", [])
                for ext in extensions:
                    if ext.get("extension") == "tokenMetadata":
                        metadata["name"] = ext.get("state", {}).get("name")
                        metadata["symbol"] = ext.get("state", {}).get("symbol")
                        metadata["uri"] = ext.get("state", {}).get("uri")
                        break
    except Exception as e:
        logger.debug(f"No se pudo extraer metadata de {mint_address}: {e}")

    return metadata

# ========================================
# PROCESAMIENTO DE TRANSACCIONES
# ========================================

def extract_token_info(tx_result, amm_name, signature):
    """Extrae información de tokens de una transacción"""
    tokens = []

    if not tx_result or "result" not in tx_result or not tx_result["result"]:
        return tokens

    result = tx_result["result"]
    meta = result.get("meta", {})
    block_time = result.get("blockTime")

    # Obtener token balances post-transacción
    post_balances = meta.get("postTokenBalances", [])

    # Extraer información de cada token
    seen_mints = set()
    for balance in post_balances:
        mint = balance.get("mint")
        if not mint or mint in seen_mints:
            continue

        seen_mints.add(mint)

        token_amount = balance.get("uiTokenAmount", {})

        token_info = {
            "mint_address": mint,
            "name": None,  # Se obtiene después con getAccountInfo
            "symbol": None,
            "total_supply": token_amount.get("amount"),
            "decimals": token_amount.get("decimals"),
            "uri": None,
            "image_url": None,
            "amm": amm_name,
            "created_at": datetime.fromtimestamp(block_time) if block_time else datetime.now(),
            "signature": signature,
            "instruction": "CreatePool"  # Genérico
        }

        tokens.append(token_info)

    return tokens

def check_logs_for_instruction(logs, patterns):
    """Verifica si los logs contienen patrones de instrucción"""
    if not logs:
        return False

    log_text = " ".join(logs)
    return any(pattern in log_text for pattern in patterns)

# ========================================
# MONITOR DE AMM
# ========================================

class AMMMonitor:
    def __init__(self, amm_key, amm_info, db_manager):
        self.amm_key = amm_key
        self.amm_info = amm_info
        self.db = db_manager
        self.last_signature = None
        self.tokens_detected = 0
        self.running = False
        self.thread = None

    def process_new_transactions(self):
        """Procesa transacciones nuevas del AMM"""
        address = self.amm_info["address"]
        name = self.amm_info["name"]

        result = get_signatures(address, limit=10)

        if "error" in result or "result" not in result:
            return

        signatures = result["result"]
        if not signatures:
            return

        # Procesar solo transacciones nuevas
        for tx in signatures:
            signature = tx.get("signature")

            # Si llegamos a la última conocida, paramos
            if signature == self.last_signature:
                break

            # Solo transacciones exitosas
            if tx.get("err") is not None:
                continue

            # Obtener detalles de la transacción
            tx_detail = get_transaction(signature)

            if "result" not in tx_detail or not tx_detail["result"]:
                continue

            # Verificar si contiene instrucción de creación de pool
            logs = tx_detail["result"].get("meta", {}).get("logMessages", [])

            if check_logs_for_instruction(logs, self.amm_info["log_patterns"]):
                logger.info(f"🔔 [{name}] Pool detectado! {signature[:30]}...")

                # Extraer tokens
                tokens = extract_token_info(tx_detail, name, signature)

                # Guardar en base de datos
                for token in tokens:
                    token_id = self.db.save_token(token)
                    if token_id:
                        self.tokens_detected += 1

        # Actualizar última firma procesada
        if signatures:
            self.last_signature = signatures[0].get("signature")

    def run(self):
        """Ejecuta el monitoreo continuo"""
        logger.info(f"🚀 Iniciando monitor para {self.amm_info['name']}")
        self.running = True

        while self.running:
            try:
                self.process_new_transactions()
                time.sleep(POLLING_INTERVAL)
            except Exception as e:
                logger.error(f"❌ Error en {self.amm_info['name']}: {e}")
                time.sleep(5)

    def start(self):
        """Inicia el thread del monitor"""
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self):
        """Detiene el monitor"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)

# ========================================
# SISTEMA PRINCIPAL
# ========================================

class MemecoinsDetector:
    def __init__(self):
        self.db = None
        self.monitors = []
        self.running = False

    def verify_node(self):
        """Verifica que el nodo RPC esté disponible"""
        logger.info("🔍 Verificando conexión al nodo RPC...")

        result = rpc_call("getHealth")
        if "error" in result:
            logger.error(f"❌ No se puede conectar al nodo: {result['error']}")
            return False

        if result.get("result") != "ok":
            logger.warning(f"⚠️  Nodo no saludable: {result}")
            return False

        slot = rpc_call("getSlot").get("result", 0)
        logger.info(f"✅ Nodo OK - Slot actual: {slot:,}")
        return True

    def verify_database(self):
        """Verifica que PostgreSQL esté disponible"""
        logger.info("🔍 Verificando conexión a PostgreSQL...")
        try:
            self.db = DatabaseManager(DB_CONFIG)
            count = self.db.get_token_count()
            logger.info(f"✅ PostgreSQL OK - Tokens en BD: {count:,}")
            return True
        except Exception as e:
            logger.error(f"❌ Error con PostgreSQL: {e}")
            return False

    def start_monitors(self):
        """Inicia todos los monitores de AMM"""
        logger.info(f"🚀 Iniciando {len(AMMS)} monitores de AMM...")

        for amm_key, amm_info in AMMS.items():
            monitor = AMMMonitor(amm_key, amm_info, self.db)
            monitor.start()
            self.monitors.append(monitor)
            logger.info(f"   ✅ {amm_info['name']}")

        logger.info("✅ Todos los monitores iniciados")

    def print_stats(self):
        """Imprime estadísticas periódicas"""
        while self.running:
            time.sleep(60)  # Cada minuto

            total_detected = sum(m.tokens_detected for m in self.monitors)
            total_db = self.db.get_token_count()

            logger.info("=" * 60)
            logger.info("📊 ESTADÍSTICAS DEL SISTEMA")
            logger.info("=" * 60)
            logger.info(f"Tokens detectados (esta sesión): {total_detected:,}")
            logger.info(f"Tokens totales en BD: {total_db:,}")
            logger.info("\nPor AMM:")
            for monitor in self.monitors:
                logger.info(f"  {monitor.amm_info['name']}: {monitor.tokens_detected:,}")
            logger.info("=" * 60)

    def run(self):
        """Ejecuta el sistema completo"""
        logger.info("=" * 60)
        logger.info("🎯 SISTEMA DE DETECCIÓN DE MEMECOINS")
        logger.info("=" * 60)
        logger.info(f"RPC: {RPC_URL}")
        logger.info(f"Base de datos: {DB_CONFIG['database']}")
        logger.info(f"Intervalo de polling: {POLLING_INTERVAL}s")
        logger.info(f"AMMs monitoreados: {len(AMMS)}")
        logger.info("=" * 60)

        # Verificar nodo RPC
        if not self.verify_node():
            logger.error("🛑 No se puede iniciar sin conexión al nodo")
            return

        # Verificar base de datos
        if not self.verify_database():
            logger.error("🛑 No se puede iniciar sin conexión a PostgreSQL")
            return

        # Iniciar monitores
        self.start_monitors()

        # Thread de estadísticas
        self.running = True
        stats_thread = threading.Thread(target=self.print_stats, daemon=True)
        stats_thread.start()

        # Mantener vivo
        try:
            logger.info("\n✅ Sistema en ejecución... (Ctrl+C para detener)\n")
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n🛑 Deteniendo sistema...")
            self.stop()

    def stop(self):
        """Detiene el sistema"""
        self.running = False

        # Detener monitores
        for monitor in self.monitors:
            monitor.stop()

        # Cerrar base de datos
        if self.db:
            self.db.close()

        logger.info("✅ Sistema detenido correctamente")

# ========================================
# EJECUCIÓN
# ========================================

if __name__ == "__main__":
    detector = MemecoinsDetector()
    detector.run()
