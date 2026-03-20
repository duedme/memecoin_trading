#!/usr/bin/env python3
"""
webhook_server.py
Servidor de webhooks para recibir eventos de Helius en tiempo real

VERSIÓN CORREGIDA v2:
  - load_dotenv() al inicio
  - DB_CONFIG desde variables de entorno (no hardcodeado)
  - AMM_PROGRAMS completo (12 AMMs, no 3)
  - authHeader fix: compara sin prefijo Bearer
  - Nombres de columnas SQL corregidos (snake_case)
  - process_wallet_transaction() implementada completamente
  - Connection pooling con psycopg2.pool
"""

import os
import logging
import json
from datetime import datetime
from aiohttp import web
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN DESDE .env
# ============================================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "memecoins_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

WEBHOOK_AUTH_TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN", "")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", 8765))

# ============================================================
# 12 AMM PROGRAMS (COMPLETO - antes solo tenía 3)
# ============================================================
AMM_PROGRAMS = {
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

KNOWN_TOKEN_BLACKLIST = {
    "So11111111111111111111111111111111111111112",      # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",   # USDT
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",   # RAY
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",    # mSOL
}

SKIP_PROGRAMS = {
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
}

# ============================================================
# ESTADÍSTICAS
# ============================================================
stats = {
    "tokens_received": 0,
    "tokens_saved": 0,
    "wallets_received": 0,
    "wallets_processed": 0,
    "errors": 0,
    "started_at": datetime.now().isoformat(),
}

# ============================================================
# CONNECTION POOL
# ============================================================
db_pool = None


def init_db_pool():
    """Inicializa el pool de conexiones"""
    global db_pool
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
        )
        logger.info("✓ Connection pool inicializado (2-10 conexiones)")
    except Exception as e:
        logger.error(f"Error inicializando connection pool: {e}")
        raise


def get_db_connection():
    """Obtiene una conexión del pool"""
    return db_pool.getconn()


def return_db_connection(conn):
    """Devuelve una conexión al pool"""
    db_pool.putconn(conn)


# ============================================================
# MIDDLEWARE DE AUTENTICACIÓN
# ============================================================
@web.middleware
async def auth_middleware(request, handler):
    """Valida el token de autenticación en cada request"""
    if request.path == "/health":
        return await handler(request)

    if WEBHOOK_AUTH_TOKEN:
        auth_header = request.headers.get("Authorization", "")
        # FIX: Helius envía authHeader tal cual (sin prefijo Bearer)
        # Comparamos directo contra el token configurado
        if auth_header != WEBHOOK_AUTH_TOKEN:
            logger.warning(f"⚠ Auth fallida desde {request.remote}: got '{auth_header[:20]}...'")
            return web.Response(status=401, text="Unauthorized")

    return await handler(request)


# ============================================================
# HANDLERS
# ============================================================
async def health_check(request):
    """Health check endpoint"""
    return web.json_response({
        "status": "healthy",
        "stats": stats,
        "amm_count": len(AMM_PROGRAMS),
    })


async def webhook_handler(request):
    """
    Handler principal para eventos de Helius.
    Recibe enhanced transactions y las clasifica como:
    - Creación de token (si involucra un AMM conocido y hay tokenTransfers nuevos)
    - Transacción de wallet (si el feePayer es un wallet rastreado)
    """
    try:
        payload = await request.json()
        transactions = payload if isinstance(payload, list) else [payload]

        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            for tx in transactions:
                try:
                    # Intentar procesar como creación de token
                    token_saved = process_token_creation(cursor, tx)
                    if token_saved:
                        stats["tokens_received"] += 1
                        continue

                    # Si no es token nuevo, intentar como wallet transaction
                    wallet_processed = process_wallet_transaction(conn, cursor, tx)
                    if wallet_processed:
                        stats["wallets_received"] += 1

                except Exception as e:
                    logger.error(f"Error procesando tx individual: {e}")
                    stats["errors"] += 1
                    conn.rollback()
                    continue

            conn.commit()
            cursor.close()
        finally:
            return_db_connection(conn)

        return web.Response(status=200, text="OK")

    except Exception as e:
        logger.error(f"Error en webhook_handler: {e}")
        stats["errors"] += 1
        return web.Response(status=500, text=str(e))


# ============================================================
# PROCESAMIENTO DE TOKENS
# ============================================================
def process_token_creation(cursor, tx):
    """
    Procesa evento de creación de token desde webhook Enhanced de Helius.
    Retorna True si se guardó un token nuevo, False si no aplica.
    """
    signature = tx.get("signature", "")
    timestamp_val = tx.get("timestamp", 0)

    # Detectar AMM involucrado revisando instructions Y accountData
    instructions = tx.get("instructions", [])
    account_data = tx.get("accountData", [])
    amm_name = None

    # Método 1: Buscar en instructions
    for ix in instructions:
        pid = ix.get("programId", "")
        if pid in AMM_PROGRAMS:
            amm_name = AMM_PROGRAMS[pid]
            break

    # Método 2: Buscar en accountData (algunas enhanced tx lo ponen aquí)
    if not amm_name:
        for acc in account_data:
            owner = acc.get("owner", "")
            if owner in AMM_PROGRAMS:
                amm_name = AMM_PROGRAMS[owner]
                break

    if not amm_name:
        return False

    # Extraer tokens del evento
    token_transfers = tx.get("tokenTransfers", [])
    if not token_transfers:
        return False

    saved_any = False
    for transfer in token_transfers:
        mint = transfer.get("mint", "")
        if not mint or mint in KNOWN_TOKEN_BLACKLIST:
            continue

        token_amount = transfer.get("tokenAmount", 0)
        decimals = transfer.get("decimals", 9)

        created_at = (
            datetime.fromtimestamp(timestamp_val)
            if timestamp_val
            else datetime.now()
        )

        try:
            cursor.execute("""
                INSERT INTO tokens (
                    mint_address, name, symbol, total_supply, decimals,
                    uri, image_url, amm, created_at, detected_at,
                    creation_signature, creation_instruction,
                    status, retention_category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mint_address) DO NOTHING
                RETURNING token_id
            """, (
                mint,
                None,       # name: requiere llamada adicional a metadata
                None,       # symbol: requiere llamada adicional
                token_amount,
                decimals,
                None,       # uri
                None,       # image_url
                amm_name,
                created_at,
                datetime.now(),
                signature,
                "webhook",
                "active",
                "webhook",
            ))
            result = cursor.fetchone()
            if result:
                stats["tokens_saved"] += 1
                saved_any = True
                logger.info(
                    f"🆕 [WEBHOOK] Token: {mint[:16]}... en {amm_name} "
                    f"(ID:{result[0]})"
                )
        except Exception as e:
            logger.error(f"Error guardando token {mint[:16]}: {e}")
            raise

    return saved_any


# ============================================================
# PROCESAMIENTO DE WALLETS
# ============================================================
def process_wallet_transaction(conn, cursor, tx):
    """
    Procesa transacción de wallet desde webhook Enhanced de Helius.
    Usa el stored procedure process_transaction() del sistema existente.
    Retorna True si se procesó, False si no aplica.
    """
    signature = tx.get("signature", "")
    fee_payer = tx.get("feePayer", "")
    timestamp_val = tx.get("timestamp", 0)

    if not fee_payer:
        return False

    # Verificar si es un wallet que estamos rastreando
    cursor.execute(
        "SELECT 1 FROM tracked_wallets WHERE wallet_address = %s AND is_active = TRUE",
        (fee_payer,)
    )
    if not cursor.fetchone():
        # También verificar en tabla wallets (descubiertos)
        cursor.execute(
            "SELECT 1 FROM wallets WHERE wallet_address = %s AND is_active = TRUE",
            (fee_payer,)
        )
        if not cursor.fetchone():
            return False

    token_transfers = tx.get("tokenTransfers", [])
    native_transfers = tx.get("nativeTransfers", [])

    if not token_transfers:
        return False

    # Detectar tipo de transacción (buy/sell)
    sol_mints = {
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    }

    memecoin_mint = None
    token_amount = 0
    sol_amount = 0
    tx_type = None

    for transfer in token_transfers:
        mint = transfer.get("mint", "")
        amount = abs(float(transfer.get("tokenAmount", 0)))
        from_account = transfer.get("fromUserAccount", "")
        to_account = transfer.get("toUserAccount", "")

        if mint in sol_mints:
            sol_amount += amount
            if from_account == fee_payer:
                tx_type = "buy"
            elif to_account == fee_payer:
                tx_type = "sell"
        elif mint not in KNOWN_TOKEN_BLACKLIST:
            memecoin_mint = mint
            token_amount += amount
            if to_account == fee_payer:
                tx_type = "buy"
            elif from_account == fee_payer:
                tx_type = "sell"

    if not memecoin_mint or not tx_type:
        return False

    # Calcular SOL desde native transfers si no se detectó en token transfers
    if sol_amount == 0:
        for nt in native_transfers:
            amount = abs(float(nt.get("amount", 0))) / 1_000_000_000
            if amount > 0.0001:
                from_acc = nt.get("fromUserAccount", "")
                to_acc = nt.get("toUserAccount", "")
                if from_acc == fee_payer or to_acc == fee_payer:
                    sol_amount += amount

    price = sol_amount / token_amount if token_amount > 0 else 0.0

    tx_time = (
        datetime.fromtimestamp(timestamp_val)
        if timestamp_val
        else datetime.now()
    )

    try:
        cursor.execute("""
            SELECT process_transaction(%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            fee_payer,
            memecoin_mint,
            signature,
            tx_type,
            token_amount,
            sol_amount,
            price,
            tx_time,
        ))
        conn.commit()
        stats["wallets_processed"] += 1
        logger.info(
            f"{'🟢' if tx_type == 'buy' else '🔴'} [WEBHOOK] {tx_type.upper()} "
            f"{fee_payer[:12]}... {token_amount:.2f} tokens @ {price:.8f} SOL"
        )
        return True
    except Exception as e:
        logger.error(f"Error en process_transaction: {e}")
        conn.rollback()
        return False


# ============================================================
# INICIALIZACIÓN DEL SERVIDOR
# ============================================================
def main():
    """Inicializa y arranca el servidor"""
    logger.info("=" * 70)
    logger.info("🚀 INICIANDO WEBHOOK SERVER v2")
    logger.info("=" * 70)
    logger.info(f"  DB: {DB_CONFIG['database']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
    logger.info(f"  AMMs monitoreados: {len(AMM_PROGRAMS)}")
    for addr, name in AMM_PROGRAMS.items():
        logger.info(f"    - {name}: {addr[:16]}...")

    init_db_pool()

    app = web.Application(middlewares=[auth_middleware])
    app.router.add_get("/health", health_check)
    app.router.add_post("/webhook", webhook_handler)

    logger.info(f"✓ Rutas: GET /health, POST /webhook")
    logger.info(f"✓ Puerto: {WEBHOOK_PORT}")
    logger.info(f"✓ Auth token: {'Sí' if WEBHOOK_AUTH_TOKEN else 'NO (INSEGURO)'}")
    logger.info("=" * 70)

    web.run_app(app, host="0.0.0.0", port=WEBHOOK_PORT)


if __name__ == "__main__":
    main()
