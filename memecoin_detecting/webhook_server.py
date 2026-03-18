#!/usr/bin/env python3
"""
webhook_server.py
Servidor de webhooks para recibir eventos de Helius en tiempo real
VERSIÓN CORREGIDA - Fixes aplicados:
  - Nombres de columnas SQL corregidos (snake_case)
  - authHeader fix: compara sin prefijo "Bearer"
  - process_wallet_transaction() implementada completamente
  - Connection pooling con psycopg2.pool
  - Manejo de errores mejorado
"""

import os
import logging
import json
from datetime import datetime
from aiohttp import web
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

# ============================================================
# CONFIGURACIÓN
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/rebelforce/scripts/memecoin_detecting/webhook_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Variables de entorno
WEBHOOK_AUTH_TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN", "tu_token_secreto_aqui")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8765"))

# Configuración de base de datos
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "memecoins_db",
    "user": "postgres",
    "password": "12345"
}

# Connection pool global
db_pool = None

# ============================================================
# INICIALIZACIÓN DE CONNECTION POOL
# ============================================================

def init_db_pool():
    """Inicializa el pool de conexiones"""
    global db_pool
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        logger.info("✓ Connection pool inicializado (2-10 conexiones)")
    except Exception as e:
        logger.error(f"❌ Error inicializando connection pool: {e}")
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
    
    # Permitir health check sin autenticación
    if request.path == '/health':
        return await handler(request)
    
    # Validar Authorization header
    auth_header = request.headers.get('Authorization')
    
    if not auth_header:
        logger.warning(f"Request sin Authorization header desde {request.remote}")
        return web.json_response(
            {"error": "Missing Authorization header"},
            status=401
        )
    
    # FIX: Helius envía solo el token (sin "Bearer "), así que comparamos directo
    if auth_header != WEBHOOK_AUTH_TOKEN:
        logger.warning(f"Token inválido desde {request.remote}")
        return web.json_response(
            {"error": "Invalid token"},
            status=401
        )
    
    return await handler(request)

# ============================================================
# PROCESAMIENTO DE TOKENS
# ============================================================

def process_token_creation(event_data: dict) -> bool:
    """
    Procesa un evento TOKEN_CREATION de Helius
    FIX: Usa nombres de columnas correctos (snake_case)
    """
    conn = None
    try:
        # Extraer datos del evento
        mint_address = event_data.get('mint')
        signature = event_data.get('signature')
        timestamp = event_data.get('timestamp')
        
        if not mint_address or not signature:
            logger.error("Evento TOKEN_CREATION sin mint o signature")
            return False
        
        # Convertir timestamp Unix a datetime
        if timestamp:
            created_at = datetime.fromtimestamp(timestamp)
        else:
            created_at = datetime.now()
        
        detected_at = datetime.now()
        
        # Extraer metadata si existe (usualmente NULL en TOKEN_CREATION)
        token_transfers = event_data.get('tokenTransfers', [])
        name = None
        symbol = None
        decimals = 9  # Default para SPL tokens
        total_supply = None
        
        if token_transfers:
            first_transfer = token_transfers[0]
            decimals = first_transfer.get('decimals', 9)
            # Nota: name y symbol usualmente vienen NULL en TOKEN_CREATION
            # Se enriquecen después con getAsset de Helius DAS
        
        # Determinar AMM desde las cuentas involucradas
        amm = 'unknown'
        accounts = event_data.get('accountData', [])
        
        # Program IDs conocidos
        amm_programs = {
            '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': 'pump.fun',
            'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA': 'pumpswap',
            'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK': 'raydium',
        }
        
        for account in accounts:
            account_addr = account.get('account')
            if account_addr in amm_programs:
                amm = amm_programs[account_addr]
                break
        
        # Obtener conexión del pool
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # FIX: Nombres de columnas corregidos (snake_case)
        # FIX: Incluye detected_at, creation_signature, retention_category
        cursor.execute("""
            INSERT INTO tokens (
                mint_address, 
                name, 
                symbol, 
                decimals, 
                total_supply, 
                amm, 
                created_at, 
                detected_at,
                creation_signature,
                status,
                retention_category
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (mint_address) DO NOTHING
            RETURNING token_id
        """, (
            mint_address,
            name,
            symbol,
            decimals,
            total_supply,
            amm,
            created_at,
            detected_at,
            signature,
            'active',
            'webhook'  # Categoría de retención para tokens desde webhook
        ))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        
        if result:
            token_id = result[0]
            logger.info(f"✅ Nuevo token guardado: {mint_address[:16]}... (ID: {token_id}, AMM: {amm})")
            return True
        else:
            logger.info(f"ℹ️  Token ya existe: {mint_address[:16]}...")
            return True
            
    except Exception as e:
        logger.error(f"❌ Error procesando TOKEN_CREATION: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_db_connection(conn)

# ============================================================
# PROCESAMIENTO DE TRANSACCIONES DE WALLET
# ============================================================

def process_wallet_transaction(event_data: dict) -> bool:
    """
    Procesa transacciones de wallets rastreados
    FIX: Implementación completa usando la stored procedure process_transaction()
    """
    conn = None
    try:
        signature = event_data.get('signature')
        timestamp = event_data.get('timestamp')
        
        if not signature:
            logger.error("Evento sin signature")
            return False
        
        # Convertir timestamp
        if timestamp:
            tx_time = datetime.fromtimestamp(timestamp)
        else:
            tx_time = datetime.now()
        
        # Extraer token transfers
        token_transfers = event_data.get('tokenTransfers', [])
        if not token_transfers:
            logger.debug(f"Transacción {signature[:16]}... sin token transfers")
            return True
        
        # Extraer native transfers (para detectar SOL)
        native_transfers = event_data.get('nativeTransfers', [])
        
        # Determinar wallet, tipo de transacción, y montos
        # Necesitamos identificar swaps (intercambios token <-> SOL)
        
        sol_mint = 'So11111111111111111111111111111111111111112'
        
        # Buscar transferencias de tokens que no sean SOL
        memecoin_transfer = None
        sol_amount = 0.0
        
        for transfer in token_transfers:
            mint = transfer.get('mint')
            if mint and mint != sol_mint:
                memecoin_transfer = transfer
                break
        
        if not memecoin_transfer:
            logger.debug(f"Transacción {signature[:16]}... no es swap de memecoin")
            return True
        
        # Calcular SOL involucrado desde native_transfers
        for native in native_transfers:
            amount = native.get('amount', 0)
            sol_amount += amount / 1_000_000_000  # lamports a SOL
        
        # Extraer datos del memecoin transfer
        mint_address = memecoin_transfer.get('mint')
        token_amount = memecoin_transfer.get('tokenAmount', 0)
        from_address = memecoin_transfer.get('fromUserAccount')
        to_address = memecoin_transfer.get('toUserAccount')
        
        # Determinar dirección del swap
        # Si el token sale del wallet → SELL
        # Si el token entra al wallet → BUY
        
        # Obtener wallet del evento (feePayer usualmente es el wallet)
        wallet_address = event_data.get('feePayer')
        
        if not wallet_address:
            logger.error(f"No se pudo determinar wallet para tx {signature[:16]}...")
            return False
        
        # Determinar tipo
        if from_address == wallet_address:
            tx_type = 'sell'
        elif to_address == wallet_address:
            tx_type = 'buy'
        else:
            logger.debug(f"Wallet {wallet_address[:12]}... no es origen ni destino del transfer")
            return True
        
        # Calcular precio
        if token_amount > 0:
            price = sol_amount / token_amount
        else:
            price = 0.0
        
        # Obtener conexión del pool
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Usar la stored procedure process_transaction() del sistema existente
        cursor.execute("""
            SELECT process_transaction(
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            wallet_address,
            mint_address,
            signature,
            tx_type,
            token_amount,
            sol_amount,
            price,
            tx_time
        ))
        
        conn.commit()
        cursor.close()
        
        logger.info(
            f"{'🟢' if tx_type == 'buy' else '🔴'} {tx_type.upper()} "
            f"{wallet_address[:12]}... {token_amount:.2f} tokens @ {price:.8f} SOL "
            f"(Total: {sol_amount:.4f} SOL)"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error procesando wallet transaction: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_db_connection(conn)

# ============================================================
# RUTAS HTTP
# ============================================================

async def health_check(request):
    """Health check endpoint"""
    return web.json_response({
        "status": "healthy",
        "service": "memecoin-webhook-server",
        "timestamp": datetime.now().isoformat()
    })

async def webhook_handler(request):
    """
    Endpoint principal para recibir webhooks de Helius
    POST /webhook
    """
    try:
        # Parsear body JSON
        try:
            body = await request.json()
        except json.JSONDecodeError:
            logger.error("Body JSON inválido")
            return web.json_response(
                {"error": "Invalid JSON body"},
                status=400
            )
        
        # Validar estructura
        if not isinstance(body, list):
            logger.error("Body no es un array")
            return web.json_response(
                {"error": "Body must be an array of events"},
                status=400
            )
        
        # Procesar cada evento
        events_processed = 0
        events_failed = 0
        
        for event in body:
            event_type = event.get('type')
            
            if event_type == 'TOKEN_CREATION':
                if process_token_creation(event):
                    events_processed += 1
                else:
                    events_failed += 1
                    
            elif event_type == 'SWAP':
                # Los swaps de wallets rastreados vienen como SWAP events
                if process_wallet_transaction(event):
                    events_processed += 1
                else:
                    events_failed += 1
                    
            else:
                logger.debug(f"Tipo de evento no manejado: {event_type}")
        
        logger.info(
            f"📊 Batch procesado: {events_processed} OK, {events_failed} errores "
            f"(total: {len(body)} eventos)"
        )
        
        return web.json_response({
            "status": "success",
            "events_received": len(body),
            "events_processed": events_processed,
            "events_failed": events_failed
        })
        
    except Exception as e:
        logger.error(f"❌ Error en webhook_handler: {e}")
        return web.json_response(
            {"error": "Internal server error"},
            status=500
        )

# ============================================================
# INICIALIZACIÓN DEL SERVIDOR
# ============================================================

def main():
    """Inicializa y arranca el servidor"""
    logger.info("=" * 70)
    logger.info("🚀 INICIANDO WEBHOOK SERVER")
    logger.info("=" * 70)
    
    # Inicializar connection pool
    init_db_pool()
    
    # Crear aplicación aiohttp
    app = web.Application(middlewares=[auth_middleware])
    
    # Registrar rutas
    app.router.add_get('/health', health_check)
    app.router.add_post('/webhook', webhook_handler)
    
    logger.info(f"✓ Rutas configuradas: GET /health, POST /webhook")
    logger.info(f"✓ Puerto: {WEBHOOK_PORT}")
    logger.info(f"✓ Auth token configurado: {'Sí' if WEBHOOK_AUTH_TOKEN else 'NO (INSEGURO)'}")
    logger.info("=" * 70)
    
    # Arrancar servidor
    web.run_app(app, host='0.0.0.0', port=WEBHOOK_PORT)

if __name__ == "__main__":
    main()
