"""
geyser_tx_worker.py
Reemplaza a chain_listener y tx_parser.
Escucha transacciones directamente desde la RAM del nodo (Yellowstone gRPC),
calcula los deltas de tokens/SOL e inserta directo a PostgreSQL.
"""
import time
import signal
import grpc
import base58
import psycopg2
from datetime import datetime, timezone

import geyser_pb2
import geyser_pb2_grpc

from shared_config import DB_CONFIG, get_logger

log = get_logger("geyser-tx-worker")
STOP = False

def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Deteniendo geyser-tx-worker...")

signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

WSOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ==========================================
# QUERIES SQL (Iguales a tu tx_parser.py)
# ==========================================
UPSERT_WALLET = """
INSERT INTO wallets (walletaddress, firstseen, lastseen)
VALUES (%s, NOW(), NOW())
ON CONFLICT (walletaddress) DO UPDATE SET lastseen = NOW()
"""

INSERT_TX = """
INSERT INTO wallettransactions
    (time, signature, walletaddress, mintaddress, side,
     amounttoken, amountsol, pricesol, slot, amm, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""

ENQUEUE_REDUCER = """
INSERT INTO reducer_queue
    (event_type, walletaddress, mintaddress, signature, priority, status, created_at)
VALUES (%s, %s, %s, %s, %s, 'pending', NOW())
ON CONFLICT (event_type, signature, walletaddress, mintaddress) DO NOTHING
"""

def db_connect():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn

def process_transaction(conn, tx_update):
    """Extrae la información de la transacción gRPC con blindaje de errores y guarda en DB"""
    try:
        # Acceso ultra-seguro a los datos crudos
        tx_data = tx_update.transaction
        tx = tx_data.transaction
        meta = tx_data.meta
        slot = tx_update.slot
        
        # --- AJUSTE DE FIRMA (El culpable del error) ---
        # Intentamos obtener la firma de las dos formas posibles en gRPC
        raw_sig = getattr(tx, 'signature', getattr(tx, 'sig', None))
        
        if not raw_sig:
            return # Si no hay firma, no podemos procesar

        signature = base58.b58encode(raw_sig).decode('utf-8')
        
        # --- LUZ DE RAYOS X ---
        log.info(f"🔍 RECIBIDA TX CRUDA: {signature[:10]}...")

        # 1. Validar estado (meta ya lo tenemos arriba)
        if not meta or meta.err: 
            return

        # 2. Extraer cuentas involucradas
        # En algunas versiones es 'tx.message.account_keys', en otras es 'tx.message.static_account_keys'
        msg = tx.message
        raw_keys = getattr(msg, 'account_keys', getattr(msg, 'static_account_keys', []))
        account_keys = [base58.b58encode(k).decode('utf-8') for k in raw_keys]
        
        if not account_keys: 
            return
            
        trader = account_keys[0]

        # 3. Buscar diferencias en balances (A prueba de Bots)
        pre_tokens = {}
        post_tokens = {}
        
        # Filtramos solo lo que nos importa (Pump.fun)
        for tb in getattr(meta, 'pre_token_balances', []):
            if tb.mint and tb.owner and tb.mint != WSOL_MINT:
                try: pre_tokens[(tb.mint, tb.owner)] = float(tb.ui_token_amount.ui_amount or 0.0)
                except: pass

        for tb in getattr(meta, 'post_token_balances', []):
            if tb.mint and tb.owner and tb.mint != WSOL_MINT:
                try: post_tokens[(tb.mint, tb.owner)] = float(tb.ui_token_amount.ui_amount or 0.0)
                except: pass
        
        best_mint = None
        best_delta = 0.0
        actual_trader = trader
        
        for key in set(pre_tokens.keys()).union(set(post_tokens.keys())):
            mint, owner = key
            delta = post_tokens.get(key, 0.0) - pre_tokens.get(key, 0.0)
            if abs(delta) > abs(best_delta):
                best_delta = delta
                best_mint = mint
                actual_trader = owner 
                
        if not best_mint or best_delta == 0.0:
            # --- LUZ DE RAYOS X 2: Ver por qué se descarta ---
            log.info(f"⏭️ Saltando {signature}: No es un swap de tokens (Delta 0).")
            return 

        # 4. Calcular delta de SOL gastado de forma segura
        amount_sol = 0.0
        try:
            if actual_trader in account_keys:
                trader_idx = account_keys.index(actual_trader)
                if len(meta.post_balances) > trader_idx and len(meta.pre_balances) > trader_idx:
                    sol_delta_lamports = meta.post_balances[trader_idx] - meta.pre_balances[trader_idx]
                    amount_sol = abs(sol_delta_lamports) / LAMPORTS_PER_SOL
        except Exception:
            amount_sol = 0.0
            
        # 5. Formatear salida final
        amount_token = abs(best_delta)
        side = "buy" if best_delta > 0 else "sell"
        price_sol = (amount_sol / amount_token) if amount_token > 0 else 0.0
        ts = datetime.now(timezone.utc)
        
        # 6. Inserción protegida en la base de datos (Usando actual_trader)
        try:
            with conn.cursor() as cur:
                cur.execute(UPSERT_WALLET, (actual_trader,))
                cur.execute(INSERT_TX, (ts, signature, actual_trader, best_mint, side, amount_token, amount_sol, price_sol, slot, "pumpfun", "geyser"))
                
                # Inyectar en la cola del pipeline interno (Reducers)
                cur.execute(ENQUEUE_REDUCER, ("position_update", actual_trader, best_mint, signature, 10))
                cur.execute(ENQUEUE_REDUCER, ("wallet_pnl_update", actual_trader, None, signature, 8))
                cur.execute(ENQUEUE_REDUCER, ("token_trader_update", None, best_mint, signature, 8))
                cur.execute(ENQUEUE_REDUCER, ("classification_update", actual_trader, None, signature, 3))
                
            conn.commit()
            log.info(f"✅ Trade guardado en Postgres: {side.upper()} {amount_token:.0f} {best_mint[:4]}... por {amount_sol:.4f} SOL")
        except Exception as db_err:
            conn.rollback()
            log.error(f"⚠️ Error de inserción SQL (saltando txn {signature[:8]}): {db_err}")

    except Exception as general_err:
        log.error(f"❌ Error crítico de análisis estructural: {general_err}")

def run_stream():
    log.info("Iniciando geyser-tx-worker...")
    
    while not STOP:
        conn = None
        try:
            conn = db_connect()
            channel = grpc.insecure_channel('host.docker.internal:10000')
            stub = geyser_pb2_grpc.GeyserStub(channel)

            request = geyser_pb2.SubscribeRequest()
            request.transactions["pumpfun_memecoins"].account_include.append("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
            request.commitment = 1 # Confirmed

            log.info("📡 Conectado al stream de transacciones gRPC (Pump.fun)...")
            
            responses = stub.Subscribe(iter([request]))
            for response in responses:
                if STOP: break
                if response.HasField("transaction"):
                    process_transaction(conn, response.transaction)
                    
        except grpc.RpcError as e:
            log.warning(f"Desconectado del nodo: {e}. Reconectando en 3s...")
            time.sleep(3)
        except Exception as e:
            log.error(f"Error fatal: {e}")
            time.sleep(3)
        finally:
            if conn: conn.close()

if __name__ == "__main__":
    run_stream()