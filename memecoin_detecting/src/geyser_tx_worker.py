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
    """Extrae la información de la transacción gRPC y la guarda en DB"""
    try:
        # 1. Información básica
        tx = tx_update.transaction.transaction
        meta = tx_update.transaction.meta
        slot = tx_update.slot
        
        # Si la transacción falló en la blockchain, la ignoramos
        if meta.err: 
            return

        signature = base58.b58encode(tx.signature).decode('utf-8')
        
        # 2. Extraer las llaves (cuentas) involucradas
        account_keys = [base58.b58encode(k).decode('utf-8') for k in tx.message.account_keys]
        if not account_keys: 
            return
            
        trader = account_keys[0] # El que firma la transacción es el trader

        # 3. Buscar diferencias en los balances de TOKENS para el trader
        pre_tokens = {tb.mint: float(tb.ui_token_amount.ui_amount or 0) 
                      for tb in meta.pre_token_balances if tb.owner == trader and tb.mint != WSOL_MINT}
                      
        post_tokens = {tb.mint: float(tb.ui_token_amount.ui_amount or 0) 
                       for tb in meta.post_token_balances if tb.owner == trader and tb.mint != WSOL_MINT}
        
        best_mint = None
        best_delta = 0.0
        
        # Determinamos qué token fue el que se compró/vendió
        for mint in set(pre_tokens.keys()).union(set(post_tokens.keys())):
            delta = post_tokens.get(mint, 0.0) - pre_tokens.get(mint, 0.0)
            if abs(delta) > abs(best_delta):
                best_delta = delta
                best_mint = mint
                
        if not best_mint or best_delta == 0:
            return # No hubo movimiento de memecoins

        # 4. Buscar diferencias en el balance de SOL (Cuánto costó)
        try:
            trader_idx = account_keys.index(trader)
            sol_delta_lamports = meta.post_balances[trader_idx] - meta.pre_balances[trader_idx] + meta.fee
            amount_sol = abs(sol_delta_lamports) / LAMPORTS_PER_SOL
        except ValueError:
            amount_sol = 0.0
            
        # 5. Formatear los datos finales
        amount_token = abs(best_delta)
        side = "buy" if best_delta > 0 else "sell"
        price_sol = (amount_sol / amount_token) if amount_token > 0 else 0.0
        ts = datetime.now(timezone.utc)
        
        # 6. Insertar en la Base de Datos
        with conn.cursor() as cur:
            cur.execute(UPSERT_WALLET, (trader,))
            cur.execute(INSERT_TX, (ts, signature, trader, best_mint, side, amount_token, amount_sol, price_sol, slot, "pumpfun", "geyser"))
            
            # Encolar para que los Reducers calculen el P&L en background
            cur.execute(ENQUEUE_REDUCER, ("position_update", trader, best_mint, signature, 10))
            cur.execute(ENQUEUE_REDUCER, ("wallet_pnl_update", trader, None, signature, 8))
            cur.execute(ENQUEUE_REDUCER, ("token_trader_update", None, best_mint, signature, 8))
            cur.execute(ENQUEUE_REDUCER, ("classification_update", trader, None, signature, 3))
            
        conn.commit()
        log.info(f"✅ Trade guardado: {side.upper()} {amount_token:.0f} {best_mint[:4]}... por {amount_sol:.4f} SOL")

    except Exception as e:
        conn.rollback()
        log.error(f"Error procesando tx: {e}")

def run_stream():
    log.info("Iniciando geyser-tx-worker...")
    
    while not STOP:
        conn = None
        try:
            conn = db_connect()
            channel = grpc.insecure_channel('127.0.0.1:10000')
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