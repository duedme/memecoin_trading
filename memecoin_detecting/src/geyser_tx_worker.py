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
    """Extrae la info, busca en ALTs (V0 Txs) y guarda en DB"""
    try:
        tx_data = tx_update.transaction
        tx = tx_data.transaction
        meta = tx_data.meta
        slot = tx_update.slot
        
        # 1. Extraer firma correcta
        raw_sig = getattr(tx_data, 'signature', getattr(tx, 'signature', None))
        if not raw_sig: return
        signature = base58.b58encode(raw_sig).decode('utf-8')

        # 2. Recopilar TODAS las llaves (Estáticas + Dinámicas)
        account_keys = []
        if hasattr(tx.message, 'account_keys'):
            account_keys.extend([base58.b58encode(k).decode('utf-8') for k in tx.message.account_keys])
        if meta:
            if hasattr(meta, 'loaded_writable_addresses'):
                account_keys.extend([base58.b58encode(k).decode('utf-8') for k in meta.loaded_writable_addresses])
            if hasattr(meta, 'loaded_readonly_addresses'):
                account_keys.extend([base58.b58encode(k).decode('utf-8') for k in meta.loaded_readonly_addresses])

        # 3. Filtro Pump.fun 
        if "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" not in account_keys:
            return

        # ==========================================================
        # 🔥 ZONA PUMP.FUN: Corregida para la Trampa Protobuf
        # ==========================================================
        try:
            # Validación correcta de error en gRPC / Protobuf
            has_error = False
            try:
                if meta.HasField("err"): has_error = True
            except:
                if bool(str(getattr(meta, 'err', '')).strip()): has_error = True
                
            if has_error:
                # Callamos el print porque los bots fallan el 70% de las veces y harían spam
                return
                
            trader = account_keys[0]

            pre_tokens = {}
            post_tokens = {}
            
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
                # También callamos el delta 0 (Bots creando tokens sin liquidez)
                return 

            amount_sol = 0.0
            if actual_trader in account_keys:
                trader_idx = account_keys.index(actual_trader)
                if len(meta.post_balances) > trader_idx and len(meta.pre_balances) > trader_idx:
                    sol_delta_lamports = meta.post_balances[trader_idx] - meta.pre_balances[trader_idx]
                    amount_sol = abs(sol_delta_lamports) / LAMPORTS_PER_SOL
                
            amount_token = abs(best_delta)
            side = "buy" if best_delta > 0 else "sell"
            price_sol = (amount_sol / amount_token) if amount_token > 0 else 0.0
            ts = datetime.now(timezone.utc)
            
            with conn.cursor() as cur:
                cur.execute(UPSERT_WALLET, (actual_trader,))
                cur.execute(INSERT_TX, (ts, signature, actual_trader, best_mint, side, amount_token, amount_sol, price_sol, slot, "pumpfun", "geyser"))
                
                cur.execute(ENQUEUE_REDUCER, ("position_update", actual_trader, best_mint, signature, 10))
                cur.execute(ENQUEUE_REDUCER, ("wallet_pnl_update", actual_trader, None, signature, 8))
                cur.execute(ENQUEUE_REDUCER, ("token_trader_update", None, best_mint, signature, 8))
                cur.execute(ENQUEUE_REDUCER, ("classification_update", actual_trader, None, signature, 3))
                
            conn.commit()
            log.info(f"✅ Trade Pump.fun guardado: {side.upper()} {amount_token:.0f} {best_mint[:4]}... por {amount_sol:.4f} SOL")

        except Exception as e:
            log.error(f"❌ Error en lógica Pump.fun: {e}")

    except Exception:
        pass

def run_stream():
    log.info("Iniciando geyser-tx-worker en MODO NUCLEAR DEFINITIVO...")
    
    while not STOP:
        conn = None
        try:
            conn = db_connect()
            channel = grpc.insecure_channel('host.docker.internal:10000')
            stub = geyser_pb2_grpc.GeyserStub(channel)

            request = geyser_pb2.SubscribeRequest()
            
            # 1. El Latido (Para asegurar que la conexión no muera)
            request.slots["monitor"].CopyFrom(geyser_pb2.SubscribeRequestFilterSlots())
            
            # 2. MODO NUCLEAR EXACTO (Sin condiciones booleanas que rompan el plugin)
            filter_tx = geyser_pb2.SubscribeRequestFilterTransactions()
            request.transactions["nuclear_stream"].CopyFrom(filter_tx)
            
            log.info("📡 Suscrito a TODA LA RED con Filtro Rayos X interno...")
            
            responses = stub.Subscribe(iter([request]))
            
            for response in responses:
                if STOP: break
                
                if response.HasField("transaction"):
                    # Enviamos la transacción cruda a los Rayos X
                    process_transaction(conn, response.transaction)
                elif response.HasField("slot"):
                    if response.slot.slot % 50 == 0:
                        log.info(f"💓 Latido de Solana - Slot: {response.slot.slot}")

        except Exception as e:
            log.error(f"Error en el stream: {e}")
            time.sleep(3)
        finally:
            if conn: conn.close()

if __name__ == "__main__":
    run_stream()