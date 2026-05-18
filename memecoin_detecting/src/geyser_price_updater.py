"""
geyser_price_updater.py
Reemplaza al antiguo price_updater.py (que consultaba cada 15s).
Escucha cambios en las Bonding Curves directamente desde la RAM vía gRPC
y actualiza Postgres en el milisegundo exacto en que el precio cambia.
"""
import time
import signal
import grpc
import base58
import struct
import psycopg2
from datetime import datetime

import geyser_pb2
import geyser_pb2_grpc

from shared_config import DB_CONFIG, get_logger
from parsers.pumpfun_curve import get_bonding_curve_pda

log = get_logger("geyser-price-updater")
STOP = False

def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Deteniendo geyser-price-updater...")

signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)

LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_DECIMALS = 6

# ==========================================
# QUERIES SQL
# ==========================================
UPSERT_PRICE = """
INSERT INTO token_price_cache (mintaddress, pricesol, priceusd, source, updated_at)
VALUES (%s, %s, %s, 'geyser', NOW())
ON CONFLICT (mintaddress) DO UPDATE SET
    pricesol = EXCLUDED.pricesol,
    priceusd = EXCLUDED.priceusd,
    source = EXCLUDED.source,
    updated_at = NOW();
"""

UPDATE_UNREALIZED = """
UPDATE walletpositions
   SET unrealized_pnl_sol = amounttoken * (%s - avg_buy_price_sol),
       total_pnl_sol = realized_pnl_sol + (amounttoken * (%s - avg_buy_price_sol)),
       lastupdate = NOW(),
       updated_at = NOW()
 WHERE mintaddress = %s AND amounttoken > 0;
"""

def db_connect():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn

def get_sol_usd(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT priceusd FROM sol_price_cache WHERE id = 1")
        row = cur.fetchone()
        conn.commit()
    return float(row[0]) if row and row[0] else 150.0

def load_pda_to_mint_map(conn):
    """Carga los tokens activos para saber a qué token pertenece cada PDA"""
    mapping = {}
    with conn.cursor() as cur:
        cur.execute("SELECT mintaddress FROM tokens WHERE status = 'active'")
        for row in cur.fetchall():
            mint = row[0]
            try:
                pda = get_bonding_curve_pda(mint)
                mapping[pda] = mint
            except Exception:
                pass
    return mapping

def decode_curve_and_get_price(raw_data: bytes):
    if len(raw_data) < 41:
        return None
    vtok = struct.unpack_from("<Q", raw_data, 8)[0]
    vsol = struct.unpack_from("<Q", raw_data, 16)[0]
    
    if vtok == 0:
        return None
    return (vsol / LAMPORTS_PER_SOL) / (vtok / (10 ** TOKEN_DECIMALS))

def run_stream():
    log.info("Iniciando geyser-price-updater...")
    
    while not STOP:
        conn = None
        try:
            conn = db_connect()
            sol_usd = get_sol_usd(conn)
            pda_to_mint = load_pda_to_mint_map(conn)
            last_map_refresh = time.time()
            
            channel = grpc.insecure_channel('127.0.0.1:10000')
            stub = geyser_pb2_grpc.GeyserStub(channel)

            request = geyser_pb2.SubscribeRequest()
            request.accounts["pumpfun_curves"].owner.append("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
            request.commitment = 1

            log.info(f"📡 Conectado a Geyser. Mapeados {len(pda_to_mint)} tokens.")
            
            responses = stub.Subscribe(iter([request]))
            for response in responses:
                if STOP: break
                
                # Refrescar mapa de tokens nuevos cada 60 segundos
                if time.time() - last_map_refresh > 60:
                    pda_to_mint = load_pda_to_mint_map(conn)
                    sol_usd = get_sol_usd(conn)
                    last_map_refresh = time.time()
                
                if response.HasField("account"):
                    acc_info = response.account.account
                    pda = base58.b58encode(acc_info.pubkey).decode('utf-8')
                    
                    mint = pda_to_mint.get(pda)
                    if not mint:
                        continue # Token desconocido o no activo, lo ignoramos

                    price_sol = decode_curve_and_get_price(acc_info.data)
                    if not price_sol:
                        continue
                        
                    price_usd = price_sol * sol_usd
                    
                    # Actualizar DB
                    with conn.cursor() as cur:
                        cur.execute(UPSERT_PRICE, (mint, price_sol, price_usd))
                        cur.execute(UPDATE_UNREALIZED, (price_sol, price_sol, mint))
                    conn.commit()
                    
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