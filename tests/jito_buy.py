import os
import time
import base64
import requests
import json
import base58
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.message import MessageV0

# Load environment variables
load_dotenv()

def run_jito_purchase():
    print("Iniciando prueba de compra automatizada con motor Jito (Ajuste Agresivo)...")
    
    # Fetch environment variables
    private_key_str = os.getenv("PHANTOM_PRIVATE_KEY")
    token_mint = os.getenv("TARGET_TOKEN_MINT")
    
    # Safety checks
    if not private_key_str or not token_mint:
        print("Error: Faltan variables requeridas en el archivo .env")
        return

    # Define purchase amount
    amount_sol = 0.001
    amount_lamports = int(amount_sol * 1_000_000_000)
    
    # Reconstruct wallet
    try:
        if private_key_str.startswith("["):
            import ast
            secret_bytes = bytes(ast.literal_eval(private_key_str))
            real_wallet = Keypair.from_bytes(secret_bytes)
        else:
            real_wallet = Keypair.from_base58_string(private_key_str)
        print(f"Billetera autenticada exitosamente: {real_wallet.pubkey()}")
    except Exception as wallet_err:
        print(f"Fallo al restaurar la billetera: {wallet_err}")
        return

    try:
        # 1. Fetch quote from new Jupiter API (SLIPPAGE AUMENTADO AL 10%)
        print("Obteniendo ruta y calculos desde la plataforma Jupiter...")
        sol_mint = "So11111111111111111111111111111111111111112"
        quote_url = f"https://lite-api.jup.ag/swap/v1/quote?inputMint={sol_mint}&outputMint={token_mint}&amount={amount_lamports}&slippageBps=1000"
        
        quote_response = requests.get(quote_url).json()
        if "error" in quote_response:
            print(f"Error al cotizar: {quote_response['error']}")
            return

        # 2. Request swap transaction from new Jupiter API
        swap_url = "https://lite-api.jup.ag/swap/v1/swap"
        payload = {
            "quoteResponse": quote_response,
            "userPublicKey": str(real_wallet.pubkey()),
            "wrapAndUnwrapSol": True
        }
        
        swap_response = requests.post(swap_url, json=payload).json()
        if "swapTransaction" not in swap_response:
            print("No se pudo obtener la transaccion de intercambio.")
            return
            
        swap_tx_b64 = swap_response["swapTransaction"]

        # 3. Deserialize and sign Jupiter swap transaction
        print("Procesando y firmando transacciones localmente...")
        raw_tx_bytes = base64.b64decode(swap_tx_b64)
        unsigned_swap_tx = VersionedTransaction.from_bytes(raw_tx_bytes)
        signed_swap_tx = VersionedTransaction(unsigned_swap_tx.message, [real_wallet])
        
        # 4. Build Jito tip transaction (PROPINA AUMENTADA A 100,000)
        jito_tip_account = Pubkey.from_string("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")
        tip_ix = transfer(
            TransferParams(
                from_pubkey=real_wallet.pubkey(),
                to_pubkey=jito_tip_account,
                lamports=1000000
            )
        )
        
        tip_msg = MessageV0.try_compile(
            payer=real_wallet.pubkey(),
            instructions=[tip_ix],
            address_lookup_table_accounts=[],
            recent_blockhash=unsigned_swap_tx.message.recent_blockhash,
        )
        tip_tx = VersionedTransaction(tip_msg, [real_wallet])
        
        # 5. Serialize both to base58
        swap_b58 = base58.b58encode(bytes(signed_swap_tx)).decode('ascii')
        tip_b58 = base58.b58encode(bytes(tip_tx)).decode('ascii')
        
        # 6. Send bundle
        print("Enviando el paquete de operaciones al motor de bloques...")
        jito_url = "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles"
        bundle_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                [swap_b58, tip_b58]
            ]
        }
        
        start_time = time.time()
        response = requests.post(
            jito_url, 
            headers={"Content-Type": "application/json"},
            data=json.dumps(bundle_payload)
        )
        end_time = time.time()
        
        print(f"Envio completado. Codigo Http: {response.status_code}")
        print(f"Respuesta del servidor: {response.text}")
        print(f"Latencia de conexion: {(end_time - start_time) * 1000:.2f} ms")
        print("Nota: Si el codigo es 200, la compra se esta procesando en la red.")

    except Exception as e:
        print(f"Fallo la ejecucion del proceso: {e}")

if __name__ == "__main__":
    run_jito_purchase()