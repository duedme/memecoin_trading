import os
import time
import base64
import requests
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

# Load environment variables
load_dotenv()

def run_fast_purchase():
    print("Iniciando compra de alta velocidad desde VPS...")
    
    helius_key = os.getenv("HELIUS_API_KEY")
    private_key_str = os.getenv("PHANTOM_PRIVATE_KEY")
    token_mint = os.getenv("TARGET_TOKEN_MINT")

    if not all([helius_key, private_key_str, token_mint]):
        print("Error: Faltan variables en el archivo .env")
        return

    amount_lamports = 1000000  # 0.001 SOL
    
    # Restaurar Billetera
    try:
        if private_key_str.startswith("["):
            import ast
            real_wallet = Keypair.from_bytes(bytes(ast.literal_eval(private_key_str)))
        else:
            real_wallet = Keypair.from_base58_string(private_key_str)
        print(f"Billetera autenticada: {real_wallet.pubkey()}")
    except Exception as e:
        print(f"Error con la llave privada: {e}")
        return

    try:
        print("1. Cotizando en Jupiter...")
        sol_mint = "So11111111111111111111111111111111111111112"
        quote_url = f"https://lite-api.jup.ag/swap/v1/quote?inputMint={sol_mint}&outputMint={token_mint}&amount={amount_lamports}&slippageBps=1000"
        quote_response = requests.get(quote_url).json()

        if "error" in quote_response:
            print(f"Error de Jupiter: {quote_response['error']}")
            return

        print("2. Ensamblando transacción...")
        swap_url = "https://lite-api.jup.ag/swap/v1/swap"
        payload_jup = {
            "quoteResponse": quote_response, 
            "userPublicKey": str(real_wallet.pubkey()), 
            "wrapAndUnwrapSol": True
        }
        swap_response = requests.post(swap_url, json=payload_jup).json()
        swap_tx_b64 = swap_response["swapTransaction"]

        print("3. Firmando con fondos reales...")
        raw_tx_bytes = base64.b64decode(swap_tx_b64)
        tx = VersionedTransaction.from_bytes(raw_tx_bytes)
        tx = VersionedTransaction(tx.message, [real_wallet])

        print("4. Enviando directamente a Solana por Helius (Modo Crudo)...")
        # Empaquetamos la transaccion firmada a base64 para el RPC
        tx_b64 = base64.b64encode(bytes(tx)).decode('utf-8')
        
        payload_rpc = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [tx_b64, {"encoding": "base64"}]
        }

        url = f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
        
        start_time = time.time()
        response = requests.post(url, json=payload_rpc)
        end_time = time.time()

        print(f"\nLatencia HTTP: {(end_time - start_time) * 1000:.2f} ms")
        print(f"Código HTTP: {response.status_code}")
        print(f"Respuesta cruda de Helius:\n{response.text}")

    except Exception as e:
        print(f"\n❌ Error interno de ejecución:")
        print(e)

if __name__ == "__main__":
    run_fast_purchase()