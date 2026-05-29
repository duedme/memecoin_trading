import os
import time
import base64
import requests
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.api import Client

# Load environment variables
load_dotenv()

def run_token_purchase():
    print("Starting automated token purchase test...")
    
    # Fetch environment variables
    helius_key = os.getenv("HELIUS_API_KEY")
    private_key_str = os.getenv("PHANTOM_PRIVATE_KEY")
    token_mint = os.getenv("TARGET_TOKEN_MINT")
    
    # Safety checks for configuration
    if not all([helius_key, private_key_str, token_mint]):
        print("Error: Missing required variables in .env file (HELIUS_API_KEY, PHANTOM_PRIVATE_KEY, or TARGET_TOKEN_MINT).")
        return

    # Define purchase amount (0.001 SOL)
    amount_sol = 0.001
    amount_lamports = int(amount_sol * 1_000_000_000)
    
    # Reconstruct Phantom wallet keypair
    try:
        if private_key_str.startswith("["):
            import ast
            secret_bytes = bytes(ast.literal_eval(private_key_str))
            real_wallet = Keypair.from_bytes(secret_bytes)
        else:
            real_wallet = Keypair.from_base58_string(private_key_str)
        print(f"Wallet authentication successful: {real_wallet.pubkey()}")
    except Exception as wallet_err:
        print(f"Wallet restoration failed: {wallet_err}")
        return

    # Initialize Helius RPC client
    helius_url = f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
    rpc_client = Client(helius_url)

    try:
        # 1. Fetch quote from Jupiter API (Using native SOL mint as input)
        print("Fetching route and math calculations from Jupiter...")
        sol_mint = "So11111111111111111111111111111111111111112"
        quote_url = f"https://quote-api.jup.ag/v6/quote?inputMint={sol_mint}&outputMint={token_mint}&amount={amount_lamports}&slippageBps=100"
        
        quote_response = requests.get(quote_url).json()
        if "error" in quote_response:
            print(f"Jupiter quote error: {quote_response['error']}")
            return

        # 2. Request serialized transaction from Jupiter
        print("Assembling protected transaction structure...")
        swap_url = "https://quote-api.jup.ag/v6/swap"
        payload = {
            "quoteResponse": quote_response,
            "userPublicKey": str(real_wallet.pubkey()),
            "wrapAndUnwrapSol": True
        }
        
        swap_response = requests.post(swap_url, json=payload).json()
        if "swapTransaction" not in swap_response:
            print("Failed to retrieve swap transaction from Jupiter API.")
            return
            
        swap_tx_b64 = swap_response["swapTransaction"]

        # 3. Deserialize and sign transaction locally
        print("Signing transaction with real funds...")
        raw_tx_bytes = base64.b64decode(swap_tx_b64)
        tx = VersionedTransaction.from_bytes(raw_tx_bytes)
        tx = VersionedTransaction(tx.message, [real_wallet])

        # 4. Broadcast transaction through Helius RPC
        print("Broadcasting purchase transaction to the network...")
        start_time = time.time()
        tx_response = rpc_client.send_transaction(tx)
        end_time = time.time()
        
        print(f"Purchase transaction successfully broadcasted.")
        print(f"Transaction Signature: {tx_response.value}")
        print(f"Network Latency: {(end_time - start_time) * 1000:.2f} ms")
        print("You can verify inclusion by pasting the signature into solscan.io")

    except Exception as e:
        print(f"Execution failed during purchase flow: {e}")

if __name__ == "__main__":
    run_token_purchase()