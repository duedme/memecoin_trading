import os
import time
from dotenv import load_dotenv
from solana.rpc.api import Client
from solders.pubkey import Pubkey

# Load environment variables from .env file
load_dotenv()

helius_api_key = os.getenv("HELIUS_API_KEY")

def test_helius():
    print("Starting Helius test...")
    
    if not helius_api_key:
        print("Error: HELIUS_API_KEY not found in .env file.")
        return

    helius_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
    client = Client(helius_url)
    
    # Using a public wallet for the test (e.g., Binance Hot Wallet)
    test_wallet = Pubkey.from_string("5tzFkiKscXHK5ZXCGbXZzV7MgeDkheEhhHhhZ1b2nK7t")
    
    start_time = time.time()
    try:
        response = client.get_balance(test_wallet)
        end_time = time.time()
        
        sol_balance = response.value / 1_000_000_000
        latency = (end_time - start_time) * 1000
        
        print("Connection successful.")
        print(f"Balance retrieved: {sol_balance} SOL")
        print(f"Approximate HTTP latency: {latency:.2f} ms")
        
    except Exception as e:
        print(f"Error connecting to Helius: {e}")

if __name__ == "__main__":
    test_helius()