import os
import time
from dotenv import load_dotenv
from solana.rpc.api import Client
from solders.pubkey import Pubkey

# Load environment variables from .env file
load_dotenv()

triton_url = os.getenv("TRITON_RPC_URL")

def test_triton():
    print("Starting Triton test...")
    
    if not triton_url:
        print("Error: TRITON_RPC_URL not found in .env file.")
        return

    client = Client(triton_url)
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
        print(f"Error connecting to Triton: {e}")

if __name__ == "__main__":
    test_triton()