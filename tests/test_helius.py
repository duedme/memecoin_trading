import os
import time
from dotenv import load_dotenv
from solana.rpc.api import Client
from solders.keypair import Keypair
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.message import MessageV0

load_dotenv()
helius_api_key = os.getenv("HELIUS_API_KEY")

def test_helius_send_real_funds():
    print("Transacción con fondos reales")
    
    if not helius_api_key:
        print("Error: HELIUS_API_KEY not found.")
        return

    private_key_str = os.getenv("PHANTOM_PRIVATE_KEY")
    if not private_key_str:
        print("Error: PHANTOM_PRIVATE_KEY not found in .env file.")
        return

    helius_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
    client = Client(helius_url)
    
    try:
        if private_key_str.startswith("["):
            import ast
            secret_bytes = bytes(ast.literal_eval(private_key_str))
            real_wallet = Keypair.from_bytes(secret_bytes)
        else:
            real_wallet = Keypair.from_base58_string(private_key_str)
            
        print(f"Phantom wallet: {real_wallet.pubkey()}")
        
        blockhash_resp = client.get_latest_blockhash()
        recent_blockhash = blockhash_resp.value.blockhash
        
        test_instruction = transfer(
            TransferParams(
                from_pubkey=real_wallet.pubkey(),
                to_pubkey=real_wallet.pubkey(),
                lamports=0
            )
        )
        
        message = MessageV0.try_compile(
            payer=real_wallet.pubkey(),
            instructions=[test_instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=recent_blockhash,
        )
        tx = VersionedTransaction(message, [real_wallet])
        
        print("Transacción a Helius...")
        start_time = time.time()
        
        response = client.send_transaction(tx)
        end_time = time.time()
        
        latency = (end_time - start_time) * 1000
        print(f"Respuesta de Helius: {response}")
        print(f"Latencia: {latency:.2f} ms")
        print("Transacción exitosa en mainnet.")
            
    except Exception as e:
        print(f"Error during real funds Helius test: {e}")

if __name__ == "__main__":
    test_helius_send_real_funds()