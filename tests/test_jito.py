import os
import requests
import json
import base58
import time
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solana.rpc.api import Client

load_dotenv()

jito_frankfurt_url = "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles"
rpc_client = Client("https://api.mainnet-beta.solana.com")
jito_tip_account = Pubkey.from_string("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")

def test_jito_real_funds():
    
    private_key_str = os.getenv("PHANTOM_PRIVATE_KEY")
    if not private_key_str:
        print("Error: PHANTOM_PRIVATE_KEY not found in .env file.")
        return
        
    try:
        if private_key_str.startswith("["):
            import ast
            secret_bytes = bytes(ast.literal_eval(private_key_str))
            real_wallet = Keypair.from_bytes(secret_bytes)
        else:
            real_wallet = Keypair.from_base58_string(private_key_str)
            
        print(f"Phantom wallet: {real_wallet.pubkey()}")
        
        blockhash_response = rpc_client.get_latest_blockhash()
        recent_blockhash = blockhash_response.value.blockhash
        
        test_instruction = transfer(
            TransferParams(
                from_pubkey=real_wallet.pubkey(),
                to_pubkey=real_wallet.pubkey(),
                lamports=0
            )
        )
        
        tip_instruction = transfer(
            TransferParams(
                from_pubkey=real_wallet.pubkey(),
                to_pubkey=jito_tip_account,
                lamports=10000
            )
        )
        
        message = MessageV0.try_compile(
            payer=real_wallet.pubkey(),
            instructions=[test_instruction, tip_instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=recent_blockhash,
        )
        tx = VersionedTransaction(message, [real_wallet])
        
        tx_bytes = bytes(tx)
        tx_base58 = base58.b58encode(tx_bytes).decode('ascii')
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                [tx_base58]
            ]
        }
        
        print("Enviando bundle a Jito...")
        start_time = time.time()
        
        response = requests.post(
            jito_frankfurt_url, 
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload)
        )
        
        end_time = time.time()
        latency = (end_time - start_time) * 1000
        
        print(f"Código HTTP: {response.status_code}")
        print(f"Respuesta de Jito: {response.text}")
        print(f"Latencia: {latency:.2f} ms")
        
    except Exception as e:
        print(f"Error during real funds Jito test: {e}")

if __name__ == "__main__":
    test_jito_real_funds()