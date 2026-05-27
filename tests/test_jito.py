import requests
import json
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solana.rpc.api import Client

# Jito Block Engine URL (Frankfurt endpoint for testing)
jito_frankfurt_url = "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles"

# Public RPC node just to fetch the recent blockhash
rpc_client = Client("https://api.mainnet-beta.solana.com")

# Official Jito account to receive the tip
jito_tip_account = Pubkey.from_string("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")

def test_jito():
    print("Starting Jito bundle submission test...")
    
    # Generate a temporary wallet for signing
    temp_wallet = Keypair()
    print(f"Temporary wallet generated: {temp_wallet.pubkey()}")
    
    try:
        # 1. Get recent blockhash
        blockhash_response = rpc_client.get_latest_blockhash()
        recent_blockhash = blockhash_response.value.blockhash
        
        # 2. Instruction 1: Test transfer (0 SOL)
        test_instruction = transfer(
            TransferParams(
                from_pubkey=temp_wallet.pubkey(),
                to_pubkey=temp_wallet.pubkey(),
                lamports=0
            )
        )
        
        # 3. Instruction 2: Jito tip (10,000 lamports)
        tip_instruction = transfer(
            TransferParams(
                from_pubkey=temp_wallet.pubkey(),
                to_pubkey=jito_tip_account,
                lamports=10000
            )
        )
        
        # 4. Compile and sign the transaction
        message = MessageV0.try_compile(
            payer=temp_wallet.pubkey(),
            instructions=[test_instruction, tip_instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=recent_blockhash,
        )
        tx = VersionedTransaction(message, [temp_wallet])
        
        # 5. Serialize to base58 as required by Jito
        tx_base58 = str(tx)
        
        # 6. Prepare the bundle payload
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                [tx_base58]
            ]
        }
        
        # 7. Send via HTTP POST
        response = requests.post(
            jito_frankfurt_url, 
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload)
        )
        
        print(f"HTTP response code: {response.status_code}")
        print(f"Jito response: {response.text}")
        
    except Exception as e:
        print(f"Error during Jito test: {e}")

if __name__ == "__main__":
    test_jito()