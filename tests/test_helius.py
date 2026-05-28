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

def test_helius_send():
    print("Starting Helius transaction submission test...")
    
    if not helius_api_key:
        print("Error: HELIUS_API_KEY not found.")
        return

    helius_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
    client = Client(helius_url)
    
    temp_wallet = Keypair()
    print(f"Wallet temporal generada: {temp_wallet.pubkey()}")
    
    try:
        blockhash_resp = client.get_latest_blockhash()
        recent_blockhash = blockhash_resp.value.blockhash
        
        test_instruction = transfer(
            TransferParams(
                from_pubkey=temp_wallet.pubkey(),
                to_pubkey=temp_wallet.pubkey(),
                lamports=0
            )
        )
        
        message = MessageV0.try_compile(
            payer=temp_wallet.pubkey(),
            instructions=[test_instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=recent_blockhash,
        )
        tx = VersionedTransaction(message, [temp_wallet])
        
        print("Enviando transacción a Helius...")
        start_time = time.time()
        
        # Bloque interno para capturar la latencia del rechazo
        try:
            response = client.send_transaction(tx)
            end_time = time.time()
            print(f"Respuesta de Helius: {response}")
            print(f"Latencia: {(end_time - start_time) * 1000:.2f} ms")
        except Exception as sim_error:
            end_time = time.time()
            print(f"Error esperado por falta de balance: {sim_error}")
            print(f"Latencia hasta el rechazo: {(end_time - start_time) * 1000:.2f} ms")
            
    except Exception as e:
        print(f"Error en la preparación de la transacción: {e}")

if __name__ == "__main__":
    test_helius_send()