import grpc
import geyser_pb2
import geyser_pb2_grpc

def stream_from_geyser():
    # 1. Nos conectamos al puerto que abriste en geyser.json
    channel = grpc.insecure_channel('127.0.0.1:10000')
    stub = geyser_pb2_grpc.GeyserStub(channel)

    # 2. Creamos la suscripción: "Solo dame transacciones que toquen Pump.fun"
    request = geyser_pb2.SubscribeRequest()
    
    # Filtro: Program ID de Pump.fun
    filter_name = "pumpfun_memecoins"
    request.transactions[filter_name].account_include.append("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
    
    # Pedimos nivel "confirmed" para velocidad
    request.commitment = 1 

    print("📡 Escuchando transacciones de Pump.fun directamente desde la RAM...")

    # 3. ¡El Stream! Esto es un loop infinito que recibe data empujada por el nodo
    try:
        # Enviamos el request de suscripción como un iterador
        responses = stub.Subscribe(iter([request]))
        
        for response in responses:
            if response.HasField("transaction"):
                tx = response.transaction
                
                # ¡AQUÍ ESTÁ LA MAGIA!
                # La variable 'tx' ya tiene TODO: preBalances, postTokenBalances, instructions.
                # Ya no necesitas hacer "getTransaction" jamás.
                
                signature = base58.b58encode(tx.transaction.signature).decode('utf-8')
                print(f"🔥 Nueva Tx interceptada en milisegundos: {signature}")
                
                # ---> AQUÍ llamas a tu función insert_staging(conn, signature, raw_json...)
                # o la mandas directo a Postgres.
                
    except grpc.RpcError as e:
        print(f"Stream desconectado: {e}")

if __name__ == "__main__":
    stream_from_geyser()
