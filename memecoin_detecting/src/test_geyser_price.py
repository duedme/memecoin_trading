import grpc
import base58
import struct
import geyser_pb2
import geyser_pb2_grpc

LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_DECIMALS = 6

def decode_curve_and_get_price(raw_data: bytes):
    """Extrae el precio a partir de los bytes crudos de la cuenta"""
    if len(raw_data) < 41: # Debe tener al menos los datos básicos
        return None
    
    # Saltamos 8 bytes de discriminator
    vtok = struct.unpack_from("<Q", raw_data, 8)[0]
    vsol = struct.unpack_from("<Q", raw_data, 16)[0]
    
    if vtok == 0:
        return None
        
    price_sol = (vsol / LAMPORTS_PER_SOL) / (vtok / (10 ** TOKEN_DECIMALS))
    return price_sol

def stream_pumpfun_curves():
    channel = grpc.insecure_channel('127.0.0.1:10000')
    stub = geyser_pb2_grpc.GeyserStub(channel)

    # 1. Creamos la suscripción
    request = geyser_pb2.SubscribeRequest()
    
    # 2. Le decimos a Geyser: "Envíame cualquier cuenta cuyo dueño sea Pump.fun"
    request.accounts["pumpfun_curves"].owner.append("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
    request.commitment = 1 # Confirmed

    print("📡 Escuchando cambios de precio (Bonding Curves) en tiempo real...")

    try:
        responses = stub.Subscribe(iter([request]))
        for response in responses:
            # 3. Si el mensaje es una actualización de cuenta
            if response.HasField("account"):
                acc_info = response.account.account
                pubkey = base58.b58encode(response.account.pubkey).decode('utf-8')
                
                # acc_info.data ya viene en bytes crudos, no hay que decodificar base64
                price = decode_curve_and_get_price(acc_info.data)
                
                if price:
                    print(f"💎 PDA Actualizado: {pubkey} | Nuevo Precio: {price:.10f} SOL")

    except grpc.RpcError as e:
        print(f"❌ Stream desconectado: {e}")

if __name__ == "__main__":
    stream_pumpfun_curves()