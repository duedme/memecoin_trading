#!/usr/bin/env python3
"""
register_webhooks.py
Script para registrar webhooks en Helius
VERSIÓN CORREGIDA:
  - FIX: authHeader sin prefijo "Bearer" (Helius lo envía tal cual)
  - accountAddresses corregido a lista de strings
"""

import os
import requests
import json

# ============================================================
# CONFIGURACIÓN
# ============================================================

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "TU_API_KEY_AQUI")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://tu-servidor.com/webhook")
WEBHOOK_AUTH_TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN", "tu_token_secreto_aqui")

HELIUS_WEBHOOK_API = f"https://api.helius.xyz/v0/webhooks?api-key={HELIUS_API_KEY}"

# ============================================================
# FUNCIONES
# ============================================================

def create_token_creation_webhook():
    """
    Crea webhook para detectar creación de nuevos tokens en Pump.fun
    """
    payload = {
        "webhookURL": WEBHOOK_URL,
        "transactionTypes": ["TOKEN_CREATION"],
        "accountAddresses": [
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"  # Pump.fun program ID
        ],
        "webhookType": "enhanced",
        # FIX: authHeader SIN prefijo "Bearer" - Helius lo manda tal cual
        "authHeader": WEBHOOK_AUTH_TOKEN
    }
    
    print("\n" + "=" * 70)
    print("📡 Registrando webhook: TOKEN_CREATION (Pump.fun)")
    print("=" * 70)
    print(f"URL: {WEBHOOK_URL}")
    print(f"Cuenta monitoreada: Pump.fun Program")
    
    response = requests.post(HELIUS_WEBHOOK_API, json=payload)
    
    if response.status_code == 200:
        result = response.json()
        webhook_id = result.get('webhookID')
        print(f"✅ Webhook creado exitosamente!")
        print(f"   Webhook ID: {webhook_id}")
        return webhook_id
    else:
        print(f"❌ Error creando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return None

def create_wallet_tracking_webhook(wallet_addresses: list):
    """
    Crea webhook para rastrear transacciones de wallets específicos
    
    Args:
        wallet_addresses: Lista de direcciones de wallets a rastrear
    """
    if not wallet_addresses:
        print("⚠️  No hay wallets para rastrear")
        return None
    
    payload = {
        "webhookURL": WEBHOOK_URL,
        "transactionTypes": ["SWAP"],
        "accountAddresses": wallet_addresses,
        "webhookType": "enhanced",
        # FIX: authHeader SIN prefijo "Bearer"
        "authHeader": WEBHOOK_AUTH_TOKEN
    }
    
    print("\n" + "=" * 70)
    print(f"📡 Registrando webhook: WALLET TRACKING ({len(wallet_addresses)} wallets)")
    print("=" * 70)
    print(f"URL: {WEBHOOK_URL}")
    print(f"Wallets monitoreados: {len(wallet_addresses)}")
    for i, wallet in enumerate(wallet_addresses[:5], 1):
        print(f"  {i}. {wallet}")
    if len(wallet_addresses) > 5:
        print(f"  ... y {len(wallet_addresses) - 5} más")
    
    response = requests.post(HELIUS_WEBHOOK_API, json=payload)
    
    if response.status_code == 200:
        result = response.json()
        webhook_id = result.get('webhookID')
        print(f"✅ Webhook creado exitosamente!")
        print(f"   Webhook ID: {webhook_id}")
        return webhook_id
    else:
        print(f"❌ Error creando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return None

def list_webhooks():
    """Lista todos los webhooks activos"""
    print("\n" + "=" * 70)
    print("📋 Listando webhooks activos")
    print("=" * 70)
    
    response = requests.get(HELIUS_WEBHOOK_API)
    
    if response.status_code == 200:
        webhooks = response.json()
        
        if not webhooks:
            print("No hay webhooks registrados")
            return []
        
        print(f"Total de webhooks: {len(webhooks)}\n")
        
        for i, webhook in enumerate(webhooks, 1):
            webhook_id = webhook.get('webhookID')
            webhook_url = webhook.get('webhookURL')
            tx_types = webhook.get('transactionTypes', [])
            accounts = webhook.get('accountAddresses', [])
            
            print(f"{i}. Webhook ID: {webhook_id}")
            print(f"   URL: {webhook_url}")
            print(f"   Transaction Types: {', '.join(tx_types)}")
            print(f"   Accounts: {len(accounts)} monitoreadas")
            print()
        
        return webhooks
    else:
        print(f"❌ Error listando webhooks: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return []

def delete_webhook(webhook_id: str):
    """Elimina un webhook por ID"""
    url = f"{HELIUS_WEBHOOK_API}&webhookID={webhook_id}"
    
    print(f"\n🗑️  Eliminando webhook: {webhook_id}")
    
    response = requests.delete(url)
    
    if response.status_code == 200:
        print(f"✅ Webhook eliminado exitosamente")
        return True
    else:
        print(f"❌ Error eliminando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return False

def load_tracked_wallets_from_db():
    """Carga wallets desde la base de datos (tracked_wallets table)"""
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="memecoins_db",
            user="postgres",
            password="12345"
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT wallet_address 
            FROM tracked_wallets 
            WHERE is_active = TRUE
            LIMIT 100
        """)
        
        wallets = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        return wallets
        
    except Exception as e:
        print(f"❌ Error cargando wallets desde DB: {e}")
        return []

# ============================================================
# MENÚ INTERACTIVO
# ============================================================

def main():
    print("\n" + "=" * 70)
    print("🔧 HELIUS WEBHOOK MANAGER")
    print("=" * 70)
    print("\nOpciones:")
    print("1. Registrar webhook TOKEN_CREATION (Pump.fun)")
    print("2. Registrar webhook WALLET TRACKING (desde DB)")
    print("3. Listar webhooks activos")
    print("4. Eliminar webhook")
    print("5. Registrar ambos (TOKEN_CREATION + WALLET)")
    print("0. Salir")
    
    choice = input("\nSelecciona una opción: ").strip()
    
    if choice == "1":
        create_token_creation_webhook()
        
    elif choice == "2":
        wallets = load_tracked_wallets_from_db()
        if wallets:
            create_wallet_tracking_webhook(wallets)
        else:
            print("⚠️  No hay wallets en la base de datos")
            
    elif choice == "3":
        list_webhooks()
        
    elif choice == "4":
        webhooks = list_webhooks()
        if webhooks:
            webhook_id = input("\nIngresa el Webhook ID a eliminar: ").strip()
            delete_webhook(webhook_id)
        
    elif choice == "5":
        # Registrar TOKEN_CREATION
        token_wh_id = create_token_creation_webhook()
        
        # Registrar WALLET TRACKING
        wallets = load_tracked_wallets_from_db()
        if wallets:
            wallet_wh_id = create_wallet_tracking_webhook(wallets)
        
        print("\n" + "=" * 70)
        print("✅ Proceso completado")
        if token_wh_id:
            print(f"   TOKEN_CREATION webhook ID: {token_wh_id}")
        if wallets and wallet_wh_id:
            print(f"   WALLET_TRACKING webhook ID: {wallet_wh_id}")
        print("=" * 70)
        
    elif choice == "0":
        print("👋 Saliendo...")
        return
    else:
        print("❌ Opción inválida")

if __name__ == "__main__":
    main()
