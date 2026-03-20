#!/usr/bin/env python3
"""
register_webhooks.py
Script para registrar webhooks en Helius

VERSIÓN CORREGIDA v2:
  - Endpoint corregido: api-mainnet.helius-rpc.com (no api.helius.xyz)
  - Token webhook: 12 AMMs con transactionTypes ["ANY"] (no solo Pump.fun con TOKEN_CREATION)
  - Wallet webhook: ["SWAP", "TRANSFER"] (no solo SWAP)
  - load_dotenv() al inicio
  - DB credentials desde .env (no hardcodeadas)
  - authHeader sin prefijo Bearer (match con webhook_server.py)
"""

import os
import sys
import requests
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURACIÓN DESDE .env
# ============================================================
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_AUTH_TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN")

# FIX CRÍTICO: Endpoint correcto (antes usaba api.helius.xyz)
HELIUS_WEBHOOK_API = f"https://api-mainnet.helius-rpc.com/v0/webhooks?api-key={HELIUS_API_KEY}"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "memecoins_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# ============================================================
# 12 AMM PROGRAM IDs (COMPLETO)
# ============================================================
AMM_ADDRESSES = [
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",   # Pump.fun
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",   # PumpSwap
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium AMM
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",  # Raydium LaunchLab
    "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",  # FluxBeam
    "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o",  # HeavenDEX
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",   # Meteora DLMM
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",   # Meteora DYN2
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",  # Meteora DYN
    "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",   # Meteora DBC
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",   # Moonit
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Orca
]

AMM_NAMES = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "Pump.fun",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium AMM",
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj": "Raydium LaunchLab",
    "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X": "FluxBeam",
    "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o": "HeavenDEX",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "Meteora DYN2",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora DYN",
    "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN": "Meteora DBC",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG": "Moonit",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca",
}


# ============================================================
# FUNCIONES DE REGISTRO
# ============================================================

def validate_config():
    """Valida que la configuración esté completa"""
    issues = []
    if not HELIUS_API_KEY or HELIUS_API_KEY == "tu_api_key_de_helius_aqui":
        issues.append("HELIUS_API_KEY no configurada en .env")
    if not WEBHOOK_URL or "tu-dominio" in WEBHOOK_URL:
        issues.append("WEBHOOK_URL no configurada en .env")
    if not WEBHOOK_AUTH_TOKEN or WEBHOOK_AUTH_TOKEN == "genera_un_token_aleatorio_seguro_aqui":
        issues.append("WEBHOOK_AUTH_TOKEN no configurado en .env")
    if not DB_CONFIG["password"]:
        issues.append("DB_PASSWORD no configurada en .env")
    return issues


def create_token_detection_webhook():
    """
    Crea webhook para detectar actividad en los 12 AMMs.

    FIX CRÍTICO vs versión anterior:
    - Antes: solo Pump.fun con transactionTypes=["TOKEN_CREATION"]
    - Ahora: 12 AMMs con transactionTypes=["ANY"] y filtrado server-side

    Razón: TOKEN_CREATION solo cubre Candy Machine y SPL mints,
    no cubre creación de pools en la mayoría de AMMs.
    Helius recomienda usar "ANY" + accountAddresses y filtrar en el server.
    """
    payload = {
        "webhookURL": WEBHOOK_URL,
        "transactionTypes": ["ANY"],
        "accountAddresses": AMM_ADDRESSES,
        "webhookType": "enhanced",
        # FIX: authHeader sin prefijo Bearer - Helius lo envía tal cual
        "authHeader": WEBHOOK_AUTH_TOKEN,
    }

    print("=" * 70)
    print("📡 Registrando webhook TOKEN DETECTION (12 AMMs)")
    print("=" * 70)
    print(f"  URL: {WEBHOOK_URL}")
    print(f"  Tipo: enhanced")
    print(f"  transactionTypes: ['ANY']")
    print(f"  AMMs monitoreados: {len(AMM_ADDRESSES)}")
    for addr in AMM_ADDRESSES:
        print(f"    - {AMM_NAMES[addr]}: {addr[:20]}...")
    print(f"  Endpoint API: api-mainnet.helius-rpc.com")
    print()

    response = requests.post(HELIUS_WEBHOOK_API, json=payload)

    if response.status_code == 200:
        result = response.json()
        webhook_id = result.get("webhookID")
        print(f"✅ Webhook creado exitosamente!")
        print(f"📋 Webhook ID: {webhook_id}")
        print(f"⚠️  Guarda este ID para futuras actualizaciones")
        return webhook_id
    else:
        print(f"❌ Error creando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return None


def create_wallet_tracking_webhook(wallet_addresses: list):
    """
    Crea webhook para rastrear transacciones de wallets específicos.

    FIX: Ahora incluye "TRANSFER" además de "SWAP" para capturar
    transferencias directas de tokens entre wallets.
    """
    if not wallet_addresses:
        print("❌ No hay wallets para rastrear")
        return None

    payload = {
        "webhookURL": WEBHOOK_URL,
        "transactionTypes": ["SWAP", "TRANSFER"],
        "accountAddresses": wallet_addresses,
        "webhookType": "enhanced",
        # FIX: authHeader sin prefijo Bearer
        "authHeader": WEBHOOK_AUTH_TOKEN,
    }

    print("=" * 70)
    print(f"📡 Registrando webhook WALLET TRACKING ({len(wallet_addresses)} wallets)")
    print("=" * 70)
    print(f"  URL: {WEBHOOK_URL}")
    print(f"  transactionTypes: ['SWAP', 'TRANSFER']")
    print(f"  Wallets monitoreados: {len(wallet_addresses)}")
    for i, wallet in enumerate(wallet_addresses[:5], 1):
        print(f"    {i}. {wallet}")
    if len(wallet_addresses) > 5:
        print(f"    ... y {len(wallet_addresses) - 5} más")
    print()

    response = requests.post(HELIUS_WEBHOOK_API, json=payload)

    if response.status_code == 200:
        result = response.json()
        webhook_id = result.get("webhookID")
        print(f"✅ Webhook creado exitosamente!")
        print(f"📋 Webhook ID: {webhook_id}")
        return webhook_id
    else:
        print(f"❌ Error creando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return None


def update_webhook(webhook_id: str, wallet_addresses: list):
    """Actualiza las direcciones de un webhook existente"""
    url = f"https://api-mainnet.helius-rpc.com/v0/webhooks/{webhook_id}?api-key={HELIUS_API_KEY}"

    response = requests.put(url, json={
        "accountAddresses": wallet_addresses,
    })

    if response.status_code == 200:
        print(f"✅ Webhook {webhook_id} actualizado con {len(wallet_addresses)} direcciones")
        return True
    else:
        print(f"❌ Error actualizando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return False


def list_webhooks():
    """Lista todos los webhooks activos"""
    print("=" * 70)
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
            webhook_id = webhook.get("webhookID")
            webhook_url = webhook.get("webhookURL")
            tx_types = webhook.get("transactionTypes", [])
            accounts = webhook.get("accountAddresses", [])
            wh_type = webhook.get("webhookType", "unknown")

            print(f"{i}. Webhook ID: {webhook_id}")
            print(f"   URL: {webhook_url}")
            print(f"   Tipo: {wh_type}")
            print(f"   Transaction Types: {', '.join(tx_types)}")
            print(f"   Accounts: {len(accounts)} monitoreadas")

            # Mostrar AMMs reconocidos
            known = [AMM_NAMES.get(a, "") for a in accounts if a in AMM_NAMES]
            if known:
                print(f"   AMMs: {', '.join(known)}")
            print()

        return webhooks
    else:
        print(f"❌ Error listando webhooks: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return []


def delete_webhook(webhook_id: str):
    """Elimina un webhook por ID"""
    url = f"https://api-mainnet.helius-rpc.com/v0/webhooks/{webhook_id}?api-key={HELIUS_API_KEY}"
    print(f"🗑️  Eliminando webhook {webhook_id}")

    response = requests.delete(url)

    if response.status_code == 200:
        print(f"✅ Webhook eliminado exitosamente")
        return True
    else:
        print(f"❌ Error eliminando webhook: {response.status_code}")
        print(f"   Respuesta: {response.text}")
        return False


def load_tracked_wallets_from_db():
    """Carga wallets activos desde la base de datos"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT wallet_address FROM tracked_wallets WHERE is_active = TRUE LIMIT 100"
        )
        wallets = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        print(f"✓ Cargados {len(wallets)} wallets desde la BD")
        return wallets
    except Exception as e:
        print(f"❌ Error cargando wallets desde DB: {e}")
        return []


# ============================================================
# MENÚ PRINCIPAL
# ============================================================
def main():
    print("=" * 70)
    print("🔧 HELIUS WEBHOOK MANAGER v2")
    print("=" * 70)

    # Validar configuración
    issues = validate_config()
    if issues:
        print("\n⚠️  Problemas de configuración detectados:")
        for issue in issues:
            print(f"   - {issue}")
        print("\n   Corrige el archivo .env antes de continuar.")
        print("   Puedes continuar bajo tu propio riesgo.\n")

    print(f"  API Key: {HELIUS_API_KEY[:8]}...{HELIUS_API_KEY[-4:]}" if len(HELIUS_API_KEY) > 12 else f"  API Key: {HELIUS_API_KEY or '(vacía)'}")
    print(f"  Webhook URL: {WEBHOOK_URL}")
    print(f"  Endpoint: api-mainnet.helius-rpc.com")
    print()
    print("1. Registrar webhook TOKEN DETECTION (12 AMMs)")
    print("2. Registrar webhook WALLET TRACKING (desde DB)")
    print("3. Listar webhooks activos")
    print("4. Eliminar webhook")
    print("5. Registrar ambos (TOKEN + WALLET)")
    print("6. Actualizar webhook existente con nuevos wallets")
    print("0. Salir")

    choice = input("\nElige una opción: ").strip()

    if choice == "1":
        create_token_detection_webhook()

    elif choice == "2":
        wallets = load_tracked_wallets_from_db()
        if wallets:
            create_wallet_tracking_webhook(wallets)

    elif choice == "3":
        list_webhooks()

    elif choice == "4":
        webhooks = list_webhooks()
        if webhooks:
            webhook_id = input("\nIngresa el Webhook ID a eliminar: ").strip()
            delete_webhook(webhook_id)

    elif choice == "5":
        print("\n--- Paso 1: TOKEN DETECTION ---")
        token_wh_id = create_token_detection_webhook()

        print("\n--- Paso 2: WALLET TRACKING ---")
        wallets = load_tracked_wallets_from_db()
        wallet_wh_id = None
        if wallets:
            wallet_wh_id = create_wallet_tracking_webhook(wallets)
        else:
            print("⚠️  No hay wallets en la BD, saltando wallet webhook")

        print("\n" + "=" * 70)
        print("📊 RESULTADO FINAL")
        print("=" * 70)
        if token_wh_id:
            print(f"  TOKEN DETECTION webhook ID: {token_wh_id}")
        if wallet_wh_id:
            print(f"  WALLET TRACKING webhook ID: {wallet_wh_id}")
        print("=" * 70)

    elif choice == "6":
        webhook_id = input("Ingresa el Webhook ID a actualizar: ").strip()
        wallets = load_tracked_wallets_from_db()
        if wallets and webhook_id:
            update_webhook(webhook_id, wallets)

    elif choice == "0":
        print("Saliendo...")
        return

    else:
        print("Opción inválida")


if __name__ == "__main__":
    main()
