#!/usr/bin/env python3
"""
shared_config.py - Configuración centralizada del proyecto Memecoins
Todas las constantes, credenciales y AMMs en un solo lugar.
Importar desde aquí para evitar duplicación y credenciales hardcodeadas.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# BASE DE DATOS
# ============================================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ============================================================
# RPC
# ============================================================
LOCAL_RPC_URL = os.getenv("LOCAL_RPC_URL", "http://127.0.0.1:7211")

# ============================================================
# HELIUS
# ============================================================
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
HELIUS_WEBHOOK_API = f"https://api-mainnet.helius-rpc.com/v0/webhooks?api-key={HELIUS_API_KEY}"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# ============================================================
# WEBHOOK SERVER
# ============================================================
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://tu-dominio.com/webhook")
WEBHOOK_AUTH_TOKEN = os.getenv("WEBHOOK_AUTH_TOKEN", "")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", 8765))

# ============================================================
# 12 AMM PROGRAM IDs
# ============================================================
AMM_PROGRAMS = {
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

AMM_ADDRESSES = list(AMM_PROGRAMS.keys())

# ============================================================
# TOKEN BLACKLIST (no son memecoins)
# ============================================================
KNOWN_TOKEN_BLACKLIST = {
    "So11111111111111111111111111111111111111112",      # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",   # USDT
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",   # RAY
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",    # mSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",   # stSOL
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",   # jitoSOL
}

# ============================================================
# SYSTEM PROGRAM IDs (skip en parsing)
# ============================================================
SKIP_PROGRAMS = {
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
}
