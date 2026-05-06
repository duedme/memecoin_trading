#!/usr/bin/env python3
"""
shared_config.py — Configuración central 100% local.

- DB: Postgres/Timescale interna.
- RPC: SOLO nodo Agave local (RPC_HTTP_URL / RPC_WS_URL).
- No se permiten endpoints públicos (helius, quicknode, alchemy, etc.).
"""

import os
import sys

# -------------------------------------------------------------
# Database
# -------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "memecoins_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# -------------------------------------------------------------
# RPC local
# -------------------------------------------------------------
RPC_HTTP_URL = os.getenv("RPC_HTTP_URL", "http://host.docker.internal:7211")
RPC_WS_URL   = os.getenv("RPC_WS_URL",   "ws://host.docker.internal:7212")
RPC_TIMEOUT  = int(os.getenv("RPC_TIMEOUT", "30"))

# Lista negra: si detectamos un host público, cortamos.
_FORBIDDEN_HOSTS = (
    "api.mainnet-beta.solana.com",
    "mainnet.helius-rpc.com",
    "rpc.helius.xyz",
    "api.helius.xyz",
    "quicknode.pro",
    "quiknode.pro",
    "alchemy.com",
    "ankr.com",
    "genesysgo.net",
    "syndica.io",
    "triton.one",
    "rpcpool.com",
    "birdeye.so",
)

def _assert_local(url: str, name: str) -> None:
    low = url.lower()
    for bad in _FORBIDDEN_HOSTS:
        if bad in low:
            print(
                f"[shared_config] {name} apunta a un host externo: {url}. "
                f"Este deployment es 100% local. Aborto.",
                file=sys.stderr,
            )
            sys.exit(1)

_assert_local(RPC_HTTP_URL, "RPC_HTTP_URL")
_assert_local(RPC_WS_URL,   "RPC_WS_URL")

# -------------------------------------------------------------
# Clasificador
# -------------------------------------------------------------
CLASSIFIER_WINDOW_DAYS = int(os.getenv("CLASSIFIER_WINDOW_DAYS", "30"))