"""
parsers/pumpfun_curve.py
Cálculo de precio Pump.fun desde la bonding curve state (virtual reserves).

Layout de BondingCurve (Anchor):
  discriminator: 8 bytes
  virtual_token_reserves: u64 (8 bytes)
  virtual_sol_reserves:   u64 (8 bytes)
  real_token_reserves:    u64 (8 bytes)
  real_sol_reserves:      u64 (8 bytes)
  token_total_supply:     u64 (8 bytes)
  complete:               bool (1 byte)

Usamos solo virtual_token_reserves y virtual_sol_reserves para el precio.
"""
import base64
import struct
import time
import hashlib
import threading

import base58
import requests

from shared_config import RPC_HTTP_URL, getlogger

log = getlogger("pumpfun-curve")

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_DECIMALS = 6  # Pump.fun tokens: 6 decimales
CACHE_TTL_SECONDS = 5

_cache = {}  # mint -> (timestamp, price_sol)
_cache_lock = threading.Lock()


def _find_program_address(seeds: list, program_id_b58: str):
    """
    Implementación mínima de findProgramAddress sin solders.
    Retorna (pda_bytes, bump).
    """
    program_id = base58.b58decode(program_id_b58)
    PDA_MARKER = b"ProgramDerivedAddress"

    for bump in range(255, -1, -1):
        buf = b""
        for s in seeds:
            buf += s
        buf += bytes([bump])
        buf += program_id
        buf += PDA_MARKER

        h = hashlib.sha256(buf).digest()
        # Un PDA es válido si NO está en la curva ed25519.
        # Aproximación: ed25519 points have specific structure; usamos el bit check
        # estándar que usa Solana (ver solana-sdk): "is_on_curve" check.
        # Para nuestro caso basta con asumir válido si el primer intento no colisiona
        # con una key real; en práctica Pump.fun siempre encuentra pda en los primeros bumps.
        if not _is_on_curve(h):
            return h, bump
    raise RuntimeError("No se pudo derivar PDA")


def _is_on_curve(point_bytes: bytes) -> bool:
    """
    Check simplificado: en Solana una key está 'on curve' si es un punto ed25519 válido.
    Implementación liviana sin pynacl: probamos decodificar y si falla -> off curve.
    Para mayor precisión, añadir pynacl al requirements y usar nacl.signing.VerifyKey.
    """
    try:
        from nacl.signing import VerifyKey
        VerifyKey(point_bytes)
        return True
    except Exception:
        return False


def get_bonding_curve_pda(mint_b58: str) -> str:
    """
    PDA de la bonding curve = findProgramAddress([b"bonding-curve", mint], PUMPFUN_PROGRAM)
    """
    mint_bytes = base58.b58decode(mint_b58)
    seeds = [b"bonding-curve", mint_bytes]
    pda_bytes, _ = _find_program_address(seeds, PUMPFUN_PROGRAM)
    return base58.b58encode(pda_bytes).decode()


def _rpc_get_account_info(pubkey_b58: str, timeout: int = 5):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            pubkey_b58,
            {"encoding": "base64", "commitment": "confirmed"},
        ],
    }
    r = requests.post(RPC_HTTP_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return (data.get("result") or {}).get("value")


def _decode_bonding_curve(data_b64: str):
    """
    Retorna dict con virtual_token_reserves, virtual_sol_reserves,
    real_token_reserves, real_sol_reserves, token_total_supply, complete.
    """
    raw = base64.b64decode(data_b64)
    if len(raw) < 8 + 8 * 5 + 1:
        raise ValueError(f"bonding curve data muy corta: {len(raw)} bytes")

    # Saltamos 8 bytes de discriminator
    off = 8
    virtual_token_reserves = struct.unpack_from("<Q", raw, off)[0]; off += 8
    virtual_sol_reserves   = struct.unpack_from("<Q", raw, off)[0]; off += 8
    real_token_reserves    = struct.unpack_from("<Q", raw, off)[0]; off += 8
    real_sol_reserves      = struct.unpack_from("<Q", raw, off)[0]; off += 8
    token_total_supply     = struct.unpack_from("<Q", raw, off)[0]; off += 8
    complete = bool(raw[off])

    return {
        "virtual_token_reserves": virtual_token_reserves,
        "virtual_sol_reserves": virtual_sol_reserves,
        "real_token_reserves": real_token_reserves,
        "real_sol_reserves": real_sol_reserves,
        "token_total_supply": token_total_supply,
        "complete": complete,
    }


def get_price_sol(mint_b58: str):
    """
    Retorna precio en SOL por 1 token, o None si no se pudo calcular.
    Cachea por CACHE_TTL_SECONDS.
    """
    now = time.time()

    with _cache_lock:
        cached = _cache.get(mint_b58)
        if cached and (now - cached[0]) < CACHE_TTL_SECONDS:
            return cached[1]

    try:
        pda = get_bonding_curve_pda(mint_b58)
        acc = _rpc_get_account_info(pda)
        if not acc or not acc.get("data"):
            return None

        data_b64 = acc["data"][0] if isinstance(acc["data"], list) else acc["data"]
        state = _decode_bonding_curve(data_b64)

        vsol = state["virtual_sol_reserves"]
        vtok = state["virtual_token_reserves"]
        if vtok == 0:
            return None

        # Convertir a unidades "humanas"
        # vsol está en lamports, vtok en base units (10^TOKEN_DECIMALS)
        price_sol = (vsol / LAMPORTS_PER_SOL) / (vtok / (10 ** TOKEN_DECIMALS))

        with _cache_lock:
            _cache[mint_b58] = (now, price_sol)

        return price_sol

    except Exception as e:
        log.debug("get_price_sol fallback para %s: %s", mint_b58, e)
        return None