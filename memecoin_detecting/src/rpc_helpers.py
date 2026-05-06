#!/usr/bin/env python3
"""
rpc_helpers.py — Wrappers mínimos sobre el nodo Agave local.

Cubre las llamadas que usan los workers:
- getHealth
- getSlot
- getAccountInfo
- getMultipleAccounts
- getTokenSupply
- getTokenAccountBalance
- getSignaturesForAddress
- getTransaction
- getProgramAccounts

Todas usan RPC_HTTP_URL. El WS PubSub se maneja en los workers que lo necesitan
(detector / wallet tracker) con 'websockets' + RPC_WS_URL.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from shared_config import RPC_HTTP_URL, RPC_TIMEOUT

logger = logging.getLogger(__name__)

# Metaplex Token Metadata program (para leer nombre/symbol/uri on-chain)
METAPLEX_METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"


class RpcError(Exception):
    pass


def _rpc_call(method: str, params: List[Any], retries: int = 3,
              backoff: float = 0.5) -> Any:
    """
    JSON-RPC POST al nodo local. Reintenta con backoff exponencial.
    Devuelve el campo 'result'. Lanza RpcError en fallo.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = requests.post(
                RPC_HTTP_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=RPC_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RpcError(f"{method} -> {data['error']}")
            return data.get("result")
        except Exception as e:
            last_err = e
            wait = backoff * (2 ** attempt)
            logger.warning(
                "RPC %s fallo (intento %d/%d): %s — reintento en %.1fs",
                method, attempt + 1, retries, e, wait,
            )
            time.sleep(wait)
    raise RpcError(f"{method} agotó {retries} reintentos: {last_err}")


# --------- Helpers puntuales ---------

def get_health() -> str:
    return _rpc_call("getHealth", [])

def get_slot() -> int:
    return int(_rpc_call("getSlot", [{"commitment": "confirmed"}]))

def get_account_info(pubkey: str, encoding: str = "base64") -> Optional[Dict]:
    res = _rpc_call("getAccountInfo", [pubkey, {"encoding": encoding,
                                                "commitment": "confirmed"}])
    return res.get("value") if isinstance(res, dict) else None

def get_multiple_accounts(pubkeys: List[str], encoding: str = "base64"
                          ) -> List[Optional[Dict]]:
    res = _rpc_call("getMultipleAccounts",
                    [pubkeys, {"encoding": encoding, "commitment": "confirmed"}])
    return res.get("value", []) if isinstance(res, dict) else []

def get_token_supply(mint: str) -> Optional[Dict]:
    res = _rpc_call("getTokenSupply", [mint, {"commitment": "confirmed"}])
    return res.get("value") if isinstance(res, dict) else None

def get_token_account_balance(token_account: str) -> Optional[Dict]:
    res = _rpc_call("getTokenAccountBalance",
                    [token_account, {"commitment": "confirmed"}])
    return res.get("value") if isinstance(res, dict) else None

def get_signatures_for_address(address: str, limit: int = 100,
                               before: Optional[str] = None,
                               until: Optional[str] = None) -> List[Dict]:
    params: Dict[str, Any] = {"limit": limit, "commitment": "confirmed"}
    if before:
        params["before"] = before
    if until:
        params["until"] = until
    return _rpc_call("getSignaturesForAddress", [address, params]) or []

def get_transaction(signature: str) -> Optional[Dict]:
    return _rpc_call(
        "getTransaction",
        [signature, {
            "encoding": "jsonParsed",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        }],
    )

def get_program_accounts(program_id: str,
                         filters: Optional[List[Dict]] = None,
                         encoding: str = "base64") -> List[Dict]:
    config: Dict[str, Any] = {"encoding": encoding, "commitment": "confirmed"}
    if filters:
        config["filters"] = filters
    return _rpc_call("getProgramAccounts", [program_id, config]) or []