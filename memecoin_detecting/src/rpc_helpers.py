import time
import requests
from shared_config import RPC_HTTP_URL, get_logger

log = get_logger("rpc_helpers")

def rpc_call(method: str, params=None, timeout: float = 10.0, retries: int = 3):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    last_err = None
    for i in range(retries):
        try:
            r = requests.post(RPC_HTTP_URL, json=payload, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                last_err = data["error"]
                log.warning("RPC %s error: %s", method, data["error"])
                time.sleep(0.5 * (i + 1))
                continue
            return data.get("result")
        except Exception as e:
            last_err = str(e)
            log.warning("RPC %s fallo (%d/%d): %s", method, i + 1, retries, e)
            time.sleep(0.5 * (i + 1))
    log.error("RPC %s agotó reintentos: %s", method, last_err)
    return None

def get_slot():
    return rpc_call("getSlot")

def get_signatures_for_address(address: str, limit: int = 25, before: str = None):
    params = [address, {"limit": limit}]
    if before:
        params[1]["before"] = before
    return rpc_call("getSignaturesForAddress", params) or []

def get_transaction(signature: str):
    return rpc_call(
        "getTransaction",
        [signature, {"maxSupportedTransactionVersion": 0, "encoding": "jsonParsed"}],
    )