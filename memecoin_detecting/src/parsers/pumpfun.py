"""
parsers/pumpfun.py
Parser de swaps de Pump.fun con precio desde bonding curve (Fase 2b).
"""
from datetime import datetime, timezone

from parsers.pumpfun_curve import get_price_sol as curve_price_sol

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000


def _account_keys(tx):
    msg = tx.get("transaction", {}).get("message", {})
    keys = msg.get("accountKeys", []) or []
    if not keys:
        return []
    if isinstance(keys[0], dict):
        return [k.get("pubkey") for k in keys]
    return list(keys)


def _first_signer(tx):
    keys = _account_keys(tx)
    return keys[0] if keys else None


def _program_ids(tx):
    msg = tx.get("transaction", {}).get("message", {})
    instructions = msg.get("instructions", []) or []
    out = set()
    for ix in instructions:
        pid = ix.get("programId") or ix.get("program_id")
        if pid:
            out.add(pid)
    meta = tx.get("meta") or {}
    for inner in meta.get("innerInstructions", []) or []:
        for ix in inner.get("instructions", []) or []:
            pid = ix.get("programId") or ix.get("program_id")
            if pid:
                out.add(pid)
    return out


def _token_delta_for_owner(meta, owner, exclude_mint=SOL_MINT):
    pre = {}
    for b in meta.get("preTokenBalances", []) or []:
        if b.get("owner") != owner:
            continue
        mint = b.get("mint")
        if mint == exclude_mint:
            continue
        amt = b.get("uiTokenAmount", {}).get("uiAmount") or 0
        pre[mint] = float(amt or 0)

    post = {}
    for b in meta.get("postTokenBalances", []) or []:
        if b.get("owner") != owner:
            continue
        mint = b.get("mint")
        if mint == exclude_mint:
            continue
        amt = b.get("uiTokenAmount", {}).get("uiAmount") or 0
        post[mint] = float(amt or 0)

    best_mint, best_delta = None, 0.0
    mints = set(pre.keys()) | set(post.keys())
    for m in mints:
        d = post.get(m, 0.0) - pre.get(m, 0.0)
        if abs(d) > abs(best_delta):
            best_mint, best_delta = m, d
    return best_mint, best_delta


def _sol_delta_for_owner(tx, meta, owner):
    keys = _account_keys(tx)
    try:
        idx = keys.index(owner)
    except ValueError:
        return 0.0

    pre_list = meta.get("preBalances") or []
    post_list = meta.get("postBalances") or []
    if idx >= len(pre_list) or idx >= len(post_list):
        return 0.0

    pre = pre_list[idx]
    post = post_list[idx]
    fee = meta.get("fee") or 0

    delta_lamports = post - pre + fee
    return delta_lamports / LAMPORTS_PER_SOL


def parse(tx, source: str, known_mints: set) -> list:
    if not tx or (tx.get("meta") or {}).get("err") is not None:
        return []

    program_ids = _program_ids(tx)
    if PUMPFUN_PROGRAM not in program_ids:
        return []

    trader = _first_signer(tx)
    if not trader:
        return []

    meta = tx.get("meta") or {}
    block_time = tx.get("blockTime")
    slot = tx.get("slot")

    ts = (datetime.fromtimestamp(block_time, tz=timezone.utc)
          if block_time else datetime.now(timezone.utc))

    mint, token_delta = _token_delta_for_owner(meta, trader)
    if not mint or token_delta == 0:
        return []

    if mint not in known_mints:
        return []

    sol_delta = _sol_delta_for_owner(tx, meta, trader)

    side = "buy" if token_delta > 0 else "sell"
    amount_token = abs(token_delta)
    amount_sol = abs(sol_delta)

    # 1) Precio exacto desde bonding curve (Fase 2b)
    price_sol = curve_price_sol(mint)

    # 2) Fallback: precio efectivo del trade
    if not price_sol or price_sol <= 0:
        price_sol = (amount_sol / amount_token) if amount_token > 0 else 0.0

    signature = None
    sigs = tx.get("transaction", {}).get("signatures") or []
    if sigs:
        signature = sigs[0]

    return [{
        "signature": signature,
        "walletaddress": trader,
        "mintaddress": mint,
        "side": side,
        "amounttoken": amount_token,
        "amountsol": amount_sol,
        "pricesol": price_sol,
        "time": ts,
        "slot": slot,
        "amm": "pumpfun",
        "source": source or "pumpfun",
    }]