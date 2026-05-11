"""
parsers/pumpfun.py
Parser de swaps de Pump.fun.

Estrategia:
  1. Trader = primer signer (accountKeys[0]).
  2. Detectar si la tx tocó el program Pump.fun (ya viene filtrado por staging, pero doble check).
  3. Calcular delta de token del trader usando meta.pre/postTokenBalances.
  4. Calcular delta de SOL del trader usando meta.preBalances / postBalances (restando fee).
  5. Determinar side: token_delta > 0 => buy, < 0 => sell.
  6. Precio = reservas de la bonding curve (virtual) después del swap, si están en la cuenta; fallback: amountsol / amounttoken.
  7. Solo emitir si mint existe en tokens.
"""
from datetime import datetime, timezone

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000


def _account_keys(tx):
    msg = tx.get("transaction", {}).get("message", {})
    keys = msg.get("accountKeys", []) or []
    if not keys:
        return []
    # jsonParsed => cada key es dict con 'pubkey'
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
    # Inner ixs
    meta = tx.get("meta") or {}
    for inner in meta.get("innerInstructions", []) or []:
        for ix in inner.get("instructions", []) or []:
            pid = ix.get("programId") or ix.get("program_id")
            if pid:
                out.add(pid)
    return out


def _token_delta_for_owner(meta, owner, exclude_mint=SOL_MINT):
    """
    Retorna (mint, delta_ui_amount) para el mint cuyo dueño = owner,
    excluyendo WSOL. Toma el mayor delta absoluto (el relevante del swap).
    """
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
    """
    Delta de SOL (en SOL, no lamports) del owner entre pre y post.
    Se corrige sumando el fee para que el delta refleje solo el swap.
    """
    keys = _account_keys(tx)
    try:
        idx = keys.index(owner)
    except ValueError:
        return 0.0

    pre = (meta.get("preBalances") or [0] * (idx + 1))[idx]
    post = (meta.get("postBalances") or [0] * (idx + 1))[idx]
    fee = meta.get("fee") or 0

    # delta_lamports (negativo si gastó SOL, positivo si recibió)
    delta_lamports = post - pre + fee
    return delta_lamports / LAMPORTS_PER_SOL


def _price_from_bonding_curve(meta, mint):
    """
    Intenta extraer el precio post-swap desde los balances de la bonding curve.
    En Pump.fun, la cuenta de la bonding curve es ATA del program. El precio
    aproximado = sol_reserve / token_reserve (virtuales).
    Si no encontramos ambas, retornamos None para que el caller use el precio efectivo.
    """
    # Buscamos el owner que NO es el trader y que tiene tanto WSOL como el mint del memecoin
    # Nota: en Pump.fun real, la curva mantiene lamports nativos + tokens.
    # Esta es una aproximación robusta: buscamos el mayor post balance del mint
    # y asumimos la cuenta asociada como bonding curve.
    post_token_balances = meta.get("postTokenBalances") or []
    max_token_balance = 0.0
    curve_owner = None
    for b in post_token_balances:
        if b.get("mint") != mint:
            continue
        amt = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        if amt > max_token_balance:
            max_token_balance = amt
            curve_owner = b.get("owner")

    if not curve_owner or max_token_balance <= 0:
        return None

    # Lamports del owner de la curva en postBalances
    # (si el owner aparece en accountKeys; a veces la curva es PDA propia)
    # Como fallback: lo dejamos None y el caller usa amountsol/amounttoken.
    return None  # la aproximación real requiere leer la cuenta de la curva; Fase 2b


def parse(tx, source: str, known_mints: set) -> list:
    """
    tx: respuesta JSON de getTransaction (jsonParsed, maxSupportedTransactionVersion=0)
    source: 'pumpfun'
    known_mints: set de mintaddress que sí están en la tabla tokens
    return: lista de swaps normalizados (normalmente 0 o 1)
    """
    if not tx or tx.get("meta", {}).get("err") is not None:
        return []

    # Doble check: el program de Pump.fun aparece
    program_ids = _program_ids(tx)
    if PUMPFUN_PROGRAM not in program_ids:
        return []

    trader = _first_signer(tx)
    if not trader:
        return []

    meta = tx.get("meta") or {}
    block_time = tx.get("blockTime")
    slot = tx.get("slot")

    if block_time:
        ts = datetime.fromtimestamp(block_time, tz=timezone.utc)
    else:
        ts = datetime.now(timezone.utc)

    mint, token_delta = _token_delta_for_owner(meta, trader)
    if not mint or token_delta == 0:
        return []

    # Filtrado: solo mints conocidos
    if mint not in known_mints:
        return []

    sol_delta = _sol_delta_for_owner(tx, meta, trader)

    # side
    side = "buy" if token_delta > 0 else "sell"
    amount_token = abs(token_delta)
    amount_sol = abs(sol_delta)

    # Precio
    price_bc = _price_from_bonding_curve(meta, mint)
    if price_bc and price_bc > 0:
        price_sol = price_bc
    elif amount_token > 0:
        price_sol = amount_sol / amount_token
    else:
        price_sol = 0.0

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