"""
parsers/__init__.py
Router de parsers por AMM.
Cada parser expone una función parse(tx, meta, source) -> list[dict]
Cada dict normalizado tiene:
  {
    "signature": str,
    "walletaddress": str,
    "mintaddress": str,
    "side": "buy" | "sell",
    "amounttoken": float,
    "amountsol": float,
    "pricesol": float,
    "time": datetime (tz-aware UTC),
    "slot": int,
    "amm": str,
    "source": str,
  }
"""
from . import pumpfun, pumpswap, raydium


PARSER_REGISTRY = {
    "pumpfun": pumpfun.parse,
    "pumpswap": pumpswap.parse,
    "raydium_amm_v4": raydium.parse,
    "raydium_launchlab": raydium.parse,
}


def get_parser(source_or_program_name: str):
    key = (source_or_program_name or "").strip().lower()
    return PARSER_REGISTRY.get(key)


def supported_sources() -> list:
    return list(PARSER_REGISTRY.keys())