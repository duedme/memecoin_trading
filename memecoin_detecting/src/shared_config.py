"""shared_config.py — Config centralizada post-migración a Birdeye."""
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "memecoins_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

BIRDEYE = {
    "api_key": os.getenv("BIRDEYE_API_KEY", ""),
    "base_url": os.getenv("BIRDEYE_BASE_URL", "https://public-api.birdeye.so"),
    "chain": os.getenv("BIRDEYE_CHAIN", "solana"),
    "general_rps": int(os.getenv("BIRDEYE_GENERAL_RPS", 40)),
    "wallet_rpm": int(os.getenv("BIRDEYE_WALLET_RPM", 25)),
    "daily_cu_budget": int(os.getenv("BIRDEYE_DAILY_CU_BUDGET", 600_000)),
}

TTL = {
    "token_price": int(os.getenv("TTL_TOKEN_PRICE", 30)),
    "token_market": int(os.getenv("TTL_TOKEN_MARKET", 60)),
    "top_traders": int(os.getenv("TTL_TOP_TRADERS", 600)),
    "wallet_pnl": int(os.getenv("TTL_WALLET_PNL", 1800)),
    "new_listings": int(os.getenv("TTL_NEW_LISTINGS", 60)),
}

# Costos CU conocidos (referencia, puede cambiar). Sirven para el kill switch.
CU_COST = {
    "/defi/price": 3,
    "/defi/multi_price": 30,
    "/defi/token_overview": 30,
    "/defi/v3/token/market-data": 12,
    "/defi/v2/tokens/new_listing": 30,
    "/defi/v2/tokens/top_traders": 30,
    "/defi/v3/token/holder": 50,
    "/wallet/v2/pnl": 80,
    "/wallet/v2/pnl/summary": 80,
    "/v1/wallet/token_list": 80,
    "/v1/wallet/tx_list": 50,
}

WALLET_ENDPOINT_PREFIXES = ("/wallet/", "/v1/wallet/")