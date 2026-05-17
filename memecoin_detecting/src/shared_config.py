"""
shared_config.py
Configuración compartida. Soporta tanto nombres nuevos (DB_NAME, RPC_HTTP_URL)
como legacy (DBNAME, RPCHTTPURL) para compatibilidad con el código actual.
"""
import os
import sys
import logging
from urllib.parse import urlparse


# ============================================================
# Helpers
# ============================================================
def getenv_any(names, default=None):
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def getenv_bool(names, default=False):
    v = getenv_any(names, None)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def getenv_int(names, default=0):
    v = getenv_any(names, None)
    try:
        return int(v) if v is not None else int(default)
    except Exception:
        return int(default)


def getenv_float(names, default=0.0):
    v = getenv_any(names, None)
    try:
        return float(v) if v is not None else float(default)
    except Exception:
        return float(default)


# ============================================================
# Logging
# ============================================================
LOG_LEVEL = getenv_any(["LOG_LEVEL", "LOGLEVEL"], "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

log = logging.getLogger("shared_config")


def getlogger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# ============================================================
# Database
# ============================================================
DB_NAME = getenv_any(["DB_NAME", "DBNAME"], "memecoins")
DB_USER = getenv_any(["DB_USER", "DBUSER"], "memecoin")
DB_PASSWORD = getenv_any(["DB_PASSWORD", "DBPASSWORD"], "12345")
DB_HOST = getenv_any(["DB_HOST", "DBHOST"], "127.0.0.1")
DB_PORT = getenv_int(["DB_PORT", "DBPORT"], 5432)

DBCONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
}

DB_CONFIG = DBCONFIG  # alias

# ============================================================
# RPC
# ============================================================
RPC_HTTP_URL = getenv_any(["RPC_HTTP_URL", "RPCHTTPURL"], "http://127.0.0.1:8899")
RPC_WS_URL = getenv_any(["RPC_WS_URL", "RPCWSURL"], "ws://127.0.0.1:8900")

RPCHTTPURL = RPC_HTTP_URL
RPCWSURL = RPC_WS_URL

BLOCKED = ["helius", "quicknode", "alchemy", "birdeye", "mainnet-beta.solana.com"]


def assert_local(url: str, name: str) -> None:
    host = (urlparse(url).hostname or "").lower()
    if any(b in host for b in BLOCKED):
        log.error("%s apunta a un proveedor público bloqueado: %s", name, host)
        sys.exit(1)


assert_local(RPC_HTTP_URL, "RPC_HTTP_URL")
assert_local(RPC_WS_URL, "RPC_WS_URL")

# alias legacy
assertlocal = assert_local

# ============================================================
# Timings legacy
# ============================================================
DETECTOR_POLL_INTERVAL = getenv_int(["DETECTOR_POLL_INTERVAL", "DETECTORPOLLINTERVAL"], 15)
METRICS_POLL_INTERVAL = getenv_int(["METRICS_POLL_INTERVAL", "METRICSPOLLINTERVAL"], 30)
TRACKER_POLL_INTERVAL = getenv_int(["TRACKER_POLL_INTERVAL", "TRACKERPOLLINTERVAL"], 20)
CLASSIFIER_INTERVAL = getenv_int(["CLASSIFIER_INTERVAL", "CLASSIFIERINTERVAL"], 1800)

DETECTORPOLLINTERVAL = DETECTOR_POLL_INTERVAL
METRICSPOLLINTERVAL = METRICS_POLL_INTERVAL
TRACKERPOLLINTERVAL = TRACKER_POLL_INTERVAL
CLASSIFIERINTERVAL = CLASSIFIER_INTERVAL

# ============================================================
# Umbrales legacy
# ============================================================
MIN_LIQUIDITY_SOL = getenv_float(["MIN_LIQUIDITY_SOL", "MINLIQUIDITYSOL"], 5)
MAX_TOKEN_AGE_HOURS = getenv_int(["MAX_TOKEN_AGE_HOURS", "MAXTOKENAGEHOURS"], 72)
TRACKER_MAX_WALLETS = getenv_int(["TRACKER_MAX_WALLETS", "TRACKERMAXWALLETS"], 500)
TRACKER_TX_LIMIT = getenv_int(["TRACKER_TX_LIMIT", "TRACKERTXLIMIT"], 25)

MINLIQUIDITYSOL = MIN_LIQUIDITY_SOL
MAXTOKENAGEHOURS = MAX_TOKEN_AGE_HOURS
TRACKERMAXWALLETS = TRACKER_MAX_WALLETS
TRACKERTXLIMIT = TRACKER_TX_LIMIT

# ============================================================
# Realtime flags
# ============================================================
REALTIME_ENABLED = getenv_bool(["REALTIME_ENABLED"], True)
DATASOURCE_MODE = getenv_any(["DATASOURCE_MODE"], "hybrid").lower()
USE_BIRDEYE_FALLBACK = getenv_bool(["USE_BIRDEYE_FALLBACK"], True)

LISTENER_ENABLED = getenv_bool(["LISTENER_ENABLED"], True)
LISTENER_COMMITMENT = getenv_any(["LISTENER_COMMITMENT"], "confirmed")
LISTENER_RECONNECT_MIN_SECONDS = getenv_int(["LISTENER_RECONNECT_MIN_SECONDS"], 1)
LISTENER_RECONNECT_MAX_SECONDS = getenv_int(["LISTENER_RECONNECT_MAX_SECONDS"], 30)
LISTENER_HEARTBEAT_SECONDS = getenv_int(["LISTENER_HEARTBEAT_SECONDS"], 15)

PARSER_BATCH_SIZE = getenv_int(["PARSER_BATCH_SIZE"], 25)
PARSER_POLL_SECONDS = getenv_float(["PARSER_POLL_SECONDS"], 1)
PARSER_WORKERS = getenv_int(["PARSER_WORKERS"], 2)

REDUCER_BATCH_SIZE = getenv_int(["REDUCER_BATCH_SIZE"], 50)
REDUCER_WORKERS = getenv_int(["REDUCER_WORKERS"], 2)

MARKET_HOT_INTERVAL = getenv_int(["MARKET_HOT_INTERVAL"], 15)
MARKET_ACTIVE_INTERVAL = getenv_int(["MARKET_ACTIVE_INTERVAL"], 60)
MARKET_MONITOR_INTERVAL = getenv_int(["MARKET_MONITOR_INTERVAL"], 600)

TTL_WALLET_PNL = getenv_int(["TTL_WALLET_PNL"], 30)
TTL_TOP_TRADERS = getenv_int(["TTL_TOP_TRADERS"], 60)

API_PORT = getenv_int(["API_PORT"], 8200)

# ============================================================
# Catálogo de AMMs
# ============================================================
AMM_CATALOG = {
    "pumpfun":           "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "pumpswap":          "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "raydium_amm_v4":    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "raydium_launchlab": "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
    "fluxbeam":          "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",
    "heaven":            "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o",
    "meteora_dlmm":      "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "meteora_dyn":       "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
    "meteora_dyn2":      "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
    "meteora_dbc":       "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",
    "moonit":            "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",
    "orca":              "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
}


def get_listen_amms() -> dict:
    raw = getenv_any(["LISTEN_AMMS"], "")
    if not raw:
        return dict(AMM_CATALOG)
    wanted = [x.strip().lower() for x in raw.split(",") if x.strip()]
    out = {}
    for name in wanted:
        if name in AMM_CATALOG:
            out[name] = AMM_CATALOG[name]
        else:
            log.warning("AMM desconocido en LISTEN_AMMS: %s", name)
    return out


# ============================================================
# Arranque
# ============================================================
log.info(
    "Config cargada DB=%s@%s:%s/%s RPC_HTTP=%s RPC_WS=%s datasource=%s realtime=%s",
    DB_USER, DB_HOST, DB_PORT, DB_NAME, RPC_HTTP_URL, RPC_WS_URL,
    DATASOURCE_MODE, REALTIME_ENABLED,
)