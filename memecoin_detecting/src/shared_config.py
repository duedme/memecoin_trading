import os
import sys
import logging
from urllib.parse import urlparse

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("shared_config")

DB_CONFIG = {
    "dbname":   os.getenv("DB_NAME", "memecoins"),
    "user":     os.getenv("DB_USER", "memecoin"),
    "password": os.getenv("DB_PASSWORD", "changeme"),
    "host":     os.getenv("DB_HOST", "db"),
    "port":     int(os.getenv("DB_PORT", "5432")),
}

RPC_HTTP_URL = os.getenv("RPC_HTTP_URL", "http://host.docker.internal:7211")
RPC_WS_URL   = os.getenv("RPC_WS_URL",   "ws://host.docker.internal:7212")

_BLOCKED = ("helius", "quicknode", "alchemy", "birdeye", "mainnet-beta.solana.com")

def _assert_local(url: str, name: str) -> None:
    host = (urlparse(url).hostname or "").lower()
    if any(b in host for b in _BLOCKED):
        log.error("%s apunta a un proveedor público (%s). Este stack es local-only.", name, host)
        sys.exit(1)

_assert_local(RPC_HTTP_URL, "RPC_HTTP_URL")
_assert_local(RPC_WS_URL,   "RPC_WS_URL")

DETECTOR_POLL_INTERVAL = int(os.getenv("DETECTOR_POLL_INTERVAL", "15"))
METRICS_POLL_INTERVAL  = int(os.getenv("METRICS_POLL_INTERVAL",  "30"))
TRACKER_POLL_INTERVAL  = int(os.getenv("TRACKER_POLL_INTERVAL",  "20"))
CLASSIFIER_INTERVAL    = int(os.getenv("CLASSIFIER_INTERVAL",    "1800"))

MIN_LIQUIDITY_SOL     = float(os.getenv("MIN_LIQUIDITY_SOL", "5"))
MAX_TOKEN_AGE_HOURS   = int(os.getenv("MAX_TOKEN_AGE_HOURS", "72"))
TRACKER_MAX_WALLETS   = int(os.getenv("TRACKER_MAX_WALLETS", "500"))
TRACKER_TX_LIMIT      = int(os.getenv("TRACKER_TX_LIMIT", "25"))

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)