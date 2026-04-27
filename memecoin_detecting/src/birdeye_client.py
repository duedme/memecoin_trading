"""birdeye_client.py — Cliente HTTP con rate limiting dual (general + wallet) y kill switch por CUs."""
import time
import logging
import threading
from datetime import date
from collections import deque
import requests
import psycopg2
from shared_config import BIRDEYE, CU_COST, WALLET_ENDPOINT_PREFIXES, DB_CONFIG

logger = logging.getLogger(__name__)


class RateLimiter:
    """Ventana deslizante: máximo N requests en M segundos."""
    def __init__(self, max_requests: int, window_sec: int):
        self.max = max_requests
        self.window = window_sec
        self.events = deque()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                while self.events and self.events[0] < now - self.window:
                    self.events.popleft()
                if len(self.events) < self.max:
                    self.events.append(now)
                    return
                wait = self.window - (now - self.events[0]) + 0.05
            time.sleep(max(0.05, wait))


class BirdeyeClient:
    def __init__(self):
        self.base = BIRDEYE["base_url"]
        self.headers = {
            "X-API-KEY": BIRDEYE["api_key"],
            "accept": "application/json",
            "x-chain": BIRDEYE["chain"],
        }
        # Limitador general (rps del plan, con margen)
        self.general_limiter = RateLimiter(BIRDEYE["general_rps"], 1)
        # Limitador específico Wallet API (30 rpm oficial → usamos 25)
        self.wallet_limiter = RateLimiter(BIRDEYE["wallet_rpm"], 60)
        self.budget = BIRDEYE["daily_cu_budget"]
        self.lock = threading.Lock()
        self._ensure_usage_row()

    def _ensure_usage_row(self):
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("INSERT INTO birdeye_usage (day) VALUES (CURRENT_DATE) ON CONFLICT DO NOTHING")
        conn.commit()
        cur.close()
        conn.close()

    def _cu_cost(self, path: str) -> int:
        for key, cost in CU_COST.items():
            if path.startswith(key):
                return cost
        return 10  # default conservador

    def _is_wallet_endpoint(self, path: str) -> bool:
        return any(path.startswith(p) for p in WALLET_ENDPOINT_PREFIXES)

    def _check_budget_and_count(self, path: str):
        cost = self._cu_cost(path)
        is_wallet = self._is_wallet_endpoint(path)
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT cu_consumed FROM birdeye_usage WHERE day = CURRENT_DATE")
        row = cur.fetchone()
        today_cu = row[0] if row else 0
        if today_cu + cost > self.budget:
            cur.close(); conn.close()
            raise RuntimeError(f"KILL SWITCH: CU diario {today_cu}+{cost} > budget {self.budget}")
        cur.execute("""
            INSERT INTO birdeye_usage (day, cu_consumed, requests_total, wallet_requests)
            VALUES (CURRENT_DATE, %s, 1, %s)
            ON CONFLICT (day) DO UPDATE SET
                cu_consumed = birdeye_usage.cu_consumed + EXCLUDED.cu_consumed,
                requests_total = birdeye_usage.requests_total + 1,
                wallet_requests = birdeye_usage.wallet_requests + EXCLUDED.wallet_requests,
                last_updated = NOW()
        """, (cost, 1 if is_wallet else 0))
        conn.commit()
        cur.close(); conn.close()

    def get(self, path: str, params: dict = None, max_retries: int = 3):
        self.general_limiter.acquire()
        if self._is_wallet_endpoint(path):
            self.wallet_limiter.acquire()
        self._check_budget_and_count(path)

        url = f"{self.base}{path}"
        for attempt in range(max_retries):
            try:
                r = requests.get(url, headers=self.headers, params=params, timeout=20)
                if r.status_code == 429:
                    wait = 2 ** attempt + 2
                    logger.warning(f"429 en {path}, esperando {wait}s")
                    time.sleep(wait)
                    continue
                if r.status_code >= 500:
                    time.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                logger.warning(f"Error {path}: {e}, intento {attempt+1}")
                time.sleep(1 + attempt)
        return None

    # ---- Endpoints de conveniencia ----
    def new_listings(self, limit=20, meme_platform=True):
        return self.get("/defi/v2/tokens/new_listing",
                        {"limit": limit, "meme_platform_enabled": str(meme_platform).lower()})

    def token_overview(self, mint: str):
        return self.get("/defi/token_overview", {"address": mint})

    def multi_price(self, mints: list):
        return self.get("/defi/multi_price", {"list_address": ",".join(mints[:50])})

    def top_traders(self, mint: str, timeframe="24h", limit=10):
        return self.get("/defi/v2/tokens/top_traders",
                        {"address": mint, "time_frame": timeframe,
                         "sort_type": "desc", "sort_by": "volume", "limit": limit})

    def wallet_pnl_summary(self, wallet: str):
        return self.get("/wallet/v2/pnl/summary", {"wallet": wallet})

    def wallet_pnl(self, wallet: str):
        return self.get("/wallet/v2/pnl", {"wallet": wallet})