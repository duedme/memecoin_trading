#!/usr/bin/env python3
"""birdeye_token_sync.py — Sync de new listings + market data de tokens activos."""
import time
import logging
import psycopg2
from datetime import datetime
from birdeye_client import BirdeyeClient
from shared_config import DB_CONFIG, TTL

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TokenSyncer:
    def __init__(self):
        self.client = BirdeyeClient()
        self.conn = psycopg2.connect(**DB_CONFIG)

    def discover_new_listings(self):
        data = self.client.new_listings(limit=20, meme_platform=True)
        if not data or "data" not in data:
            return 0
        items = data["data"].get("items", []) if isinstance(data["data"], dict) else data["data"]
        cur = self.conn.cursor()
        inserted = 0
        for t in items or []:
            mint = t.get("address")
            if not mint:
                continue
            cur.execute("""
                INSERT INTO tokens (mint_address, name, symbol, decimals, liquidity, source, detected_at)
                VALUES (%s, %s, %s, %s, %s, 'birdeye_new_listing', NOW())
                ON CONFLICT (mint_address) DO NOTHING
            """, (mint, t.get("name"), t.get("symbol"),
                  t.get("decimals", 9), t.get("liquidity")))
            if cur.rowcount:
                inserted += 1
        self.conn.commit()
        cur.close()
        if inserted:
            logger.info(f"🆕 {inserted} tokens nuevos detectados")
        return inserted

    def refresh_market_data(self, batch_size=50):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT t.token_id, t.mint_address
            FROM tokens t
            LEFT JOIN token_market_cache c ON c.token_id = t.token_id
            WHERE t.status = 'active'
              AND (c.last_updated IS NULL OR c.last_updated < NOW() - INTERVAL '%s seconds')
            ORDER BY c.last_updated NULLS FIRST
            LIMIT %s
        """, (TTL["token_market"], batch_size))
        rows = cur.fetchall()
        if not rows:
            cur.close()
            return 0

        mints = [r[1] for r in rows]
        resp = self.client.multi_price(mints)
        prices = (resp or {}).get("data", {}) or {}

        up = self.conn.cursor()
        count = 0
        for tid, mint in rows:
            p = prices.get(mint) if isinstance(prices, dict) else None
            if not p:
                continue
            up.execute("""
                INSERT INTO token_market_cache
                  (token_id, price_usd, liquidity, raw_json, last_updated)
                VALUES (%s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (token_id) DO UPDATE SET
                  price_usd = EXCLUDED.price_usd,
                  liquidity = EXCLUDED.liquidity,
                  raw_json = EXCLUDED.raw_json,
                  last_updated = NOW()
            """, (tid, p.get("value"), p.get("liquidity"),
                  psycopg2.extras.Json(p) if hasattr(psycopg2.extras, "Json") else None))
            up.execute("""
                INSERT INTO token_price_history (token_id, price_usd)
                VALUES (%s, %s)
            """, (tid, p.get("value")))
            count += 1
        self.conn.commit()
        up.close(); cur.close()
        logger.info(f"💰 Market data refrescada en {count} tokens")
        return count

    def run(self):
        last_listings = 0
        last_market = 0
        logger.info("TokenSyncer corriendo (new_listings + market_data)")
        while True:
            try:
                now = time.time()
                if now - last_listings >= TTL["new_listings"]:
                    self.discover_new_listings()
                    last_listings = now
                if now - last_market >= TTL["token_market"]:
                    self.refresh_market_data(batch_size=50)
                    last_market = now
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error loop: {e}")
                try: self.conn.rollback()
                except: pass
                time.sleep(10)


if __name__ == "__main__":
    import psycopg2.extras  # para Json
    TokenSyncer().run()