#!/usr/bin/env python3
"""birdeye_token_sync.py — Sync de new listings + market data de tokens activos."""
import time
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from datetime import datetime
from birdeye_client import BirdeyeClient
from shared_config import DB_CONFIG, TTL

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Cada cuánto purgar history vieja (segundos)
HISTORY_RETENTION_DAYS = 7
HISTORY_CLEANUP_INTERVAL = 3600  # 1h


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
            AND (c.last_updated IS NULL
                OR c.last_updated < NOW() - make_interval(secs => %s))
            ORDER BY
            (c.last_updated IS NULL) ASC,
            c.last_updated ASC NULLS LAST,
            t.detected_at DESC
            LIMIT %s
        """, (TTL["token_market"], batch_size))
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return 0

        mints = [r[1] for r in rows]
        resp = self.client.multi_price(mints)
        prices = (resp or {}).get("data", {}) or {}

        up = self.conn.cursor()
        count = 0
        history_inserts = 0
        for tid, mint in rows:
            p = prices.get(mint) if isinstance(prices, dict) else None
            if not p:
                continue

            price_val = p.get("value")
            liquidity = p.get("liquidity")
            # Birdeye multi_price puede traer volume en distintos campos según versión
            vol_24h = p.get("volume24h") or p.get("volume_24h") or p.get("v24hUSD")

            up.execute("""
                INSERT INTO token_market_cache
                  (token_id, price_usd, liquidity, volume_24h, raw_json, last_updated)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (token_id) DO UPDATE SET
                  price_usd    = EXCLUDED.price_usd,
                  liquidity    = EXCLUDED.liquidity,
                  volume_24h   = COALESCE(EXCLUDED.volume_24h, token_market_cache.volume_24h),
                  raw_json     = EXCLUDED.raw_json,
                  last_updated = NOW()
            """, (tid, price_val, liquidity, vol_24h, Json(p)))
            count += 1

            # Solo guardamos history si hay precio real (evita ruido en pct_change)
            if price_val is not None:
                up.execute("""
                    INSERT INTO token_price_history (token_id, time, price_usd, volume_24h)
                    VALUES (%s, NOW(), %s, %s)
                """, (tid, price_val, vol_24h))
                history_inserts += 1

        self.conn.commit()
        up.close()
        logger.info(f"💰 Market data refrescada en {count} tokens "
                    f"(history +{history_inserts})")
        return count

    def cleanup_old_history(self):
        """Elimina snapshots más viejos que HISTORY_RETENTION_DAYS para evitar
        que token_price_history crezca sin tope."""
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM token_price_history
            WHERE time < NOW() - make_interval(days => %s)
        """, (HISTORY_RETENTION_DAYS,))
        deleted = cur.rowcount
        self.conn.commit()
        cur.close()
        if deleted:
            logger.info(f"🧹 Purgadas {deleted} filas viejas de token_price_history")
        return deleted

    def run(self):
        last_listings = 0
        last_market = 0
        last_cleanup = 0
        logger.info("TokenSyncer corriendo (new_listings + market_data + history)")
        while True:
            try:
                now = time.time()
                if now - last_listings >= TTL["new_listings"]:
                    self.discover_new_listings()
                    last_listings = now
                if now - last_market >= TTL["token_market"]:
                    self.refresh_market_data(batch_size=50)
                    last_market = now
                if now - last_cleanup >= HISTORY_CLEANUP_INTERVAL:
                    self.cleanup_old_history()
                    last_cleanup = now
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error loop: {e}")
                try: self.conn.rollback()
                except: pass
                time.sleep(10)


if __name__ == "__main__":
    TokenSyncer().run()