#!/usr/bin/env python3
"""birdeye_top_traders_sync.py — Refresca top traders por token y encola wallets para PnL."""
import time
import logging
import psycopg2
from birdeye_client import BirdeyeClient
from shared_config import DB_CONFIG, TTL

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TopTradersSyncer:
    def __init__(self):
        self.client = BirdeyeClient()
        self.conn = psycopg2.connect(**DB_CONFIG)

    def pick_tokens(self, limit=25):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT t.token_id, t.mint_address
            FROM tokens t
            LEFT JOIN token_market_cache c ON c.token_id = t.token_id
            WHERE t.status = 'active'
              AND t.detected_at > NOW() - INTERVAL '7 days'
            ORDER BY c.volume_24h DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return rows

    def refresh_token(self, token_id, mint):
        resp = self.client.top_traders(mint, timeframe="24h", limit=10)
        if not resp or "data" not in resp:
            return 0
        items = resp["data"].get("items", []) if isinstance(resp["data"], dict) else resp["data"]
        cur = self.conn.cursor()
        count = 0
        for i, tr in enumerate(items or [], start=1):
            wallet = tr.get("owner") or tr.get("address")
            if not wallet:
                continue
            cur.execute("""
                INSERT INTO token_top_traders_cache
                  (token_id, wallet_address, rank, volume_usd, total_pnl, trade_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (token_id, wallet_address) DO UPDATE SET
                  rank = EXCLUDED.rank,
                  volume_usd = EXCLUDED.volume_usd,
                  total_pnl = EXCLUDED.total_pnl,
                  trade_count = EXCLUDED.trade_count,
                  last_updated = NOW()
            """, (token_id, wallet, i, tr.get("volume"),
                  tr.get("totalProfit") or tr.get("pnl"),
                  tr.get("trade") or tr.get("trade_count")))
            # auto-enqueue de esa wallet para pedir PnL detallado después
            cur.execute("""
                INSERT INTO wallet_sync_queue (wallet_address, priority, next_sync_at)
                VALUES (%s, 3, NOW())
                ON CONFLICT (wallet_address) DO NOTHING
            """, (wallet,))
            cur.execute("""
                INSERT INTO wallets (wallet_address) VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (wallet,))
            count += 1
        self.conn.commit()
        cur.close()
        return count

    def run(self):
        logger.info("TopTradersSyncer corriendo")
        while True:
            try:
                tokens = self.pick_tokens(limit=25)
                if not tokens:
                    time.sleep(60); continue
                total = 0
                for tid, mint in tokens:
                    total += self.refresh_token(tid, mint)
                logger.info(f"🏆 top_traders actualizados en {len(tokens)} tokens ({total} filas)")
                time.sleep(TTL["top_traders"])
            except Exception as e:
                logger.error(f"Error: {e}")
                try: self.conn.rollback()
                except: pass
                time.sleep(30)


if __name__ == "__main__":
    TopTradersSyncer().run()