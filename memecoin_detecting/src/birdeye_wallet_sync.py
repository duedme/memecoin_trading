#!/usr/bin/env python3
"""birdeye_wallet_sync.py — Respeta los 30 rpm de Wallet API (usa 25 rpm)."""
import time
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from birdeye_client import BirdeyeClient
from shared_config import DB_CONFIG, TTL

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class WalletSyncer:
    def __init__(self):
        self.client = BirdeyeClient()
        self.conn = psycopg2.connect(**DB_CONFIG)

    def pick_batch(self, limit=25):
        cur = self.conn.cursor()
        # Sólo wallets activas (respeta rotación de MAX_TRACKED_WALLETS)
        cur.execute("""
            SELECT wallet_address, priority, refresh_interval_sec
            FROM wallet_sync_queue
            WHERE active = TRUE
              AND next_sync_at <= NOW()
            ORDER BY priority ASC, next_sync_at ASC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return rows

    def sync_wallet(self, wallet, interval_sec):
        data = self.client.wallet_pnl_summary(wallet)
        cur = self.conn.cursor()
        if not data or "data" not in data:
            cur.execute("""
                UPDATE wallet_sync_queue SET
                    fail_count     = fail_count + 1,
                    last_synced_at = NOW(),
                    next_sync_at   = NOW() + make_interval(secs => %s)
                WHERE wallet_address = %s
            """, (interval_sec * 2, wallet))
            self.conn.commit(); cur.close()
            return False

        d = data["data"] if isinstance(data["data"], dict) else {}
        realized = d.get("realized_pnl")   or d.get("realizedPnl")   or 0
        unreal   = d.get("unrealized_pnl") or d.get("unrealizedPnl") or 0
        total    = d.get("total_pnl")      or d.get("totalPnl")      or (float(realized) + float(unreal))
        roi      = d.get("roi")            or d.get("roi_pct")
        trades   = d.get("trade_count")    or d.get("tradeCount")    or d.get("trades")
        winrate  = d.get("win_rate")       or d.get("winRate")

        cur.execute("""
            INSERT INTO wallet_pnl_cache
              (wallet_address, realized_pnl_usd, unrealized_pnl_usd, total_pnl_usd,
               roi_pct, trade_count, win_rate, raw_json, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW())
            ON CONFLICT (wallet_address) DO UPDATE SET
              realized_pnl_usd   = EXCLUDED.realized_pnl_usd,
              unrealized_pnl_usd = EXCLUDED.unrealized_pnl_usd,
              total_pnl_usd      = EXCLUDED.total_pnl_usd,
              roi_pct            = EXCLUDED.roi_pct,
              trade_count        = EXCLUDED.trade_count,
              win_rate           = EXCLUDED.win_rate,
              raw_json           = EXCLUDED.raw_json,
              last_updated       = NOW()
        """, (wallet, realized, unreal, total, roi, trades, winrate, Json(d)))

        cur.execute("""
            UPDATE wallet_sync_queue SET
              last_synced_at = NOW(),
              next_sync_at   = NOW() + make_interval(secs => %s),
              fail_count     = 0
            WHERE wallet_address = %s
        """, (interval_sec, wallet))
        self.conn.commit(); cur.close()
        return True

    def seed_from_tracked(self):
        """Inyecta las tracked_wallets en la cola con alta prioridad."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO wallet_sync_queue
                (wallet_address, priority, refresh_interval_sec, active, next_sync_at)
            SELECT wallet_address, LEAST(priority, 2), 900, TRUE, NOW()
            FROM tracked_wallets WHERE is_active = TRUE
            ON CONFLICT (wallet_address) DO UPDATE SET
              active = TRUE,
              priority = LEAST(wallet_sync_queue.priority, EXCLUDED.priority),
              refresh_interval_sec = LEAST(wallet_sync_queue.refresh_interval_sec, EXCLUDED.refresh_interval_sec)
        """)
        self.conn.commit(); cur.close()

    def run(self):
        logger.info("WalletSyncer corriendo (respetando 25 rpm Wallet API)")
        last_seed = 0
        while True:
            try:
                now = time.time()
                if now - last_seed > 300:
                    self.seed_from_tracked()
                    last_seed = now

                batch = self.pick_batch(limit=25)
                if not batch:
                    time.sleep(15); continue

                ok = 0
                for wallet, prio, interval in batch:
                    if self.sync_wallet(wallet, interval or TTL["wallet_pnl"]):
                        ok += 1
                    time.sleep(0.1)
                logger.info(f"💼 PnL actualizado en {ok}/{len(batch)} wallets")
            except Exception as e:
                logger.error(f"Error: {e}")
                try: self.conn.rollback()
                except: pass
                time.sleep(10)


if __name__ == "__main__":
    WalletSyncer().run()