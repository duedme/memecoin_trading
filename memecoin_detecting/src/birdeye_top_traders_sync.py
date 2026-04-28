#!/usr/bin/env python3
"""birdeye_top_traders_sync.py — Refresca top traders por token y encola wallets para PnL.

Cambios 28-abr-2026:
- Usa TOP_TRADERS_TOKEN_LIMIT del .env (antes hardcoded a 25).
- Encola wallets con active = TRUE.
- Respeta MAX_TRACKED_WALLETS: si la cola activa está llena, la wallet nueva
  solo entra desbancando a la peor (menor total_pnl_usd, priority >= nueva).
"""
import time
import logging
import psycopg2
from birdeye_client import BirdeyeClient
from shared_config import DB_CONFIG, TTL, TOP_TRADERS_TOKEN_LIMIT, MAX_TRACKED_WALLETS

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TopTradersSyncer:
    def __init__(self):
        self.client = BirdeyeClient()
        self.conn = psycopg2.connect(**DB_CONFIG)

    def pick_tokens(self, limit):
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

    def _enqueue_wallet(self, cur, wallet, priority=3):
        """Encola respetando MAX_TRACKED_WALLETS con rotación por PnL."""
        # ¿Ya existe y está activa? → sólo refrescamos prioridad
        cur.execute("""
            SELECT active FROM wallet_sync_queue WHERE wallet_address = %s
        """, (wallet,))
        row = cur.fetchone()
        if row is not None:
            if not row[0]:  # existe pero inactiva → reactivar
                cur.execute("""
                    UPDATE wallet_sync_queue
                       SET active = TRUE,
                           priority = LEAST(priority, %s),
                           next_sync_at = NOW()
                     WHERE wallet_address = %s
                """, (priority, wallet))
            return

        # Es wallet nueva: revisar si hay cupo
        cur.execute("SELECT COUNT(*) FROM wallet_sync_queue WHERE active = TRUE")
        active_count = cur.fetchone()[0]

        if active_count >= MAX_TRACKED_WALLETS:
            # Desbancar a la peor wallet (menor PnL, priority igual o peor)
            cur.execute("""
                UPDATE wallet_sync_queue
                   SET active = FALSE
                 WHERE wallet_address = (
                     SELECT q.wallet_address
                       FROM wallet_sync_queue q
                       LEFT JOIN wallet_pnl_cache p
                              ON p.wallet_address = q.wallet_address
                      WHERE q.active = TRUE
                        AND q.priority >= %s
                      ORDER BY COALESCE(p.total_pnl_usd, -1e12) ASC,
                               q.last_synced_at ASC NULLS FIRST
                      LIMIT 1
                 )
            """, (priority,))
            if cur.rowcount == 0:
                # No hay nadie peor que podamos desbancar → la nueva no entra
                return

        cur.execute("""
            INSERT INTO wallet_sync_queue
                (wallet_address, priority, refresh_interval_sec, active, next_sync_at)
            VALUES (%s, %s, %s, TRUE, NOW())
            ON CONFLICT (wallet_address) DO UPDATE SET
                active = TRUE,
                priority = LEAST(wallet_sync_queue.priority, EXCLUDED.priority)
        """, (wallet, priority, TTL["wallet_pnl"]))

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
            self._enqueue_wallet(cur, wallet, priority=3)
            cur.execute("""
                INSERT INTO wallets (wallet_address) VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (wallet,))
            count += 1
        self.conn.commit()
        cur.close()
        return count

    def run(self):
        logger.info(
            f"TopTradersSyncer corriendo "
            f"(tokens={TOP_TRADERS_TOKEN_LIMIT}, tope wallets={MAX_TRACKED_WALLETS})"
        )
        while True:
            try:
                tokens = self.pick_tokens(limit=TOP_TRADERS_TOKEN_LIMIT)
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