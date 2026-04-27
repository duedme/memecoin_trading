#!/usr/bin/env python3
"""wallet_classifier.py v3 — Clasifica wallets usando la caché de Birdeye PnL.

Cambios v3:
- Ya no lee wallet_transactions (tabla eliminada).
- Lee directamente de wallet_pnl_cache (alimentada por birdeye_wallet_sync).
- Conserva la lógica propia de tiers/labels (valor diferencial).
"""
import logging
import psycopg2
import psycopg2.extras
from shared_config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROFIT_TIERS = {
    "tier5_whale": 5000,
    "tier4_large": 1000,
    "tier3_medium": 500,
    "tier2_small": 100,
    "tier1_micro": 0,
}

INVESTOR_TYPES = {
    "elite":       {"label": "Elite Trader",     "min_score": 80},
    "profitable":  {"label": "Profitable Trader","min_score": 60},
    "regular":     {"label": "Regular Trader",   "min_score": 40},
    "casual":      {"label": "Casual Trader",    "min_score": 15},
    "losing":      {"label": "Losing Trader",    "min_score": 0},
}


def classify_profit_tier(pnl_usd: float) -> str:
    if pnl_usd is None or pnl_usd < 0:
        return "negative"
    for tier, min_val in PROFIT_TIERS.items():
        if pnl_usd >= min_val:
            return tier
    return "tier1_micro"


def classify_investor(pnl_usd, trade_count, win_rate) -> tuple:
    score = 0
    if pnl_usd and pnl_usd > 0:
        score += min(45, int(pnl_usd / 200))
    if win_rate:
        score += min(35, int(float(win_rate) * 0.35))
    if trade_count and trade_count >= 10:
        score += 15
    score = max(0, min(100, score))

    if score >= 80:   return "elite",      INVESTOR_TYPES["elite"]["label"],      score
    if score >= 60:   return "profitable", INVESTOR_TYPES["profitable"]["label"], score
    if score >= 40:   return "regular",    INVESTOR_TYPES["regular"]["label"],    score
    if score >= 15:   return "casual",     INVESTOR_TYPES["casual"]["label"],     score
    return "losing",  INVESTOR_TYPES["losing"]["label"], score


def run_classification():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT wallet_address, total_pnl_usd, trade_count, win_rate
        FROM wallet_pnl_cache
        WHERE last_updated > NOW() - INTERVAL '24 hours'
    """)
    rows = cur.fetchall()
    logger.info(f"Clasificando {len(rows)} wallets desde caché Birdeye...")

    up = conn.cursor()
    for r in rows:
        pnl   = float(r["total_pnl_usd"] or 0)
        trades = int(r["trade_count"] or 0)
        wr    = float(r["win_rate"] or 0)

        tier = classify_profit_tier(pnl)
        inv_type, label, score = classify_investor(pnl, trades, wr)

        up.execute("""
            INSERT INTO wallet_classifications
                (wallet_address, profit_tier, investor_type, investor_score, investor_label, classified_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (wallet_address) DO UPDATE SET
                profit_tier = EXCLUDED.profit_tier,
                investor_type = EXCLUDED.investor_type,
                investor_score = EXCLUDED.investor_score,
                investor_label = EXCLUDED.investor_label,
                classified_at = NOW()
        """, (r["wallet_address"], tier, inv_type, score, label))

    conn.commit()
    up.close()
    cur.close()
    conn.close()
    logger.info("Clasificación completada.")


if __name__ == "__main__":
    run_classification()