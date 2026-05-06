#!/usr/bin/env python3
"""
wallet_classifier.py v3 — 100% local, denominación en SOL puro.

Cambios v3 respecto a v2:
- Eliminada dependencia 'requests' y llamada a CoinGecko.
- Eliminado argumento --sol-price.
- Tiers de profit ahora en SOL/día (no USD). Ajustables por env.
- Ventana configurable vía CLASSIFIER_WINDOW_DAYS (default 30).
- Columnas USD del schema se guardan como NULL (deuda técnica: limpiar en Fase 1).
"""

import argparse
import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras

from shared_config import DB_CONFIG, CLASSIFIER_WINDOW_DAYS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("wallet_classifier.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------
# Umbrales (ajustables por env sin tocar código)
# -----------------------------------------------------------------
BOT_THRESHOLDS = {
    "bot":         int(os.getenv("BOT_TRADES_PER_DAY",        "50")),
    "suspicious":  int(os.getenv("SUSPICIOUS_TRADES_PER_DAY", "20")),
}

CONSISTENCY_THRESHOLDS = {
    "high":   int(os.getenv("CONSISTENCY_HIGH_DAYS",   "5")),
    "medium": int(os.getenv("CONSISTENCY_MEDIUM_DAYS", "3")),
}

# Tiers en SOL/día (aprox. equivalentes a los antiguos USD asumiendo ~$140/SOL)
PROFIT_TIERS_SOL = {
    "tier5_whale":  float(os.getenv("TIER5_WHALE_SOL",  "35")),
    "tier4_large":  float(os.getenv("TIER4_LARGE_SOL",  "7")),
    "tier3_medium": float(os.getenv("TIER3_MEDIUM_SOL", "3.5")),
    "tier2_small":  float(os.getenv("TIER2_SMALL_SOL",  "0.7")),
    "tier1_micro":  0.0,
}

INVESTOR_TYPES = {
    "elite": {
        "label": "🟢 Elite Trader", "min_score": 80,
        "conditions": {
            "behavior":    ["human"],
            "consistency": ["high"],
            "profit":      ["tier3_medium", "tier4_large", "tier5_whale"],
        },
    },
    "profitable": {
        "label": "🔵 Profitable Trader", "min_score": 60,
        "conditions": {
            "behavior":    ["human", "suspicious"],
            "consistency": ["high", "medium"],
            "profit":      ["tier2_small", "tier3_medium", "tier4_large", "tier5_whale"],
        },
    },
    "regular": {
        "label": "⚪ Regular Trader", "min_score": 40,
        "conditions": {
            "behavior":    ["human", "suspicious"],
            "consistency": ["high", "medium", "low"],
            "profit":      ["tier1_micro", "tier2_small", "tier3_medium",
                            "tier4_large", "tier5_whale"],
        },
    },
    "bot_profitable": {
        "label": "🤖 Profitable Bot", "min_score": 30,
        "conditions": {
            "behavior":    ["bot"],
            "consistency": ["high", "medium", "low"],
            "profit":      ["tier2_small", "tier3_medium", "tier4_large", "tier5_whale"],
        },
    },
    "bot_regular": {
        "label": "🤖 Regular Bot", "min_score": 15,
        "conditions": {
            "behavior":    ["bot", "suspicious"],
            "consistency": ["high", "medium", "low"],
            "profit":      ["tier1_micro"],
        },
    },
    "casual": {
        "label": "💤 Casual Trader", "min_score": 10,
        "conditions": {
            "behavior":    ["human", "suspicious"],
            "consistency": ["low"],
            "profit":      ["tier1_micro"],
        },
    },
    "losing": {
        "label": "🔴 Losing Trader", "min_score": 0,
        "conditions": {
            "behavior":    ["human", "suspicious", "bot"],
            "consistency": ["high", "medium", "low"],
            "profit":      ["negative"],
        },
    },
}


# -----------------------------------------------------------------
# Clasificadores puros
# -----------------------------------------------------------------
def classify_behavior(avg_daily_trades: float) -> str:
    if avg_daily_trades >= BOT_THRESHOLDS["bot"]:
        return "bot"
    if avg_daily_trades >= BOT_THRESHOLDS["suspicious"]:
        return "suspicious"
    return "human"

def classify_consistency(active_days: int) -> str:
    if active_days >= CONSISTENCY_THRESHOLDS["high"]:
        return "high"
    if active_days >= CONSISTENCY_THRESHOLDS["medium"]:
        return "medium"
    return "low"

def classify_profit_tier_sol(avg_daily_pnl_sol: float) -> str:
    if avg_daily_pnl_sol <= 0:
        return "negative"
    # Orden descendente para que entre en el tier correcto
    order = ["tier5_whale", "tier4_large", "tier3_medium", "tier2_small", "tier1_micro"]
    for tier in order:
        if avg_daily_pnl_sol >= PROFIT_TIERS_SOL[tier]:
            return tier
    return "tier1_micro"

def classify_investor_type(behavior, consistency, profit_tier):
    for inv_type, config in INVESTOR_TYPES.items():
        c = config["conditions"]
        if (behavior in c["behavior"]
                and consistency in c["consistency"]
                and profit_tier in c["profit"]):
            return inv_type, config["min_score"], config["label"]
    return "unclassified", 0, "❓ Sin clasificar"

def calculate_score(behavior, consistency, profit_tier, avg_daily_pnl_sol, win_rate):
    score = 0
    if behavior == "human":
        score += 25
    elif behavior == "suspicious":
        score += 10

    score += {"high": 25, "medium": 15, "low": 5}.get(consistency, 0)

    score += {
        "tier5_whale": 35, "tier4_large": 28, "tier3_medium": 21,
        "tier2_small": 14, "tier1_micro": 7, "negative": 0,
    }.get(profit_tier, 0)

    if win_rate and win_rate > 0:
        score += min(15, int(win_rate * 0.15))

    return min(100, max(0, score))


# -----------------------------------------------------------------
# Data access
# -----------------------------------------------------------------
def fetch_wallet_metrics(conn, window_days: int):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = f"""
    WITH daily_activity AS (
        SELECT
            wt.wallet_id,
            DATE(wt.time) AS trade_date,
            COUNT(*) AS trades_count,
            SUM(CASE WHEN wt.tx_type = 'sell' THEN wt.sol_amount ELSE 0 END) -
            SUM(CASE WHEN wt.tx_type = 'buy'  THEN wt.sol_amount ELSE 0 END)
              AS daily_pnl_sol
        FROM wallet_transactions wt
        WHERE wt.time >= NOW() - INTERVAL '{window_days} days'
        GROUP BY wt.wallet_id, DATE(wt.time)
    ),
    wallet_stats AS (
        SELECT
            da.wallet_id,
            ROUND(AVG(da.trades_count)::numeric, 2) AS avg_daily_trades,
            COUNT(DISTINCT da.trade_date) FILTER (
                WHERE da.trade_date >= CURRENT_DATE - INTERVAL '7 days'
            ) AS active_days_last_week,
            ROUND(AVG(da.daily_pnl_sol)::numeric, 8) AS avg_daily_pnl_sol,
            SUM(da.daily_pnl_sol) AS total_pnl_period,
            MAX(da.trades_count) AS max_daily_trades,
            COUNT(DISTINCT da.trade_date) AS total_active_days
        FROM daily_activity da
        GROUP BY da.wallet_id
    )
    SELECT
        ws.*,
        w.wallet_address, w.total_trades, w.win_rate,
        w.total_profit_loss, w.tags,
        'txn_data' AS data_source
    FROM wallet_stats ws
    JOIN wallets w ON w.wallet_id = ws.wallet_id
    WHERE w.is_active = TRUE
    """
    cur.execute(query)
    results = list(cur.fetchall())
    logger.info(f"Wallets con transacciones ({window_days}d): {len(results)}")

    # Fallback: wallets activos pero sin txns dentro de la ventana.
    fallback_query = f"""
    SELECT
        w.wallet_id, w.wallet_address,
        w.total_trades, w.win_rate,
        w.total_profit_loss, w.total_invested, w.total_realized,
        w.tags, w.first_seen, w.last_seen,
        CASE WHEN w.first_seen IS NOT NULL
                  AND (w.last_seen - w.first_seen) > INTERVAL '0'
             THEN ROUND((w.total_trades::numeric /
                         GREATEST(EXTRACT(EPOCH FROM (w.last_seen - w.first_seen)) / 86400, 1)
                        )::numeric, 2)
             ELSE w.total_trades::numeric
        END AS est_avg_daily_trades,
        CASE WHEN w.first_seen IS NOT NULL
                  AND (w.last_seen - w.first_seen) > INTERVAL '0'
             THEN ROUND((w.total_profit_loss::numeric /
                         GREATEST(EXTRACT(EPOCH FROM (w.last_seen - w.first_seen)) / 86400, 1)
                        )::numeric, 8)
             ELSE w.total_profit_loss::numeric
        END AS est_daily_pnl_sol,
        CASE WHEN w.first_seen IS NOT NULL
                  AND (w.last_seen - w.first_seen) > INTERVAL '7 days'
             THEN LEAST(7, ROUND((
                    EXTRACT(EPOCH FROM (w.last_seen - w.first_seen)) / 86400 /
                    GREATEST(EXTRACT(EPOCH FROM (NOW() - w.first_seen)) / 86400 / 7, 1)
                  )::numeric, 0))::integer
             ELSE LEAST(7, ROUND(
                    EXTRACT(EPOCH FROM (COALESCE(w.last_seen, NOW()) - w.first_seen)) / 86400
                  )::numeric, 0)::integer
        END AS est_active_days
    FROM wallets w
    WHERE w.is_active = TRUE
      AND w.total_trades >= 3
      AND w.wallet_id NOT IN (
          SELECT DISTINCT wallet_id
          FROM wallet_transactions
          WHERE time >= NOW() - INTERVAL '{window_days} days'
      )
    """
    cur.execute(fallback_query)
    fb = cur.fetchall()
    logger.info(f"Wallets fallback (sin txns {window_days}d): {len(fb)}")

    for row in fb:
        results.append({
            "wallet_id":             row["wallet_id"],
            "wallet_address":        row["wallet_address"],
            "avg_daily_trades":      float(row["est_avg_daily_trades"] or 0),
            "active_days_last_week": int(row["est_active_days"] or 0),
            "avg_daily_pnl_sol":     float(row["est_daily_pnl_sol"] or 0),
            "total_pnl_period":      float(row["total_profit_loss"] or 0),
            "max_daily_trades":      0,
            "total_active_days":     0,
            "total_trades":          row["total_trades"],
            "win_rate":              row["win_rate"],
            "total_profit_loss":     row["total_profit_loss"],
            "tags":                  row["tags"],
            "data_source":           "fallback",
        })

    cur.close()
    return results


def save_classifications(conn, classifications, dry_run=False, window_days=30):
    if dry_run:
        logger.info(f"DRY RUN: se clasificarían {len(classifications)} wallets")
        return

    cur = conn.cursor()
    # NOTA: avg_daily_pnl_usd y sol_price_used quedan NULL a propósito
    # (deuda técnica — limpiar columnas USD del schema en Fase 1).
    query = f"""
    INSERT INTO wallet_classifications (
        wallet_id, avg_daily_trades, behavior_type,
        active_days_last_week, consistency_level,
        avg_daily_pnl_sol, avg_daily_pnl_usd, profit_tier,
        investor_type, investor_score, investor_label,
        sol_price_used, classified_at, data_window_start, data_window_end
    ) VALUES (
        %(wallet_id)s, %(avg_daily_trades)s, %(behavior_type)s,
        %(active_days)s, %(consistency)s,
        %(avg_daily_pnl_sol)s, NULL, %(profit_tier)s,
        %(investor_type)s, %(investor_score)s, %(investor_label)s,
        NULL, NOW(), NOW() - INTERVAL '{window_days} days', NOW()
    )
    ON CONFLICT (wallet_id) DO UPDATE SET
        avg_daily_trades      = EXCLUDED.avg_daily_trades,
        behavior_type         = EXCLUDED.behavior_type,
        active_days_last_week = EXCLUDED.active_days_last_week,
        consistency_level     = EXCLUDED.consistency_level,
        avg_daily_pnl_sol     = EXCLUDED.avg_daily_pnl_sol,
        avg_daily_pnl_usd     = NULL,
        profit_tier           = EXCLUDED.profit_tier,
        investor_type         = EXCLUDED.investor_type,
        investor_score        = EXCLUDED.investor_score,
        investor_label        = EXCLUDED.investor_label,
        sol_price_used        = NULL,
        classified_at         = NOW(),
        data_window_start     = NOW() - INTERVAL '{window_days} days',
        data_window_end       = NOW()
    """
    for c in classifications:
        cur.execute(query, c)
    conn.commit()
    cur.close()
    logger.info(f"Guardadas {len(classifications)} clasificaciones")


# -----------------------------------------------------------------
# Loop
# -----------------------------------------------------------------
def run_classification(dry_run: bool = False):
    window_days = CLASSIFIER_WINDOW_DAYS
    logger.info("=" * 60)
    logger.info("WALLET CLASSIFIER v3 (SOL-only, 100% local)")
    logger.info(f"Ventana: {window_days} días + fallback")
    logger.info("=" * 60)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        wallets = fetch_wallet_metrics(conn, window_days)
        logger.info(f"Total wallets a clasificar: {len(wallets)}")
        if not wallets:
            logger.warning("No hay wallets para clasificar")
            return

        classifications = []
        stats = {"behavior": {}, "consistency": {}, "profit": {},
                 "investor": {}, "sources": {"txn_data": 0, "fallback": 0}}

        for w in wallets:
            avg_trades   = float(w["avg_daily_trades"] or 0)
            active_days  = int(w["active_days_last_week"] or 0)
            avg_pnl_sol  = float(w["avg_daily_pnl_sol"] or 0)
            win_rate     = float(w["win_rate"] or 0)
            source       = w.get("data_source", "txn_data")

            behavior    = classify_behavior(avg_trades)
            consistency = classify_consistency(active_days)
            profit_tier = classify_profit_tier_sol(avg_pnl_sol)
            inv_type, _, label = classify_investor_type(
                behavior, consistency, profit_tier
            )
            score = calculate_score(
                behavior, consistency, profit_tier, avg_pnl_sol, win_rate
            )

            classifications.append({
                "wallet_id":         w["wallet_id"],
                "avg_daily_trades":  avg_trades,
                "behavior_type":     behavior,
                "active_days":       active_days,
                "consistency":       consistency,
                "avg_daily_pnl_sol": avg_pnl_sol,
                "profit_tier":       profit_tier,
                "investor_type":     inv_type,
                "investor_score":    score,
                "investor_label":    label,
            })

            stats["behavior"][behavior]       = stats["behavior"].get(behavior, 0) + 1
            stats["consistency"][consistency] = stats["consistency"].get(consistency, 0) + 1
            stats["profit"][profit_tier]      = stats["profit"].get(profit_tier, 0) + 1
            stats["investor"][inv_type]       = stats["investor"].get(inv_type, 0) + 1
            stats["sources"][source]          = stats["sources"].get(source, 0) + 1

        total = len(classifications)
        logger.info("-" * 60)
        logger.info(f"RESUMEN ({total} wallets)")
        logger.info("-" * 60)
        logger.info(
            f"Fuente: {stats['sources']['txn_data']} con txns, "
            f"{stats['sources']['fallback']} fallback"
        )
        for bucket in ("behavior", "consistency", "profit", "investor"):
            logger.info("")
            logger.info(f"{bucket.upper()}:")
            for k, v in sorted(stats[bucket].items(), key=lambda x: -x[1]):
                logger.info(f"  {k:20s}: {v:5d} ({v/total*100:.1f}%)")
        logger.info("-" * 60)

        save_classifications(conn, classifications, dry_run, window_days)

    except Exception as e:
        logger.error(f"Error en clasificación: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Clasificador de wallets v3 (SOL-only)")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar, no guardar")
    args = parser.parse_args()
    run_classification(dry_run=args.dry_run)


if __name__ == "__main__":
    main()