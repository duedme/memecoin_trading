#!/usr/bin/env python3
"""
wallet_classifier.py v2 - Clasifica wallets en tipos de inversor

Cambios v2:
  - Ventana principal ampliada a 30 días
  - Fallback: wallets sin transacciones recientes se clasifican con datos
    agregados de la tabla wallets (total_trades, total_profit_loss, etc.)
  - Consistencia calculada sobre los últimos 7 días DENTRO de la ventana

Uso:
  python wallet_classifier.py                # Clasificar todos
  python wallet_classifier.py --dry-run      # Solo mostrar, no guardar
  python wallet_classifier.py --sol-price 140 # Fijar precio SOL manualmente
"""

import psycopg2
import psycopg2.extras
import requests
import argparse
import logging
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'memecoins_db',
    'user': 'postgres',
    'password': '12345'
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wallet_classifier.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Umbrales ---
BOT_THRESHOLDS = { 'bot': 50, 'suspicious': 20 }
CONSISTENCY_THRESHOLDS = { 'high': 5, 'medium': 3 }
PROFIT_TIERS = {
    'tier5_whale': 5000,
    'tier4_large': 1000,
    'tier3_medium': 500,
    'tier2_small': 100,
    'tier1_micro': 0,
}

INVESTOR_TYPES = {
    'elite': {
        'label': '🟢 Elite Trader', 'min_score': 80,
        'conditions': {
            'behavior': ['human'],
            'consistency': ['high'],
            'profit': ['tier3_medium', 'tier4_large', 'tier5_whale']
        }
    },
    'profitable': {
        'label': '🔵 Profitable Trader', 'min_score': 60,
        'conditions': {
            'behavior': ['human', 'suspicious'],
            'consistency': ['high', 'medium'],
            'profit': ['tier2_small', 'tier3_medium', 'tier4_large', 'tier5_whale']
        }
    },
    'regular': {
        'label': '⚪ Regular Trader', 'min_score': 40,
        'conditions': {
            'behavior': ['human', 'suspicious'],
            'consistency': ['high', 'medium', 'low'],
            'profit': ['tier1_micro', 'tier2_small', 'tier3_medium', 'tier4_large', 'tier5_whale']
        }
    },
    'bot_profitable': {
        'label': '🤖 Profitable Bot', 'min_score': 30,
        'conditions': {
            'behavior': ['bot'],
            'consistency': ['high', 'medium', 'low'],
            'profit': ['tier2_small', 'tier3_medium', 'tier4_large', 'tier5_whale']
        }
    },
    'bot_regular': {
        'label': '🤖 Regular Bot', 'min_score': 15,
        'conditions': {
            'behavior': ['bot', 'suspicious'],
            'consistency': ['high', 'medium', 'low'],
            'profit': ['tier1_micro']
        }
    },
    'casual': {
        'label': '💤 Casual Trader', 'min_score': 10,
        'conditions': {
            'behavior': ['human', 'suspicious'],
            'consistency': ['low'],
            'profit': ['tier1_micro']
        }
    },
    'losing': {
        'label': '🔴 Losing Trader', 'min_score': 0,
        'conditions': {
            'behavior': ['human', 'suspicious', 'bot'],
            'consistency': ['high', 'medium', 'low'],
            'profit': ['negative']
        }
    },
}


def get_sol_price_usd(manual_price=None):
    if manual_price:
        return manual_price
    try:
        resp = requests.get(
            'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
            timeout=10
        )
        price = resp.json()['solana']['usd']
        logger.info(f"Precio SOL/USD: ${price:.2f}")
        return price
    except Exception as e:
        logger.warning(f"Error obteniendo precio SOL: {e}. Usando $150 por defecto.")
        return 150.0


def classify_behavior(avg_daily_trades):
    if avg_daily_trades >= BOT_THRESHOLDS['bot']:
        return 'bot'
    elif avg_daily_trades >= BOT_THRESHOLDS['suspicious']:
        return 'suspicious'
    return 'human'


def classify_consistency(active_days):
    if active_days >= CONSISTENCY_THRESHOLDS['high']:
        return 'high'
    elif active_days >= CONSISTENCY_THRESHOLDS['medium']:
        return 'medium'
    return 'low'


def classify_profit_tier(avg_daily_pnl_usd):
    if avg_daily_pnl_usd <= 0:
        return 'negative'
    for tier, min_usd in PROFIT_TIERS.items():
        if avg_daily_pnl_usd >= min_usd:
            return tier
    return 'tier1_micro'


def classify_investor_type(behavior, consistency, profit_tier):
    for inv_type, config in INVESTOR_TYPES.items():
        conds = config['conditions']
        if (behavior in conds['behavior'] and
            consistency in conds['consistency'] and
            profit_tier in conds['profit']):
            return inv_type, config['min_score'], config['label']
    return 'unclassified', 0, '❓ Sin clasificar'


def calculate_score(behavior, consistency, profit_tier, avg_daily_pnl_usd, win_rate):
    score = 0
    if behavior == 'human':
        score += 25
    elif behavior == 'suspicious':
        score += 10

    consistency_map = {'high': 25, 'medium': 15, 'low': 5}
    score += consistency_map.get(consistency, 0)

    profit_map = {
        'tier5_whale': 35, 'tier4_large': 28, 'tier3_medium': 21,
        'tier2_small': 14, 'tier1_micro': 7, 'negative': 0
    }
    score += profit_map.get(profit_tier, 0)

    if win_rate and win_rate > 0:
        score += min(15, int(win_rate * 0.15))

    return min(100, max(0, score))


def fetch_wallet_metrics(conn):
    """Obtiene métricas usando ventana de 30 días en wallet_transactions."""
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = """
    WITH daily_activity AS (
        SELECT
            wt.wallet_id,
            DATE(wt.time) AS trade_date,
            COUNT(*) AS trades_count,
            SUM(CASE WHEN wt.tx_type = 'sell' THEN wt.sol_amount ELSE 0 END) -
            SUM(CASE WHEN wt.tx_type = 'buy' THEN wt.sol_amount ELSE 0 END) AS daily_pnl_sol
        FROM wallet_transactions wt
        WHERE wt.time >= NOW() - INTERVAL '30 days'
        GROUP BY wt.wallet_id, DATE(wt.time)
    ),
    wallet_stats AS (
        SELECT
            da.wallet_id,
            ROUND(AVG(da.trades_count)::numeric, 2) AS avg_daily_trades,
            -- Consistencia: solo contar días activos en últimos 7 días
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
        w.wallet_address,
        w.total_trades,
        w.win_rate,
        w.total_profit_loss,
        w.tags,
        'txn_data' AS data_source
    FROM wallet_stats ws
    JOIN wallets w ON w.wallet_id = ws.wallet_id
    WHERE w.is_active = TRUE
    """
    cursor.execute(query)
    results = list(cursor.fetchall())

    logger.info(f"Wallets con transacciones (30d): {len(results)}")

    # ── FALLBACK: wallets activos SIN transacciones en 30 días ──
    fallback_query = """
    SELECT
        w.wallet_id,
        w.wallet_address,
        w.total_trades,
        w.win_rate,
        w.total_profit_loss,
        w.total_invested,
        w.total_realized,
        w.tags,
        w.first_seen,
        w.last_seen,
        -- Estimar avg daily trades: total_trades / días desde first_seen
        CASE WHEN w.first_seen IS NOT NULL AND (w.last_seen - w.first_seen) > INTERVAL '0'
            THEN ROUND(
                (w.total_trades::numeric /
                 GREATEST(EXTRACT(EPOCH FROM (w.last_seen - w.first_seen)) / 86400, 1)
                )::numeric, 2)
            ELSE w.total_trades::numeric
        END AS est_avg_daily_trades,
        -- Estimar PnL diario: total_pnl / días activos
        CASE WHEN w.first_seen IS NOT NULL AND (w.last_seen - w.first_seen) > INTERVAL '0'
            THEN ROUND(
                (w.total_profit_loss::numeric /
                 GREATEST(EXTRACT(EPOCH FROM (w.last_seen - w.first_seen)) / 86400, 1)
                )::numeric, 8)
            ELSE w.total_profit_loss::numeric
        END AS est_daily_pnl_sol,
        -- Estimar días activos por semana basándose en actividad histórica
        CASE WHEN w.first_seen IS NOT NULL AND (w.last_seen - w.first_seen) > INTERVAL '7 days'
            THEN LEAST(7, ROUND(
                (EXTRACT(EPOCH FROM (w.last_seen - w.first_seen)) / 86400 /
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
          WHERE time >= NOW() - INTERVAL '30 days'
      )
    """
    cursor.execute(fallback_query)
    fallback_rows = cursor.fetchall()
    logger.info(f"Wallets fallback (sin txns 30d): {len(fallback_rows)}")

    for row in fallback_rows:
        results.append({
            'wallet_id': row['wallet_id'],
            'wallet_address': row['wallet_address'],
            'avg_daily_trades': float(row['est_avg_daily_trades'] or 0),
            'active_days_last_week': int(row['est_active_days'] or 0),
            'avg_daily_pnl_sol': float(row['est_daily_pnl_sol'] or 0),
            'total_pnl_period': float(row['total_profit_loss'] or 0),
            'max_daily_trades': 0,
            'total_active_days': 0,
            'total_trades': row['total_trades'],
            'win_rate': row['win_rate'],
            'total_profit_loss': row['total_profit_loss'],
            'tags': row['tags'],
            'data_source': 'fallback',
        })

    cursor.close()
    return results


def save_classifications(conn, classifications, dry_run=False):
    if dry_run:
        logger.info(f"DRY RUN: Se clasificarían {len(classifications)} wallets")
        return

    cursor = conn.cursor()
    query = """
    INSERT INTO wallet_classifications (
        wallet_id, avg_daily_trades, behavior_type,
        active_days_last_week, consistency_level,
        avg_daily_pnl_sol, avg_daily_pnl_usd, profit_tier,
        investor_type, investor_score, investor_label,
        sol_price_used, classified_at, data_window_start, data_window_end
    ) VALUES (
        %(wallet_id)s, %(avg_daily_trades)s, %(behavior_type)s,
        %(active_days)s, %(consistency)s,
        %(avg_daily_pnl_sol)s, %(avg_daily_pnl_usd)s, %(profit_tier)s,
        %(investor_type)s, %(investor_score)s, %(investor_label)s,
        %(sol_price)s, NOW(), NOW() - INTERVAL '30 days', NOW()
    )
    ON CONFLICT (wallet_id) DO UPDATE SET
        avg_daily_trades = EXCLUDED.avg_daily_trades,
        behavior_type = EXCLUDED.behavior_type,
        active_days_last_week = EXCLUDED.active_days_last_week,
        consistency_level = EXCLUDED.consistency_level,
        avg_daily_pnl_sol = EXCLUDED.avg_daily_pnl_sol,
        avg_daily_pnl_usd = EXCLUDED.avg_daily_pnl_usd,
        profit_tier = EXCLUDED.profit_tier,
        investor_type = EXCLUDED.investor_type,
        investor_score = EXCLUDED.investor_score,
        investor_label = EXCLUDED.investor_label,
        sol_price_used = EXCLUDED.sol_price_used,
        classified_at = NOW(),
        data_window_start = NOW() - INTERVAL '30 days',
        data_window_end = NOW()
    """
    for c in classifications:
        cursor.execute(query, c)
    conn.commit()
    cursor.close()
    logger.info(f"Guardadas {len(classifications)} clasificaciones")


def run_classification(sol_price=None, dry_run=False):
    logger.info("=" * 60)
    logger.info("WALLET CLASSIFIER v2 - Iniciando clasificación")
    logger.info("  Ventana: 30 días + fallback para wallets inactivos")
    logger.info("=" * 60)

    sol_usd = get_sol_price_usd(sol_price)
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        wallets = fetch_wallet_metrics(conn)
        logger.info(f"Total wallets a clasificar: {len(wallets)}")

        if not wallets:
            logger.warning("No hay wallets para clasificar")
            return

        classifications = []
        stats = {
            'behavior': {}, 'consistency': {}, 'profit': {}, 'investor': {},
            'sources': {'txn_data': 0, 'fallback': 0}
        }

        for w in wallets:
            avg_trades = float(w['avg_daily_trades'] or 0)
            active_days = int(w['active_days_last_week'] or 0)
            avg_pnl_sol = float(w['avg_daily_pnl_sol'] or 0)
            avg_pnl_usd = avg_pnl_sol * sol_usd
            win_rate = float(w['win_rate'] or 0)
            source = w.get('data_source', 'txn_data')

            behavior = classify_behavior(avg_trades)
            consistency = classify_consistency(active_days)
            profit_tier = classify_profit_tier(avg_pnl_usd)
            inv_type, base_score, label = classify_investor_type(
                behavior, consistency, profit_tier
            )
            score = calculate_score(
                behavior, consistency, profit_tier, avg_pnl_usd, win_rate
            )

            classifications.append({
                'wallet_id': w['wallet_id'],
                'avg_daily_trades': avg_trades,
                'behavior_type': behavior,
                'active_days': active_days,
                'consistency': consistency,
                'avg_daily_pnl_sol': avg_pnl_sol,
                'avg_daily_pnl_usd': avg_pnl_usd,
                'profit_tier': profit_tier,
                'investor_type': inv_type,
                'investor_score': score,
                'investor_label': label,
                'sol_price': sol_usd
            })

            stats['behavior'][behavior] = stats['behavior'].get(behavior, 0) + 1
            stats['consistency'][consistency] = stats['consistency'].get(consistency, 0) + 1
            stats['profit'][profit_tier] = stats['profit'].get(profit_tier, 0) + 1
            stats['investor'][inv_type] = stats['investor'].get(inv_type, 0) + 1
            stats['sources'][source] = stats['sources'].get(source, 0) + 1

        total = len(classifications)
        logger.info("-" * 60)
        logger.info(f"RESUMEN DE CLASIFICACIÓN ({total} wallets)")
        logger.info("-" * 60)
        logger.info(f"Precio SOL: ${sol_usd:.2f}")
        logger.info(
            f"Fuente datos: {stats['sources']['txn_data']} con txns, "
            f"{stats['sources']['fallback']} fallback"
        )
        logger.info("")
        logger.info("REQ1 - Comportamiento:")
        for k, v in stats['behavior'].items():
            logger.info(f"  {k:15s}: {v:5d} ({v/total*100:.1f}%)")
        logger.info("")
        logger.info("REQ2 - Consistencia:")
        for k, v in stats['consistency'].items():
            logger.info(f"  {k:15s}: {v:5d} ({v/total*100:.1f}%)")
        logger.info("")
        logger.info("REQ3 - Rango de ganancia:")
        for k, v in sorted(stats['profit'].items()):
            logger.info(f"  {k:15s}: {v:5d} ({v/total*100:.1f}%)")
        logger.info("")
        logger.info("REQ4 - Tipo de inversor:")
        for k, v in sorted(stats['investor'].items(), key=lambda x: -x[1]):
            logger.info(f"  {k:20s}: {v:5d} ({v/total*100:.1f}%)")
        logger.info("-" * 60)

        save_classifications(conn, classifications, dry_run)

    except Exception as e:
        logger.error(f"Error en clasificación: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Clasificador de wallets v2')
    parser.add_argument('--dry-run', action='store_true', help='Solo mostrar, no guardar')
    parser.add_argument('--sol-price', type=float, default=None,
                        help='Precio SOL/USD manual')
    args = parser.parse_args()
    run_classification(sol_price=args.sol_price, dry_run=args.dry_run)


if __name__ == '__main__':
    main()

