#!/usr/bin/env python3
"""
api_server.py
API REST para alimentar el frontend — Tokens + Top Traders
v2.1 FIXES:
  - Tokens: mostrar mint_address abreviada cuando no hay name/symbol
  - Top Traders: filtro por intervalo de tiempo (time_range)
  - Win rate: corregido cálculo (no siempre 100%)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from datetime import datetime

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "memecoins_db",
    "user": "rebelforce",
    "password": "Lol123123!"
}


def get_db():
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        cursor_factory=psycopg2.extras.RealDictCursor
    )


# ═══════════════════════════════════════
# ENDPOINT: Top Traders
# ═══════════════════════════════════════

@app.route('/api/top-traders', methods=['GET'])
def get_top_traders():
    """
    Top traders ordenados por P&L, win rate, ROI, etc.
    Query params:
      - sort: pnl, win_rate, roi, trades, invested (default: pnl)
      - order: asc/desc (default: desc)
      - limit: máximo resultados (default: 50, max: 200)
      - min_trades: mínimo de trades para aparecer (default: 3)
      - time_range: 1h, 6h, 24h, 7d, 30d, all (default: all)
    """
    sort_field = request.args.get('sort', 'pnl')
    order = request.args.get('order', 'desc').upper()
    limit = min(int(request.args.get('limit', 50)), 200)
    min_trades = int(request.args.get('min_trades', 3))
    time_range = request.args.get('time_range', 'all')

    sort_map = {
        'pnl': 'total_pnl',
        'win_rate': 'win_rate',
        'roi': 'roi_percentage',
        'trades': 'total_trades',
        'invested': 'total_invested',
        'realized': 'total_realized',
        'best_trade': 'best_trade',
        'last_seen': 'last_seen',
    }
    sort_col = sort_map.get(sort_field, 'total_pnl')
    order_dir = 'DESC' if order == 'DESC' else 'ASC'

    # Mapear time_range a intervalo SQL
    time_filter_map = {
        '1h':  "AND wt.time >= NOW() - INTERVAL '1 hour'",
        '6h':  "AND wt.time >= NOW() - INTERVAL '6 hours'",
        '24h': "AND wt.time >= NOW() - INTERVAL '24 hours'",
        '7d':  "AND wt.time >= NOW() - INTERVAL '7 days'",
        '30d': "AND wt.time >= NOW() - INTERVAL '30 days'",
        'all': '',
    }
    time_filter = time_filter_map.get(time_range, '')

    conn = get_db()
    cursor = conn.cursor()

    if time_range == 'all':
        # ─── Modo ALL: usa tabla wallets pre-calculada ───
        # PERO recalculamos win_rate correctamente
        query = f"""
        SELECT
            w.wallet_address,
            w.total_trades,
            w.total_profit_loss AS total_pnl,
            w.total_invested,
            w.total_realized,
            w.best_trade,
            w.worst_trade,
            w.first_seen,
            w.last_seen,
            w.is_active,
            w.tags,

            -- ROI
            CASE WHEN w.total_invested > 0
                THEN ROUND((w.total_profit_loss / w.total_invested * 100)::numeric, 2)
                ELSE 0
            END AS roi_percentage,

            -- Win rate CORREGIDO: basado en posiciones cerradas con ganancia
            COALESCE((
                SELECT CASE WHEN COUNT(*) > 0
                    THEN ROUND(
                        COUNT(*) FILTER (WHERE wp.realized_pnl > 0)::numeric
                        / COUNT(*)::numeric * 100, 2
                    )
                    ELSE 0
                END
                FROM wallet_positions wp
                WHERE wp.wallet_id = w.wallet_id
                  AND wp.status = 'closed'
            ), 0) AS win_rate,

            -- Posiciones abiertas
            (SELECT COUNT(*)
             FROM wallet_positions wp
             WHERE wp.wallet_id = w.wallet_id
               AND wp.status != 'closed'
               AND wp.current_balance > 0
            ) AS open_positions,

            -- Tokens distintos
            (SELECT COUNT(DISTINCT wp.token_id)
             FROM wallet_positions wp
             WHERE wp.wallet_id = w.wallet_id
            ) AS tokens_traded

        FROM wallets w
        WHERE w.total_trades >= %s
        ORDER BY {sort_col} {order_dir} NULLS LAST
        LIMIT %s
        """
        cursor.execute(query, (min_trades, limit))

    else:
        # ─── Modo TIME RANGE: calcula todo on-the-fly desde wallet_transactions ───
        query = f"""
        WITH filtered_txs AS (
            SELECT
                wt.wallet_id,
                wt.token_id,
                wt.tx_type,
                wt.token_amount,
                wt.sol_amount,
                wt.price,
                wt.time
            FROM wallet_transactions wt
            WHERE 1=1 {time_filter}
        ),
        wallet_stats AS (
            SELECT
                ft.wallet_id,
                COUNT(*) AS total_trades,

                -- Invertido = suma de sol_amount en buys
                COALESCE(SUM(ft.sol_amount) FILTER (WHERE ft.tx_type = 'buy'), 0) AS total_invested,

                -- Realizado = suma de sol_amount en sells
                COALESCE(SUM(ft.sol_amount) FILTER (WHERE ft.tx_type = 'sell'), 0) AS total_realized,

                -- P&L = sells - buys
                COALESCE(SUM(ft.sol_amount) FILTER (WHERE ft.tx_type = 'sell'), 0)
                - COALESCE(SUM(ft.sol_amount) FILTER (WHERE ft.tx_type = 'buy'), 0) AS total_pnl,

                -- Best/worst individual trade (por sol_amount en sells, negativo en buys)
                MAX(CASE WHEN ft.tx_type = 'sell' THEN ft.sol_amount ELSE NULL END) AS best_trade,
                MAX(CASE WHEN ft.tx_type = 'buy' THEN -ft.sol_amount ELSE NULL END) AS worst_trade,

                MIN(ft.time) AS first_seen,
                MAX(ft.time) AS last_seen,

                COUNT(DISTINCT ft.token_id) AS tokens_traded

            FROM filtered_txs ft
            GROUP BY ft.wallet_id
            HAVING COUNT(*) >= %s
        ),
        -- Win rate por token en el periodo
        token_pnl AS (
            SELECT
                ft.wallet_id,
                ft.token_id,
                COALESCE(SUM(ft.sol_amount) FILTER (WHERE ft.tx_type = 'sell'), 0)
                - COALESCE(SUM(ft.sol_amount) FILTER (WHERE ft.tx_type = 'buy'), 0) AS token_pnl
            FROM filtered_txs ft
            GROUP BY ft.wallet_id, ft.token_id
            -- Solo tokens donde hubo al menos 1 buy Y 1 sell (ciclo completo)
            HAVING COUNT(*) FILTER (WHERE ft.tx_type = 'buy') > 0
               AND COUNT(*) FILTER (WHERE ft.tx_type = 'sell') > 0
        ),
        win_rates AS (
            SELECT
                wallet_id,
                CASE WHEN COUNT(*) > 0
                    THEN ROUND(
                        COUNT(*) FILTER (WHERE token_pnl > 0)::numeric
                        / COUNT(*)::numeric * 100, 2
                    )
                    ELSE 0
                END AS win_rate
            FROM token_pnl
            GROUP BY wallet_id
        )
        SELECT
            w.wallet_address,
            ws.total_trades,
            ws.total_pnl,
            ws.total_invested,
            ws.total_realized,
            COALESCE(wr.win_rate, 0) AS win_rate,
            ws.best_trade,
            ws.worst_trade,
            ws.first_seen,
            ws.last_seen,
            w.is_active,
            w.tags,
            ws.tokens_traded,

            CASE WHEN ws.total_invested > 0
                THEN ROUND((ws.total_pnl / ws.total_invested * 100)::numeric, 2)
                ELSE 0
            END AS roi_percentage,

            0 AS open_positions

        FROM wallet_stats ws
        JOIN wallets w ON ws.wallet_id = w.wallet_id
        LEFT JOIN win_rates wr ON ws.wallet_id = wr.wallet_id
        ORDER BY {sort_col} {order_dir} NULLS LAST
        LIMIT %s
        """
        cursor.execute(query, (min_trades, limit))

    try:
        rows = cursor.fetchall()

        traders = []
        for row in rows:
            last_seen = row['last_seen']
            activity_str = _format_age(last_seen) if last_seen else '??'

            traders.append({
                'wallet_address': row['wallet_address'],
                'total_trades': int(row['total_trades'] or 0),
                'total_pnl': float(row['total_pnl'] or 0),
                'total_invested': float(row['total_invested'] or 0),
                'total_realized': float(row['total_realized'] or 0),
                'win_rate': float(row['win_rate'] or 0),
                'best_trade': float(row['best_trade'] or 0),
                'worst_trade': float(row['worst_trade'] or 0),
                'roi_percentage': float(row['roi_percentage'] or 0),
                'open_positions': int(row.get('open_positions') or 0),
                'tokens_traded': int(row.get('tokens_traded') or 0),
                'tags': row.get('tags') or '',
                'is_active': row.get('is_active'),
                'first_seen': row['first_seen'].isoformat() if row.get('first_seen') else None,
                'last_seen': row['last_seen'].isoformat() if row.get('last_seen') else None,
                'last_activity': activity_str,
            })

        return jsonify({
            'success': True,
            'count': len(traders),
            'time_range': time_range,
            'traders': traders
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/trader/<wallet_address>', methods=['GET'])
def get_trader_detail(wallet_address):
    """Detalle de un trader con sus posiciones"""
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT * FROM wallets WHERE wallet_address = %s
        """, (wallet_address,))
        wallet = cursor.fetchone()

        if not wallet:
            return jsonify({'success': False, 'error': 'Wallet not found'}), 404

        cursor.execute("""
            SELECT
                t.symbol, t.name, t.mint_address,
                wp.total_bought, wp.total_sold, wp.current_balance,
                wp.avg_buy_price, wp.realized_pnl, wp.unrealized_pnl,
                wp.status, wp.first_buy, wp.last_sell
            FROM wallet_positions wp
            JOIN tokens t ON wp.token_id = t.token_id
            WHERE wp.wallet_id = %s
            ORDER BY (wp.realized_pnl + wp.unrealized_pnl) DESC
            LIMIT 50
        """, (wallet['wallet_id'],))
        positions = cursor.fetchall()

        return jsonify({
            'success': True,
            'wallet': dict(wallet),
            'positions': [dict(p) for p in positions]
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════
# ENDPOINT: Tokens — FIX: mostrar mint abreviada
# ═══════════════════════════════════════

@app.route('/api/tokens', methods=['GET'])
def get_tokens():
    sort_field = request.args.get('sort', 'volume')
    order = request.args.get('order', 'desc').upper()
    limit = min(int(request.args.get('limit', 50)), 200)
    status = request.args.get('status', 'active')

    sort_map = {
        'volume': 'latest.volume_24h',
        'mcap': 'latest.market_cap',
        'price': 'latest.price',
        'txns': 'latest.transactions_count',
        'makers': 'latest.holders_count',
        'liquidity': 'latest.liquidity',
        'age': 't.created_at',
        'change_5m': 'pct_5m',
        'change_1h': 'pct_1h',
        'change_6h': 'pct_6h',
        'change_24h': 'pct_24h',
    }
    sort_col = sort_map.get(sort_field, 'latest.volume_24h')
    order_dir = 'DESC' if order == 'DESC' else 'ASC'

    conn = get_db()
    cursor = conn.cursor()

    query = f"""
    WITH latest_metrics AS (
        SELECT DISTINCT ON (token_id)
            token_id, time, price, liquidity, volume_1h, volume_24h,
            market_cap, fdv, holders_count, transactions_count
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '30 minutes'
        ORDER BY token_id, time DESC
    ),
    price_5m AS (
        SELECT DISTINCT ON (token_id) token_id, price
        FROM token_metrics
        WHERE time BETWEEN NOW() - INTERVAL '6 minutes' AND NOW() - INTERVAL '4 minutes'
        ORDER BY token_id, time DESC
    ),
    price_1h AS (
        SELECT DISTINCT ON (token_id) token_id, price
        FROM token_metrics
        WHERE time BETWEEN NOW() - INTERVAL '65 minutes' AND NOW() - INTERVAL '55 minutes'
        ORDER BY token_id, time DESC
    ),
    price_6h AS (
        SELECT DISTINCT ON (token_id) token_id, price
        FROM token_metrics
        WHERE time BETWEEN NOW() - INTERVAL '6 hours 5 minutes' AND NOW() - INTERVAL '5 hours 55 minutes'
        ORDER BY token_id, time DESC
    ),
    price_24h AS (
        SELECT DISTINCT ON (token_id) token_id, price
        FROM token_metrics
        WHERE time BETWEEN NOW() - INTERVAL '24 hours 5 minutes' AND NOW() - INTERVAL '23 hours 55 minutes'
        ORDER BY token_id, time DESC
    ),
    txn_counts AS (
        SELECT token_id, COUNT(*) AS txns_24h, COUNT(DISTINCT wallet_id) AS makers_24h
        FROM wallet_transactions
        WHERE time >= NOW() - INTERVAL '24 hours'
        GROUP BY token_id
    )
    SELECT
        t.token_id, t.mint_address, t.name, t.symbol, t.image_url, t.amm,
        t.created_at, t.detected_at,
        latest.price AS current_price, latest.liquidity, latest.volume_24h,
        latest.market_cap, latest.fdv, latest.holders_count, latest.transactions_count,
        COALESCE(tc.txns_24h, latest.transactions_count, 0) AS txns,
        COALESCE(tc.makers_24h, latest.holders_count, 0) AS makers,
        CASE WHEN p5.price > 0  THEN ROUND(((latest.price - p5.price) / p5.price * 100)::numeric, 2)   ELSE NULL END AS pct_5m,
        CASE WHEN p1h.price > 0 THEN ROUND(((latest.price - p1h.price) / p1h.price * 100)::numeric, 2) ELSE NULL END AS pct_1h,
        CASE WHEN p6h.price > 0 THEN ROUND(((latest.price - p6h.price) / p6h.price * 100)::numeric, 2) ELSE NULL END AS pct_6h,
        CASE WHEN p24h.price > 0 THEN ROUND(((latest.price - p24h.price) / p24h.price * 100)::numeric, 2) ELSE NULL END AS pct_24h
    FROM tokens t
    JOIN latest_metrics latest ON t.token_id = latest.token_id
    LEFT JOIN price_5m p5 ON t.token_id = p5.token_id
    LEFT JOIN price_1h p1h ON t.token_id = p1h.token_id
    LEFT JOIN price_6h p6h ON t.token_id = p6h.token_id
    LEFT JOIN price_24h p24h ON t.token_id = p24h.token_id
    LEFT JOIN txn_counts tc ON t.token_id = tc.token_id
    WHERE t.status = %s AND latest.price IS NOT NULL AND latest.price > 0
    ORDER BY {sort_col} {order_dir} NULLS LAST
    LIMIT %s
    """

    try:
        cursor.execute(query, (status, limit))
        rows = cursor.fetchall()
        tokens = []
        for row in rows:
            created = row['created_at']

            # FIX: Si no hay name/symbol, mostrar mint abreviada
            mint = row['mint_address']
            display_name = row['name'] if row['name'] else f"{mint[:4]}...{mint[-4:]}"
            display_symbol = row['symbol'] if row['symbol'] else f"{mint[:6]}...{mint[-4:]}"

            tokens.append({
                'token_id': row['token_id'],
                'mint_address': mint,
                'name': display_name,
                'symbol': display_symbol,
                'image_url': row['image_url'],
                'amm': row['amm'],
                'age': _format_age(created) if created else '??',
                'created_at': created.isoformat() if created else None,
                'price': float(row['current_price']) if row['current_price'] else 0,
                'liquidity': float(row['liquidity']) if row['liquidity'] else 0,
                'volume_24h': float(row['volume_24h']) if row['volume_24h'] else 0,
                'market_cap': float(row['market_cap']) if row['market_cap'] else 0,
                'fdv': float(row['fdv']) if row['fdv'] else 0,
                'txns': int(row['txns'] or 0),
                'makers': int(row['makers'] or 0),
                'pct_5m': float(row['pct_5m']) if row['pct_5m'] is not None else None,
                'pct_1h': float(row['pct_1h']) if row['pct_1h'] is not None else None,
                'pct_6h': float(row['pct_6h']) if row['pct_6h'] is not None else None,
                'pct_24h': float(row['pct_24h']) if row['pct_24h'] is not None else None,
            })
        return jsonify({'success': True, 'count': len(tokens), 'tokens': tokens})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════
# ENDPOINT: Stats
# ═══════════════════════════════════════

@app.route('/api/stats', methods=['GET'])
def get_stats():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) AS total FROM tokens WHERE status = 'active'")
        total_tokens = cursor.fetchone()['total']
        cursor.execute("SELECT COUNT(*) AS total FROM wallets WHERE is_active = TRUE")
        total_wallets = cursor.fetchone()['total']
        cursor.execute("SELECT COUNT(*) AS total FROM wallet_transactions WHERE time >= NOW() - INTERVAL '24 hours'")
        txns_24h = cursor.fetchone()['total']
        return jsonify({
            'success': True,
            'total_tokens': total_tokens,
            'total_wallets': total_wallets,
            'transactions_24h': txns_24h
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


def _format_age(dt):
    if not dt:
        return '??'
    now = datetime.now()
    if dt.tzinfo:
        from datetime import timezone
        now = datetime.now(timezone.utc)
    diff = now - dt
    seconds = diff.total_seconds()
    if seconds < 0:
        return 'just now'
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m"
    elif seconds < 86400:
        return f"{int(seconds/3600)}h"
    elif seconds < 2592000:
        return f"{int(seconds/86400)}d"
    else:
        return f"{int(seconds/2592000)}mo"


if __name__ == '__main__':
    port = 8200
    print(f"\n🚀 API Server iniciando en http://localhost:{port}")
    print("📡 Endpoints:")
    print("   GET /api/top-traders?time_range=24h  → Top traders filtrados por tiempo")
    print("   GET /api/trader/<wallet>             → Detalle de un trader")
    print("   GET /api/tokens                      → Lista de tokens")
    print("   GET /api/stats                       → Estadísticas generales\n")
    app.run(host='0.0.0.0', port=port, debug=True)
