#!/usr/bin/env python3
"""
api_server.py
API REST para alimentar el frontend DexScreener clone
Conecta con tu base de datos memecoins_db (PostgreSQL + TimescaleDB)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Permitir peticiones del frontend

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "memecoins_db",
    "user": "postgres",
    "password": ""
}


def get_db():
    """Conexión a PostgreSQL"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        cursor_factory=psycopg2.extras.RealDictCursor
    )


@app.route('/api/tokens', methods=['GET'])
def get_tokens():
    """
    Endpoint principal: tokens con métricas actuales + cambios porcentuales.
    Query params:
      - sort: campo para ordenar (volume, mcap, price, txns, makers) default: volume
      - order: asc/desc, default: desc
      - limit: máximo de resultados, default: 50
      - status: filtrar por status del token, default: active
    """
    sort_field = request.args.get('sort', 'volume')
    order = request.args.get('order', 'desc').upper()
    limit = min(int(request.args.get('limit', 50)), 200)
    status = request.args.get('status', 'active')

    # Mapeo de campos de ordenamiento
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
        -- Última métrica por token (la más reciente)
        SELECT DISTINCT ON (token_id)
            token_id,
            time,
            price,
            liquidity,
            volume_1h,
            volume_24h,
            market_cap,
            fdv,
            holders_count,
            transactions_count
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '30 minutes'
        ORDER BY token_id, time DESC
    ),
    price_5m AS (
        SELECT DISTINCT ON (token_id)
            token_id, price
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '6 minutes'
          AND time <= NOW() - INTERVAL '4 minutes'
        ORDER BY token_id, time DESC
    ),
    price_1h AS (
        SELECT DISTINCT ON (token_id)
            token_id, price
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '65 minutes'
          AND time <= NOW() - INTERVAL '55 minutes'
        ORDER BY token_id, time DESC
    ),
    price_6h AS (
        SELECT DISTINCT ON (token_id)
            token_id, price
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '6 hours 5 minutes'
          AND time <= NOW() - INTERVAL '5 hours 55 minutes'
        ORDER BY token_id, time DESC
    ),
    price_24h AS (
        SELECT DISTINCT ON (token_id)
            token_id, price
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '24 hours 5 minutes'
          AND time <= NOW() - INTERVAL '23 hours 55 minutes'
        ORDER BY token_id, time DESC
    ),
    txn_counts AS (
        -- Transacciones totales en las últimas 24h desde wallet_transactions
        SELECT
            token_id,
            COUNT(*) AS txns_24h,
            COUNT(DISTINCT wallet_id) AS makers_24h
        FROM wallet_transactions
        WHERE time >= NOW() - INTERVAL '24 hours'
        GROUP BY token_id
    )
    SELECT
        t.token_id,
        t.mint_address,
        t.name,
        t.symbol,
        t.image_url,
        t.amm,
        t.created_at,
        t.detected_at,

        -- Métricas actuales
        latest.price AS current_price,
        latest.liquidity,
        latest.volume_24h,
        latest.market_cap,
        latest.fdv,
        latest.holders_count,
        latest.transactions_count,

        -- Transacciones y makers de wallet_transactions
        COALESCE(tc.txns_24h, latest.transactions_count, 0) AS txns,
        COALESCE(tc.makers_24h, latest.holders_count, 0) AS makers,

        -- Cambios porcentuales
        CASE WHEN p5.price > 0
            THEN ROUND(((latest.price - p5.price) / p5.price * 100)::numeric, 2)
            ELSE NULL END AS pct_5m,
        CASE WHEN p1h.price > 0
            THEN ROUND(((latest.price - p1h.price) / p1h.price * 100)::numeric, 2)
            ELSE NULL END AS pct_1h,
        CASE WHEN p6h.price > 0
            THEN ROUND(((latest.price - p6h.price) / p6h.price * 100)::numeric, 2)
            ELSE NULL END AS pct_6h,
        CASE WHEN p24h.price > 0
            THEN ROUND(((latest.price - p24h.price) / p24h.price * 100)::numeric, 2)
            ELSE NULL END AS pct_24h

    FROM tokens t
    JOIN latest_metrics latest ON t.token_id = latest.token_id
    LEFT JOIN price_5m p5 ON t.token_id = p5.token_id
    LEFT JOIN price_1h p1h ON t.token_id = p1h.token_id
    LEFT JOIN price_6h p6h ON t.token_id = p6h.token_id
    LEFT JOIN price_24h p24h ON t.token_id = p24h.token_id
    LEFT JOIN txn_counts tc ON t.token_id = tc.token_id
    WHERE t.status = %s
      AND latest.price IS NOT NULL
      AND latest.price > 0
    ORDER BY {sort_col} {order_dir} NULLS LAST
    LIMIT %s
    """

    try:
        cursor.execute(query, (status, limit))
        rows = cursor.fetchall()

        tokens = []
        for row in rows:
            created = row['created_at']
            age_str = _format_age(created) if created else '??'

            tokens.append({
                'token_id': row['token_id'],
                'mint_address': row['mint_address'],
                'name': row['name'] or '???',
                'symbol': row['symbol'] or '???',
                'image_url': row['image_url'],
                'amm': row['amm'],
                'age': age_str,
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

        return jsonify({
            'success': True,
            'count': len(tokens),
            'tokens': tokens
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/token/<mint_address>', methods=['GET'])
def get_token_detail(mint_address):
    """Detalle de un token específico con historial de precios"""
    conn = get_db()
    cursor = conn.cursor()

    try:
        # Info del token
        cursor.execute("""
            SELECT t.*, 
                   (SELECT price FROM token_metrics WHERE token_id = t.token_id ORDER BY time DESC LIMIT 1) AS current_price,
                   (SELECT liquidity FROM token_metrics WHERE token_id = t.token_id ORDER BY time DESC LIMIT 1) AS current_liquidity,
                   (SELECT market_cap FROM token_metrics WHERE token_id = t.token_id ORDER BY time DESC LIMIT 1) AS current_mcap
            FROM tokens t
            WHERE t.mint_address = %s
        """, (mint_address,))
        token = cursor.fetchone()

        if not token:
            return jsonify({'success': False, 'error': 'Token not found'}), 404

        # Top wallets que tradean este token
        cursor.execute("""
            SELECT 
                w.wallet_address,
                wp.total_bought,
                wp.total_sold,
                wp.current_balance,
                wp.realized_pnl,
                wp.status
            FROM wallet_positions wp
            JOIN wallets w ON wp.wallet_id = w.wallet_id
            WHERE wp.token_id = %s
            ORDER BY (wp.realized_pnl + wp.unrealized_pnl) DESC
            LIMIT 20
        """, (token['token_id'],))
        top_wallets = cursor.fetchall()

        return jsonify({
            'success': True,
            'token': dict(token),
            'top_wallets': [dict(w) for w in top_wallets]
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Estadísticas generales del sistema"""
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) AS total FROM tokens WHERE status = 'active'")
        total_tokens = cursor.fetchone()['total']

        cursor.execute("SELECT COUNT(*) AS total FROM wallets WHERE is_active = TRUE")
        total_wallets = cursor.fetchone()['total']

        cursor.execute("""
            SELECT COUNT(*) AS total 
            FROM wallet_transactions 
            WHERE time >= NOW() - INTERVAL '24 hours'
        """)
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


def _format_age(created_at):
    """Formatea la edad del token (12h, 3d, 2mo, etc.)"""
    if not created_at:
        return '??'
    now = datetime.now()
    if created_at.tzinfo:
        from datetime import timezone
        now = datetime.now(timezone.utc)
    diff = now - created_at
    seconds = diff.total_seconds()

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
    print("🚀 API Server iniciando en http://localhost:5000")
    print("📡 Endpoints disponibles:")
    print("   GET /api/tokens         → Lista de tokens con métricas")
    print("   GET /api/token/<mint>   → Detalle de un token")
    print("   GET /api/stats          → Estadísticas generales")
    app.run(host='0.0.0.0', port=5000, debug=True)
