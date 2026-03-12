#!/usr/bin/env python3
"""
apiserver.py v3 - API REST: Tokens + Top Traders con clasificación de inversores
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from datetime import datetime

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'memecoins_db',
    'user': 'postgres',
    'password': '12345'
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

TIME_RANGE_MAP = {
    '1h': '1 hour',
    '6h': '6 hours',
    '24h': '24 hours',
    '7d': '7 days',
    '30d': '30 days',
}

# ============================================================================
# REQ1: Top Traders con clasificación
# ============================================================================
@app.route('/api/top-traders', methods=['GET'])
def get_top_traders():
    """
    Top traders ordenados por P&L, win rate, ROI, score, etc.
    Query params:
    - sort: pnl, winrate, roi, trades, invested, besttrade, score
    - order: asc|desc
    - limit: max results (default 50, max 200)
    - mintrades: minimum trades to appear (default 3)
    - timerange: 1h, 6h, 24h, 7d, 30d, all (default all)
    """
    sortfield = request.args.get('sort', 'pnl')
    order = request.args.get('order', 'desc').upper()
    limit = min(int(request.args.get('limit', 50)), 200)
    mintrades = int(request.args.get('mintrades', 3))
    timerange = request.args.get('timerange', 'all')
    
    order_dir = 'DESC' if order == 'DESC' else 'ASC'
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if timerange == 'all' or timerange not in TIME_RANGE_MAP:
            # Query sobre toda la data histórica (wallets table)
            sortmap = {
                'pnl': 'w.total_profit_loss',
                'winrate': 'real_winrate',
                'roi': 'roi_percentage',
                'trades': 'w.total_trades',
                'invested': 'w.total_invested',
                'realized': 'w.total_realized',
                'besttrade': 'w.best_trade',
                'lastseen': 'w.last_seen',
                'score': 'wc.investor_score',
            }
            sortcol = sortmap.get(sortfield, 'w.total_profit_loss')
            
            query = f"""
            SELECT 
                w.wallet_address,
                w.total_trades,
                w.total_profit_loss,
                w.total_invested,
                w.total_realized,
                w.avg_profit_per_trade,
                w.best_trade,
                w.worst_trade,
                w.first_seen,
                w.last_seen,
                w.is_active,
                w.tags,
                
                -- ROI
                CASE WHEN w.total_invested > 0 
                     THEN ROUND((w.total_profit_loss / w.total_invested) * 100::numeric, 2)
                     ELSE 0 
                END AS roi_percentage,
                
                -- Win rate RECALCULADO: posiciones cerradas con realized_pnl != NULL
                COALESCE((
                    SELECT CASE WHEN COUNT(*) > 0 
                                THEN ROUND((COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / COUNT(*)::numeric) * 100, 2)
                                ELSE NULL 
                           END
                    FROM wallet_positions wp2
                    WHERE wp2.wallet_id = w.wallet_id
                      AND wp2.status = 'closed'
                      AND wp2.realized_pnl IS NOT NULL
                ), NULL) AS real_winrate,
                
                -- Posiciones abiertas
                (SELECT COUNT(*) FROM wallet_positions wp 
                 WHERE wp.wallet_id = w.wallet_id 
                   AND wp.status != 'closed' 
                   AND wp.current_balance > 0) AS open_positions,
                
                -- Total unrealized
                (SELECT COALESCE(SUM(wp.unrealized_pnl), 0) FROM wallet_positions wp 
                 WHERE wp.wallet_id = w.wallet_id) AS total_unrealized_pnl,
                
                -- Tokens traded
                (SELECT COUNT(DISTINCT wp.token_id) FROM wallet_positions wp 
                 WHERE wp.wallet_id = w.wallet_id) AS tokens_traded,
                
                -- CLASIFICACIÓN
                wc.behavior_type,
                wc.consistency_level,
                wc.profit_tier,
                wc.investor_type,
                wc.investor_score,
                wc.investor_label
                
            FROM wallets w
            LEFT JOIN wallet_classifications wc ON w.wallet_id = wc.wallet_id
            WHERE w.total_trades >= %s
            ORDER BY {sortcol} {order_dir} NULLS LAST
            LIMIT %s
            """
            cursor.execute(query, (mintrades, limit))
            
        else:
            # Query sobre periodo específico (wallet_transactions)
            interval = TIME_RANGE_MAP[timerange]
            sortmap = {
                'pnl': 'total_pnl',
                'winrate': 'real_winrate',
                'roi': 'roi_percentage',
                'trades': 'total_trades',
                'invested': 'total_invested',
                'besttrade': 'best_trade',
                'score': 'wc.investor_score',
            }
            sortcol = sortmap.get(sortfield, 'total_pnl')
            
            query = f"""
            WITH period_txns AS (
                SELECT wt.wallet_id, wt.token_id, wt.tx_type, wt.token_amount, wt.sol_amount, wt.price
                FROM wallet_transactions wt
                WHERE wt.time >= NOW() - INTERVAL '{interval}'
            ),
            wallet_token_stats AS (
                SELECT 
                    pt.wallet_id,
                    pt.token_id,
                    SUM(CASE WHEN pt.tx_type = 'buy' THEN pt.sol_amount ELSE 0 END) AS bought_sol,
                    SUM(CASE WHEN pt.tx_type = 'sell' THEN pt.sol_amount ELSE 0 END) AS sold_sol,
                    SUM(CASE WHEN pt.tx_type = 'buy' THEN pt.token_amount ELSE 0 END) AS bought_tokens,
                    SUM(CASE WHEN pt.tx_type = 'sell' THEN pt.token_amount ELSE 0 END) AS sold_tokens,
                    COUNT(*) AS txn_count,
                    COUNT(*) FILTER (WHERE pt.tx_type = 'buy') AS buy_count,
                    COUNT(*) FILTER (WHERE pt.tx_type = 'sell') AS sell_count
                FROM period_txns pt
                GROUP BY pt.wallet_id, pt.token_id
            ),
            wallet_stats AS (
                SELECT 
                    wts.wallet_id,
                    SUM(wts.txn_count) AS total_trades,
                    SUM(wts.bought_sol) AS total_invested,
                    SUM(wts.sold_sol) AS total_realized,
                    SUM(wts.sold_sol - wts.bought_sol) AS total_pnl,
                    MAX(wts.sold_sol - wts.bought_sol) AS best_trade,
                    MIN(wts.sold_sol - wts.bought_sol) AS worst_trade,
                    COUNT(DISTINCT wts.token_id) AS tokens_traded,
                    
                    -- Win rate: tokens con ciclo completo (buy+sell) donde ganó
                    CASE WHEN COUNT(*) FILTER (WHERE wts.buy_count > 0 AND wts.sell_count > 0) > 0 
                         THEN ROUND((COUNT(*) FILTER (WHERE wts.buy_count > 0 AND wts.sell_count > 0 AND wts.sold_sol > wts.bought_sol)::numeric / 
                                    COUNT(*) FILTER (WHERE wts.buy_count > 0 AND wts.sell_count > 0)::numeric) * 100, 2)
                         ELSE NULL 
                    END AS real_winrate,
                    
                    -- ROI
                    CASE WHEN SUM(wts.bought_sol) > 0 
                         THEN ROUND(((SUM(wts.sold_sol - wts.bought_sol) / SUM(wts.bought_sol)) * 100)::numeric, 2)
                         ELSE 0 
                    END AS roi_percentage
                    
                FROM wallet_token_stats wts
                GROUP BY wts.wallet_id
            )
            SELECT 
                w.wallet_address,
                ws.total_trades::integer,
                ws.total_pnl AS total_profit_loss,
                ws.total_invested,
                ws.total_realized,
                ws.real_winrate,
                CASE WHEN ws.tokens_traded > 0 
                     THEN ROUND((ws.total_pnl / ws.tokens_traded)::numeric, 4) 
                     ELSE 0 
                END AS avg_profit_per_trade,
                ws.best_trade,
                ws.worst_trade,
                w.first_seen,
                w.last_seen,
                w.is_active,
                w.tags,
                ws.roi_percentage,
                NULL AS open_positions,
                NULL AS total_unrealized_pnl,
                ws.tokens_traded,
                
                -- CLASIFICACIÓN
                wc.behavior_type,
                wc.consistency_level,
                wc.profit_tier,
                wc.investor_type,
                wc.investor_score,
                wc.investor_label
                
            FROM wallet_stats ws
            JOIN wallets w ON ws.wallet_id = w.wallet_id
            LEFT JOIN wallet_classifications wc ON w.wallet_id = wc.wallet_id
            WHERE ws.total_trades >= %s
            ORDER BY {sortcol} {order_dir} NULLS LAST
            LIMIT %s
            """
            cursor.execute(query, (mintrades, limit))
        
        rows = cursor.fetchall()
        traders = []
        
        for row in rows:
            lastseen = row['last_seen']
            activitystr = format_age(lastseen) if lastseen else '??'
            
            # Construcción del objeto classification
            classification = None
            if row.get('behavior_type'):
                classification = {
                    'behavior': row['behavior_type'],
                    'consistency': row['consistency_level'],
                    'profittier': row['profit_tier'],
                    'investortype': row['investor_type'],
                    'investorscore': int(row['investor_score'] or 0),
                    'label': row['investor_label']
                }
            
            traders.append({
                'walletaddress': row['wallet_address'],
                'totaltrades': int(row['total_trades'] or 0),
                'totalpnl': float(row['total_profit_loss']) if row['total_profit_loss'] is not None else None,
                'totalinvested': float(row['total_invested']) if row['total_invested'] is not None else None,
                'totalrealized': float(row['total_realized']) if row['total_realized'] is not None else None,
                'winrate': float(row['real_winrate']) if row['real_winrate'] is not None else None,
                'avgprofitpertrade': float(row['avg_profit_per_trade']) if row['avg_profit_per_trade'] is not None else None,
                'besttrade': float(row['best_trade']) if row['best_trade'] is not None else None,
                'worsttrade': float(row['worst_trade']) if row['worst_trade'] is not None else None,
                'roipercentage': float(row['roi_percentage']) if row['roi_percentage'] is not None else None,
                'openpositions': int(row['open_positions'] or 0),
                'unrealizedpnl': float(row['total_unrealized_pnl']) if row['total_unrealized_pnl'] is not None else None,
                'tokenstraded': int(row['tokens_traded'] or 0),
                'tags': row['tags'] or '',
                'isactive': row['is_active'],
                'firstseen': row['first_seen'].isoformat() if row['first_seen'] else None,
                'lastseen': row['last_seen'].isoformat() if row['last_seen'] else None,
                'lastactivity': activitystr,
                'classification': classification
            })
        
        return jsonify({
            'success': True,
            'count': len(traders),
            'timerange': timerange,
            'traders': traders
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# REQ2: Detalle de un trader específico
# ============================================================================
@app.route('/api/trader/<walletaddress>', methods=['GET'])
def get_trader_detail(walletaddress):
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Wallet info + clasificación
        cursor.execute("""
            SELECT w.*, 
                   wc.behavior_type, wc.consistency_level, wc.profit_tier,
                   wc.investor_type, wc.investor_score, wc.investor_label
            FROM wallets w
            LEFT JOIN wallet_classifications wc ON w.wallet_id = wc.wallet_id
            WHERE w.wallet_address = %s
        """, (walletaddress,))
        wallet = cursor.fetchone()
        
        if not wallet:
            return jsonify({'success': False, 'error': 'Wallet not found'}), 404
        
        # Posiciones (top 50)
        cursor.execute("""
            SELECT 
                t.symbol, t.name, t.mint_address,
                wp.total_bought, wp.total_sold, wp.current_balance,
                wp.avg_buy_price, wp.realized_pnl, wp.unrealized_pnl, wp.status,
                wp.first_buy, wp.last_sell
            FROM wallet_positions wp
            JOIN tokens t ON wp.token_id = t.token_id
            WHERE wp.wallet_id = %s
            ORDER BY (wp.realized_pnl + wp.unrealized_pnl) DESC
            LIMIT 50
        """, (wallet['wallet_id'],))
        positions = cursor.fetchall()
        
        # Construir classification object
        classification = None
        if wallet.get('behavior_type'):
            classification = {
                'behavior': wallet['behavior_type'],
                'consistency': wallet['consistency_level'],
                'profittier': wallet['profit_tier'],
                'investortype': wallet['investor_type'],
                'investorscore': int(wallet['investor_score'] or 0),
                'label': wallet['investor_label']
            }
        
        walletdict = dict(wallet)
        walletdict['classification'] = classification
        
        return jsonify({
            'success': True,
            'wallet': walletdict,
            'positions': [dict(p) for p in positions]
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# REQ3: Lista de tokens con breakdown de inversores
# ============================================================================
@app.route('/api/tokens', methods=['GET'])
def get_tokens():
    sortfield = request.args.get('sort', 'volume')
    order = request.args.get('order', 'desc').upper()
    limit = min(int(request.args.get('limit', 50)), 200)
    status = request.args.get('status', 'active')
    
    sortmap = {
        'volume': 'va.vol_24h',
        'mcap': 'latest.market_cap',
        'price': 'latest.price',
        'txns': 'latest.transactions_count',
        'makers': 'latest.holders_count',
        'liquidity': 'latest.liquidity',
        'age': 't.created_at',
        'change5m': 'pct_5m',
        'change1h': 'pct_1h',
        'change6h': 'pct_6h',
        'change24h': 'pct_24h',
        'investors': 'inv.total_investors',
    }
    sortcol = sortmap.get(sortfield, 'latest.volume_24h')
    order_dir = 'DESC' if order == 'DESC' else 'ASC'
    
    conn = get_db()
    cursor = conn.cursor()
    
    query = f"""
    WITH latest_metrics AS (
        SELECT DISTINCT ON (token_id)
            token_id, time, price, liquidity,
            market_cap, fdv, holders_count, transactions_count
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '30 minutes'
        ORDER BY token_id, time DESC
    ),
    volume_agg AS (
        SELECT 
            token_id,
            COALESCE(SUM(volume_10m) FILTER (WHERE time >= NOW() - INTERVAL '1 hour'), 0) AS vol_1h,
            COALESCE(SUM(volume_10m) FILTER (WHERE time >= NOW() - INTERVAL '24 hours'), 0) AS vol_24h,
            COALESCE(SUM(swap_count) FILTER (WHERE time >= NOW() - INTERVAL '24 hours'), 0) AS txns_24h_metrics
        FROM token_metrics
        WHERE time >= NOW() - INTERVAL '24 hours'
        GROUP BY token_id
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
        latest.price AS current_price,
        latest.liquidity,
        COALESCE(va.vol_24h, 0) AS volume_24h,
        latest.market_cap,
        latest.fdv,
        latest.holders_count,
        latest.transactions_count,
        COALESCE(tc.txns_24h, latest.transactions_count, 0) AS txns,
        COALESCE(tc.makers_24h, latest.holders_count, 0) AS makers,
        
        -- Price changes
        CASE WHEN p5.price > 0 THEN ROUND(((latest.price - p5.price) / p5.price * 100)::numeric, 2) ELSE NULL END AS pct_5m,
        CASE WHEN p1h.price > 0 THEN ROUND(((latest.price - p1h.price) / p1h.price * 100)::numeric, 2) ELSE NULL END AS pct_1h,
        CASE WHEN p6h.price > 0 THEN ROUND(((latest.price - p6h.price) / p6h.price * 100)::numeric, 2) ELSE NULL END AS pct_6h,
        CASE WHEN p24h.price > 0 THEN ROUND(((latest.price - p24h.price) / p24h.price * 100)::numeric, 2) ELSE NULL END AS pct_24h,
        
        -- INVESTORS BREAKDOWN
        inv.total_investors,
        inv.elite_count,
        inv.profitable_count,
        inv.regular_count,
        inv.human_count,
        inv.bot_count,
        inv.avg_score
        
    FROM tokens t
    JOIN latest_metrics latest ON t.token_id = latest.token_id
    LEFT JOIN volume_agg va ON t.token_id = va.token_id
    LEFT JOIN price_5m p5 ON t.token_id = p5.token_id
    LEFT JOIN price_1h p1h ON t.token_id = p1h.token_id
    LEFT JOIN price_6h p6h ON t.token_id = p6h.token_id
    LEFT JOIN price_24h p24h ON t.token_id = p24h.token_id
    LEFT JOIN txn_counts tc ON t.token_id = tc.token_id
    LEFT JOIN LATERAL (
        SELECT 
            COUNT(DISTINCT wc.wallet_id) AS total_investors,
            COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'elite') AS elite_count,
            COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'profitable') AS profitable_count,
            COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'regular') AS regular_count,
            COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.behavior_type = 'human') AS human_count,
            COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.behavior_type = 'bot') AS bot_count,
            ROUND(AVG(wc.investor_score)::numeric, 1) AS avg_score
        FROM wallet_positions wp
        JOIN wallet_classifications wc ON wc.wallet_id = wp.wallet_id
        WHERE wp.token_id = t.token_id
          AND wp.status != 'closed'
          AND wp.current_balance > 0
    ) inv ON true
    WHERE t.status = %s
      AND latest.price IS NOT NULL
      AND latest.price > 0
    ORDER BY {sortcol} {order_dir} NULLS LAST
    LIMIT %s
    """
    
    try:
        cursor.execute(query, (status, limit))
        rows = cursor.fetchall()
        
        tokens = []
        for row in rows:
            created = row['created_at']
            mint = row['mint_address']
            
            # Fix: Token name/symbol if NULL, use abbreviated mint
            displayname = row['name']
            displaysymbol = row['symbol']
            if not displayname or displayname in ['???', 'Unknown']:
                displayname = f"{mint[:6]}...{mint[-4:]}"
            if not displaysymbol or displaysymbol in ['???']:
                displaysymbol = f"{mint[:6]}...{mint[-4:]}"
            
            # Investors object
            investors = {
                'total': int(row['total_investors'] or 0),
                'elite': int(row['elite_count'] or 0),
                'profitable': int(row['profitable_count'] or 0),
                'regular': int(row['regular_count'] or 0),
                'humans': int(row['human_count'] or 0),
                'bots': int(row['bot_count'] or 0),
                'avgscore': float(row['avg_score'] or 0)
            }
            
            tokens.append({
                'tokenid': row['token_id'],
                'mintaddress': mint,
                'name': displayname,
                'symbol': displaysymbol,
                'imageurl': row['image_url'],
                'amm': row['amm'] if row['amm'] != 'auto-discovered' else None,
                'age': format_age(created) if created else '??',
                'createdat': created.isoformat() if created else None,
                'price': float(row['current_price']) if row['current_price'] else 0,
                'liquidity': float(row['liquidity']) if row['liquidity'] else 0,
                'volume24h': float(row['volume_24h']) if row['volume_24h'] else 0,
                'marketcap': float(row['market_cap']) if row['market_cap'] else 0,
                'fdv': float(row['fdv']) if row['fdv'] else 0,
                'txns': int(row['txns'] or 0),
                'makers': int(row['makers'] or 0),
                'pct5m': float(row['pct_5m']) if row['pct_5m'] is not None else None,
                'pct1h': float(row['pct_1h']) if row['pct_1h'] is not None else None,
                'pct6h': float(row['pct_6h']) if row['pct_6h'] is not None else None,
                'pct24h': float(row['pct_24h']) if row['pct_24h'] is not None else None,
                'investors': investors
            })
        
        return jsonify({'success': True, 'count': len(tokens), 'tokens': tokens})
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# REQ4: Estadísticas generales con clasificaciones
# ============================================================================
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
        
        # Clasificaciones
        cursor.execute("""
            SELECT 
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE behavior_type = 'human') AS humans,
                COUNT(*) FILTER (WHERE behavior_type = 'bot') AS bots,
                COUNT(*) FILTER (WHERE investor_type = 'elite') AS elite,
                COUNT(*) FILTER (WHERE investor_type = 'profitable') AS profitable
            FROM wallet_classifications
        """)
        classrow = cursor.fetchone()
        classifications = {
            'total': classrow['total'],
            'humans': classrow['humans'],
            'bots': classrow['bots'],
            'elite': classrow['elite'],
            'profitable': classrow['profitable']
        }
        
        return jsonify({
            'success': True,
            'totaltokens': total_tokens,
            'totalwallets': total_wallets,
            'transactions24h': txns_24h,
            'classifications': classifications
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# REQ5: Inversores de un token específico
# ============================================================================
@app.route('/api/token/<mintaddress>/investors', methods=['GET'])
def get_token_investors(mintaddress):
    """
    REQ5: Inversores activos en un token específico, desglosados por tipo.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                t.token_id, t.symbol, t.name,
                
                -- Conteos por tipo
                COUNT(DISTINCT wc.wallet_id) AS total_investors,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'elite') AS elite,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'profitable') AS profitable,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'regular') AS regular,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'bot-profitable') AS bot_profitable,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'bot-regular') AS bot_regular,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.investor_type = 'losing') AS losing,
                
                -- Bot vs Human
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.behavior_type = 'human') AS humans,
                COUNT(DISTINCT wc.wallet_id) FILTER (WHERE wc.behavior_type = 'bot') AS bots,
                
                -- Score promedio
                ROUND(AVG(wc.investor_score)::numeric, 1) AS avg_score
                
            FROM tokens t
            JOIN wallet_positions wp ON wp.token_id = t.token_id
            JOIN wallet_classifications wc ON wc.wallet_id = wp.wallet_id
            WHERE t.mint_address = %s
              AND wp.status != 'closed'
              AND wp.current_balance > 0
            GROUP BY t.token_id, t.symbol, t.name
        """, (mintaddress,))
        
        row = cursor.fetchone()
        
        if not row:
            return jsonify({'success': True, 'investors': {
                'total': 0, 'elite': 0, 'profitable': 0, 'regular': 0,
                'bot_profitable': 0, 'bot_regular': 0, 'losing': 0,
                'humans': 0, 'bots': 0, 'avg_score': 0
            }})
        
        return jsonify({'success': True, 'investors': {
            'total': row['total_investors'],
            'elite': row['elite'],
            'profitable': row['profitable'],
            'regular': row['regular'],
            'bot_profitable': row['bot_profitable'],
            'bot_regular': row['bot_regular'],
            'losing': row['losing'],
            'humans': row['humans'],
            'bots': row['bots'],
            'avg_score': float(row['avg_score'] or 0)
        }})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# REQ6: Resumen de tipos de inversores
# ============================================================================
@app.route('/api/investor-types', methods=['GET'])
def get_investor_types():
    """
    Resumen de distribución de tipos de inversores.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                investor_type,
                investor_label,
                COUNT(*) as count,
                ROUND(AVG(investor_score)::numeric, 1) as avg_score,
                ROUND(AVG(avg_daily_pnl_usd)::numeric, 2) as avg_daily_pnl_usd,
                ROUND(AVG(avg_daily_trades)::numeric, 1) as avg_daily_trades,
                ROUND(AVG(active_days_last_week)::numeric, 1) as avg_active_days
            FROM wallet_classifications
            GROUP BY investor_type, investor_label
            ORDER BY AVG(investor_score) DESC
        """)
        rows = cursor.fetchall()
        
        types = []
        for row in rows:
            types.append({
                'type': row['investor_type'],
                'label': row['investor_label'],
                'count': row['count'],
                'avg_score': float(row['avg_score'] or 0),
                'avg_daily_pnl_usd': float(row['avg_daily_pnl_usd'] or 0),
                'avg_daily_trades': float(row['avg_daily_trades'] or 0),
                'avg_active_days': float(row['avg_active_days'] or 0),
            })
        
        return jsonify({'success': True, 'types': types})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# Utilities
# ============================================================================
def format_age(dt):
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
        return f'{int(seconds)}s'
    elif seconds < 3600:
        return f'{int(seconds/60)}m'
    elif seconds < 86400:
        return f'{int(seconds/3600)}h'
    elif seconds < 2592000:
        return f'{int(seconds/86400)}d'
    else:
        return f'{int(seconds/2592000)}mo'

# ============================================================================
# Run
# ============================================================================
if __name__ == '__main__':
    port = 8200
    print(f'✅ API Server v3 iniciando en http://localhost:{port}')
    print('Endpoints:')
    print('  GET /api/top-traders?timerange=24h&sort=score  - Top traders con clasificación')
    print('  GET /api/trader/<wallet>                      - Detalle de trader con clasificación')
    print('  GET /api/tokens?sort=investors                - Tokens con breakdown de inversores')
    print('  GET /api/investor-types                        - Resumen de tipos de inversores')
    print('  GET /api/token/<mint>/investors                - Inversores de un token específico')
    print('  GET /api/stats                                 - Stats con clasificaciones')
    app.run(host='0.0.0.0', port=port, debug=True)
