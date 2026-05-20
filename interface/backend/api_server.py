import os
import math
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

DB_CONFIG = {
    "dbname":   os.getenv("DB_NAME", "memecoins"),
    "user":     os.getenv("DB_USER", "memecoin"),
    "password": os.getenv("DB_PASSWORD", "changeme"),
    "host":     os.getenv("DB_HOST", "db"),
    "port":     int(os.getenv("DB_PORT", "5432")),
}

FRONTEND_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "frontend")
)

app = Flask(__name__, static_folder=None)
CORS(app)

def db():
    # Buscador de Conexión Inteligente
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError:
        print(f"⚠️ Falló conexión a {DB_CONFIG.get('host')}. Intentando host de Docker ('db')...")
        docker_config = DB_CONFIG.copy()
        docker_config['host'] = 'db'
        conn = psycopg2.connect(**docker_config)
        
    conn.autocommit = True
    return conn

# ---------- Stats ----------
@app.route("/api/stats")
def stats():
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tokens WHERE status='active'")
            total_tokens = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM wallets")
            total_wallets = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM wallet_transactions
                 WHERE time > NOW() - INTERVAL '24 hours'
            """)
            tx24 = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    COUNT(*)                                     AS total,
                    COUNT(*) FILTER (WHERE behavior='human')     AS humans,
                    COUNT(*) FILTER (WHERE behavior='bot')       AS bots,
                    COUNT(*) FILTER (WHERE investor_type='elite') AS elite
                FROM wallet_classifications
            """)
            total, humans, bots, elite = cur.fetchone()

        return jsonify({
            "success": True,
            "totaltokens": total_tokens,
            "totalwallets": total_wallets,
            "transactions24h": tx24,
            "classifications": {
                "total": total, "humans": humans, "bots": bots, "elite": elite,
            },
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ---------- Tokens ----------
TOKEN_SORT_MAP = {
    "volume":    "m.volume_24h",
    "mcap":      "m.market_cap_usd",
    "change24h": "m.change_24h",
    "txns":      "m.txns_24h",
    "age":       "t.detected_at",
    "investors": "inv_total",
}

@app.route("/api/tokens")
def tokens():
    sort  = request.args.get("sort",  "volume")
    order = request.args.get("order", "desc").lower()
    limit = int(request.args.get("limit", "50"))

    sort_col = TOKEN_SORT_MAP.get(sort, "m.volume_24h")
    order_sql = "DESC" if order == "desc" else "ASC"

    try:
        with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                WITH latest AS (
                    SELECT DISTINCT ON (mint_address) *
                      FROM token_metrics
                     ORDER BY mint_address, time DESC
                ),
                inv AS (
                    SELECT wp.mint_address,
                           COUNT(*) AS inv_total,
                           COUNT(*) FILTER (WHERE wc.behavior='human')        AS humans,
                           COUNT(*) FILTER (WHERE wc.behavior='bot')          AS bots,
                           COUNT(*) FILTER (WHERE wc.investor_type='elite')       AS elite,
                           COUNT(*) FILTER (WHERE wc.investor_type='profitable')  AS profitable,
                           COUNT(*) FILTER (WHERE wc.investor_type='regular')     AS regular,
                           COUNT(*) FILTER (WHERE wc.investor_type='losing')      AS losing
                      FROM wallet_positions wp
                      LEFT JOIN wallet_classifications wc
                             ON wc.wallet_address = wp.wallet_address
                     WHERE wp.amount_token > 0
                     GROUP BY wp.mint_address
                )
                SELECT t.mint_address, t.symbol, t.name, t.detected_at, t.amm,
                       m.price_sol, m.price_usd, m.liquidity_sol, m.market_cap_usd,
                       m.volume_5m, m.volume_1h, m.volume_6h, m.volume_24h,
                       m.change_5m, m.change_1h, m.change_6h, m.change_24h,
                       m.txns_24h, m.makers_24h,
                       COALESCE(inv.inv_total, 0)   AS inv_total,
                       COALESCE(inv.humans, 0)      AS humans,
                       COALESCE(inv.bots, 0)        AS bots,
                       COALESCE(inv.elite, 0)       AS elite,
                       COALESCE(inv.profitable, 0)  AS profitable,
                       COALESCE(inv.regular, 0)     AS regular,
                       COALESCE(inv.losing, 0)      AS losing
                  FROM tokens t
                  LEFT JOIN latest m ON m.mint_address = t.mint_address
                  LEFT JOIN inv    ON inv.mint_address = t.mint_address
                 WHERE t.status = 'active'
                 ORDER BY {sort_col} {order_sql} NULLS LAST
                 LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "mintaddress": r["mint_address"],
                "symbol":      r["symbol"],
                "name":        r["name"],
                "amm":         r["amm"],
                "detectedat":  r["detected_at"].isoformat() if r["detected_at"] else None,
                "pricesol":    r["price_sol"],
                "priceusd":    r["price_usd"],
                "liquiditysol":r["liquidity_sol"],
                "marketcap":   r["market_cap_usd"],
                "volume5m":    r["volume_5m"],
                "volume1h":    r["volume_1h"],
                "volume6h":    r["volume_6h"],
                "volume24h":   r["volume_24h"],
                "change5m":    r["change_5m"],
                "change1h":    r["change_1h"],
                "change6h":    r["change_6h"],
                "change24h":   r["change_24h"],
                "txns24h":     r["txns_24h"],
                "makers24h":   r["makers_24h"],
                "investors": {
                    "total":      int(r["inv_total"]),
                    "humans":     int(r["humans"]),
                    "bots":       int(r["bots"]),
                    "elite":      int(r["elite"]),
                    "profitable": int(r["profitable"]),
                    "regular":    int(r["regular"]),
                    "losing":     int(r["losing"]),
                },
            })

        return jsonify({"success": True, "count": len(out), "tokens": out})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ---------- Top Traders ----------
TRADER_SORT_MAP = {
    "pnl":       "total_pnl_sol",
    "winrate":   "win_rate",
    "roi":       "roi_percentage",
    "score":     "investor_score",
    "trades":    "total_trades",
    "invested":  "invested_sol",
    "besttrade": "best_trade",
}

@app.route("/api/top-traders")
def top_traders():
    sort       = request.args.get("sort", "pnl")
    order      = request.args.get("order", "desc").lower()
    limit      = int(request.args.get("limit", "50"))
    min_trades = int(request.args.get("mintrades", "3"))
    time_range = request.args.get("timerange", "all")

    interval_map = {
        "1h":  "1 hour", "6h":  "6 hours", "24h": "24 hours",
        "7d":  "7 days", "30d": "30 days",
    }
    time_filter = ""
    if time_range in interval_map:
        time_filter = f"AND wt.time > NOW() - INTERVAL '{interval_map[time_range]}'"

    sort_col  = TRADER_SORT_MAP.get(sort, "total_pnl_sol")
    order_sql = "DESC" if order == "desc" else "ASC"

    try:
        with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                WITH agg AS (
                    SELECT
                        wt.wallet_address,
                        COUNT(*) AS total_trades,
                        COUNT(DISTINCT wt.mint_address) AS tokens_traded,
                        SUM(CASE WHEN wt.side='buy'  THEN wt.amount_sol ELSE 0 END) AS invested_sol,
                        SUM(CASE WHEN wt.side='sell' THEN wt.amount_sol ELSE 0 END) AS realized_sol,
                        MAX(wt.amount_sol) AS best_trade,
                        MIN(wt.amount_sol) AS worst_trade,
                        MAX(wt.time)       AS last_activity
                      FROM wallet_transactions wt
                     WHERE TRUE {time_filter}
                     GROUP BY wt.wallet_address
                )
                SELECT a.*,
                       (a.realized_sol - a.invested_sol) AS total_pnl_sol,
                       CASE WHEN a.invested_sol > 0
                            THEN ((a.realized_sol - a.invested_sol) / a.invested_sol) * 100
                            ELSE 0 END AS roi_percentage,
                       COALESCE(c.win_rate, 0)       AS win_rate,
                       COALESCE(c.investor_score, 0) AS investor_score,
                       c.behavior, c.investor_type,
                       w.tags
                  FROM agg a
                  LEFT JOIN wallet_classifications c ON c.wallet_address = a.wallet_address
                  LEFT JOIN wallets w                ON w.wallet_address = a.wallet_address
                 WHERE a.total_trades >= %s
                 ORDER BY {sort_col} {order_sql} NULLS LAST
                 LIMIT %s
            """, (min_trades, limit))
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "walletaddress": r["wallet_address"],
                "totaltrades":   int(r["total_trades"] or 0),
                "tokenstraded":  int(r["tokens_traded"] or 0),
                "totalinvested": float(r["invested_sol"] or 0),
                "totalrealized": float(r["realized_sol"] or 0),
                "totalpnl":      float(r["total_pnl_sol"] or 0),
                "roipercentage": float(r["roi_percentage"] or 0),
                "winrate":       float(r["win_rate"] or 0),
                "besttrade":     float(r["best_trade"] or 0),
                "worsttrade":    float(r["worst_trade"] or 0),
                "lastactivity":  r["last_activity"].isoformat() if r["last_activity"] else None,
                "tags":          r["tags"],
                "classification": {
                    "behavior":      r["behavior"]       or "unclassified",
                    "investortype":  r["investor_type"]  or "unclassified",
                    "investorscore": int(r["investor_score"] or 0),
                },
            })

        return jsonify({"success": True, "count": len(out), "traders": out})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8200)