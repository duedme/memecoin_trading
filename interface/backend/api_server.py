import os
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request
from flask_cors import CORS

DB_CONFIG = {
    "dbname":   os.getenv("DB_NAME", "memecoins"),
    "user":     os.getenv("DB_USER", "memecoin"),
    "password": os.getenv("DB_PASSWORD", "changeme"),
    "host":     os.getenv("DB_HOST", "db"),
    "port":     int(os.getenv("DB_PORT", "5432")),
}

# ---------- Tokens ----------
TOKEN_SORT_MAP = {
    "volume":    "m.volume_24h",
    "mcap":      "m.market_cap_usd",
    "change24h": "m.change_24h",
    "txns":      "m.txns_24h",
    "age":       "t.detected_at",
    "investors": "inv_total",
}

app = Flask(__name__)
CORS(app)

def db():
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

            cur.execute("SELECT COUNT(*) FROM wallettransactions WHERE time > NOW() - INTERVAL '24 hours'")
            tx24 = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE behavior='human') AS humans,
                    COUNT(*) FILTER (WHERE behavior='bot') AS bots,
                    COUNT(*) FILTER (WHERE investortype='elite') AS elite
                FROM walletclassifications
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

# ---------- Top Traders ----------
TRADER_SORT_MAP = {
    "pnl":       "total_pnl_sol",
    "winrate":   "winrate",
    "roi":       "roipercentage",
    "score":     "investorscore",
    "trades":    "totaltrades",
    "invested":  "investedsol",
    "besttrade": "besttrade",
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
                        wt.walletaddress,
                        COUNT(*) AS totaltrades,
                        COUNT(DISTINCT wt.mintaddress) AS tokenstraded,
                        SUM(CASE WHEN wt.side='buy'  THEN wt.amountsol ELSE 0 END) AS investedsol,
                        SUM(CASE WHEN wt.side='sell' THEN wt.amountsol ELSE 0 END) AS realizedsol,
                        MAX(wt.amountsol) AS besttrade,
                        MIN(wt.amountsol) AS worsttrade,
                        MAX(wt.time)       AS lastactivity
                      FROM wallettransactions wt
                     WHERE TRUE {time_filter}
                     GROUP BY wt.walletaddress
                )
                SELECT a.*,
                       (a.realizedsol - a.investedsol) AS total_pnl_sol,
                       CASE WHEN a.investedsol > 0
                            THEN ((a.realizedsol - a.investedsol) / a.investedsol) * 100
                            ELSE 0 END AS roipercentage,
                       COALESCE(c.winrate, 0)       AS winrate,
                       COALESCE(c.investorscore, 0) AS investorscore,
                       c.behavior, c.investortype,
                       w.tags
                  FROM agg a
                  LEFT JOIN walletclassifications c ON c.walletaddress = a.walletaddress
                  LEFT JOIN wallets w               ON w.walletaddress = a.walletaddress
                 WHERE a.totaltrades >= %s
                 ORDER BY {sort_col} {order_sql} NULLS LAST
                 LIMIT %s
            """, (min_trades, limit))
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "walletaddress": r["walletaddress"],
                "totaltrades":   int(r["totaltrades"] or 0),
                "tokenstraded":  int(r["tokenstraded"] or 0),
                "totalinvested": float(r["investedsol"] or 0),
                "totalrealized": float(r["realizedsol"] or 0),
                "totalpnl":      float(r["total_pnl_sol"] or 0),
                "roipercentage": float(r["roipercentage"] or 0),
                "winrate":       float(r["winrate"] or 0),
                "besttrade":     float(r["besttrade"] or 0),
                "worsttrade":    float(r["worsttrade"] or 0),
                "lastactivity":  r["lastactivity"].isoformat() if r["lastactivity"] else None,
                "tags":          r["tags"],
                "classification": {
                    "behavior":      r["behavior"]      or "unclassified",
                    "investortype":  r["investortype"]  or "unclassified",
                    "investorscore": int(r["investorscore"] or 0),
                },
            })

        return jsonify({"success": True, "count": len(out), "traders": out})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ---------- Tokens ----------
@app.route("/api/tokens")
def tokens():
    sort  = request.args.get("sort",  "volume")
    order = request.args.get("order", "desc").lower()
    limit = int(request.args.get("limit", "50"))

    # Mapeamos los filtros del frontend a las columnas calculadas
    sort_map = {
        "volume": "volume_24h",
        "txns": "txns_24h",
        "investors": "inv_total",
        "newest": "detected_at"
    }
    sort_col = sort_map.get(sort, "volume_24h")
    order_sql = "DESC" if order == "desc" else "ASC"

    try:
        # Consulta a prueba de balas: Solo lee de wallettransactions
        with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT 
                    wt.mintaddress AS mint_address,
                    wt.mintaddress AS symbol,
                    'Token Detectado' AS name,
                    MIN(wt.time) AS detected_at,
                    SUM(wt.amountsol) AS volume_24h,
                    COUNT(wt.signature) AS txns_24h,
                    COUNT(DISTINCT wt.walletaddress) AS inv_total,
                    
                    COUNT(DISTINCT CASE WHEN c.investortype='elite' THEN wt.walletaddress END) AS elite,
                    COUNT(DISTINCT CASE WHEN c.investortype='profitable' THEN wt.walletaddress END) AS profitable,
                    COUNT(DISTINCT CASE WHEN c.investortype='regular' THEN wt.walletaddress END) AS regular,
                    COUNT(DISTINCT CASE WHEN c.investortype='losing' THEN wt.walletaddress END) AS losing,
                    COUNT(DISTINCT CASE WHEN c.behavior='human' THEN wt.walletaddress END) AS humans,
                    COUNT(DISTINCT CASE WHEN c.behavior='bot' THEN wt.walletaddress END) AS bots
                    
                FROM wallettransactions wt
                LEFT JOIN walletclassifications c ON c.walletaddress = wt.walletaddress
                GROUP BY wt.mintaddress
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
                "amm":         "Pump.fun",
                "detectedat":  r["detected_at"].isoformat() if r["detected_at"] else None,
                "pricesol":    0.0,
                "priceusd":    0.0,
                "liquiditysol":0.0,
                "marketcap":   0.0,
                "volume5m":    0.0,
                "volume1h":    0.0,
                "volume6h":    0.0,
                "volume24h":   float(r["volume_24h"] or 0),
                "change5m":    0.0,
                "change1h":    0.0,
                "change6h":    0.0,
                "change24h":   0.0,
                "txns24h":     int(r["txns_24h"] or 0),
                "makers24h":   0,
                "investors": {
                    "total":      int(r["inv_total"] or 0),
                    "humans":     int(r["humans"] or 0),
                    "bots":       int(r["bots"] or 0),
                    "elite":      int(r["elite"] or 0),
                    "profitable": int(r["profitable"] or 0),
                    "regular":    int(r["regular"] or 0),
                    "losing":     int(r["losing"] or 0),
                },
            })

        return jsonify({"success": True, "count": len(out), "tokens": out})
    except Exception as e:
        print(f"Error en /api/tokens: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8200)