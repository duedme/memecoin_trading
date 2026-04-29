#!/usr/bin/env python3
"""api_server.py v4.1 — FastAPI que SOLO lee de Postgres (caché de Birdeye).

Cambios 29-abr-2026:
- /api/top-traders ahora devuelve tokenstraded real (conteo desde token_top_traders_cache).
- Resto intacto.
"""
import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="Memecoin Screener API v4.1 (Birdeye-backed)")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

DB = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "memecoins_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def qall(sql, params=None):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(sql, params or ())
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def qone(sql, params=None):
    rows = qall(sql, params)
    return rows[0] if rows else None


def format_age(dt):
    if not dt:
        return "??"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    s = int(delta.total_seconds())
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s//60}m"
    if s < 86400: return f"{s//3600}h"
    return f"{s//86400}d"


@app.get("/health")
def health():
    try:
        qone("SELECT 1 AS ok")
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))


# =========================================================
# /api/top-traders
# =========================================================
@app.get("/api/top-traders")
def top_traders(
    sort: str = "pnl",
    order: str = "desc",
    limit: int = 50,
    mintrades: int = Query(3, alias="mintrades"),
    timerange: str = "all",
):
    sort_map = {
        "pnl": "pc.total_pnl_usd",
        "winrate": "pc.win_rate",
        "roi": "pc.roi_pct",
        "trades": "pc.trade_count",
        "score": "wc.investor_score",
        "besttrade": "tt.best_trade_usd",
        "worsttrade": "tt.worst_trade_usd",
        "invested": "pc.unrealized_pnl_usd",
        "realized": "pc.realized_pnl_usd",
        "tokens": "tt.tokens_traded",
    }
    col = sort_map.get(sort, "pc.total_pnl_usd")
    direction = "DESC" if order.lower() == "desc" else "ASC"
    limit = min(max(int(limit), 1), 200)

    rows = qall(f"""
        SELECT pc.wallet_address, pc.total_pnl_usd, pc.realized_pnl_usd,
               pc.unrealized_pnl_usd, pc.roi_pct, pc.trade_count,
               pc.win_rate, pc.last_updated,
               w.first_seen, w.last_seen, w.tags, w.is_active,
               wc.behavior_type, wc.consistency_level, wc.profit_tier,
               wc.investor_type, wc.investor_score, wc.investor_label,
               COALESCE(tt.tokens_traded, 0) AS tokens_traded,
               tt.best_trade_usd,
               tt.worst_trade_usd
        FROM wallet_pnl_cache pc
        LEFT JOIN wallets w ON w.wallet_address = pc.wallet_address
        LEFT JOIN wallet_classifications wc ON wc.wallet_address = pc.wallet_address
        LEFT JOIN (
            SELECT wallet_address,
                   COUNT(DISTINCT token_id) AS tokens_traded,
                   MAX(total_pnl)           AS best_trade_usd,
                   MIN(total_pnl)           AS worst_trade_usd
            FROM token_top_traders_cache
            GROUP BY wallet_address
        ) tt ON tt.wallet_address = pc.wallet_address
        WHERE COALESCE(pc.trade_count, 0) >= %s
        ORDER BY {col} {direction} NULLS LAST
        LIMIT %s
    """, (mintrades, limit))

    traders = []
    for r in rows:
        classification = None
        if r.get("behavior_type") or r.get("investor_type"):
            classification = {
                "behavior": r.get("behavior_type"),
                "consistency": r.get("consistency_level"),
                "profittier": r.get("profit_tier"),
                "investortype": r.get("investor_type"),
                "investorscore": int(r.get("investor_score") or 0),
                "label": r.get("investor_label"),
            }
        traders.append({
            "walletaddress": r["wallet_address"],
            "totaltrades": int(r.get("trade_count") or 0),
            "totalpnl": float(r["total_pnl_usd"]) if r.get("total_pnl_usd") is not None else 0,
            "totalinvested": None,
            "totalrealized": float(r["realized_pnl_usd"]) if r.get("realized_pnl_usd") is not None else None,
            "winrate": float(r["win_rate"]) if r.get("win_rate") is not None else 0,
            "avgprofitpertrade": None,
            "besttrade": float(r["best_trade_usd"])  if r.get("best_trade_usd")  is not None else None,
            "worsttrade": float(r["worst_trade_usd"]) if r.get("worst_trade_usd") is not None else None,
            "roipercentage": float(r["roi_pct"]) if r.get("roi_pct") is not None else 0,
            "openpositions": 0,
            "unrealizedpnl": float(r["unrealized_pnl_usd"]) if r.get("unrealized_pnl_usd") is not None else None,
            "tokenstraded": int(r.get("tokens_traded") or 0),
            "tags": r.get("tags") or "",
            "isactive": bool(r.get("is_active")) if r.get("is_active") is not None else True,
            "firstseen": r["first_seen"].isoformat() if r.get("first_seen") else None,
            "lastseen": r["last_seen"].isoformat() if r.get("last_seen") else None,
            "lastactivity": format_age(r.get("last_updated")),
            "classification": classification,
        })
    return {"success": True, "count": len(traders), "timerange": timerange, "traders": traders}


# =========================================================
# /api/trader/<wallet>
# =========================================================
@app.get("/api/trader/{wallet}")
def trader_detail(wallet: str):
    r = qone("""
        SELECT pc.*, wc.behavior_type, wc.consistency_level, wc.profit_tier,
               wc.investor_type, wc.investor_score, wc.investor_label,
               w.tags, w.first_seen, w.last_seen, w.is_active
        FROM wallet_pnl_cache pc
        LEFT JOIN wallets w ON w.wallet_address = pc.wallet_address
        LEFT JOIN wallet_classifications wc ON wc.wallet_address = pc.wallet_address
        WHERE pc.wallet_address = %s
    """, (wallet,))
    if not r:
        conn = psycopg2.connect(**DB); cur = conn.cursor()
        cur.execute("""
            INSERT INTO wallet_sync_queue (wallet_address, priority, next_sync_at)
            VALUES (%s, 1, NOW()) ON CONFLICT DO NOTHING
        """, (wallet,))
        conn.commit(); cur.close(); conn.close()
        return JSONResponse({"success": False, "error": "Wallet encolada, disponible en 1-2 min"}, status_code=202)

    positions = qall("""
        SELECT t.symbol, t.name, t.mint_address,
               ttc.rank, ttc.volume_usd, ttc.total_pnl, ttc.trade_count,
               ttc.last_updated
        FROM token_top_traders_cache ttc
        JOIN tokens t ON t.token_id = ttc.token_id
        WHERE ttc.wallet_address = %s
        ORDER BY ttc.total_pnl DESC NULLS LAST
        LIMIT 50
    """, (wallet,))

    classification = None
    if r.get("investor_type"):
        classification = {
            "behavior": r.get("behavior_type"),
            "consistency": r.get("consistency_level"),
            "profittier": r.get("profit_tier"),
            "investortype": r.get("investor_type"),
            "investorscore": int(r.get("investor_score") or 0),
            "label": r.get("investor_label"),
        }
    return {
        "success": True,
        "wallet": {
            "wallet_address": r["wallet_address"],
            "total_pnl_usd": float(r.get("total_pnl_usd") or 0),
            "realized_pnl_usd": float(r.get("realized_pnl_usd") or 0),
            "unrealized_pnl_usd": float(r.get("unrealized_pnl_usd") or 0),
            "roi_pct": float(r.get("roi_pct") or 0),
            "trade_count": int(r.get("trade_count") or 0),
            "win_rate": float(r.get("win_rate") or 0),
            "last_updated": r["last_updated"].isoformat() if r.get("last_updated") else None,
            "tags": r.get("tags") or "",
            "classification": classification,
        },
        "positions": positions,
    }


# =========================================================
# /api/tokens
# =========================================================
@app.get("/api/tokens")
def tokens(
    sort: str = "volume",
    order: str = "desc",
    limit: int = 50,
    status: str = "active",
):
    sort_map = {
        "volume": "c.volume_24h",
        "mcap": "c.market_cap",
        "price": "c.price_usd",
        "liquidity": "c.liquidity",
        "age": "t.detected_at",
        "txns": "c.volume_24h",
        "investors": "inv.total_investors",
    }
    col = sort_map.get(sort, "c.volume_24h")
    direction = "DESC" if order.lower() == "desc" else "ASC"
    limit = min(max(int(limit), 1), 200)

    rows = qall(f"""
        SELECT t.token_id, t.mint_address, t.name, t.symbol,
               t.detected_at, t.liquidity AS t_liquidity, t.source,
               c.price_usd, c.market_cap, c.fdv, c.liquidity AS c_liquidity,
               c.volume_24h, c.holder_count, c.last_updated,
               inv.total_investors, inv.elite_count, inv.profitable_count,
               inv.regular_count, inv.humans_count, inv.bots_count, inv.avg_score
        FROM tokens t
        LEFT JOIN token_market_cache c ON c.token_id = t.token_id
        LEFT JOIN LATERAL (
            SELECT
              COUNT(*) AS total_investors,
              COUNT(*) FILTER (WHERE wc.investor_type = 'elite') AS elite_count,
              COUNT(*) FILTER (WHERE wc.investor_type = 'profitable') AS profitable_count,
              COUNT(*) FILTER (WHERE wc.investor_type = 'regular') AS regular_count,
              COUNT(*) FILTER (WHERE wc.behavior_type = 'human') AS humans_count,
              COUNT(*) FILTER (WHERE wc.behavior_type = 'bot') AS bots_count,
              ROUND(AVG(wc.investor_score)::numeric, 1) AS avg_score
            FROM token_top_traders_cache ttc
            JOIN wallet_classifications wc ON wc.wallet_address = ttc.wallet_address
            WHERE ttc.token_id = t.token_id
        ) inv ON TRUE
        WHERE t.status = %s
        ORDER BY {col} {direction} NULLS LAST
        LIMIT %s
    """, (status, limit))

    out = []
    for r in rows:
        mint = r["mint_address"]
        name = r.get("name") or f"{mint[:6]}...{mint[-4:]}"
        symbol = r.get("symbol") or f"{mint[:4]}..."
        out.append({
            "tokenid": r["token_id"],
            "mintaddress": mint,
            "name": name,
            "symbol": symbol,
            "imageurl": None,
            "amm": r.get("source"),
            "age": format_age(r.get("detected_at")),
            "createdat": r["detected_at"].isoformat() if r.get("detected_at") else None,
            "price": float(r["price_usd"]) if r.get("price_usd") else 0,
            "liquidity": float(r["c_liquidity"] or r.get("t_liquidity") or 0),
            "volume24h": float(r.get("volume_24h") or 0),
            "marketcap": float(r.get("market_cap") or 0),
            "fdv": float(r.get("fdv") or 0),
            "txns": 0,
            "makers": int(r.get("holder_count") or 0),
            "pct5m": None, "pct1h": None, "pct6h": None, "pct24h": None,
            "investors": {
                "total": int(r.get("total_investors") or 0),
                "elite": int(r.get("elite_count") or 0),
                "profitable": int(r.get("profitable_count") or 0),
                "regular": int(r.get("regular_count") or 0),
                "humans": int(r.get("humans_count") or 0),
                "bots": int(r.get("bots_count") or 0),
                "avgscore": float(r.get("avg_score") or 0),
            },
        })
    return {"success": True, "count": len(out), "tokens": out}


# =========================================================
# /api/stats
# =========================================================
@app.get("/api/stats")
def stats():
    total_tokens = qone("SELECT COUNT(*) AS n FROM tokens WHERE status='active'")["n"]
    total_wallets = qone("SELECT COUNT(*) AS n FROM wallets WHERE is_active=TRUE")["n"]
    total_pnl = qone("SELECT COUNT(*) AS n FROM wallet_pnl_cache WHERE last_updated > NOW() - INTERVAL '24 hours'")["n"]
    cls = qone("""
        SELECT COUNT(*) AS total,
          COUNT(*) FILTER (WHERE behavior_type='human') AS humans,
          COUNT(*) FILTER (WHERE behavior_type='bot') AS bots,
          COUNT(*) FILTER (WHERE investor_type='elite') AS elite,
          COUNT(*) FILTER (WHERE investor_type='profitable') AS profitable
        FROM wallet_classifications
    """) or {}
    return {
        "success": True,
        "totaltokens": total_tokens,
        "totalwallets": total_wallets,
        "transactions24h": total_pnl,
        "classifications": {
            "total": cls.get("total", 0),
            "humans": cls.get("humans", 0),
            "bots": cls.get("bots", 0),
            "elite": cls.get("elite", 0),
            "profitable": cls.get("profitable", 0),
        },
    }


# =========================================================
# /api/token/<mint>/investors
# =========================================================
@app.get("/api/token/{mint}/investors")
def token_investors(mint: str):
    r = qone("""
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE wc.investor_type='elite') AS elite,
          COUNT(*) FILTER (WHERE wc.investor_type='profitable') AS profitable,
          COUNT(*) FILTER (WHERE wc.investor_type='regular') AS regular,
          COUNT(*) FILTER (WHERE wc.investor_type='casual') AS casual,
          COUNT(*) FILTER (WHERE wc.investor_type='losing') AS losing,
          COUNT(*) FILTER (WHERE wc.behavior_type='human') AS humans,
          COUNT(*) FILTER (WHERE wc.behavior_type='bot') AS bots,
          ROUND(AVG(wc.investor_score)::numeric, 1) AS avg_score
        FROM tokens t
        JOIN token_top_traders_cache ttc ON ttc.token_id = t.token_id
        JOIN wallet_classifications wc ON wc.wallet_address = ttc.wallet_address
        WHERE t.mint_address = %s
    """, (mint,)) or {}
    return {"success": True, "investors": {
        "total": r.get("total", 0),
        "elite": r.get("elite", 0),
        "profitable": r.get("profitable", 0),
        "regular": r.get("regular", 0),
        "casual": r.get("casual", 0),
        "losing": r.get("losing", 0),
        "humans": r.get("humans", 0),
        "bots": r.get("bots", 0),
        "avg_score": float(r.get("avg_score") or 0),
    }}


# =========================================================
# /api/usage — consumo diario de Birdeye
# =========================================================
@app.get("/api/usage")
def usage():
    return {"success": True, "days": qall("SELECT * FROM birdeye_usage ORDER BY day DESC LIMIT 14")}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("API_PORT", 8200)))