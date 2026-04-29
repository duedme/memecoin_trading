#!/usr/bin/env python3
"""
api_server.py v4.2
FastAPI que SOLO lee de Postgres (cache de Birdeye).

Cambios 29-abr-2026 (v4.2):
- /api/tokens ahora devuelve pct_1h, pct_6h, pct_24h calculados desde token_price_history.
- pct_5m queda en None hasta confirmar TTL real (<2 min).
- txns sigue en 0 hasta tener fuente.
- /api/top-traders mantiene tokens_traded real desde token_top_traders_cache.
"""
import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="Memecoin Screener API v4.2 (Birdeye-backed)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------
DB = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "memecoins_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def q_all(sql, params=None):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(sql, params or ())
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def q_one(sql, params=None):
    rows = q_all(sql, params)
    return rows[0] if rows else None


def format_age(dt):
    if not dt:
        return "??"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def pct_change(now_price, past_price):
    """% de cambio entre dos precios. Devuelve None si falta dato."""
    if now_price is None or past_price is None:
        return None
    try:
        np_ = float(now_price)
        pp_ = float(past_price)
        if pp_ == 0:
            return None
        return (np_ / pp_ - 1) * 100
    except (TypeError, ValueError):
        return None


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    try:
        q_one("SELECT 1 AS ok")
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))


# -----------------------------------------------------------------------------
# Top Traders
# -----------------------------------------------------------------------------
@app.get("/api/top-traders")
def top_traders(
    sort: str = "pnl",
    order: str = "desc",
    limit: int = 50,
    min_trades: int = Query(3, alias="min_trades"),
    timerange: str = "all",
):
    sortmap = {
        "pnl":       "pc.total_pnl_usd",
        "winrate":   "pc.win_rate",
        "roi":       "pc.roi_pct",
        "trades":    "pc.trade_count",
        "score":     "wc.investor_score",
        "besttrade": "tt.best_trade_usd",
        "worsttrade":"tt.worst_trade_usd",
        "invested":  "pc.unrealized_pnl_usd",
        "realized":  "pc.realized_pnl_usd",
        "tokens":    "tt.tokens_traded",
    }
    col = sortmap.get(sort, "pc.total_pnl_usd")
    direction = "DESC" if order.lower() == "desc" else "ASC"
    limit = min(max(int(limit), 1), 200)

    rows = q_all(f"""
      SELECT
        pc.wallet_address,
        pc.total_pnl_usd, pc.realized_pnl_usd, pc.unrealized_pnl_usd,
        pc.roi_pct, pc.trade_count, pc.win_rate, pc.last_updated,
        w.first_seen, w.last_seen, w.tags, w.is_active,
        wc.behavior_type, wc.consistency_level, wc.profit_tier,
        wc.investor_type, wc.investor_score, wc.investor_label,
        COALESCE(tt.tokens_traded, 0) AS tokens_traded,
        tt.best_trade_usd,
        tt.worst_trade_usd
      FROM wallet_pnl_cache pc
      LEFT JOIN wallets w
        ON w.wallet_address = pc.wallet_address
      LEFT JOIN wallet_classifications wc
        ON wc.wallet_address = pc.wallet_address
      LEFT JOIN (
        SELECT
          wallet_address,
          COUNT(DISTINCT token_id) AS tokens_traded,
          MAX(total_pnl) AS best_trade_usd,
          MIN(total_pnl) AS worst_trade_usd
        FROM token_top_traders_cache
        GROUP BY wallet_address
      ) tt ON tt.wallet_address = pc.wallet_address
      WHERE COALESCE(pc.trade_count, 0) >= %s
      ORDER BY {col} {direction} NULLS LAST
      LIMIT %s
    """, (min_trades, limit))

    traders = []
    for r in rows:
        classification = None
        if r.get("behavior_type") or r.get("investor_type"):
            classification = {
                "behavior":       r.get("behavior_type"),
                "consistency":    r.get("consistency_level"),
                "profit_tier":    r.get("profit_tier"),
                "investor_type":  r.get("investor_type"),
                "investor_score": int(r.get("investor_score") or 0),
                "label":          r.get("investor_label"),
            }
        traders.append({
            "wallet_address":      r["wallet_address"],
            "total_trades":        int(r.get("trade_count") or 0),
            "total_pnl":           float(r["total_pnl_usd"]) if r.get("total_pnl_usd") is not None else 0,
            "total_invested":      None,
            "total_realized":      float(r["realized_pnl_usd"]) if r.get("realized_pnl_usd") is not None else None,
            "win_rate":            float(r["win_rate"]) if r.get("win_rate") is not None else 0,
            "avg_profit_per_trade":None,
            "best_trade":          float(r["best_trade_usd"])  if r.get("best_trade_usd")  is not None else None,
            "worst_trade":         float(r["worst_trade_usd"]) if r.get("worst_trade_usd") is not None else None,
            "roi_percentage":      float(r["roi_pct"]) if r.get("roi_pct") is not None else 0,
            "open_positions":      0,
            "unrealized_pnl":      float(r["unrealized_pnl_usd"]) if r.get("unrealized_pnl_usd") is not None else None,
            "tokens_traded":       int(r.get("tokens_traded") or 0),
            "tags":                r.get("tags") or "",
            "is_active":           bool(r.get("is_active")) if r.get("is_active") is not None else True,
            "first_seen":          r["first_seen"].isoformat() if r.get("first_seen") else None,
            "last_seen":           r["last_seen"].isoformat()  if r.get("last_seen")  else None,
            "last_activity":       format_age(r.get("last_updated")),
            "classification":      classification,
        })

    return {
        "success":   True,
        "count":     len(traders),
        "timerange": timerange,
        "traders":   traders,
    }


# -----------------------------------------------------------------------------
# Trader detail
# -----------------------------------------------------------------------------
@app.get("/api/trader/{wallet}")
def trader_detail(wallet: str):
    r = q_one("""
      SELECT pc.*,
             wc.behavior_type, wc.consistency_level, wc.profit_tier,
             wc.investor_type, wc.investor_score, wc.investor_label,
             w.tags, w.first_seen, w.last_seen, w.is_active
      FROM wallet_pnl_cache pc
      LEFT JOIN wallets w
        ON w.wallet_address = pc.wallet_address
      LEFT JOIN wallet_classifications wc
        ON wc.wallet_address = pc.wallet_address
      WHERE pc.wallet_address = %s
    """, (wallet,))

    if not r:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO wallet_sync_queue (wallet_address, priority, next_sync_at)
          VALUES (%s, 1, NOW())
          ON CONFLICT DO NOTHING
        """, (wallet,))
        conn.commit()
        cur.close()
        conn.close()
        return JSONResponse(
            {"success": False, "error": "Wallet encolada, disponible en 1-2 min"},
            status_code=202,
        )

    positions = q_all("""
      SELECT t.symbol, t.name, t.mint_address,
             ttc.rank, ttc.volume_usd, ttc.total_pnl,
             ttc.trade_count, ttc.last_updated
      FROM token_top_traders_cache ttc
      JOIN tokens t ON t.token_id = ttc.token_id
      WHERE ttc.wallet_address = %s
      ORDER BY ttc.total_pnl DESC NULLS LAST
      LIMIT 50
    """, (wallet,))

    classification = None
    if r.get("investor_type"):
        classification = {
            "behavior":       r.get("behavior_type"),
            "consistency":    r.get("consistency_level"),
            "profit_tier":    r.get("profit_tier"),
            "investor_type":  r.get("investor_type"),
            "investor_score": int(r.get("investor_score") or 0),
            "label":          r.get("investor_label"),
        }

    return {
        "success": True,
        "wallet": {
            "wallet_address":     r["wallet_address"],
            "total_pnl_usd":      float(r.get("total_pnl_usd") or 0),
            "realized_pnl_usd":   float(r.get("realized_pnl_usd") or 0),
            "unrealized_pnl_usd": float(r.get("unrealized_pnl_usd") or 0),
            "roi_pct":            float(r.get("roi_pct") or 0),
            "trade_count":        int(r.get("trade_count") or 0),
            "win_rate":           float(r.get("win_rate") or 0),
            "last_updated":       r["last_updated"].isoformat() if r.get("last_updated") else None,
            "tags":               r.get("tags") or "",
            "classification":     classification,
        },
        "positions": positions,
    }


# -----------------------------------------------------------------------------
# Tokens (CON pct_1h / pct_6h / pct_24h)
# -----------------------------------------------------------------------------
@app.get("/api/tokens")
def tokens(
    sort: str = "volume",
    order: str = "desc",
    limit: int = 50,
    status: str = "active",
):
    sortmap = {
        "volume":    "c.volume_24h",
        "mcap":      "c.market_cap",
        "price":     "c.price_usd",
        "liquidity": "c.liquidity",
        "age":       "t.detected_at",
        "txns":      "c.volume_24h",
        "investors": "inv.total_investors",
    }
    col = sortmap.get(sort, "c.volume_24h")
    direction = "DESC" if order.lower() == "desc" else "ASC"
    limit = min(max(int(limit), 1), 200)

    rows = q_all(f"""
      SELECT
        t.token_id, t.mint_address, t.name, t.symbol, t.detected_at,
        t.liquidity AS t_liquidity, t.source,
        c.price_usd, c.market_cap, c.fdv,
        c.liquidity AS c_liquidity, c.volume_24h,
        c.holder_count, c.last_updated,
        inv.total_investors, inv.elite_count, inv.profitable_count,
        inv.regular_count, inv.humans_count, inv.bots_count, inv.avg_score,
        ph1h.price_usd  AS price_1h_ago,
        ph6h.price_usd  AS price_6h_ago,
        ph24h.price_usd AS price_24h_ago
      FROM tokens t
      LEFT JOIN token_market_cache c ON c.token_id = t.token_id
      LEFT JOIN LATERAL (
        SELECT price_usd FROM token_price_history
        WHERE token_id = t.token_id
          AND time <= NOW() - INTERVAL '1 hour'
        ORDER BY time DESC
        LIMIT 1
      ) ph1h ON TRUE
      LEFT JOIN LATERAL (
        SELECT price_usd FROM token_price_history
        WHERE token_id = t.token_id
          AND time <= NOW() - INTERVAL '6 hours'
        ORDER BY time DESC
        LIMIT 1
      ) ph6h ON TRUE
      LEFT JOIN LATERAL (
        SELECT price_usd FROM token_price_history
        WHERE token_id = t.token_id
          AND time <= NOW() - INTERVAL '24 hours'
        ORDER BY time DESC
        LIMIT 1
      ) ph24h ON TRUE
      LEFT JOIN LATERAL (
        SELECT
          COUNT(*) AS total_investors,
          COUNT(*) FILTER (WHERE wc.investor_type='elite')      AS elite_count,
          COUNT(*) FILTER (WHERE wc.investor_type='profitable') AS profitable_count,
          COUNT(*) FILTER (WHERE wc.investor_type='regular')    AS regular_count,
          COUNT(*) FILTER (WHERE wc.behavior_type='human')      AS humans_count,
          COUNT(*) FILTER (WHERE wc.behavior_type='bot')        AS bots_count,
          ROUND(AVG(wc.investor_score)::numeric, 1)             AS avg_score
        FROM token_top_traders_cache ttc
        JOIN wallet_classifications wc
          ON wc.wallet_address = ttc.wallet_address
        WHERE ttc.token_id = t.token_id
      ) inv ON TRUE
      WHERE t.status = %s
      ORDER BY {col} {direction} NULLS LAST
      LIMIT %s
    """, (status, limit))

    out = []
    for r in rows:
        mint   = r["mint_address"]
        name   = r.get("name")   or f"{mint[:6]}...{mint[-4:]}"
        symbol = r.get("symbol") or f"{mint[:4]}..."

        out.append({
            "token_id":    r["token_id"],
            "mint_address": mint,
            "name":        name,
            "symbol":      symbol,
            "image_url":   None,
            "amm":         r.get("source"),
            "age":         format_age(r.get("detected_at")),
            "created_at":  r["detected_at"].isoformat() if r.get("detected_at") else None,
            "price":       float(r["price_usd"]) if r.get("price_usd") else 0,
            "liquidity":   float(r.get("c_liquidity") or r.get("t_liquidity") or 0),
            "volume_24h":  float(r.get("volume_24h") or 0),
            "market_cap":  float(r.get("market_cap") or 0),
            "fdv":         float(r.get("fdv") or 0),
            "txns":        0,                                # pendiente: sin fuente
            "makers":      int(r.get("holder_count") or 0),
            "pct_5m":      None,                             # apagada hasta TTL <2min
            "pct_1h":      pct_change(r.get("price_usd"), r.get("price_1h_ago")),
            "pct_6h":      pct_change(r.get("price_usd"), r.get("price_6h_ago")),
            "pct_24h":     pct_change(r.get("price_usd"), r.get("price_24h_ago")),
            "investors": {
                "total":      int(r.get("total_investors") or 0),
                "elite":      int(r.get("elite_count") or 0),
                "profitable": int(r.get("profitable_count") or 0),
                "regular":    int(r.get("regular_count") or 0),
                "humans":     int(r.get("humans_count") or 0),
                "bots":       int(r.get("bots_count") or 0),
                "avg_score":  float(r.get("avg_score") or 0),
            },
        })

    return {"success": True, "count": len(out), "tokens": out}


# -----------------------------------------------------------------------------
# Stats bar
# -----------------------------------------------------------------------------
@app.get("/api/stats")
def stats():
    total_tokens   = q_one("SELECT COUNT(*) AS n FROM tokens WHERE status='active'")["n"]
    total_wallets  = q_one("SELECT COUNT(*) AS n FROM wallets WHERE is_active=TRUE")["n"]
    total_pnl      = q_one(
        "SELECT COUNT(*) AS n FROM wallet_pnl_cache WHERE last_updated > NOW() - INTERVAL '24 hours'"
    )["n"]

    cls = q_one("""
      SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE behavior_type='human') AS humans,
        COUNT(*) FILTER (WHERE behavior_type='bot')   AS bots,
        COUNT(*) FILTER (WHERE investor_type='elite') AS elite,
        COUNT(*) FILTER (WHERE investor_type='profitable') AS profitable
      FROM wallet_classifications
    """) or {}

    return {
        "success":          True,
        "total_tokens":     total_tokens,
        "total_wallets":    total_wallets,
        "transactions_24h": total_pnl,
        "classifications": {
            "total":      cls.get("total", 0),
            "humans":     cls.get("humans", 0),
            "bots":       cls.get("bots", 0),
            "elite":      cls.get("elite", 0),
            "profitable": cls.get("profitable", 0),
        },
    }


# -----------------------------------------------------------------------------
# Token investors
# -----------------------------------------------------------------------------
@app.get("/api/token/{mint}/investors")
def token_investors(mint: str):
    r = q_one("""
      SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE wc.investor_type='elite')      AS elite,
        COUNT(*) FILTER (WHERE wc.investor_type='profitable') AS profitable,
        COUNT(*) FILTER (WHERE wc.investor_type='regular')    AS regular,
        COUNT(*) FILTER (WHERE wc.investor_type='casual')     AS casual,
        COUNT(*) FILTER (WHERE wc.investor_type='losing')     AS losing,
        COUNT(*) FILTER (WHERE wc.behavior_type='human')      AS humans,
        COUNT(*) FILTER (WHERE wc.behavior_type='bot')        AS bots,
        ROUND(AVG(wc.investor_score)::numeric, 1)             AS avg_score
      FROM tokens t
      JOIN token_top_traders_cache ttc ON ttc.token_id = t.token_id
      JOIN wallet_classifications wc   ON wc.wallet_address = ttc.wallet_address
      WHERE t.mint_address = %s
    """, (mint,)) or {}

    return {
        "success": True,
        "investors": {
            "total":      r.get("total", 0),
            "elite":      r.get("elite", 0),
            "profitable": r.get("profitable", 0),
            "regular":    r.get("regular", 0),
            "casual":     r.get("casual", 0),
            "losing":     r.get("losing", 0),
            "humans":     r.get("humans", 0),
            "bots":       r.get("bots", 0),
            "avg_score":  float(r.get("avg_score") or 0),
        },
    }


# -----------------------------------------------------------------------------
# Usage (CU consumption últimos 14 días)
# -----------------------------------------------------------------------------
@app.get("/api/usage")
def usage():
    return {
        "success": True,
        "days": q_all(
            "SELECT * FROM birdeye_usage ORDER BY day DESC LIMIT 14"
        ),
    }


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("API_PORT", "8200")))