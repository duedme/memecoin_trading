"""
walletclassifier.py
Clasifica wallets a partir de walletpnlcache, walletpositions y wallettransactions.
Refactor Fase 7: deja de ser un loop; ahora es una función pura que el
classifier_worker invoca por wallet, y además provee un modo CLI para corrida batch.

Categorías:
  behavior:     'human' | 'bot' | 'suspicious'
  investortype: 'elite' | 'profitable' | 'regular' | 'casual' | 'losing' | 'unclassified'
  profittier:   'S' | 'A' | 'B' | 'C' | 'D' | 'F'
  consistencylevel: 'high' | 'medium' | 'low'
  investorscore: 0..100
  investorlabel: texto corto descriptivo
"""
import sys
import psycopg2
import psycopg2.extras

from sharedconfig import DBCONFIG, getlogger

log = getlogger("walletclassifier")

# Umbrales (ajustables)
BOT_MIN_TRADES_PER_HOUR = 20
BOT_MIN_TOTAL_TRADES = 50
SUSPICIOUS_MIN_LOSING_RATIO = 0.9

ELITE_ROI_PCT = 100.0       # >100% ROI y suficientes trades
ELITE_MIN_TRADES = 20
PROFITABLE_ROI_PCT = 20.0
REGULAR_ROI_PCT = 0.0
LOSING_ROI_PCT = -20.0


def _fetch_wallet_stats(cur, wallet):
    cur.execute(
        """
        SELECT pc.walletaddress,
               pc.tradecount,
               pc.buycount,
               pc.sellcount,
               pc.tokenstraded,
               pc.winrate,
               pc.roipct,
               pc.realized_sol,
               pc.invested_sol,
               pc.totalpnl_sol,
               pc.realizedpnl_sol,
               pc.unrealizedpnl_sol,
               pc.firstactivity,
               pc.lastactivity,
               pc.openpositions
        FROM walletpnlcache pc
        WHERE pc.walletaddress = %s
        """,
        (wallet,),
    )
    return cur.fetchone()


def _fetch_activity_window(cur, wallet):
    """Detecta frecuencia de trading en última hora y día para behavior."""
    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '1 hour')  AS tx_1h,
            COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours') AS tx_24h,
            COUNT(*) AS tx_total
        FROM wallettransactions
        WHERE walletaddress = %s
        """,
        (wallet,),
    )
    return cur.fetchone()


def _compute_behavior(stats, act):
    tx_1h = int(act["tx_1h"] or 0)
    tx_total = int(act["tx_total"] or 0)

    if tx_1h >= BOT_MIN_TRADES_PER_HOUR and tx_total >= BOT_MIN_TOTAL_TRADES:
        return "bot"

    # Suspicious: muchas pérdidas consistentes
    roi = float(stats["roipct"] or 0)
    trades = int(stats["tradecount"] or 0)
    if trades >= 10 and roi <= -50.0:
        return "suspicious"

    return "human"


def _compute_investortype(stats):
    trades = int(stats["tradecount"] or 0)
    roi = float(stats["roipct"] or 0)

    if trades == 0:
        return "unclassified"
    if roi >= ELITE_ROI_PCT and trades >= ELITE_MIN_TRADES:
        return "elite"
    if roi >= PROFITABLE_ROI_PCT:
        return "profitable"
    if roi >= REGULAR_ROI_PCT:
        return "regular"
    if roi >= LOSING_ROI_PCT:
        return "casual"
    return "losing"


def _compute_profittier(stats):
    roi = float(stats["roipct"] or 0)
    if roi >= 200: return "S"
    if roi >= 100: return "A"
    if roi >= 50:  return "B"
    if roi >= 0:   return "C"
    if roi >= -30: return "D"
    return "F"


def _compute_consistency(stats):
    winrate = float(stats["winrate"] or 0)
    trades = int(stats["tradecount"] or 0)
    if trades < 5:
        return "low"
    if winrate >= 65:
        return "high"
    if winrate >= 45:
        return "medium"
    return "low"


def _compute_score(stats, behavior, itype, ptier, clevel):
    score = 50  # base
    roi = float(stats["roipct"] or 0)
    winrate = float(stats["winrate"] or 0)
    trades = int(stats["tradecount"] or 0)

    # ROI component (hasta +30)
    score += min(max(int(roi / 5), -30), 30)

    # Winrate component (hasta +15)
    score += min(int((winrate - 50) / 5), 15) if winrate > 0 else 0

    # Trades volume (hasta +5)
    score += min(int(trades / 20), 5)

    # Bot penalty
    if behavior == "bot":
        score -= 15
    if behavior == "suspicious":
        score -= 25

    # Consistency bonus
    if clevel == "high":
        score += 5
    elif clevel == "low":
        score -= 5

    return max(0, min(100, score))


def _compute_label(stats, behavior, itype, ptier):
    roi = float(stats["roipct"] or 0)
    trades = int(stats["tradecount"] or 0)
    if trades == 0:
        return "Sin actividad"
    if behavior == "bot":
        return f"Bot trader · {itype} · ROI {roi:.0f}%"
    if behavior == "suspicious":
        return f"Sospechoso · ROI {roi:.0f}%"
    if itype == "elite":
        return f"Elite · Tier {ptier} · ROI {roi:.0f}%"
    if itype == "profitable":
        return f"Rentable · Tier {ptier} · ROI {roi:.0f}%"
    if itype == "losing":
        return f"Perdedor · ROI {roi:.0f}%"
    return f"{itype.capitalize()} · ROI {roi:.0f}%"


UPSERT_SQL = """
INSERT INTO walletclassifications (
    walletaddress,
    behavior,
    behaviortype,
    investortype,
    investorscore,
    investorlabel,
    profittier,
    consistencylevel,
    totaltrades,
    winrate,
    totalpnlsol,
    roipercentage,
    updatedat
)
VALUES (%(wallet)s, %(behavior)s, %(behavior)s, %(itype)s, %(score)s,
        %(label)s, %(ptier)s, %(clevel)s, %(trades)s, %(winrate)s,
        %(totalpnl)s, %(roi)s, NOW())
ON CONFLICT (walletaddress) DO UPDATE SET
    behavior         = EXCLUDED.behavior,
    behaviortype     = EXCLUDED.behaviortype,
    investortype     = EXCLUDED.investortype,
    investorscore    = EXCLUDED.investorscore,
    investorlabel    = EXCLUDED.investorlabel,
    profittier       = EXCLUDED.profittier,
    consistencylevel = EXCLUDED.consistencylevel,
    totaltrades      = EXCLUDED.totaltrades,
    winrate          = EXCLUDED.winrate,
    totalpnlsol      = EXCLUDED.totalpnlsol,
    roipercentage    = EXCLUDED.roipercentage,
    updatedat        = NOW();
"""


def classify_wallet(conn, wallet: str) -> dict:
    """
    Función pura: lee stats, calcula clasificación y UPSERT en walletclassifications.
    Retorna el dict con la clasificación aplicada, o None si no hay datos.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        stats = _fetch_wallet_stats(cur, wallet)
        if not stats or (stats["tradecount"] or 0) == 0:
            return None

        act = _fetch_activity_window(cur, wallet)

        behavior = _compute_behavior(stats, act)
        itype = _compute_investortype(stats)
        ptier = _compute_profittier(stats)
        clevel = _compute_consistency(stats)
        score = _compute_score(stats, behavior, itype, ptier, clevel)
        label = _compute_label(stats, behavior, itype, ptier)

        params = {
            "wallet": wallet,
            "behavior": behavior,
            "itype": itype,
            "score": score,
            "label": label,
            "ptier": ptier,
            "clevel": clevel,
            "trades": int(stats["tradecount"] or 0),
            "winrate": float(stats["winrate"] or 0),
            "totalpnl": float(stats["totalpnl_sol"] or 0),
            "roi": float(stats["roipct"] or 0),
        }

        cur.execute(UPSERT_SQL, params)
        cur.execute(
            """
            INSERT INTO classifier_throttle (walletaddress, last_run_at)
            VALUES (%s, NOW())
            ON CONFLICT (walletaddress) DO UPDATE SET last_run_at = NOW()
            """,
            (wallet,),
        )
    conn.commit()
    return params


# ------------------------------------------------------------------
# CLI: clasificación batch (compatibilidad con uso manual)
# ------------------------------------------------------------------
def _classify_all():
    conn = psycopg2.connect(**DBCONFIG)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT walletaddress FROM walletpnlcache WHERE tradecount > 0"
            )
            wallets = [r[0] for r in cur.fetchall()]
        log.info("CLI batch: clasificando %s wallets", len(wallets))
        done = 0
        for w in wallets:
            try:
                classify_wallet(conn, w)
                done += 1
            except Exception as e:
                log.warning("fallo clasificando %s: %s", w, e)
                conn.rollback()
        log.info("CLI batch terminado. clasificadas=%s", done)
    finally:
        conn.close()


if __name__ == "__main__":
    _classify_all()
    sys.exit(0)