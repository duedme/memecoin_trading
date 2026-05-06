import psycopg2
from shared_config import DB_CONFIG, get_logger

log = get_logger("wallet_classifier")

SELECT_AGG = """
    SELECT
        w.wallet_address,
        COUNT(*) FILTER (WHERE wt.side IN ('buy','sell')) AS total_trades,
        COALESCE(SUM(CASE WHEN wt.side='sell' THEN wt.amount_sol ELSE 0 END), 0)
          - COALESCE(SUM(CASE WHEN wt.side='buy'  THEN wt.amount_sol ELSE 0 END), 0)
          AS total_pnl_sol,
        COALESCE(SUM(CASE WHEN wt.side='buy'  THEN wt.amount_sol ELSE 0 END), 0) AS invested_sol,
        COUNT(DISTINCT wt.signature) AS unique_sigs,
        COUNT(DISTINCT date_trunc('minute', wt.time)) AS unique_minutes
    FROM wallets w
    JOIN wallet_transactions wt ON wt.wallet_address = w.wallet_address
    WHERE wt.time > NOW() - INTERVAL '30 days'
    GROUP BY w.wallet_address
"""

UPSERT = """
    INSERT INTO wallet_classifications
        (wallet_address, behavior, investor_type, investor_score,
         total_trades, win_rate, total_pnl_sol, roi_percentage, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (wallet_address) DO UPDATE
       SET behavior       = EXCLUDED.behavior,
           investor_type  = EXCLUDED.investor_type,
           investor_score = EXCLUDED.investor_score,
           total_trades   = EXCLUDED.total_trades,
           win_rate       = EXCLUDED.win_rate,
           total_pnl_sol  = EXCLUDED.total_pnl_sol,
           roi_percentage = EXCLUDED.roi_percentage,
           updated_at     = NOW()
"""

def classify(trades, pnl_sol, invested_sol, unique_sigs, unique_minutes):
    # Comportamiento
    if unique_minutes > 0 and trades / max(unique_minutes, 1) > 3:
        behavior = "bot"
    elif trades >= 500 and unique_minutes < trades * 0.3:
        behavior = "suspicious"
    else:
        behavior = "human"

    roi = (pnl_sol / invested_sol * 100.0) if invested_sol > 0 else 0.0
    profitable = pnl_sol > 0

    # Tipo de inversor
    if behavior == "human":
        if trades >= 10 and roi >= 50:
            inv_type, score = "elite", min(100, 70 + int(roi / 10))
        elif profitable:
            inv_type, score = "profitable", min(79, 50 + int(roi / 5))
        elif trades >= 3:
            inv_type, score = "regular", max(20, min(49, 30 + int(roi / 10)))
        else:
            inv_type, score = "casual", 10
        if pnl_sol < 0 and trades >= 3:
            inv_type, score = "losing", max(0, 30 + int(roi))
    else:
        inv_type = "bot-profitable" if profitable else "bot-regular"
        score = 0

    win_rate = 60.0 if profitable else 30.0  # placeholder: cámbialo cuando haya PnL por trade
    return behavior, inv_type, int(score), float(win_rate), float(roi)

def run():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        cur.execute(SELECT_AGG)
        rows = cur.fetchall()
        log.info("Clasificando %d wallets...", len(rows))
        for addr, trades, pnl_sol, invested_sol, unique_sigs, unique_minutes in rows:
            behavior, inv_type, score, win_rate, roi = classify(
                int(trades or 0), float(pnl_sol or 0.0),
                float(invested_sol or 0.0),
                int(unique_sigs or 0), int(unique_minutes or 0),
            )
            cur.execute(
                UPSERT,
                (addr, behavior, inv_type, score,
                 int(trades or 0), win_rate,
                 float(pnl_sol or 0.0), roi),
            )
    log.info("Clasificación terminada.")

if __name__ == "__main__":
    run()