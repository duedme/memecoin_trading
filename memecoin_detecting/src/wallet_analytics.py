#!/usr/bin/env python3
"""wallet_analytics.py v2 — CLI de análisis sobre caché de Birdeye."""
import argparse
import psycopg2
from tabulate import tabulate
from shared_config import DB_CONFIG


def connect():
    return psycopg2.connect(**DB_CONFIG)


def top_traders(limit=20):
    conn = connect(); cur = conn.cursor()
    cur.execute("""
        SELECT pc.wallet_address, pc.total_pnl_usd, pc.win_rate,
               pc.trade_count, pc.roi_pct, pc.last_updated,
               wc.investor_label, wc.investor_score
        FROM wallet_pnl_cache pc
        LEFT JOIN wallet_classifications wc USING (wallet_address)
        WHERE pc.trade_count >= 3
        ORDER BY pc.total_pnl_usd DESC NULLS LAST
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    headers = ["Wallet", "PnL USD", "Win%", "Trades", "ROI%", "Updated", "Label", "Score"]
    print("\n🏆 TOP TRADERS (según caché Birdeye)")
    print(tabulate([
        [r[0][:16]+"...", f"{float(r[1] or 0):.2f}",
         f"{float(r[2] or 0):.1f}", r[3] or 0,
         f"{float(r[4] or 0):.1f}" if r[4] else "-",
         r[5].strftime("%Y-%m-%d %H:%M") if r[5] else "-",
         r[6] or "-", r[7] or "-"]
        for r in rows
    ], headers=headers, tablefmt="grid"))
    cur.close(); conn.close()


def wallet_detail(address):
    conn = connect(); cur = conn.cursor()
    cur.execute("""
        SELECT pc.wallet_address, pc.realized_pnl_usd, pc.unrealized_pnl_usd,
               pc.total_pnl_usd, pc.roi_pct, pc.trade_count, pc.win_rate,
               pc.last_updated, wc.investor_label, wc.investor_score
        FROM wallet_pnl_cache pc
        LEFT JOIN wallet_classifications wc USING (wallet_address)
        WHERE pc.wallet_address = %s
    """, (address,))
    r = cur.fetchone()
    if not r:
        print(f"❌ No hay datos cacheados para {address}.")
        print("   Encólalo en wallet_sync_queue y espera al siguiente ciclo.")
        return
    print(f"\n📊 {r[0]}")
    print(f"  Realized PnL:   {r[1]}")
    print(f"  Unrealized PnL: {r[2]}")
    print(f"  Total PnL USD:  {r[3]}")
    print(f"  ROI %:          {r[4]}")
    print(f"  Trades:         {r[5]}")
    print(f"  Win rate:       {r[6]}")
    print(f"  Clasificación:  {r[8]} (score {r[9]})")
    print(f"  Actualizado:    {r[7]}")
    cur.close(); conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["top", "wallet"])
    p.add_argument("--address")
    p.add_argument("--limit", type=int, default=20)
    args = p.parse_args()
    if args.command == "top":
        top_traders(args.limit)
    else:
        if not args.address:
            print("--address requerido"); return
        wallet_detail(args.address)


if __name__ == "__main__":
    main()