#!/usr/bin/env python3
"""
wallet_analytics.py
Herramienta CLI para analizar ganancias/pérdidas de wallets
"""

import psycopg2
from tabulate import tabulate
from datetime import datetime, timedelta
import argparse
import sys

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "memecoins_db",
    "user": "postgres",
    "password": "12345"
}


def connect_db():
    """Conecta a la base de datos"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )


def top_traders(limit=20):
    """Muestra los mejores traders"""
    conn = connect_db()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            wallet_address,
            total_profit_loss,
            win_rate,
            total_trades,
            ROUND((total_profit_loss / NULLIF(total_invested, 0) * 100)::numeric, 2) as roi_percentage,
            last_seen
        FROM wallets
        WHERE total_trades >= 3
        ORDER BY total_profit_loss DESC
        LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    print("\n🏆 TOP TRADERS - Mayores Ganancias")
    print("=" * 100)
    
    headers = ["Wallet", "P&L (SOL)", "Win Rate %", "Trades", "ROI %", "Última actividad"]
    rows = [
        [
            row[0][:16] + "...",
            f"{float(row[1]):.4f}" if row[1] else "0",
            f"{float(row[2]):.1f}" if row[2] else "0",
            row[3],
            f"{float(row[4]):.1f}" if row[4] else "N/A",
            row[5].strftime("%Y-%m-%d %H:%M") if row[5] else "N/A"
        ]
        for row in results
    ]
    
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    
    cursor.close()
    conn.close()


def wallet_details(wallet_address: str):
    """Muestra detalles de un wallet específico"""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Información general del wallet
    cursor.execute("""
        SELECT 
            wallet_address,
            total_trades,
            total_profit_loss,
            total_invested,
            total_realized,
            win_rate,
            avg_profit_per_trade,
            best_trade,
            worst_trade,
            first_seen,
            last_seen
        FROM wallets
        WHERE wallet_address = %s
    """, (wallet_address,))
    
    wallet_info = cursor.fetchone()
    
    if not wallet_info:
        print(f"❌ Wallet {wallet_address} no encontrado")
        return
    
    print("\n" + "=" * 100)
    print(f"📊 ANÁLISIS DEL WALLET: {wallet_address}")
    print("=" * 100)
    
    print("\n💰 ESTADÍSTICAS GENERALES")
    print(f"  Total de trades:        {wallet_info[1]}")
    print(f"  P&L Total:             {float(wallet_info[2]):.6f} SOL")
    print(f"  Total invertido:       {float(wallet_info[3]):.6f} SOL")
    print(f"  Total realizado:       {float(wallet_info[4]):.6f} SOL")
    print(f"  Win rate:              {float(wallet_info[5]):.1f}%")
    print(f"  P&L promedio/trade:    {float(wallet_info[6]):.6f} SOL")
    print(f"  Mejor trade:           {float(wallet_info[7]):.6f} SOL")
    print(f"  Peor trade:            {float(wallet_info[8]):.6f} SOL")
    print(f"  Primera actividad:     {wallet_info[9].strftime('%Y-%m-%d %H:%M')}")
    print(f"  Última actividad:      {wallet_info[10].strftime('%Y-%m-%d %H:%M')}")
    
    # ROI
    roi = (float(wallet_info[2]) / float(wallet_info[3]) * 100) if wallet_info[3] and wallet_info[3] > 0 else 0
    print(f"  ROI:                   {roi:.2f}%")
    
    # Posiciones abiertas
    cursor.execute("""
        SELECT 
            t.symbol,
            t.name,
            wp.current_balance,
            wp.avg_buy_price,
            wp.unrealized_pnl,
            wp.first_buy,
            wp.last_buy
        FROM wallet_positions wp
        JOIN tokens t ON wp.token_id = t.token_id
        WHERE wp.wallet_id = (SELECT wallet_id FROM wallets WHERE wallet_address = %s)
            AND wp.status != 'closed'
            AND wp.current_balance > 0
        ORDER BY wp.unrealized_pnl DESC
    """, (wallet_address,))
    
    positions = cursor.fetchall()
    
    if positions:
        print("\n📈 POSICIONES ABIERTAS")
        headers = ["Token", "Balance", "Precio Prom", "P&L No Realizado", "Primer compra", "Última compra"]
        rows = [
            [
                f"{row[0]} ({row[1][:20]}...)" if row[1] else row[0],
                f"{float(row[2]):.2f}",
                f"{float(row[3]):.8f}",
                f"{float(row[4]):.6f} SOL" if row[4] else "0",
                row[5].strftime("%Y-%m-%d %H:%M") if row[5] else "N/A",
                row[6].strftime("%Y-%m-%d %H:%M") if row[6] else "N/A"
            ]
            for row in positions
        ]
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    
    # Últimas transacciones
    cursor.execute("""
        SELECT 
            wt.time,
            t.symbol,
            wt.tx_type,
            wt.token_amount,
            wt.sol_amount,
            wt.price,
            wt.is_partial,
            wt.signature
        FROM wallet_transactions wt
        JOIN tokens t ON wt.token_id = t.token_id
        WHERE wt.wallet_id = (SELECT wallet_id FROM wallets WHERE wallet_address = %s)
        ORDER BY wt.time DESC
        LIMIT 20
    """, (wallet_address,))
    
    transactions = cursor.fetchall()
    
    if transactions:
        print("\n📝 ÚLTIMAS 20 TRANSACCIONES")
        headers = ["Fecha/Hora", "Token", "Tipo", "Cantidad Token", "SOL", "Precio", "Parcial", "Signature"]
        rows = [
            [
                row[0].strftime("%Y-%m-%d %H:%M:%S"),
                row[1] or "???",
                "🟢 BUY" if row[2] == 'buy' else "🔴 SELL",
                f"{float(row[3]):.4f}",
                f"{float(row[4]):.6f}",
                f"{float(row[5]):.8f}",
                "✓" if row[6] else "",
                row[7][:16] + "..."
            ]
            for row in transactions
        ]
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    
    cursor.close()
    conn.close()
    print("\n" + "=" * 100)


def wallet_pnl_by_token(wallet_address: str):
    """Muestra P&L del wallet desglosado por token"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            t.symbol,
            t.name,
            wp.total_bought,
            wp.total_sold,
            wp.current_balance,
            wp.avg_buy_price,
            wp.avg_sell_price,
            wp.realized_pnl,
            wp.unrealized_pnl,
            wp.status
        FROM wallet_positions wp
        JOIN tokens t ON wp.token_id = t.token_id
        WHERE wp.wallet_id = (SELECT wallet_id FROM wallets WHERE wallet_address = %s)
        ORDER BY (wp.realized_pnl + wp.unrealized_pnl) DESC
    """, (wallet_address,))
    
    positions = cursor.fetchall()
    
    if not positions:
        print(f"❌ No hay posiciones para el wallet {wallet_address}")
        return
    
    print(f"\n💎 P&L POR TOKEN - Wallet: {wallet_address[:16]}...")
    print("=" * 130)
    
    headers = [
        "Token", "Comprado", "Vendido", "Balance", 
        "Precio Comp Prom", "Precio Venta Prom",
        "P&L Realizado", "P&L No Realizado", "Estado"
    ]
    
    rows = []
    total_realized = 0
    total_unrealized = 0
    
    for row in positions:
        realized = float(row[7]) if row[7] else 0
        unrealized = float(row[8]) if row[8] else 0
        total_realized += realized
        total_unrealized += unrealized
        
        rows.append([
            f"{row[0]} ({row[1][:15]}...)" if row[1] else row[0],
            f"{float(row[2]):.2f}" if row[2] else "0",
            f"{float(row[3]):.2f}" if row[3] else "0",
            f"{float(row[4]):.2f}" if row[4] else "0",
            f"{float(row[5]):.8f}" if row[5] else "N/A",
            f"{float(row[6]):.8f}" if row[6] else "N/A",
            f"{realized:.6f}",
            f"{unrealized:.6f}",
            row[9]
        ])
    
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    
    print("\n📊 RESUMEN")
    print(f"  P&L Total Realizado:      {total_realized:.6f} SOL")
    print(f"  P&L Total No Realizado:   {total_unrealized:.6f} SOL")
    print(f"  P&L TOTAL:                {(total_realized + total_unrealized):.6f} SOL")
    
    cursor.close()
    conn.close()


def recent_activity(hours=24):
    """Muestra actividad reciente de todos los wallets"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            w.wallet_address,
            t.symbol,
            wt.tx_type,
            wt.token_amount,
            wt.sol_amount,
            wt.price,
            wt.time,
            wt.is_partial
        FROM wallet_transactions wt
        JOIN wallets w ON wt.wallet_id = w.wallet_id
        JOIN tokens t ON wt.token_id = t.token_id
        WHERE wt.time >= NOW() - INTERVAL '%s hours'
        ORDER BY wt.time DESC
        LIMIT 100
    """, (hours,))
    
    transactions = cursor.fetchall()
    
    if not transactions:
        print(f"❌ No hay transacciones en las últimas {hours} horas")
        return
    
    print(f"\n🔥 ACTIVIDAD RECIENTE - Últimas {hours} horas")
    print("=" * 120)
    
    headers = ["Wallet", "Token", "Tipo", "Cantidad", "SOL", "Precio", "Tiempo", "Parcial"]
    rows = [
        [
            row[0][:16] + "...",
            row[1] or "???",
            "🟢 BUY" if row[2] == 'buy' else "🔴 SELL",
            f"{float(row[3]):.2f}",
            f"{float(row[4]):.6f}",
            f"{float(row[5]):.8f}",
            row[6].strftime("%H:%M:%S"),
            "✓" if row[7] else ""
        ]
        for row in transactions
    ]
    
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    print(f"\nTotal transacciones: {len(transactions)}")
    
    cursor.close()
    conn.close()


def partial_orders():
    """Muestra órdenes que se completaron en partes"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            order_id,
            COUNT(*) as parts,
            MIN(time) as start_time,
            MAX(time) as end_time,
            SUM(token_amount) as total_tokens,
            SUM(sol_amount) as total_sol,
            MAX(w.wallet_address) as wallet
        FROM wallet_transactions wt
        JOIN wallets w ON wt.wallet_id = w.wallet_id
        WHERE is_partial = TRUE
        GROUP BY order_id
        ORDER BY start_time DESC
        LIMIT 50
    """)
    
    orders = cursor.fetchall()
    
    if not orders:
        print("❌ No hay órdenes parciales registradas")
        return
    
    print("\n✂️  ÓRDENES PARCIALES DETECTADAS")
    print("=" * 120)
    
    headers = ["Order ID", "Partes", "Inicio", "Fin", "Total Tokens", "Total SOL", "Wallet"]
    rows = [
        [
            row[0][:20] + "...",
            row[1],
            row[2].strftime("%Y-%m-%d %H:%M:%S"),
            row[3].strftime("%Y-%m-%d %H:%M:%S"),
            f"{float(row[4]):.4f}",
            f"{float(row[5]):.6f}",
            row[6][:16] + "..."
        ]
        for row in orders
    ]
    
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    
    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Análisis de Ganancias/Pérdidas de Wallets")
    
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
    
    # Top traders
    top_parser = subparsers.add_parser('top', help='Mostrar top traders')
    top_parser.add_argument('-l', '--limit', type=int, default=20, help='Número de resultados')
    
    # Detalles de wallet
    wallet_parser = subparsers.add_parser('wallet', help='Detalles de un wallet')
    wallet_parser.add_argument('address', help='Dirección del wallet')
    
    # P&L por token
    pnl_parser = subparsers.add_parser('pnl', help='P&L por token de un wallet')
    pnl_parser.add_argument('address', help='Dirección del wallet')
    
    # Actividad reciente
    activity_parser = subparsers.add_parser('activity', help='Actividad reciente')
    activity_parser.add_argument('--hours', type=int, default=24, help='Horas hacia atrás')
    
    # Órdenes parciales
    subparsers.add_parser('partials', help='Ver órdenes parciales')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'top':
            top_traders(args.limit)
        elif args.command == 'wallet':
            wallet_details(args.address)
        elif args.command == 'pnl':
            wallet_pnl_by_token(args.address)
        elif args.command == 'activity':
            recent_activity(args.hours)
        elif args.command == 'partials':
            partial_orders()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
