import os
import time
import threading
import psycopg2
from flask import Flask, render_template
from flask_socketio import SocketIO
from shared_config import DB_CONFIG

app = Flask(__name__)
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*")

def get_db_connection():
    """Conexión inteligente: Intenta el .env y si falla, asume que está en Docker"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"⚠️ Falló conexión a {DB_CONFIG.get('host')}. Intentando host de Docker ('db')...")
        docker_config = DB_CONFIG.copy()
        docker_config['host'] = 'db'
        return psycopg2.connect(**docker_config)

def listen_to_db():
    conn = None
    last_time_all = None
    last_time_smart = None
    
    while True:
        try:
            if conn is None or conn.closed != 0:
                conn = get_db_connection()
                print("✅ Conectado a la Base de Datos exitosamente.")

            with conn.cursor() as cur:
                # 1. FLUJO GENERAL (Ahora incluye comportamiento y tipo de inversor)
                if last_time_all is None:
                    cur.execute("""
                        SELECT t.time, t.signature, t.side, t.amountsol, t.mintaddress, t.walletaddress,
                               c.behavior, c.investortype
                        FROM wallettransactions t
                        LEFT JOIN walletclassifications c ON t.walletaddress = c.walletaddress
                        ORDER BY t.time DESC LIMIT 1
                    """)
                else:
                    cur.execute("""
                        SELECT t.time, t.signature, t.side, t.amountsol, t.mintaddress, t.walletaddress,
                               c.behavior, c.investortype
                        FROM wallettransactions t
                        LEFT JOIN walletclassifications c ON t.walletaddress = c.walletaddress
                        WHERE t.time > %s ORDER BY t.time ASC
                    """, (last_time_all,))

                for row in cur.fetchall():
                    tx_time, sig, side, sol, mint, wallet, behavior, inv_type = row
                    last_time_all = tx_time

                    # ENVIAMOS DATOS ESTRUCTURADOS Y CLASIFICADOS
                    socketio.emit('new_trade', {
                        'wallet': wallet,
                        'mint': mint,
                        'side': side,
                        'sol': float(sol),
                        'behavior': behavior or 'unclassified',
                        'investortype': inv_type or 'unclassified'
                    })

                # 2. FLUJO SMART MONEY
                if last_time_smart is None:
                    cur.execute("""
                        SELECT t.time, t.signature, t.amountsol, t.mintaddress, c.investortype, t.walletaddress 
                        FROM wallettransactions t
                        JOIN walletclassifications c ON t.walletaddress = c.walletaddress
                        WHERE c.investortype IN ('elite', 'profitable') AND t.side = 'buy'
                        ORDER BY t.time DESC LIMIT 1
                    """)
                else:
                    cur.execute("""
                        SELECT t.time, t.signature, t.amountsol, t.mintaddress, c.investortype, t.walletaddress 
                        FROM wallettransactions t
                        JOIN walletclassifications c ON t.walletaddress = c.walletaddress
                        WHERE c.investortype IN ('elite', 'profitable') AND t.side = 'buy'
                          AND t.time > %s
                        ORDER BY t.time ASC
                    """, (last_time_smart,))

                for row in cur.fetchall():
                    tx_time, sig, sol, mint, inv_type, wallet = row
                    last_time_smart = tx_time
                    tag = "👑 ELITE" if inv_type == 'elite' else "📈 RENTABLE"
                    short_wallet = f"{wallet[:4]}...{wallet[-4:]}" if wallet and len(wallet) > 8 else "Unknown"
                    msg = f"[{tx_time.strftime('%H:%M:%S')}] 🔥 {tag} ({short_wallet}) COMPRÓ {sol:.2f} SOL del Token {mint[:8]}..."
                    
                    # ENVIAMOS DATOS ESTRUCTURADOS
                    socketio.emit('smart_money', {
                        'text': msg, 'wallet': wallet, 'mint': mint, 'side': 'buy', 'sol': float(sol), 'tag': tag
                    })
                    
        except Exception as e:
            print(f"❌ Error BD: {e}")
            if conn:
                try: conn.rollback()
                except: pass
                conn = None 
            
        time.sleep(0.5)

@app.route('/admin/live-feed')
def index():
    return render_template('terminal.html')

if __name__ == '__main__':
    print("🚀 Servidor WebSocket iniciado en el puerto 5001...")
    threading.Thread(target=listen_to_db, daemon=True).start()
    socketio.run(app, host='0.0.0.0', port=5001, allow_unsafe_werkzeug=True)