"""
replay.py
Utilidades operativas para rehacer datos sin reiniciar servicios.

Uso:
  python3 replay.py --rebuild-wallet <WALLET>
  python3 replay.py --rebuild-token  <MINT>
  python3 replay.py --reparse-signature <SIG>
  python3 replay.py --recompute-position <WALLET> <MINT>
  python3 replay.py --dead-letter [--max-attempts N]
  python3 replay.py --retention
  python3 replay.py --health
"""
import argparse
import json
import sys

import psycopg2
import psycopg2.extras

from shared_config import DBCONFIG, getlogger

log = getlogger("replay")


def _db():
    conn = psycopg2.connect(**DBCONFIG)
    conn.autocommit = False
    return conn


def _enqueue(conn, event_type, wallet=None, mint=None, signature=None, priority=5):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reducer_queue
                (event_type, walletaddress, mintaddress, signature, priority, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            (event_type, wallet, mint, signature, priority),
        )
    conn.commit()


def rebuild_wallet(wallet):
    """Recomputa posiciones + pnl cache + clasificación desde cero."""
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT mintaddress FROM wallettransactions WHERE walletaddress=%s",
                (wallet,),
            )
            mints = [r[0] for r in cur.fetchall()]

        for m in mints:
            _enqueue(conn, "position_update", wallet=wallet, mint=m, priority=9)

        _enqueue(conn, "wallet_pnl_update", wallet=wallet, priority=8)
        _enqueue(conn, "classification_update", wallet=wallet, priority=3)

        for m in mints:
            _enqueue(conn, "token_trader_update", mint=m, priority=5)

        log.info("rebuild-wallet encolado: wallet=%s mints=%s", wallet, len(mints))
    finally:
        conn.close()


def rebuild_token(mint):
    """Recomputa ranking + stats del token y todas las posiciones del mint."""
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT walletaddress FROM wallettransactions WHERE mintaddress=%s",
                (mint,),
            )
            wallets = [r[0] for r in cur.fetchall()]

        for w in wallets:
            _enqueue(conn, "position_update", wallet=w, mint=mint, priority=9)
            _enqueue(conn, "wallet_pnl_update", wallet=w, priority=6)

        _enqueue(conn, "token_trader_update", mint=mint, priority=7)
        log.info("rebuild-token encolado: mint=%s wallets=%s", mint, len(wallets))
    finally:
        conn.close()


def reparse_signature(sig):
    """Re-encola una signature a chain_events_staging para reparseo."""
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE chain_events_staging
                   SET status='pending', attempts=0, last_error=NULL,
                       updated_at=NOW()
                 WHERE signature=%s
                """,
                (sig,),
            )
            changed = cur.rowcount
        conn.commit()
        log.info("reparse-signature: %s filas marcadas pending", changed)
    finally:
        conn.close()


def recompute_position(wallet, mint):
    conn = _db()
    try:
        _enqueue(conn, "position_update", wallet=wallet, mint=mint, priority=9)
        _enqueue(conn, "wallet_pnl_update", wallet=wallet, priority=8)
        _enqueue(conn, "token_trader_update", mint=mint, priority=7)
        log.info("recompute-position encolado: %s/%s", wallet, mint)
    finally:
        conn.close()


def dead_letter(max_attempts=10):
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT reducer_queue_dead_letter(%s)", (max_attempts,))
            moved = cur.fetchone()[0]
        conn.commit()
        log.info("dead-letter: %s filas movidas a 'dead'", moved)
    finally:
        conn.close()


def retention():
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM run_retention_cleanup()")
            rows = cur.fetchall()
        conn.commit()
        for tbl, n in rows:
            log.info("retention: %s -> %s filas eliminadas", tbl, n)
    finally:
        conn.close()


def health():
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM system_health_view")
            row = cur.fetchone()
        conn.commit()
        print(json.dumps(dict(row), default=str, indent=2))
    finally:
        conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rebuild-wallet", metavar="WALLET")
    p.add_argument("--rebuild-token", metavar="MINT")
    p.add_argument("--reparse-signature", metavar="SIG")
    p.add_argument("--recompute-position", nargs=2, metavar=("WALLET", "MINT"))
    p.add_argument("--dead-letter", action="store_true")
    p.add_argument("--max-attempts", type=int, default=10)
    p.add_argument("--retention", action="store_true")
    p.add_argument("--health", action="store_true")
    a = p.parse_args()

    if a.rebuild_wallet:
        rebuild_wallet(a.rebuild_wallet)
    elif a.rebuild_token:
        rebuild_token(a.rebuild_token)
    elif a.reparse_signature:
        reparse_signature(a.reparse_signature)
    elif a.recompute_position:
        recompute_position(*a.recompute_position)
    elif a.dead_letter:
        dead_letter(a.max_attempts)
    elif a.retention:
        retention()
    elif a.health:
        health()
    else:
        p.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()