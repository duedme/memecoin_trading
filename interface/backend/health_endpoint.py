"""
health_endpoint.py
Blueprint de /health consolidado.
"""
import os
import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "memecoins")
DB_USER = os.getenv("DB_USER", "memecoin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "12345")

health_bp = Blueprint("health", __name__)


def _db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


@health_bp.route("/health")
def health():
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM system_health_view")
            row = cur.fetchone()

            cur.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM wallettransactions WHERE time > NOW() - INTERVAL '5 minutes') AS tx_5m,
                  (SELECT COUNT(*) FROM wallettransactions WHERE time > NOW() - INTERVAL '1 hour')   AS tx_1h,
                  (SELECT COUNT(*) FROM walletpositions WHERE lastupdate > NOW() - INTERVAL '5 minutes') AS pos_updated_5m,
                  (SELECT COUNT(*) FROM walletpnlcache WHERE lastupdated > NOW() - INTERVAL '5 minutes') AS pnl_updated_5m,
                  (SELECT COUNT(*) FROM tokenmarketcache WHERE lastupdated > NOW() - INTERVAL '5 minutes') AS market_updated_5m
                """
            )
            counts = cur.fetchone()
    finally:
        conn.close()

    workers = (row or {}).get("workers") or {}
    any_unhealthy = any(
        not (w or {}).get("healthy", False) for w in workers.values()
    ) if workers else True

    return jsonify({
        "status": "degraded" if any_unhealthy else "ok",
        "workers": workers,
        "staging": (row or {}).get("staging"),
        "queue": (row or {}).get("queue"),
        "counts_5m_1h": counts,
    }), (200 if not any_unhealthy else 503)