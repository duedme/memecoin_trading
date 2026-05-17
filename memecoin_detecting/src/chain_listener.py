"""
chain_listener.py
Escucha logs del nodo Solana por WebSocket para los AMMs configurados
y escribe las firmas a chain_events_staging. No parsea todavía (eso es Fase 2).
"""
import asyncio
import json
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import websockets

from shared_config import (
    DBCONFIG,
    RPC_WS_URL,
    REALTIME_ENABLED,
    LISTENER_ENABLED,
    LISTENER_COMMITMENT,
    LISTENER_RECONNECT_MIN_SECONDS,
    LISTENER_RECONNECT_MAX_SECONDS,
    LISTENER_HEARTBEAT_SECONDS,
    get_listen_amms,
    getlogger,
)

log = getlogger("chain-listener")

PROCESS_NAME = "chain-listener"
STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    log.warning("Señal recibida (%s), apagando chain-listener...", signum)


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def db_connect():
    conn = psycopg2.connect(**DBCONFIG)
    conn.autocommit = True
    return conn


def heartbeat(conn, metadata=None):
    metadata = metadata or {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO process_heartbeat (process_name, last_seen_at, metadata, updated_at)
                VALUES (%s, NOW(), %s::jsonb, NOW())
                ON CONFLICT (process_name) DO UPDATE SET
                    last_seen_at = EXCLUDED.last_seen_at,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                (PROCESS_NAME, json.dumps(metadata)),
            )
    except Exception as e:
        log.error("heartbeat falló: %s", e)


def insert_staging(conn, signature, slot, source, program_id, program_name, raw_json, commitment):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chain_events_staging (
                    signature, slot, source, program_id, program_name,
                    commitment, raw_json, status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, 'pending', NOW(), NOW())
                ON CONFLICT (signature, source) DO NOTHING
                """,
                (signature, slot, source, program_id, program_name, commitment, json.dumps(raw_json)),
            )
            return cur.rowcount
    except Exception as e:
        log.error("insert staging falló sig=%s: %s", signature, e)
        return 0


async def subscribe_programs(ws, amms):
    """
    Envía logsSubscribe por cada program_id.
    Mapea request_id -> amm_name, y luego (en el handler) subscription_id -> amm_name.
    """
    req_to_amm = {}
    req_id = 1
    for name, program_id in amms.items():
        req = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [program_id]},
                {"commitment": LISTENER_COMMITMENT},
            ],
        }
        await ws.send(json.dumps(req))
        req_to_amm[req_id] = (name, program_id)
        log.info("subscribed req_id=%s amm=%s program=%s", req_id, name, program_id)
        req_id += 1
        await asyncio.sleep(0.05)
    return req_to_amm


async def listen_once(conn, amms, stats):
    log.info("conectando WS a %s", RPC_WS_URL)

    async with websockets.connect(
        RPC_WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=8 * 1024 * 1024,
        max_queue=4096,
    ) as ws:
        log.info("WS conectado")
        heartbeat(conn, {"status": "connected", "time": utcnow_iso(),
                         "amms": list(amms.keys())})

        req_to_amm = await subscribe_programs(ws, amms)
        sub_to_amm = {}  # subscription_id -> (name, program_id)
        last_hb = time.time()

        while not STOP:
            msg = await ws.recv()
            try:
                data = json.loads(msg)
            except Exception:
                continue

            # Respuestas de suscripción
            if "result" in data and "id" in data and isinstance(data.get("result"), int):
                sub_id = data["result"]
                info = req_to_amm.get(data.get("id"))
                if info:
                    sub_to_amm[sub_id] = info
                    log.info("sub_id=%s -> amm=%s", sub_id, info[0])
                continue

            if "error" in data:
                log.warning("JSON-RPC error: %s", data.get("error"))
                continue

            if data.get("method") != "logsNotification":
                continue

            params = data.get("params") or {}
            sub_id = params.get("subscription")
            result = params.get("result") or {}
            value = result.get("value") or {}
            ctx = result.get("context") or {}

            signature = value.get("signature")
            if not signature:
                continue
            if value.get("err") is not None:
                continue  # tx fallida on-chain

            slot = ctx.get("slot")
            amm_info = sub_to_amm.get(sub_id)
            if amm_info:
                amm_name, program_id = amm_info
            else:
                amm_name, program_id = "unknown", None

            raw = {
                "subscription": sub_id,
                "logs": value.get("logs", []),
            }

            inserted = insert_staging(
                conn=conn,
                signature=signature,
                slot=slot,
                source=amm_name,
                program_id=program_id,
                program_name=amm_name,
                raw_json=raw,
                commitment=LISTENER_COMMITMENT,
            )
            stats["events_total"] += 1
            stats["events_since_hb"] += inserted

            if stats["events_total"] % 100 == 0:
                log.info("events_total=%s last_sig=%s slot=%s",
                         stats["events_total"], signature, slot)

            if time.time() - last_hb >= LISTENER_HEARTBEAT_SECONDS:
                heartbeat(conn, {
                    "status": "running",
                    "events_total": stats["events_total"],
                    "events_since_hb": stats["events_since_hb"],
                    "reconnects": stats["reconnects"],
                    "last_signature": signature,
                    "last_slot": slot,
                    "time": utcnow_iso(),
                })
                stats["events_since_hb"] = 0
                last_hb = time.time()


async def run():
    if not REALTIME_ENABLED:
        log.warning("REALTIME_ENABLED=false. No arrancamos.")
        return
    if not LISTENER_ENABLED:
        log.warning("LISTENER_ENABLED=false. No arrancamos.")
        return

    amms = get_listen_amms()
    if not amms:
        log.error("No hay AMMs configurados en LISTEN_AMMS.")
        sys.exit(1)

    log.info("AMMs a escuchar: %s", list(amms.keys()))

    stats = {"events_total": 0, "events_since_hb": 0, "reconnects": 0}
    backoff = LISTENER_RECONNECT_MIN_SECONDS

    while not STOP:
        conn = None
        try:
            conn = db_connect()
            await listen_once(conn, amms, stats)
            backoff = LISTENER_RECONNECT_MIN_SECONDS
        except websockets.ConnectionClosed as e:
            stats["reconnects"] += 1
            log.warning("WS cerrado: %s. Reconectando en %ss", e, backoff)
        except Exception as e:
            stats["reconnects"] += 1
            log.exception("Error chain-listener: %s. Reconectando en %ss", e, backoff)
        finally:
            if conn is not None:
                try:
                    heartbeat(conn, {"status": "disconnected", "time": utcnow_iso()})
                    conn.close()
                except Exception:
                    pass

        if STOP:
            break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, LISTENER_RECONNECT_MAX_SECONDS)

    log.warning("chain-listener detenido.")


if __name__ == "__main__":
    asyncio.run(run())