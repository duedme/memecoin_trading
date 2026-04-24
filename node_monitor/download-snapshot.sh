#!/bin/bash
set -uo pipefail

SNAPSHOT_DIR="${1:-/mnt/snapshot/remote}"
RPC="https://api.mainnet-beta.solana.com"

# Aceptamos fulls hasta 300k slots atrás (~28h, un intervalo full)
MAX_AGE_SLOTS=300000
# Cuántos peers evaluar
TOP_N=40
# Cuántos candidatos considerar tras ordenar por frescura
MAX_CANDIDATES=10

log() { echo "$(date '+%H:%M:%S') | DL | $*"; }

# ── 1. Slot actual del cluster ──
CURRENT_SLOT=$(curl -s --max-time 10 "$RPC" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' | jq -r '.result')
log "Current cluster slot: $CURRENT_SLOT"

if [ -z "$CURRENT_SLOT" ] || [ "$CURRENT_SLOT" = "null" ]; then
  log "ERROR: Could not get current slot"
  exit 1
fi

# ── 2. Lista de peers con RPC activo ──
PEERS=$(curl -s --max-time 30 "$RPC" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getClusterNodes"}' \
  | jq -r '.result[] | select(.rpc != null) | .rpc' | head -"$TOP_N")

if [ -z "$PEERS" ]; then
  log "ERROR: No RPC peers found"
  exit 1
fi

# ── 3. Consultar frescura de snapshot de cada peer ──
log "Querying $(echo "$PEERS" | wc -l) peers for snapshot freshness..."
CANDIDATES_FILE=$(mktemp)

for PEER in $PEERS; do
  SNAP_INFO=$(curl -s --max-time 3 "http://$PEER" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getHighestSnapshotSlot"}' 2>/dev/null)

  FULL_SLOT=$(echo "$SNAP_INFO" | jq -r '.result.full // empty' 2>/dev/null)
  INCR_SLOT=$(echo "$SNAP_INFO" | jq -r '.result.incremental // empty' 2>/dev/null)

  if [ -z "$FULL_SLOT" ] || [ "$FULL_SLOT" = "null" ]; then
    continue
  fi

  AGE=$((CURRENT_SLOT - FULL_SLOT))
  FRESH_SLOT="$FULL_SLOT"
  if [ -n "$INCR_SLOT" ] && [ "$INCR_SLOT" != "null" ]; then
    FRESH_SLOT="$INCR_SLOT"
  fi
  FRESH_AGE=$((CURRENT_SLOT - FRESH_SLOT))

  if [ "$AGE" -gt "$MAX_AGE_SLOTS" ]; then
    continue
  fi

  echo "$FRESH_AGE $PEER $FULL_SLOT $INCR_SLOT" >> "$CANDIDATES_FILE"
done

# Ordenar por frescura (menor fresh_age primero) y tomar TOP
CANDIDATES=$(sort -n "$CANDIDATES_FILE" | head -"$MAX_CANDIDATES")
rm -f "$CANDIDATES_FILE"

if [ -z "$CANDIDATES" ]; then
  log "ERROR: No peer within MAX_AGE_SLOTS=$MAX_AGE_SLOTS"
  exit 1
fi

log "Top $MAX_CANDIDATES candidates (fresh_age peer full incr):"
echo "$CANDIDATES" | while read -r LINE; do log "  $LINE"; done

# ── 4. Probar peer por peer, hasta que uno descargue bien ──
while read -r FRESH_AGE PEER FULL_SLOT INCR_SLOT; do
  log "Trying peer $PEER (full=$FULL_SLOT incr=$INCR_SLOT fresh_age=$FRESH_AGE)..."

  # Verificar HTTP
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -L \
    "http://$PEER/snapshot.tar.bz2")
  if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "302" ]; then
    log "  HTTP $HTTP_CODE, skipping"
    continue
  fi

  # Descargar FULL con aria2c (1 conexión, peer Solana no soporta Range)
  log "  Downloading full snapshot from $PEER..."
  cd "$SNAPSHOT_DIR"

  if sudo -u solv aria2c \
      --max-connection-per-server=1 \
      --split=1 \
      --allow-overwrite=true \
      --auto-file-renaming=false \
      --console-log-level=warn \
      --summary-interval=30 \
      --max-tries=3 \
      --retry-wait=10 \
      --connect-timeout=15 \
      --timeout=120 \
      --content-disposition-default-utf8=true \
      --remote-time=true \
      --file-allocation=none \
      -d "$SNAPSHOT_DIR" \
      "http://$PEER/snapshot.tar.bz2"; then

    log "  Full downloaded from $PEER. Fetching incremental..."
    sudo -u solv aria2c \
        --max-connection-per-server=1 \
        --split=1 \
        --allow-overwrite=true \
        --auto-file-renaming=false \
        --console-log-level=warn \
        --max-tries=3 \
        --retry-wait=5 \
        --connect-timeout=15 \
        --timeout=60 \
        --content-disposition-default-utf8=true \
        --remote-time=true \
        --file-allocation=none \
        -d "$SNAPSHOT_DIR" \
        "http://$PEER/incremental-snapshot.tar.bz2" || \
        log "  Incremental not available (OK)"

    log "SUCCESS: Download complete from $PEER"
    exit 0
  else
    log "  aria2c failed on $PEER, trying next peer..."
    sudo rm -f "$SNAPSHOT_DIR"/*.aria2 2>/dev/null
  fi
done <<< "$CANDIDATES"

log "ERROR: All download attempts failed"
exit 1