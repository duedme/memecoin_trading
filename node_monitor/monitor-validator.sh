#!/bin/bash
set -uo pipefail

# ──────────────────────────────────────────────────────────────
#  Solana RPC Node Monitor — v3 (con reinicio preventivo por tiempo)
# ──────────────────────────────────────────────────────────────

RPC_URL="http://localhost:7211"
PUBLIC_RPC="https://api.mainnet-beta.solana.com"

# --- Umbrales ---
MAX_SLOTS_BEHIND=500          # Gap instantáneo máximo permitido
AVG_GAP_THRESHOLD=150         # Promedio móvil máximo del gap (últimas N muestras)
MIN_SLOT_VELOCITY=0.50        # Ratio mínimo: (local_delta / cluster_delta). <1 = cayendo atrás
WINDOW_SIZE=10                # Cuántas muestras guardar para el promedio móvil
MAX_UPTIME_MINUTES=90         # Reinicio preventivo cada X minutos (0 = deshabilitado)

# --- Archivos de estado ---
GRACE_PERIOD_FILE="/tmp/solana-monitor-grace"
GRACE_MINUTES=40
LOG_FILE="/var/log/solana-monitor.log"
RESTART_SCRIPT="/etc/solana/restart-validator.sh"
LOCKFILE="/tmp/solana-restart.lock"
CONSECUTIVE_FILE="/tmp/solana-unhealthy-count"
CONSECUTIVE_THRESHOLD=3
GAP_HISTORY_FILE="/tmp/solana-gap-history"        # Historial de gaps (una línea por muestra)
LAST_SLOTS_FILE="/tmp/solana-last-slots"          # Última lectura: "local_slot cluster_slot"
VELOCITY_FAIL_FILE="/tmp/solana-velocity-fail"    # Contador de fallos de velocidad

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') | MONITOR | $1" >> "$LOG_FILE"
}

# ── Verificar lockfile de reinicio ──
if [ -f "$LOCKFILE" ]; then
  LOCK_PID=$(cat "$LOCKFILE" 2>/dev/null || true)
  if kill -0 "$LOCK_PID" 2>/dev/null; then
    log "Restart in progress (PID $LOCK_PID). Skipping check."
    exit 0
  fi
fi

# ══════════════════════════════════════════════════════════════
#  REINICIO PREVENTIVO POR TIEMPO
# ══════════════════════════════════════════════════════════════
if [ "$MAX_UPTIME_MINUTES" -gt 0 ] && [ -f "$GRACE_PERIOD_FILE" ]; then
  LAST_RESTART=$(cat "$GRACE_PERIOD_FILE")
  NOW=$(date +%s)
  UPTIME_MIN=$(( (NOW - LAST_RESTART) / 60 ))

  if [ "$UPTIME_MIN" -ge "$MAX_UPTIME_MINUTES" ]; then
    log "PREVENTIVE RESTART: ${UPTIME_MIN} min since last restart (max=${MAX_UPTIME_MINUTES})"
    date +%s > "$GRACE_PERIOD_FILE"
    rm -f "$GAP_HISTORY_FILE" "$LAST_SLOTS_FILE" "$VELOCITY_FAIL_FILE"
    echo "0" > "$CONSECUTIVE_FILE"
    bash "$RESTART_SCRIPT" >> "$LOG_FILE" 2>&1 &
    exit 0
  fi
fi

# ── Período de gracia post-reinicio ──
if [ -f "$GRACE_PERIOD_FILE" ]; then
  GRACE_TIME=$(cat "$GRACE_PERIOD_FILE")
  NOW=$(date +%s)
  ELAPSED=$(( (NOW - GRACE_TIME) / 60 ))
  if [ "$ELAPSED" -lt "$GRACE_MINUTES" ]; then
    log "In grace period ($ELAPSED/${GRACE_MINUTES} min since last restart). Skipping."
    exit 0
  fi
fi

# ── Verificar que el servicio esté corriendo ──
if ! systemctl is-active --quiet solv.service; then
  log "WARNING: solv.service is not running! Attempting start..."
  sudo systemctl start solv.service
  sleep 10
fi

# ── Método 1: getHealth ──
HEALTH_RESPONSE=$(curl -s --max-time 10 "$RPC_URL" \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' 2>/dev/null || echo "CURL_FAILED")

if [ "$HEALTH_RESPONSE" = "CURL_FAILED" ] || [ -z "$HEALTH_RESPONSE" ]; then
  log "WARNING: Could not reach RPC at $RPC_URL."
  COUNT=$(cat "$CONSECUTIVE_FILE" 2>/dev/null || echo 0)
  COUNT=$((COUNT + 1))
  echo "$COUNT" > "$CONSECUTIVE_FILE"
  if [ "$COUNT" -ge 10 ]; then
    log "RPC unreachable for $COUNT consecutive checks. Triggering restart."
    echo "0" > "$CONSECUTIVE_FILE"
    date +%s > "$GRACE_PERIOD_FILE"
    rm -f "$GAP_HISTORY_FILE" "$LAST_SLOTS_FILE" "$VELOCITY_FAIL_FILE"
    bash "$RESTART_SCRIPT" >> "$LOG_FILE" 2>&1 &
  fi
  exit 0
fi

IS_HEALTHY=$(echo "$HEALTH_RESPONSE" | jq -r '.result // empty' 2>/dev/null)

# ── Método 2: Comparar slot local vs cluster ──
LOCAL_SLOT=$(curl -s --max-time 10 "$RPC_URL" \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' 2>/dev/null | jq -r '.result // empty' 2>/dev/null)

CLUSTER_SLOT=$(curl -s --max-time 10 "$PUBLIC_RPC" \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' 2>/dev/null | jq -r '.result // empty' 2>/dev/null)

if [ -n "$LOCAL_SLOT" ] && [ -n "$CLUSTER_SLOT" ] && [ "$CLUSTER_SLOT" != "null" ] && [ "$LOCAL_SLOT" != "null" ]; then
  REAL_GAP=$((CLUSTER_SLOT - LOCAL_SLOT))
  SLOTS_BEHIND=$REAL_GAP
else
  log "Could not compare slots (local=$LOCAL_SLOT cluster=$CLUSTER_SLOT). Falling back to getHealth."
  if [ "$IS_HEALTHY" = "ok" ]; then
    SLOTS_BEHIND=0
  else
    SLOTS_BEHIND_RAW=$(echo "$HEALTH_RESPONSE" | jq -r '.error.data.numSlotsBehind // empty' 2>/dev/null)
    if [ -z "$SLOTS_BEHIND_RAW" ] || [ "$SLOTS_BEHIND_RAW" = "null" ]; then
      SLOTS_BEHIND=999999
    else
      SLOTS_BEHIND=$SLOTS_BEHIND_RAW
    fi
  fi
  LOCAL_SLOT=""
  CLUSTER_SLOT=""
fi

# ══════════════════════════════════════════════════════════════
#  DETECCIÓN 1: VELOCIDAD DE SLOTS (¿el nodo avanza lo suficiente?)
# ══════════════════════════════════════════════════════════════
VELOCITY_OK=true
VELOCITY_STR="n/a"

if [ -n "$LOCAL_SLOT" ] && [ -n "$CLUSTER_SLOT" ] && [ -f "$LAST_SLOTS_FILE" ]; then
  read -r PREV_LOCAL PREV_CLUSTER < "$LAST_SLOTS_FILE" 2>/dev/null || true
  if [ -n "$PREV_LOCAL" ] && [ -n "$PREV_CLUSTER" ]; then
    LOCAL_DELTA=$((LOCAL_SLOT - PREV_LOCAL))
    CLUSTER_DELTA=$((CLUSTER_SLOT - PREV_CLUSTER))

    if [ "$CLUSTER_DELTA" -gt 0 ]; then
      # bash no hace float; multiplicamos por 100 para comparar
      VELOCITY_X100=$((LOCAL_DELTA * 100 / CLUSTER_DELTA))
      THRESHOLD_X100=$(echo "$MIN_SLOT_VELOCITY" | awk '{printf "%d", $1 * 100}')
      VELOCITY_STR="${VELOCITY_X100}%"

      if [ "$VELOCITY_X100" -lt "$THRESHOLD_X100" ]; then
        VELOCITY_OK=false
      fi
    fi
  fi
fi

# Guardar slots actuales para la próxima iteración
if [ -n "$LOCAL_SLOT" ] && [ -n "$CLUSTER_SLOT" ]; then
  echo "$LOCAL_SLOT $CLUSTER_SLOT" > "$LAST_SLOTS_FILE"
fi

# ══════════════════════════════════════════════════════════════
#  DETECCIÓN 2: PROMEDIO MÓVIL DEL GAP
# ══════════════════════════════════════════════════════════════
echo "$SLOTS_BEHIND" >> "$GAP_HISTORY_FILE"
# Mantener solo las últimas WINDOW_SIZE líneas
tail -n "$WINDOW_SIZE" "$GAP_HISTORY_FILE" > "${GAP_HISTORY_FILE}.tmp" && mv "${GAP_HISTORY_FILE}.tmp" "$GAP_HISTORY_FILE"

AVG_GAP=$(awk '{ sum += $1; n++ } END { if(n>0) printf "%d", sum/n; else print 0 }' "$GAP_HISTORY_FILE")
SAMPLE_COUNT=$(wc -l < "$GAP_HISTORY_FILE" | tr -d ' ')

AVG_EXCEEDED=false
if [ "$SAMPLE_COUNT" -ge 3 ] && [ "$AVG_GAP" -ge "$AVG_GAP_THRESHOLD" ]; then
  AVG_EXCEEDED=true
fi

# ══════════════════════════════════════════════════════════════
#  LOG DETALLADO
# ══════════════════════════════════════════════════════════════
log "Slot check: local=$LOCAL_SLOT cluster=$CLUSTER_SLOT gap=$SLOTS_BEHIND avg_gap=$AVG_GAP(${SAMPLE_COUNT}samples) velocity=$VELOCITY_STR getHealth=$IS_HEALTHY"

# ══════════════════════════════════════════════════════════════
#  DECISIÓN: ¿Está saludable?
# ══════════════════════════════════════════════════════════════
NODE_UNHEALTHY=false
REASON=""

# Criterio 1: Gap instantáneo excede el máximo
if [ "$SLOTS_BEHIND" -gt "$MAX_SLOTS_BEHIND" ] 2>/dev/null; then
  NODE_UNHEALTHY=true
  REASON="gap=${SLOTS_BEHIND}>${MAX_SLOTS_BEHIND}"
fi

# Criterio 2: Promedio móvil excede umbral
if [ "$AVG_EXCEEDED" = true ]; then
  NODE_UNHEALTHY=true
  REASON="${REASON:+$REASON, }avg_gap=${AVG_GAP}>=${AVG_GAP_THRESHOLD}"
fi

# Criterio 3: Velocidad de procesamiento muy baja
if [ "$VELOCITY_OK" = false ]; then
  VFAIL=$(cat "$VELOCITY_FAIL_FILE" 2>/dev/null || echo 0)
  VFAIL=$((VFAIL + 1))
  echo "$VFAIL" > "$VELOCITY_FAIL_FILE"
  if [ "$VFAIL" -ge "$CONSECUTIVE_THRESHOLD" ]; then
    NODE_UNHEALTHY=true
    REASON="${REASON:+$REASON, }low_velocity=${VELOCITY_STR}(${VFAIL}consecutive)"
  else
    log "Velocity low ($VELOCITY_STR), fail $VFAIL/$CONSECUTIVE_THRESHOLD"
  fi
else
  # Decrementar en vez de resetear a 0
  VFAIL=$(cat "$VELOCITY_FAIL_FILE" 2>/dev/null || echo 0)
  if [ "$VFAIL" -gt 0 ]; then
    VFAIL=$((VFAIL - 1))
    echo "$VFAIL" > "$VELOCITY_FAIL_FILE"
  fi
fi

if [ "$NODE_UNHEALTHY" = false ]; then
  log "OK: $SLOTS_BEHIND slots behind (avg=$AVG_GAP). Within thresholds."
  # Decrementar en vez de resetear a 0
  COUNT=$(cat "$CONSECUTIVE_FILE" 2>/dev/null || echo 0)
  if [ "$COUNT" -gt 0 ]; then
    COUNT=$((COUNT - 1))
    echo "$COUNT" > "$CONSECUTIVE_FILE"
  fi
  exit 0
fi

# ── Nodo no saludable: incrementar contador ──
log "UNHEALTHY: $REASON"

COUNT=$(cat "$CONSECUTIVE_FILE" 2>/dev/null || echo 0)
COUNT=$((COUNT + 1))
echo "$COUNT" > "$CONSECUTIVE_FILE"
log "Unhealthy check $COUNT/$CONSECUTIVE_THRESHOLD"

if [ "$COUNT" -ge "$CONSECUTIVE_THRESHOLD" ]; then
  log "ALERT: Triggering restart! Reasons: $REASON (${COUNT} consecutive fails)"
  echo "0" > "$CONSECUTIVE_FILE"
  date +%s > "$GRACE_PERIOD_FILE"
  rm -f "$GAP_HISTORY_FILE" "$LAST_SLOTS_FILE" "$VELOCITY_FAIL_FILE"
  bash "$RESTART_SCRIPT" >> "$LOG_FILE" 2>&1 &
fi
