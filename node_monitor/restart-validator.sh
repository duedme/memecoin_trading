#!/bin/bash
set -uo pipefail

# ──────────────────────────────────────────────────────────────
#  Solana RPC Node Restart Script — v5
#  Cambios vs v4:
#  - Elimina snapshot-finder (Docker) por bug 'str' object
#  - Elimina wget a mirrors externos (todos fallan desde esta IP)
#  - Usa download-snapshot.sh propio (bash + aria2c, 1 peer)
#  - Mantiene: reuso de full hasta 24h, validación tar -tf,
#    NO arranca validator si no hay snapshot válido
# ──────────────────────────────────────────────────────────────

LOG_FILE="/var/log/solana-restart.log"
SNAPSHOT_DIR="/mnt/snapshot/remote"
ACCOUNTS_DIR="/mnt/accounts"
LEDGER_DIR="/mnt/ledger"
SERVICE_NAME="solv.service"
LOCKFILE="/tmp/solana-restart.lock"
DOWNLOADER="/etc/solana/download-snapshot.sh"

# Edad máxima del full snapshot antes de re-descargarlo (horas)
FULL_MAX_AGE_HOURS=24
# Tamaño mínimo aceptable para un full snapshot (MB)
FULL_MIN_SIZE_MB=100
# Tamaño mínimo aceptable para incremental (MB)
INCR_MIN_SIZE_MB=1

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"
}

# ══════════════════════════════════════════════════════════════
#  Lockfile
# ══════════════════════════════════════════════════════════════
if [ -f "$LOCKFILE" ]; then
  OTHER_PID=$(cat "$LOCKFILE" 2>/dev/null || true)
  if kill -0 "$OTHER_PID" 2>/dev/null; then
    log "ERROR: Another restart is already running (PID $OTHER_PID). Exiting."
    exit 1
  else
    log "WARN: Stale lockfile found. Removing."
    rm -f "$LOCKFILE"
  fi
fi
echo $$ > "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

log "========== RESTART INITIATED =========="
sudo date +%s > /tmp/solana-monitor-grace

# ══════════════════════════════════════════════════════════════
#  Step 1: Detener validator
# ══════════════════════════════════════════════════════════════
log "Step 1: Stopping $SERVICE_NAME..."
sudo systemctl stop "$SERVICE_NAME" 2>/dev/null || true
sleep 3
if pgrep -f agave-validator > /dev/null 2>&1; then
  log "WARN: Validator still running, sending SIGKILL..."
  sudo pkill -9 -f agave-validator || true
  sleep 2
fi
log "Validator stopped."

# ══════════════════════════════════════════════════════════════
#  Step 2: Limpieza selectiva
#  - Siempre: accounts, ledger, incrementales, residuos .aria2
#  - Full snapshot: solo si es muy viejo o no pasa validación
# ══════════════════════════════════════════════════════════════
log "Step 2: Cleaning accounts, ledger, and stale snapshots..."

# Limpiar siempre
sudo rm -rf "${ACCOUNTS_DIR:?}"/*
sudo rm -rf "${LEDGER_DIR:?}"/rocksdb
sudo rm -rf "${LEDGER_DIR:?}"/accounts_run
sudo rm -rf "${LEDGER_DIR:?}"/accounts_snapshot
sudo rm -f  "${SNAPSHOT_DIR:?}"/*.aria2
sudo rm -f  "${SNAPSHOT_DIR:?}"/tmp-*
sudo rm -f  "${SNAPSHOT_DIR:?}"/snapshot-finder.log
sudo rm -f  "${SNAPSHOT_DIR:?}"/wget-log*
sudo rm -f  "${SNAPSHOT_DIR:?}"/incremental-snapshot-*.tar.bz2
sudo rm -f  "${SNAPSHOT_DIR:?}"/incremental-snapshot-*.tar.zst

# ── Evaluar si conservamos el full existente ──
FULL_FILE=$(find "$SNAPSHOT_DIR" -maxdepth 1 -type f \
  \( -name "snapshot-*.tar.zst" -o -name "snapshot-*.tar.bz2" \) \
  ! -name "*.aria2" ! -name "incremental-*" 2>/dev/null | head -1)

REUSE_FULL=false
if [ -n "$FULL_FILE" ]; then
  FULL_AGE_HOURS=$(( ($(date +%s) - $(stat -c %Y "$FULL_FILE")) / 3600 ))
  FULL_SIZE_MB=$(( $(stat -c %s "$FULL_FILE") / 1024 / 1024 ))

  if [ "$FULL_AGE_HOURS" -lt "$FULL_MAX_AGE_HOURS" ] \
     && [ "$FULL_SIZE_MB" -gt "$FULL_MIN_SIZE_MB" ] \
     && tar -tf "$FULL_FILE" > /dev/null 2>&1; then
    REUSE_FULL=true
    log "Reusing full snapshot: $(basename "$FULL_FILE") (age=${FULL_AGE_HOURS}h, size=${FULL_SIZE_MB}MB)"
  else
    log "Full snapshot invalid or too old (age=${FULL_AGE_HOURS}h, size=${FULL_SIZE_MB}MB). Will re-download."
    sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot-*.tar.bz2
    sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot-*.tar.zst
  fi
else
  log "No existing full snapshot found."
fi
log "Cleanup done."

# ══════════════════════════════════════════════════════════════
#  Funciones de validación
# ══════════════════════════════════════════════════════════════
validate_full_snapshot() {
  local f
  f=$(find "$SNAPSHOT_DIR" -maxdepth 1 -type f \
    \( -name "snapshot-*.tar.zst" -o -name "snapshot-*.tar.bz2" \) \
    ! -name "*.aria2" ! -name "incremental-*" \
    -size +"${FULL_MIN_SIZE_MB}"M 2>/dev/null | head -1)
  [ -z "$f" ] && return 1
  tar -tf "$f" > /dev/null 2>&1
}

validate_incremental_snapshot() {
  local f
  f=$(find "$SNAPSHOT_DIR" -maxdepth 1 -type f \
    \( -name "incremental-snapshot-*.tar.zst" -o -name "incremental-snapshot-*.tar.bz2" \) \
    ! -name "*.aria2" \
    -size +"${INCR_MIN_SIZE_MB}"M 2>/dev/null | head -1)
  [ -z "$f" ] && return 1
  tar -tf "$f" > /dev/null 2>&1
}

# ══════════════════════════════════════════════════════════════
#  Step 3: Descargar snapshot (si no reusamos full)
# ══════════════════════════════════════════════════════════════
log "Step 3: Downloading fresh snapshot..."
cd "$SNAPSHOT_DIR"
DOWNLOAD_START=$(date +%s)
DOWNLOAD_OK=false

sudo chown -R solv:solv "$SNAPSHOT_DIR"
sudo chmod 755 "$SNAPSHOT_DIR"

if [ "$REUSE_FULL" = true ]; then
  # Reusando full viejo: solo bajar incremental fresco
  log "Reusing existing full. Fetching fresh incremental only..."
  if bash "$DOWNLOADER" "$SNAPSHOT_DIR" 2>&1 | tee -a "$LOG_FILE"; then
    # El downloader intentará bajar full + incremental; el full nuevo
    # puede coexistir o ser el mismo. Validamos al final.
    if validate_full_snapshot; then
      DOWNLOAD_OK=true
      log "SUCCESS: downloader completed (reusing or refreshed)"
    else
      log "WARN: full snapshot no longer valid after downloader"
    fi
  else
    log "WARN: downloader exited non-zero"
    # Aún así, si el full viejo sigue siendo válido, continuamos
    if validate_full_snapshot; then
      log "NOTE: existing full is still valid, proceeding without fresh incremental"
      DOWNLOAD_OK=true
    fi
  fi
else
  # Sin full válido: descarga completa
  log "Method 1: custom bash downloader (single peer)..."
  if bash "$DOWNLOADER" "$SNAPSHOT_DIR" 2>&1 | tee -a "$LOG_FILE"; then
    if validate_full_snapshot; then
      DOWNLOAD_OK=true
      log "SUCCESS: bash downloader produced valid full snapshot"
    else
      log "WARN: downloader exited 0 but full snapshot is invalid"
    fi
  else
    log "WARN: bash downloader failed"
  fi
fi

DOWNLOAD_END=$(date +%s)
DOWNLOAD_DURATION=$(( DOWNLOAD_END - DOWNLOAD_START ))
log "Download phase completed in ${DOWNLOAD_DURATION}s"

# ══════════════════════════════════════════════════════════════
#  Verificación final: si no hay snapshot, NO arrancar
# ══════════════════════════════════════════════════════════════
if [ "$DOWNLOAD_OK" = false ] || ! validate_full_snapshot; then
  log "CRITICAL: No valid snapshot available after all methods."
  log "NOT starting validator. Monitor will retry on next cron tick."
  rm -f /tmp/solana-monitor-grace
  sudo rm -f "${SNAPSHOT_DIR:?}"/*.aria2
  exit 1
fi

log "Found snapshot files:"
ls -lh "$SNAPSHOT_DIR"/snapshot-*.tar.* 2>&1 | tee -a "$LOG_FILE"
ls -lh "$SNAPSHOT_DIR"/incremental-snapshot-*.tar.* 2>&1 | tee -a "$LOG_FILE" || true

# ══════════════════════════════════════════════════════════════
#  Step 4: Permisos
# ══════════════════════════════════════════════════════════════
log "Step 4: Fixing ownership..."
sudo chmod 755 "$SNAPSHOT_DIR"
sudo chown -R solv:solv "$SNAPSHOT_DIR"
sudo chown -R solv:solv "$ACCOUNTS_DIR"
sudo chown -R solv:solv "$LEDGER_DIR"

# ══════════════════════════════════════════════════════════════
#  Step 5: Arrancar validator
# ══════════════════════════════════════════════════════════════
log "Step 5: Starting $SERVICE_NAME..."
sudo systemctl start "$SERVICE_NAME"
sleep 5

if systemctl is-active --quiet "$SERVICE_NAME"; then
  log "Validator started successfully."
else
  log "ERROR: Validator failed to start! Check: journalctl -u $SERVICE_NAME"
  exit 1
fi

log "========== RESTART COMPLETE =========="