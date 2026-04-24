#!/bin/bash
set -uo pipefail

# ──────────────────────────────────────────────────────────────
#  Solana RPC Node Restart Script
#  - snapshot-finder con versión fija (evita bug 'str' object)
#  - wget como fallback (más estable que aria2c en nuestra red)
#  - Validación real del snapshot con tar -tf
#  - Reutiliza full snapshot hasta 24h; solo re-baja incremental
#  - NO arranca validator si no hay snapshot válido
# ──────────────────────────────────────────────────────────────

LOG_FILE="/var/log/solana-restart.log"
SNAPSHOT_DIR="/mnt/snapshot/remote"
ACCOUNTS_DIR="/mnt/accounts"
LEDGER_DIR="/mnt/ledger"
SERVICE_NAME="solv.service"
LOCKFILE="/tmp/solana-restart.lock"

# Edad máxima del full snapshot antes de re-descargarlo (horas)
FULL_MAX_AGE_HOURS=24
# Tamaño mínimo aceptable para un full snapshot (bytes) — ~100 MB
FULL_MIN_SIZE_MB=100
# Tamaño mínimo aceptable para incremental (bytes) — ~1 MB
INCR_MIN_SIZE_MB=1

# Mirrors para fallback (wget)
MIRRORS=(
  "https://snapshots.avorio.network/mainnet-beta"
  "https://solana-snapshot.mainnet.rpcpool.com"
)

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"
}

# ── Lockfile ──
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
#  Función: validar un full snapshot en el directorio
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
#  Step 3: Descargar snapshot
# ══════════════════════════════════════════════════════════════
log "Step 3: Downloading fresh snapshot..."
cd "$SNAPSHOT_DIR"
DOWNLOAD_START=$(date +%s)
DOWNLOAD_OK=false

sudo chmod 777 "$SNAPSHOT_DIR"
sudo touch "$SNAPSHOT_DIR/snapshot-finder.log"
sudo chmod 666 "$SNAPSHOT_DIR/snapshot-finder.log"

# ── Método 1: snapshot-finder (versión fija, sin bug de 'str'.text) ──
if [ "$REUSE_FULL" = false ] && command -v docker &> /dev/null; then
  log "Method 1: snapshot-finder v0.3.4 (full + incremental)..."

  sudo docker run --rm \
    --user 0:0 \
    -v "$SNAPSHOT_DIR":/snapshots \
    c29r3/solana-snapshot-finder:v0.3.4 \
    --snapshot_path /snapshots \
    --num_of_retries 5 \
    --with_private_rpc \
    --min_download_speed 50 2>&1 | tee -a "$LOG_FILE"

  if validate_full_snapshot; then
    DOWNLOAD_OK=true
    log "SUCCESS: snapshot-finder downloaded valid full snapshot"
  else
    log "WARN: snapshot-finder did not produce a valid full snapshot"
    sudo rm -f "${SNAPSHOT_DIR:?}"/*.aria2
  fi
elif [ "$REUSE_FULL" = true ]; then
  log "Method 1 skipped: reusing existing full snapshot"
  DOWNLOAD_OK=true

  # Intentar bajar solo incremental con snapshot-finder
  if command -v docker &> /dev/null; then
    log "Downloading fresh incremental snapshot..."
    sudo docker run --rm \
      --user 0:0 \
      -v "$SNAPSHOT_DIR":/snapshots \
      c29r3/solana-snapshot-finder:v0.3.4 \
      --snapshot_path /snapshots \
      --num_of_retries 3 \
      --with_private_rpc \
      --min_download_speed 50 2>&1 | tee -a "$LOG_FILE" || true

    if validate_incremental_snapshot; then
      log "SUCCESS: Incremental snapshot downloaded"
    else
      log "WARN: No valid incremental; validator will rely on full + gossip repair"
      sudo rm -f "${SNAPSHOT_DIR:?}"/incremental-*.aria2
    fi
  fi
fi

# ── Método 2: wget directo a mirrors (fallback) ──
if [ "$DOWNLOAD_OK" = false ]; then
  log "Method 2: wget fallback to public mirrors..."

  for MIRROR in "${MIRRORS[@]}"; do
    log "Trying mirror: $MIRROR"

    # Limpia residuos de intento previo
    sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot.tar.* 2>/dev/null
    sudo rm -f "${SNAPSHOT_DIR:?}"/incremental-snapshot.tar.* 2>/dev/null
    sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot-*.tar.* 2>/dev/null

    # wget con trust-server-names para que guarde con el nombre real (redirect)
    sudo wget \
      --tries=3 \
      --timeout=120 \
      --continue \
      --trust-server-names \
      --content-disposition \
      --no-check-certificate \
      -P "$SNAPSHOT_DIR" \
      "$MIRROR/snapshot.tar.bz2" 2>&1 | tee -a "$LOG_FILE" || true

    sudo wget \
      --tries=3 \
      --timeout=120 \
      --continue \
      --trust-server-names \
      --content-disposition \
      --no-check-certificate \
      -P "$SNAPSHOT_DIR" \
      "$MIRROR/incremental-snapshot.tar.bz2" 2>&1 | tee -a "$LOG_FILE" || true

    if validate_full_snapshot; then
      DOWNLOAD_OK=true
      log "SUCCESS: Downloaded valid full snapshot from $MIRROR"
      break
    else
      log "WARN: Mirror $MIRROR did not produce a valid snapshot"
    fi
  done
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
  # Limpiar grace file para que el monitor pueda reintentar pronto
  rm -f /tmp/solana-monitor-grace
  # Limpiar residuos
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