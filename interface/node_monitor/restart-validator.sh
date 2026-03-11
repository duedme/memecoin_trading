#!/bin/bash
set -uo pipefail

LOG_FILE="/var/log/solana-restart.log"
SNAPSHOT_DIR="/mnt/snapshot/remote"
ACCOUNTS_DIR="/mnt/accounts"
LEDGER_DIR="/mnt/ledger"
SERVICE_NAME="solv.service"
LOCKFILE="/tmp/solana-restart.lock"

AVORIO_FULL="https://snapshots.avorio.network/mainnet-beta/snapshot.tar.bz2"
AVORIO_INCR="https://snapshots.avorio.network/mainnet-beta/incremental-snapshot.tar.bz2"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"
}

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

log "Step 1: Stopping $SERVICE_NAME..."
sudo systemctl stop "$SERVICE_NAME" 2>/dev/null || true
sleep 3
if pgrep -f agave-validator > /dev/null 2>&1; then
  log "WARN: Validator still running, sending SIGKILL..."
  sudo pkill -9 -f agave-validator || true
  sleep 2
fi
log "Validator stopped."

log "Step 2: Cleaning accounts, ledger, and old snapshots..."
sudo rm -rf "${ACCOUNTS_DIR:?}"/*
sudo rm -rf "${LEDGER_DIR:?}"/rocksdb
sudo rm -rf "${LEDGER_DIR:?}"/accounts_run
sudo rm -rf "${LEDGER_DIR:?}"/accounts_snapshot
sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot-*.tar.bz2
sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot-*.tar.zst
sudo rm -f "${SNAPSHOT_DIR:?}"/incremental-snapshot-*.tar.bz2
sudo rm -f "${SNAPSHOT_DIR:?}"/incremental-snapshot-*.tar.zst
sudo rm -f "${SNAPSHOT_DIR:?}"/tmp-*
sudo rm -f "${SNAPSHOT_DIR:?}"/snapshot-finder.log
log "Cleanup done."

log "Step 3: Downloading fresh snapshot..."
cd "$SNAPSHOT_DIR"
DOWNLOAD_START=$(date +%s)
DOWNLOAD_OK=false

# Preparar permisos para Docker
sudo chmod 777 "$SNAPSHOT_DIR"
sudo touch "$SNAPSHOT_DIR/snapshot-finder.log"
sudo chmod 666 "$SNAPSHOT_DIR/snapshot-finder.log"

# Método 1: snapshot-finder via Docker
log "Method 1: Using snapshot-finder to find and download from fastest node..."

if command -v docker &> /dev/null; then
  log "Running snapshot-finder via Docker..."

  sudo docker run --rm \
    --user 0:0 \
    -v "$SNAPSHOT_DIR":/snapshots \
    c29r3/solana-snapshot-finder:latest \
    --snapshot_path /snapshots \
    --num_of_retries 5 \
    --with_private_rpc \
    --min_download_speed 50 2>&1 | tee -a "$LOG_FILE"

  SNAP_COUNT=$(find "$SNAPSHOT_DIR" -maxdepth 1 -name "snapshot-*.tar.*" -type f 2>/dev/null | wc -l)
  if [ "$SNAP_COUNT" -gt 0 ]; then
    DOWNLOAD_OK=true
    log "SUCCESS: snapshot-finder downloaded full snapshot"
  else
    log "WARN: snapshot-finder did not produce a full snapshot"
  fi
else
  log "WARN: Docker not installed. Skipping Method 1."
fi

# Método 2: Fallback a Avorio
if [ "$DOWNLOAD_OK" = false ]; then
  log "Method 2: Trying Avorio snapshots (fallback)..."

  sudo aria2c -x16 -s16 --force-sequential=true \
    --allow-overwrite=true \
    --auto-file-renaming=false \
    --console-log-level=warn \
    --max-tries=2 \
    --retry-wait=5 \
    --connect-timeout=15 \
    --timeout=30 \
    -d "$SNAPSHOT_DIR" \
    "$AVORIO_FULL" \
    "$AVORIO_INCR" 2>&1 | tee -a "$LOG_FILE"

  SNAP_COUNT=$(find "$SNAPSHOT_DIR" -maxdepth 1 -name "snapshot-*.tar.*" -type f 2>/dev/null | wc -l)
  if [ "$SNAP_COUNT" -gt 0 ]; then
    DOWNLOAD_OK=true
    log "SUCCESS: Downloaded full snapshot from Avorio"
  else
    log "WARN: Avorio fallback failed"
  fi
fi

DOWNLOAD_END=$(date +%s)
DOWNLOAD_DURATION=$(( DOWNLOAD_END - DOWNLOAD_START ))
log "Download phase completed in ${DOWNLOAD_DURATION}s"

if [ "$DOWNLOAD_OK" = false ]; then
  log "CRITICAL ERROR: All snapshot download methods failed!"
  log "Starting validator without snapshot (will bootstrap from gossip - slower)..."
  sudo chmod 755 "$SNAPSHOT_DIR"
  sudo chown -R solv:solv "$SNAPSHOT_DIR" "$ACCOUNTS_DIR" "$LEDGER_DIR"
  sudo systemctl start "$SERVICE_NAME"
  log "Validator started in bootstrap mode. Will download snapshot from gossip."
  exit 0
fi

log "Found snapshot files:"
ls -lh "$SNAPSHOT_DIR"/snapshot-*.tar.* 2>&1 | tee -a "$LOG_FILE"
ls -lh "$SNAPSHOT_DIR"/incremental-snapshot-*.tar.* 2>&1 | tee -a "$LOG_FILE" || true

log "Step 4: Fixing ownership..."
sudo chmod 755 "$SNAPSHOT_DIR"
sudo chown -R solv:solv "$SNAPSHOT_DIR"
sudo chown -R solv:solv "$ACCOUNTS_DIR"
sudo chown -R solv:solv "$LEDGER_DIR"

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

