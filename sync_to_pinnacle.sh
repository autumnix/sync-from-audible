#!/bin/bash
# Sync completed audiobooks to Pinnacle Unraid server
# Usage: ./sync_to_pinnacle.sh [--continuous]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

SRC="${OUTPUT_DIR:-$SCRIPT_DIR/audiobooks/library}"
DEST="${SYNC_DEST:-root@pinnacle.lan:/mnt/user/media/torrents/ingest/audiobooks/}"
LOG="$SCRIPT_DIR/sync.log"

echo "[$(date)] Sync starting: $SRC -> $DEST" | tee -a "$LOG"

REMOTE_HOST="${SYNC_REMOTE_HOST:-root@pinnacle.lan}"
REMOTE_INGEST="${SYNC_REMOTE_PATH:-/mnt/user/media/torrents/ingest/audiobooks}"

sync_once() {
    if [[ -n "$(ls -A "$SRC" 2>/dev/null)" ]]; then
        rsync -avz --progress \
            -e "ssh -o StrictHostKeyChecking=accept-new" \
            "$SRC/" "$DEST" >> "$LOG" 2>&1

        # Fix ownership for Listenarr container (runs as UID 99 / nobody)
        ssh "$REMOTE_HOST" "chown -R nobody:users '$REMOTE_INGEST' && chmod -R 775 '$REMOTE_INGEST'" 2>> "$LOG"

        echo "[$(date)] Sync pass complete" | tee -a "$LOG"
    else
        echo "[$(date)] Nothing to sync" | tee -a "$LOG"
    fi
}

if [[ "${1:-}" == "--continuous" ]]; then
    echo "Running continuously (sync every 5 minutes)..."
    while true; do
        sync_once
        sleep 300
    done
else
    sync_once
fi
