#!/bin/bash
# Run sync-from-audible on macOS
# Usage: ./run.sh [--continuous] [--dry-run] [--verbose]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env if present
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

# Defaults for local Mac usage
export AUDIBLE_AUTH_FILE="${AUDIBLE_AUTH_FILE:-$HOME/.audible/audible.json}"
export AUDIBLE_PASSWORD="${AUDIBLE_PASSWORD:?Set AUDIBLE_PASSWORD in .env}"
export ACTIVATION_BYTES="${ACTIVATION_BYTES:?Set ACTIVATION_BYTES in .env}"
export OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/audiobooks/library}"
export STATE_DB="${STATE_DB:-$SCRIPT_DIR/state.db}"
export LEGACY_RAW_DIR="${LEGACY_RAW_DIR:-$SCRIPT_DIR/audiobooks/raw}"

# Check dependencies
for cmd in ffmpeg ffprobe python3; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd not found. Install it first."
        exit 1
    fi
done

python3 -c "import audible" 2>/dev/null || {
    echo "ERROR: audible python package not found. Install with: pip3 install audible audible-cli"
    exit 1
}

echo "=== sync-from-audible ==="
echo "Auth file:   $AUDIBLE_AUTH_FILE"
echo "Output dir:  $OUTPUT_DIR"
echo "State DB:    $STATE_DB"
echo "Legacy raw:  $LEGACY_RAW_DIR"
echo ""

python3 "$SCRIPT_DIR/sync_audible.py" \
    --output-dir "$OUTPUT_DIR" \
    --auth-file "$AUDIBLE_AUTH_FILE" \
    --auth-password "$AUDIBLE_PASSWORD" \
    --activation-bytes "$ACTIVATION_BYTES" \
    --state-db "$STATE_DB" \
    --legacy-raw-dir "$LEGACY_RAW_DIR" \
    "$@"
