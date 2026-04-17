#!/bin/bash
set -e

# Create group/user matching PUID/PGID
groupadd -o -g "${PGID}" appgroup 2>/dev/null || true
useradd -o -u "${PUID}" -g "${PGID}" -d /config -s /bin/bash appuser 2>/dev/null || true

# Ensure config dir is writable by the app user
chown "${PUID}:${PGID}" /config

echo "Starting sync-from-audible as UID=${PUID} GID=${PGID}"
exec gosu "${PUID}:${PGID}" python3 -u /app/sync_audible.py "$@"
