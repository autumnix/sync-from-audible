#!/bin/bash
set -e

# If PUID/PGID are set and we're root, create user and switch
if [ "$(id -u)" = "0" ] && [ -n "${PUID}" ]; then
    groupadd -o -g "${PGID:-100}" appgroup 2>/dev/null || true
    useradd -o -u "${PUID}" -g "${PGID:-100}" -d /config -s /bin/bash appuser 2>/dev/null || true
    chown "${PUID}:${PGID:-100}" /config
    exec gosu "${PUID}:${PGID:-100}" python3 -u /app/sync_audible.py "$@"
fi

# Otherwise just run directly
exec python3 -u /app/sync_audible.py "$@"
