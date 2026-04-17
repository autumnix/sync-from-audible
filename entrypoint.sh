#!/bin/bash
set -e

# If PUID/PGID are set and we're root, create user and switch
if [ "$(id -u)" = "0" ] && [ -n "${PUID}" ]; then
    groupadd -o -g "${PGID:-100}" appgroup 2>/dev/null || true
    useradd -o -u "${PUID}" -g "${PGID:-100}" -d /config -s /bin/bash appuser 2>/dev/null || true
    chown "${PUID}:${PGID:-100}" /config

    # Set up audible-cli config for the app user
    APP_HOME=$(eval echo "~appuser" 2>/dev/null || echo "/config")
    mkdir -p "$APP_HOME/.audible"
    if [ -f /config/config.toml ]; then
        cp /config/config.toml "$APP_HOME/.audible/config.toml"
    else
        # Auto-generate config pointing to the mounted auth file
        cat > "$APP_HOME/.audible/config.toml" << EOF
title = "Audible Config File"

[APP]
primary_profile = "audible"

[profile.audible]
auth_file = "${AUDIBLE_AUTH_FILE:-/config/audible.json}"
country_code = "${COUNTRY_CODE:-us}"
EOF
    fi
    chown -R "${PUID}:${PGID:-100}" "$APP_HOME/.audible"

    exec gosu "${PUID}:${PGID:-100}" python3 -u /app/sync_audible.py "$@"
fi

# Non-root fallback: set up audible config in current home
mkdir -p ~/.audible
cat > ~/.audible/config.toml << EOF
title = "Audible Config File"

[APP]
primary_profile = "audible"

[profile.audible]
auth_file = "${AUDIBLE_AUTH_FILE:-/config/audible.json}"
country_code = "${COUNTRY_CODE:-us}"
EOF

exec python3 -u /app/sync_audible.py "$@"
