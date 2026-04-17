FROM python:3.12-slim

# Install ffmpeg and gosu for UID/GID handling
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg gosu curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_audible.py .
COPY listenarr_import.sh .
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /app/listenarr_import.sh

# Config and output volumes
VOLUME ["/config", "/audiobooks"]

# Environment defaults — match Unraid nobody:users
ENV PUID=99 \
    PGID=100 \
    OUTPUT_DIR=/audiobooks \
    AUDIBLE_AUTH_FILE=/config/audible.json \
    STATE_DB=/config/state.db \
    AUDIBLE_PASSWORD="" \
    ACTIVATION_BYTES="" \
    COUNTRY_CODE=us \
    CONTINUOUS=true \
    POLL_INTERVAL=3600 \
    MAX_RETRIES=3 \
    LISTENARR_URL="" \
    LISTENARR_API_KEY="" \
    LISTENARR_ROOT_ID=9 \
    LISTENARR_IMPORT_MODE=move \
    LISTENARR_DEST_ROOT_ID=1

ENTRYPOINT ["/entrypoint.sh"]
