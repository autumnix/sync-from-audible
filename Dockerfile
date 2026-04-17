FROM python:3.12-slim

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_audible.py .

# Config and output volumes
VOLUME ["/config", "/audiobooks"]

# Environment defaults
ENV OUTPUT_DIR=/audiobooks \
    AUDIBLE_AUTH_FILE=/config/audible.json \
    STATE_DB=/config/state.db \
    AUDIBLE_PASSWORD="" \
    ACTIVATION_BYTES="" \
    COUNTRY_CODE=us \
    CONTINUOUS=true \
    POLL_INTERVAL=3600 \
    MAX_RETRIES=3

ENTRYPOINT ["python3", "/app/sync_audible.py"]
