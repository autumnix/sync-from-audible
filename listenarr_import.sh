#!/bin/bash
# Trigger Listenarr to scan, match, and import books from the ingest directory.
# Designed to run on a cron or after new books are hardlinked.
#
# Required env vars:
#   LISTENARR_URL       - e.g. http://10.42.5.23:4545
#   LISTENARR_API_KEY   - X-Api-Key value
#   LISTENARR_ROOT_ID   - Root folder ID for the downloads/ingest dir (default: 9)

set -euo pipefail

URL="${LISTENARR_URL:?Set LISTENARR_URL}"
API_KEY="${LISTENARR_API_KEY:?Set LISTENARR_API_KEY}"
ROOT_ID="${LISTENARR_ROOT_ID:-9}"
IMPORT_MODE="${LISTENARR_IMPORT_MODE:-move}"
DEST_ROOT_ID="${LISTENARR_DEST_ROOT_ID:-1}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

api() {
    local method="$1" path="$2"
    shift 2
    curl -sf -X "$method" "${URL}/api/v1${path}" \
        -H "X-Api-Key: ${API_KEY}" \
        -H "Content-Type: application/json" \
        "$@"
}

# Step 1: Trigger scan
log "Scanning root folder ${ROOT_ID} for unmatched files..."
JOB_ID=$(api POST "/rootfolders/${ROOT_ID}/scan-unmatched" | python3 -c "import json,sys; print(json.load(sys.stdin)['jobId'])")
log "Scan job: ${JOB_ID}"

# Step 2: Poll for scan completion
for i in $(seq 1 60); do
    RESULT=$(api GET "/rootfolders/unmatched-results/${JOB_ID}")
    STATUS=$(echo "$RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
    if [ "$STATUS" = "Completed" ]; then
        break
    fi
    sleep 2
done

ITEM_COUNT=$(echo "$RESULT" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('items',[])))")
log "Scan complete: ${ITEM_COUNT} unmatched items"

if [ "$ITEM_COUNT" = "0" ]; then
    log "Nothing to import."
    exit 0
fi

# Step 3: For each item that has an ASIN, search for a match and import
echo "$RESULT" | python3 -c "
import json, sys, urllib.request, time

data = json.load(sys.stdin)
url = '${URL}'
api_key = '${API_KEY}'
import_mode = '${IMPORT_MODE}'
dest_root_id = int('${DEST_ROOT_ID}')

headers = {
    'X-Api-Key': api_key,
    'Content-Type': 'application/json',
}

imported = 0
for item in data.get('items', []):
    asin = item.get('asin', '')
    title = item.get('title', 'Unknown')
    author = item.get('author', 'Unknown')
    path = item.get('fullPath', '')
    book_folder = item.get('bookFolder', '')

    if not asin:
        print(f'  SKIP (no ASIN): {title}')
        continue

    # Search by ASIN to find a match
    search_payload = json.dumps({
        'query': f'{title} {author}',
        'asin': asin,
    }).encode()
    req = urllib.request.Request(
        f'{url}/api/v1/search',
        data=search_payload,
        headers=headers,
        method='POST',
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        results = json.loads(resp.read())
    except Exception as e:
        print(f'  SEARCH FAILED: {title} - {e}')
        continue

    if not results:
        print(f'  NO MATCH: {title} (ASIN: {asin})')
        continue

    match = results[0]
    match_asin = match.get('asin', '')
    print(f'  MATCHED: {title} -> {match.get(\"title\", \"?\")} (ASIN: {match_asin})')

    # Import the matched book
    import_payload = json.dumps({
        'files': item.get('sourceFiles', [path]),
        'bookFolder': book_folder,
        'asin': match_asin,
        'title': match.get('title', title),
        'author': match.get('author', author),
        'inputMode': import_mode,
        'destinationRootFolderId': dest_root_id,
    }).encode()
    req = urllib.request.Request(
        f'{url}/api/v1/library/manual-import',
        data=import_payload,
        headers=headers,
        method='POST',
    )
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        print(f'  IMPORTED: {title}')
        imported += 1
    except Exception as e:
        print(f'  IMPORT FAILED: {title} - {e}')

    time.sleep(1)  # Be nice

print(f'Done: {imported}/{len(data.get(\"items\",[]))} imported')
"

log "Import cycle complete."
