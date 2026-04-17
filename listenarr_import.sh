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
DEST_ROOT_ID="${LISTENARR_DEST_ROOT_ID:-1}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "Starting Listenarr import cycle..."

python3 -u << 'PYEOF'
import json, os, sys, time, urllib.request, urllib.error

URL = os.environ["LISTENARR_URL"]
API_KEY = os.environ["LISTENARR_API_KEY"]
ROOT_ID = int(os.environ.get("LISTENARR_ROOT_ID", "9"))
DEST_ROOT_ID = int(os.environ.get("LISTENARR_DEST_ROOT_ID", "1"))
API = f"{URL}/api/v1"
HEADERS = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}


def api(method, path, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(f"{API}{path}", data=body, headers=HEADERS, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()[:200]
        raise RuntimeError(f"HTTP {e.code}: {err_body}")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# Step 1: Scan for unmatched files
log("Scanning ingest directory...")
job = api("POST", f"/rootfolders/{ROOT_ID}/scan-unmatched")
job_id = job["jobId"]

for _ in range(60):
    result = api("GET", f"/rootfolders/unmatched-results/{job_id}")
    if result["status"] == "Completed":
        break
    time.sleep(2)

items = result.get("items", [])
log(f"Found {len(items)} unmatched items")

if not items:
    log("Nothing to import.")
    sys.exit(0)

# Step 2: For each item, search -> add to library -> import
imported = skipped = failed = 0

for item in items:
    asin = item.get("asin", "")
    title = item.get("title", "Unknown")
    author = item.get("author", "Unknown")
    book_folder = item.get("bookFolder", "")
    full_path = item.get("fullPath", "")

    if not asin:
        log(f"  SKIP (no ASIN): {title}")
        skipped += 1
        continue

    # Search for a match in Listenarr's metadata
    try:
        results = api("POST", "/search", {"query": f"{title} {author}", "asin": asin})
    except Exception as e:
        log(f"  SEARCH FAILED: {title} - {e}")
        failed += 1
        continue

    if not results:
        log(f"  NO MATCH: {title}")
        skipped += 1
        continue

    match = results[0]
    match_asin = match.get("asin", asin)
    log(f"  MATCHED: {title} -> {match.get('title', '?')} (ASIN: {match_asin})")

    # Get full metadata for the add request
    try:
        meta_resp = api("GET", f"/metadata/{match_asin}?region=us")
        meta = meta_resp.get("metadata", meta_resp)
    except Exception as e:
        log(f"  METADATA FAILED: {title} - {e}")
        failed += 1
        continue

    # Transform metadata for the add endpoint: authors must be string array
    add_meta = {
        "asin": meta.get("asin", match_asin),
        "title": meta.get("title", title),
        "subtitle": meta.get("subtitle", ""),
        "authors": [a["name"] if isinstance(a, dict) else a for a in meta.get("authors", [])],
        "narrators": [n["name"] if isinstance(n, dict) else n for n in meta.get("narrators", [])],
        "publisher": meta.get("publisher", ""),
        "description": meta.get("description", ""),
        "imageUrl": meta.get("imageUrl", ""),
        "language": meta.get("language", "english"),
        "lengthMinutes": meta.get("lengthMinutes", 0),
        "publishDate": meta.get("publishDate", ""),
    }

    # Transform nested fields: metadata API returns objects, add endpoint wants strings
    # Genres
    raw_genres = meta.get("genres", [])
    if raw_genres and isinstance(raw_genres[0], dict):
        add_meta["genres"] = [g["name"] for g in raw_genres]
    else:
        add_meta["genres"] = raw_genres if isinstance(raw_genres, list) else []

    # Series: API returns [{asin, name, position}], add endpoint wants string + seriesNumber
    series_list = meta.get("series", [])
    if isinstance(series_list, list) and series_list:
        s = series_list[0]
        add_meta["series"] = s.get("name", "") if isinstance(s, dict) else str(s)
        add_meta["seriesNumber"] = s.get("position", "") if isinstance(s, dict) else ""
    else:
        add_meta["series"] = ""
        add_meta["seriesNumber"] = ""

    # Add to Listenarr library
    try:
        add_result = api("POST", "/library/add", {
            "metadata": add_meta,
            "monitored": True,
            "searchForDownload": False,
        })
        audiobook_id = add_result.get("audiobook", {}).get("id")
        log(f"  ADDED: ID={audiobook_id}")
    except RuntimeError as e:
        if "409" in str(e):
            # Already exists - try to find it
            try:
                err_data = json.loads(str(e).split(":", 1)[1].strip())
                audiobook_id = err_data.get("audiobook", {}).get("id")
            except Exception:
                # Search by ASIN in the library
                lib = api("GET", "/library")
                audiobook_id = None
                for book in lib:
                    if book.get("asin") == match_asin:
                        audiobook_id = book["id"]
                        break
            if audiobook_id:
                log(f"  EXISTS: ID={audiobook_id}")
            else:
                log(f"  ADD FAILED (409 but can't find ID): {title}")
                failed += 1
                continue
        else:
            log(f"  ADD FAILED: {title} - {e}")
            failed += 1
            continue

    # Import the file
    try:
        api("POST", "/library/manual-import", {
            "path": book_folder,
            "mode": "interactive",
            "inputMode": "move",
            "includeCompanionFiles": True,
            "cleanupEmptySourceFolders": True,
            "items": [{
                "fullPath": full_path,
                "matchedAudiobookId": audiobook_id,
            }],
        })
        log(f"  IMPORTED: {title}")
        imported += 1
    except Exception as e:
        log(f"  IMPORT FAILED: {title} - {e}")
        failed += 1

    time.sleep(0.5)

log(f"Done: {imported} imported, {skipped} skipped, {failed} failed out of {len(items)}")
PYEOF
