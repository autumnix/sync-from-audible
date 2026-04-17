#!/usr/bin/env python3
"""
sync_audible.py — Download, decrypt, and organize Audible audiobooks for Listenarr.

Folder structure: Author / Series / BookTitle / BookTitle.m4b
                  Author / BookTitle / BookTitle.m4b  (no series)

Features:
  - Fetches full Audible library metadata (title, author, series, ASIN)
  - Downloads AAX (fallback AAXC) with chapter metadata and cover art
  - Decrypts to M4B preserving chapters
  - Embeds metadata: short title, author, series, ASIN
  - Looks up ISBN via OpenLibrary and embeds if found
  - Organizes into Listenarr-compatible folder structure
  - SQLite state tracking to skip completed books
  - Progressive backoffs and stall detection
  - Runs continuously for new book detection
"""

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = "/audiobooks"
DEFAULT_AUTH_FILE = "/config/audible.json"
DEFAULT_STATE_DB = "/config/state.db"
DEFAULT_ACTIVATION_BYTES = ""
DEFAULT_COUNTRY_CODE = "us"
POLL_INTERVAL_SECONDS = 3600  # Check for new books every hour
STALL_TIMEOUT_SECONDS = 1800  # 30 minutes
ISBN_CACHE_DAYS = 30

# Backoff settings
INITIAL_BACKOFF = 5
MAX_BACKOFF = 300
BACKOFF_MULTIPLIER = 2

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger("sync_audible")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            asin TEXT PRIMARY KEY,
            title TEXT,
            short_title TEXT,
            subtitle TEXT,
            author TEXT,
            series_name TEXT,
            series_sequence TEXT,
            isbn TEXT,
            status TEXT DEFAULT 'pending',
            download_path TEXT,
            output_path TEXT,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            last_attempt TEXT,
            completed_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS isbn_cache (
            query_key TEXT PRIMARY KEY,
            isbn TEXT,
            looked_up_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def get_book_status(conn: sqlite3.Connection, asin: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM books WHERE asin = ?", (asin,)).fetchone()
    return dict(row) if row else None


def upsert_book(conn: sqlite3.Connection, **kwargs):
    asin = kwargs["asin"]
    existing = get_book_status(conn, asin)
    kwargs["updated_at"] = datetime.utcnow().isoformat()

    if existing:
        sets = ", ".join(f"{k} = ?" for k in kwargs if k != "asin")
        vals = [v for k, v in kwargs.items() if k != "asin"]
        vals.append(asin)
        conn.execute(f"UPDATE books SET {sets} WHERE asin = ?", vals)
    else:
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" for _ in kwargs)
        conn.execute(f"INSERT INTO books ({cols}) VALUES ({placeholders})", list(kwargs.values()))
    conn.commit()


# ---------------------------------------------------------------------------
# Audible API
# ---------------------------------------------------------------------------


def load_audible_auth(auth_file: str, password: str):
    """Load audible authenticator."""
    import audible
    return audible.Authenticator.from_file(auth_file, password=password)


def fetch_library(auth, country_code: str) -> list[dict]:
    """Fetch all books from Audible library with full metadata."""
    import audible

    all_items = []
    page = 0
    page_size = 50

    with audible.Client(auth=auth, country_code=country_code) as client:
        while True:
            log.info("Fetching library page %d (offset %d)...", page, page * page_size)
            try:
                resp = client.get(
                    "1.0/library",
                    num_results=page_size,
                    page=page + 1,
                    response_groups="product_desc,contributors,series,product_attrs,media",
                    sort_by="-PurchaseDate",
                )
            except Exception as e:
                log.error("Failed to fetch library page %d: %s", page, e)
                break

            items = resp.get("items", [])
            if not items:
                break
            all_items.extend(items)
            log.info("  Got %d items (total: %d)", len(items), len(all_items))

            if len(items) < page_size:
                break
            page += 1
            time.sleep(1)  # Be nice to the API

    log.info("Library contains %d items", len(all_items))
    return all_items


def parse_library_item(item: dict) -> dict:
    """Extract clean metadata from an Audible library item."""
    authors = item.get("authors") or []
    author = authors[0]["name"] if authors else "Unknown Author"

    series_list = item.get("series") or []
    series_name = None
    series_seq = None
    if series_list:
        s = series_list[0]
        series_name = s.get("title")
        series_seq = s.get("sequence")

    title = item.get("title", "Unknown Title")
    subtitle = item.get("subtitle")

    short_title = _extract_short_title(title, series_name)

    return {
        "asin": item["asin"],
        "title": title,
        "short_title": short_title,
        "subtitle": subtitle,
        "author": author,
        "series_name": series_name,
        "series_sequence": series_seq,
        "content_delivery_type": item.get("content_delivery_type"),
    }


def _extract_short_title(title: str, series_name: Optional[str]) -> str:
    """Extract a short, Listenarr-friendly book title.

    Strategy:
    - If the title starts with the series name followed by ':', strip the series prefix
      and use the remainder as the short title — BUT only if the remainder is a
      meaningful book title (not just "Special Edition" or "The Complete Series").
    - Otherwise keep the original title as-is.
    """
    if not series_name or ":" not in title:
        return title

    # Check if title starts with series name + ":"
    parts = title.split(":", 1)
    prefix = parts[0].strip()

    if _normalize(prefix) == _normalize(series_name):
        remainder = parts[1].strip()
        # Reject remainders that are just qualifiers, not real book titles
        reject_patterns = [
            r"^(special|deluxe|collector'?s?)\s+edition$",
            r"^the\s+complete\s+(series|collection|saga)$",
            r"^complete\s+(series|collection|saga)$",
            r"^books?\s+\d",
            r"^(the\s+)?definitive\s+(collection|edition)",
            r"^omnibus",
        ]
        for pat in reject_patterns:
            if re.match(pat, remainder, re.IGNORECASE):
                return title
        # Accept the remainder as the short title
        if len(remainder) > 2:
            return remainder

    return title


def _normalize(s: str) -> str:
    return re.sub(r"[^\w]", "", s.lower())


# ---------------------------------------------------------------------------
# ISBN Lookup (OpenLibrary)
# ---------------------------------------------------------------------------


def lookup_isbn(conn: sqlite3.Connection, title: str, author: str) -> Optional[str]:
    """Look up ISBN via OpenLibrary search. Cached in SQLite."""
    cache_key = f"{_normalize(title)}|{_normalize(author)}"

    # Check cache
    row = conn.execute(
        "SELECT isbn, looked_up_at FROM isbn_cache WHERE query_key = ?",
        (cache_key,),
    ).fetchone()
    if row:
        age = datetime.utcnow() - datetime.fromisoformat(row["looked_up_at"])
        if age < timedelta(days=ISBN_CACHE_DAYS):
            return row["isbn"] if row["isbn"] else None

    isbn = _query_openlibrary_isbn(title, author)

    conn.execute(
        "INSERT OR REPLACE INTO isbn_cache (query_key, isbn, looked_up_at) VALUES (?, ?, datetime('now'))",
        (cache_key, isbn),
    )
    conn.commit()
    return isbn


def _query_openlibrary_isbn(title: str, author: str) -> Optional[str]:
    """Query OpenLibrary for ISBN by title and author."""
    try:
        query = urllib.request.quote(f"{title} {author}")
        url = f"https://openlibrary.org/search.json?q={query}&limit=3&fields=isbn,title,author_name"
        req = urllib.request.Request(url, headers={"User-Agent": "sync-audible/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        for doc in data.get("docs", []):
            isbns = doc.get("isbn", [])
            # Prefer ISBN-13
            for i in isbns:
                if len(i) == 13 and i.startswith("978"):
                    return i
            # Fall back to ISBN-10
            for i in isbns:
                if len(i) == 10:
                    return i
        return None
    except Exception as e:
        log.debug("ISBN lookup failed for '%s': %s", title, e)
        return None


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download_book(
    auth,
    country_code: str,
    asin: str,
    output_dir: str,
    activation_bytes: str,
    legacy_raw_dir: Optional[str] = None,
) -> dict:
    """Download a single book (AAX with AAXC fallback). Returns paths dict."""
    raw_dir = os.path.join(output_dir, ".raw", asin)
    os.makedirs(raw_dir, exist_ok=True)

    # Check if already downloaded in our new per-ASIN layout
    existing = _find_audio_file(raw_dir)
    if existing:
        log.info("  Already downloaded: %s", existing)
        return _build_paths(raw_dir, existing)

    # Check legacy flat raw directory (from previous scripts)
    if legacy_raw_dir and os.path.isdir(legacy_raw_dir):
        legacy_paths = _find_legacy_raw_files(legacy_raw_dir, asin)
        if legacy_paths:
            log.info("  Found in legacy raw dir, linking...")
            for src in legacy_paths:
                dst = os.path.join(raw_dir, os.path.basename(src))
                if not os.path.exists(dst):
                    os.symlink(src, dst)
            audio = _find_audio_file(raw_dir)
            if audio:
                return _build_paths(raw_dir, audio)

    # Download with audible-cli
    audible_bin = _find_audible_cli()
    if not audible_bin:
        raise RuntimeError("audible-cli not found")

    cmd = [
        audible_bin,
        "-p", os.environ.get("AUDIBLE_PASSWORD", ""),
        "download",
        "--asin", asin,
        "--aax-fallback",
        "--output-dir", raw_dir,
        "--cover",
        "--cover-size", "500",
        "--chapter",
        "--no-confirm",
        "--quality", "best",
    ]

    log.info("  Downloading ASIN %s...", asin)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "not downloadable" in stderr.lower() or "no download url" in stderr.lower():
            raise RuntimeError(f"Book not downloadable: {stderr}")
        raise RuntimeError(f"Download failed (rc={result.returncode}): {stderr}")

    audio_file = _find_audio_file(raw_dir)
    if not audio_file:
        raise RuntimeError("Download completed but no audio file found")

    return _build_paths(raw_dir, audio_file)


def _build_paths(raw_dir: str, audio_file: str) -> dict:
    return {
        "audio": audio_file,
        "chapters": _find_file(raw_dir, "-chapters.json"),
        "cover": _find_file(raw_dir, ".jpg"),
        "voucher": _find_file(raw_dir, ".voucher"),
    }


def _find_legacy_raw_files(legacy_dir: str, asin: str) -> list[str]:
    """Search a flat raw directory for files belonging to an ASIN.

    The legacy layout uses title-based filenames. We match by checking
    voucher files for ASIN, or fall back to the library metadata.
    """
    matches = []
    # Check all voucher files for this ASIN
    for f in Path(legacy_dir).glob("*.voucher"):
        try:
            with open(f) as vf:
                v = json.load(vf)
            v_asin = (v.get("asin", "") or
                      v.get("content_license", {}).get("asin", ""))
            if v_asin == asin:
                base = f.stem  # e.g. "Title-AAX_44_128"
                for related in Path(legacy_dir).glob(f"{base}*"):
                    matches.append(str(related))
                # Also find chapter/cover files with similar prefix
                prefix = base.split("-")[0] if "-" in base else base
                for related in Path(legacy_dir).iterdir():
                    rname = related.name
                    if rname.startswith(prefix) and str(related) not in matches:
                        matches.append(str(related))
                break
        except Exception:
            continue
    return matches


def _find_audible_cli() -> Optional[str]:
    """Find the audible CLI binary."""
    # Check common locations
    candidates = [
        shutil.which("audible"),
        os.path.expanduser("~/Library/Python/3.9/bin/audible"),
        "/usr/local/bin/audible",
        "/usr/bin/audible",
    ]
    for c in candidates:
        if c and os.path.isfile(c):
            return c
    return None


def _find_audio_file(directory: str) -> Optional[str]:
    for ext in (".aax", ".aaxc"):
        for f in Path(directory).glob(f"*{ext}"):
            return str(f)
    return None


def _find_file(directory: str, suffix: str) -> Optional[str]:
    for f in Path(directory).iterdir():
        if f.name.endswith(suffix):
            return str(f)
    return None


# ---------------------------------------------------------------------------
# Decrypt & Convert
# ---------------------------------------------------------------------------


def decrypt_to_m4b(
    audio_path: str,
    output_path: str,
    activation_bytes: str,
    voucher_path: Optional[str],
    chapters_json: Optional[str],
) -> str:
    """Decrypt AAX/AAXC to M4B preserving chapters."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    tmp_output = output_path + ".converting.m4b"

    if audio_path.endswith(".aaxc") and voucher_path:
        # AAXC: decrypt with key/iv from voucher
        with open(voucher_path) as f:
            voucher = json.load(f)
        license_resp = voucher.get("content_license", {}).get("license_response", {})
        key = license_resp.get("key", "")
        iv = license_resp.get("iv", "")

        if not key or not iv:
            raise RuntimeError("Voucher missing key/iv")

        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-audible_key", key,
            "-audible_iv", iv,
            "-i", audio_path,
            "-c", "copy",
            tmp_output,
        ]
    else:
        # AAX: decrypt with activation bytes
        if not activation_bytes:
            raise RuntimeError("Activation bytes required for AAX decryption")

        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-activation_bytes", activation_bytes,
            "-i", audio_path,
            "-c", "copy",
            tmp_output,
        ]

    log.info("  Decrypting to M4B...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        if os.path.exists(tmp_output):
            os.remove(tmp_output)
        raise RuntimeError(f"Decryption failed: {result.stderr.strip()}")

    os.rename(tmp_output, output_path)
    return output_path


def _flatten_audible_chapters(chapters: list) -> list:
    """Flatten nested Audible chapter structure into a flat list."""
    flat = []
    for ch in chapters:
        if "chapters" in ch and ch["chapters"]:
            flat.extend(_flatten_audible_chapters(ch["chapters"]))
        elif "start_offset_ms" in ch:
            flat.append(ch)
    return flat


def _parse_chapters_json(chapters_json_path: str) -> list:
    """Parse Audible chapters JSON into a flat chapter list."""
    with open(chapters_json_path) as f:
        data = json.load(f)

    # Handle Audible's nested format: content_metadata.chapter_info.chapters
    chapter_info = data
    if "content_metadata" in data:
        chapter_info = data["content_metadata"].get("chapter_info", data)
    if "chapters" in chapter_info:
        return _flatten_audible_chapters(chapter_info["chapters"])
    if isinstance(data, list):
        return _flatten_audible_chapters(data)
    return []


def embed_chapters_from_json(m4b_path: str, chapters_json_path: str):
    """Re-embed chapters from the Audible chapter JSON if ffmpeg lost them."""
    if not chapters_json_path or not os.path.exists(chapters_json_path):
        return

    # Check if file already has chapters
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_chapters", m4b_path],
        capture_output=True, text=True,
    )
    existing_chapters = json.loads(probe.stdout).get("chapters", [])
    if existing_chapters:
        log.debug("  File already has %d chapters", len(existing_chapters))
        return

    log.info("  Embedding chapters from JSON...")
    chapters = _parse_chapters_json(chapters_json_path)
    if not chapters:
        log.warning("  No chapters found in JSON")
        return

    # Build ffmetadata file
    metadata_path = m4b_path + ".ffmetadata"
    with open(metadata_path, "w") as mf:
        mf.write(";FFMETADATA1\n")
        for i, ch in enumerate(chapters):
            start_ms = ch.get("start_offset_ms", 0)
            length_ms = ch.get("length_ms", 0)
            title = ch.get("title", f"Chapter {i + 1}")

            mf.write("\n[CHAPTER]\n")
            mf.write("TIMEBASE=1/1000\n")
            mf.write(f"START={start_ms}\n")
            mf.write(f"END={start_ms + length_ms}\n")
            mf.write(f"title={title}\n")

    tmp = m4b_path + ".chapters.tmp"
    result = subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error",
         "-i", m4b_path, "-i", metadata_path,
         "-map_metadata", "1", "-map_chapters", "1",
         "-map", "0:a", "-c", "copy", tmp],
        capture_output=True, text=True,
    )
    os.remove(metadata_path)

    if result.returncode == 0:
        os.replace(tmp, m4b_path)
        log.info("  Chapters embedded successfully")
    else:
        if os.path.exists(tmp):
            os.remove(tmp)
        log.warning("  Failed to embed chapters: %s", result.stderr.strip())


# ---------------------------------------------------------------------------
# Metadata embedding
# ---------------------------------------------------------------------------


def embed_metadata(
    m4b_path: str,
    title: str,
    author: str,
    series_name: Optional[str],
    series_seq: Optional[str],
    asin: str,
    isbn: Optional[str],
    cover_path: Optional[str],
):
    """Embed rich metadata into the M4B file."""
    tmp = m4b_path + ".meta.m4b"

    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", m4b_path,
    ]

    # Add cover art if available
    if cover_path and os.path.exists(cover_path):
        cmd.extend(["-i", cover_path])
        cmd.extend(["-map", "0:a", "-map", "1:v", "-disposition:v:0", "attached_pic"])
    else:
        cmd.extend(["-map", "0:a"])

    # Preserve chapters
    cmd.extend(["-map_chapters", "0"])

    cmd.extend(["-c", "copy"])

    # Enable custom metadata tags in mp4 container
    cmd.extend(["-movflags", "+use_metadata_tags"])

    # Metadata tags
    cmd.extend(["-metadata", f"title={title}"])
    cmd.extend(["-metadata", f"artist={author}"])
    cmd.extend(["-metadata", f"album_artist={author}"])
    cmd.extend(["-metadata", f"album={title}"])
    cmd.extend(["-metadata", "genre=Audiobook"])
    cmd.extend(["-metadata", f"ASIN={asin}"])

    if isbn:
        cmd.extend(["-metadata", f"ISBN={isbn}"])

    # Also embed ASIN in grouping tag (widely supported in mp4)
    cmd.extend(["-metadata", f"grouping={asin}"])

    if series_name:
        cmd.extend(["-metadata", f"series={series_name}"])
        # Audiobookshelf/Listenarr recognizes these tags
        cmd.extend(["-metadata", f"MVNM={series_name}"])
        if series_seq:
            cmd.extend(["-metadata", f"MVIN={series_seq}"])

    cmd.append(tmp)

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode == 0:
        os.replace(tmp, m4b_path)
        log.info("  Metadata embedded")
    else:
        if os.path.exists(tmp):
            os.remove(tmp)
        log.warning("  Metadata embedding failed: %s", result.stderr.strip())


def verify_metadata(m4b_path: str) -> dict:
    """Verify embedded metadata. Returns tags dict."""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", "-show_chapters", m4b_path],
        capture_output=True, text=True,
    )
    data = json.loads(result.stdout)
    tags = data.get("format", {}).get("tags", {})
    chapters = data.get("chapters", [])
    return {"tags": tags, "chapter_count": len(chapters)}


# ---------------------------------------------------------------------------
# File organization
# ---------------------------------------------------------------------------


def sanitize_filename(name: str) -> str:
    """Make a string safe for use as a file/directory name."""
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Remove trailing dots/spaces (Windows compat)
    name = name.rstrip(". ")
    # Limit length
    if len(name) > 200:
        name = name[:200].rstrip()
    return name or "Unknown"


def organize_book(
    m4b_path: str,
    cover_path: Optional[str],
    output_base: str,
    author: str,
    series_name: Optional[str],
    short_title: str,
) -> str:
    """Move M4B into Author/Series/Book/ structure. Returns final directory."""
    safe_author = sanitize_filename(author)
    safe_title = sanitize_filename(short_title)

    if series_name:
        safe_series = sanitize_filename(series_name)
        book_dir = os.path.join(output_base, safe_author, safe_series, safe_title)
    else:
        book_dir = os.path.join(output_base, safe_author, safe_title)

    os.makedirs(book_dir, exist_ok=True)

    dest_m4b = os.path.join(book_dir, f"{safe_title}.m4b")

    if os.path.abspath(m4b_path) != os.path.abspath(dest_m4b):
        shutil.move(m4b_path, dest_m4b)

    # Copy cover art
    if cover_path and os.path.exists(cover_path):
        dest_cover = os.path.join(book_dir, "cover.jpg")
        if not os.path.exists(dest_cover):
            shutil.copy2(cover_path, dest_cover)

    return book_dir


# ---------------------------------------------------------------------------
# Main processing pipeline
# ---------------------------------------------------------------------------


class SyncAudible:
    def __init__(self, args):
        self.output_dir = args.output_dir
        self.auth_file = args.auth_file
        self.auth_password = args.auth_password
        self.activation_bytes = args.activation_bytes
        self.country_code = args.country_code
        self.state_db_path = args.state_db
        self.continuous = args.continuous
        self.poll_interval = args.poll_interval
        self.dry_run = args.dry_run
        self.max_retries = args.max_retries
        self.legacy_raw_dir = getattr(args, "legacy_raw_dir", None)

        self.conn = init_db(self.state_db_path)
        self.auth = None
        self._shutdown = False
        self._last_download_time = time.time()
        self._current_backoff = INITIAL_BACKOFF

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        log.info("Received signal %d, shutting down gracefully...", signum)
        self._shutdown = True

    def _load_auth(self):
        if self.auth is None:
            log.info("Loading Audible auth from %s", self.auth_file)
            self.auth = load_audible_auth(self.auth_file, self.auth_password)

    def run(self):
        """Main entry point."""
        self._load_auth()

        while True:
            try:
                self._sync_cycle()
            except Exception as e:
                log.error("Sync cycle failed: %s", e, exc_info=True)

            if not self.continuous or self._shutdown:
                break

            log.info("Sleeping %d seconds before next check...", self.poll_interval)
            for _ in range(self.poll_interval):
                if self._shutdown:
                    break
                time.sleep(1)

        log.info("Sync complete.")
        self._print_summary()

    def _sync_cycle(self):
        """One full sync: fetch library, process new/pending books."""
        log.info("=== Starting sync cycle ===")

        # Fetch library
        items = fetch_library(self.auth, self.country_code)
        new_count = 0

        for item in items:
            if self._shutdown:
                break

            meta = parse_library_item(item)

            # Skip podcasts
            if item.get("content_type") == "Podcast":
                continue

            existing = get_book_status(self.conn, meta["asin"])
            if existing and existing["status"] == "completed":
                continue

            # Upsert metadata
            upsert_book(
                self.conn,
                asin=meta["asin"],
                title=meta["title"],
                short_title=meta["short_title"],
                subtitle=meta["subtitle"],
                author=meta["author"],
                series_name=meta["series_name"],
                series_sequence=meta["series_sequence"],
                **({"status": "pending"} if not existing else {}),
            )

            if not existing:
                new_count += 1

        log.info("Library sync: %d total, %d new", len(items), new_count)

        # Process pending/failed books
        pending = self.conn.execute(
            "SELECT * FROM books WHERE status IN ('pending', 'downloading', 'failed') "
            "AND retry_count < ? ORDER BY created_at",
            (self.max_retries,),
        ).fetchall()

        log.info("Processing %d books...", len(pending))
        self._last_download_time = time.time()

        for row in pending:
            if self._shutdown:
                break

            book = dict(row)
            self._process_book(book)

    def _process_book(self, book: dict):
        """Process a single book through the full pipeline."""
        asin = book["asin"]
        title = book["short_title"] or book["title"]
        log.info("Processing: %s — %s [%s]", book["author"], title, asin)

        if self.dry_run:
            log.info("  [DRY RUN] Would process %s", asin)
            return

        try:
            upsert_book(self.conn, asin=asin, status="downloading",
                        last_attempt=datetime.utcnow().isoformat())

            # Step 1: Download
            paths = download_book(
                self.auth, self.country_code, asin,
                self.output_dir, self.activation_bytes,
                legacy_raw_dir=self.legacy_raw_dir,
            )
            self._last_download_time = time.time()
            self._current_backoff = INITIAL_BACKOFF

            # Step 2: Decrypt
            staging_dir = os.path.join(self.output_dir, ".staging", asin)
            os.makedirs(staging_dir, exist_ok=True)
            safe_title = sanitize_filename(title)
            m4b_path = os.path.join(staging_dir, f"{safe_title}.m4b")

            decrypt_to_m4b(
                paths["audio"], m4b_path,
                self.activation_bytes, paths.get("voucher"),
                paths.get("chapters"),
            )

            # Step 3: Embed chapters if needed
            embed_chapters_from_json(m4b_path, paths.get("chapters"))

            # Step 4: Look up ISBN
            isbn = lookup_isbn(self.conn, title, book["author"])
            if isbn:
                log.info("  Found ISBN: %s", isbn)

            # Step 5: Embed metadata
            embed_metadata(
                m4b_path,
                title=title,
                author=book["author"],
                series_name=book["series_name"],
                series_seq=book["series_sequence"],
                asin=asin,
                isbn=isbn,
                cover_path=paths.get("cover"),
            )

            # Step 6: Verify metadata
            meta_check = verify_metadata(m4b_path)
            tags = meta_check["tags"]
            log.info("  Verified: title=%s, chapters=%d, ASIN=%s, ISBN=%s",
                     tags.get("title", "?"), meta_check["chapter_count"],
                     tags.get("ASIN", "?"), tags.get("ISBN", "none"))

            # Step 7: Organize into final folder structure
            final_dir = organize_book(
                m4b_path, paths.get("cover"),
                self.output_dir,
                author=book["author"],
                series_name=book["series_name"],
                short_title=title,
            )

            # Update ISBN in DB
            upsert_book(self.conn, asin=asin, isbn=isbn)

            # Clean up staging
            shutil.rmtree(staging_dir, ignore_errors=True)

            upsert_book(
                self.conn, asin=asin, status="completed",
                output_path=final_dir,
                completed_at=datetime.utcnow().isoformat(),
            )
            log.info("  Done: %s", final_dir)

        except Exception as e:
            log.error("  Failed: %s", e)
            retry_count = book.get("retry_count", 0) + 1
            upsert_book(
                self.conn, asin=asin, status="failed",
                error_message=str(e), retry_count=retry_count,
            )
            self._backoff()

    def _backoff(self):
        """Progressive backoff after failure."""
        sleep_time = min(self._current_backoff, MAX_BACKOFF)
        log.info("  Backing off %d seconds...", sleep_time)
        for _ in range(int(sleep_time)):
            if self._shutdown:
                break
            time.sleep(1)
        self._current_backoff = min(self._current_backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
        # Update stall timer so backoff time doesn't count
        self._last_download_time = time.time()

    def _check_stall(self):
        """Check if we've stalled (no downloads in STALL_TIMEOUT_SECONDS)."""
        elapsed = time.time() - self._last_download_time
        if elapsed > STALL_TIMEOUT_SECONDS:
            log.warning("No downloads in %d seconds — possible stall", int(elapsed))
            return True
        return False

    def _print_summary(self):
        """Print final summary of all books."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM books GROUP BY status"
        ).fetchall()
        log.info("=== Summary ===")
        for row in rows:
            log.info("  %s: %d", row["status"], row["cnt"])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Download, decrypt, and organize Audible audiobooks for Listenarr",
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=os.environ.get("OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        help="Base output directory for organized audiobooks",
    )
    parser.add_argument(
        "--auth-file",
        default=os.environ.get("AUDIBLE_AUTH_FILE", DEFAULT_AUTH_FILE),
        help="Path to Audible auth JSON file",
    )
    parser.add_argument(
        "--auth-password",
        default=os.environ.get("AUDIBLE_PASSWORD", ""),
        help="Password for encrypted auth file",
    )
    parser.add_argument(
        "--activation-bytes",
        default=os.environ.get("ACTIVATION_BYTES", DEFAULT_ACTIVATION_BYTES),
        help="Activation bytes for AAX decryption",
    )
    parser.add_argument(
        "--country-code",
        default=os.environ.get("COUNTRY_CODE", DEFAULT_COUNTRY_CODE),
        help="Audible marketplace country code",
    )
    parser.add_argument(
        "--state-db",
        default=os.environ.get("STATE_DB", DEFAULT_STATE_DB),
        help="Path to SQLite state database",
    )
    parser.add_argument(
        "--continuous", "-c",
        action="store_true",
        default=os.environ.get("CONTINUOUS", "").lower() in ("1", "true", "yes"),
        help="Run continuously, polling for new books",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=int(os.environ.get("POLL_INTERVAL", POLL_INTERVAL_SECONDS)),
        help="Seconds between library checks in continuous mode",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.environ.get("MAX_RETRIES", "3")),
        help="Max retries per book before giving up",
    )
    parser.add_argument(
        "--legacy-raw-dir",
        default=os.environ.get("LEGACY_RAW_DIR", ""),
        help="Path to flat raw directory from previous scripts (for migration)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)
    SyncAudible(args).run()


if __name__ == "__main__":
    main()
