#!/bin/bash
# strm_pipeline.sh — Automated STRM processing pipeline
#
# Runs on every Zurg library change (via on_library_update) and also
# every 5 minutes via LaunchAgent as a fallback. Handles the full chain:
#   1. plex_strm.py: inject new STRM URLs + ffprobe metadata into Plex DB
#   2. organize_strm.py: create/update symlinks
#   3. Plex library scan on changed libraries
#
# Safety: lockfile prevents concurrent runs.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$SCRIPT_DIR/strm_pipeline.log"
LOCKFILE="/tmp/strm_pipeline.lock"

# ── Configuration (set these for your environment) ────────────
export PLEX_TOKEN="YOUR_PLEX_TOKEN"
export PLEX_URL="http://localhost:32400"
ENABLE_SCAN_GUARD="${ENABLE_SCAN_GUARD:-1}"
USE_TIMEOUTS="${USE_TIMEOUTS:-0}"
MAX_PLEX_STRM_TIME="${MAX_PLEX_STRM_TIME:-270}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"
TIMEOUT_BIN="${TIMEOUT_BIN:-$(command -v gtimeout || command -v timeout || true)}"

# PostgreSQL config
export PLEX_PG_HOST=localhost
export PLEX_PG_PORT=5432
export PLEX_PG_DATABASE=plex
export PLEX_PG_USER=plex
export PLEX_PG_PASSWORD=plex
export PLEX_PG_SCHEMA=plex
export FFPROBE_WORKERS=4
export FFPROBE_TIMEOUT=30

# Zurg connection
ZURG_URL="http://user:pass@localhost:9091"
ZURG_DATA_DIR="/path/to/zurg/data"
BASE_URL="https://user:pass@your-domain.com"

# Plex library IDs (find with: curl localhost:32400/library/sections?X-Plex-Token=...)
LIBRARY_IDS=(11 12)
LIBRARY_NAMES=("STRM Movies" "STRM TV Shows")

# plex_strm.py location
PLEX_STRM_PY="/path/to/plex-strm/plex_strm.py"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [pipeline] $*" >> "$LOG"; }

# Prevent concurrent runs
if [ -f "$LOCKFILE" ]; then
    pid=$(cat "$LOCKFILE" 2>/dev/null || echo "")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        exit 0  # Already running, skip silently
    fi
    rm -f "$LOCKFILE"
fi
echo $$ > "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

# Rotate log if > 1MB
if [ -f "$LOG" ] && [ "$(stat -f%z "$LOG" 2>/dev/null || stat -c%s "$LOG" 2>/dev/null || echo 0)" -gt 1048576 ]; then
    mv "$LOG" "${LOG}.old"
fi

log "=== Pipeline start ==="

if [ -z "$PYTHON_BIN" ]; then
    log "ERROR: python3 not found"
    exit 1
fi
if [ "$USE_TIMEOUTS" = "1" ] && [ -z "$TIMEOUT_BIN" ]; then
    log "ERROR: USE_TIMEOUTS=1 but no timeout binary found"
    exit 1
fi

# ── Pre-check: is Zurg up? ────────────────────────────────────
if ! curl -s -o /dev/null -w '' --max-time 5 "$ZURG_URL/dav/" 2>/dev/null; then
    log "SKIP: Zurg not responding"
    log "=== Pipeline done (skipped) ==="
    exit 0
fi

# Skip while Plex is still scanning STRM libraries.
if [ "$ENABLE_SCAN_GUARD" = "1" ]; then
SCAN_ACTIVE=$("$PYTHON_BIN" - <<'PY'
import os, urllib.request, urllib.parse, xml.etree.ElementTree as ET
url = os.environ.get("PLEX_URL", "http://localhost:32400")
token = os.environ.get("PLEX_TOKEN", "")
try:
    q = urllib.parse.urlencode({"X-Plex-Token": token})
    with urllib.request.urlopen(f"{url}/activities?{q}", timeout=10) as r:
        root = ET.fromstring(r.read())
    active = any(
        a.attrib.get("type") == "library.update.section"
        and "Scanning STRM" in (a.attrib.get("title") or "")
        for a in root.findall('.//Activity')
    )
    print("1" if active else "0")
except Exception:
    print("0")
PY
)

if [ "$SCAN_ACTIVE" = "1" ]; then
    log "SKIP: STRM scan still active"
    log "=== Pipeline done (skipped) ==="
    exit 0
fi
fi

# ── Step 1: plex_strm.py ──────────────────────────────────────
log "Step 1: Running plex_strm.py (new items only)"

LIB_ARGS=""
for name in "${LIBRARY_NAMES[@]}"; do
    LIB_ARGS="$LIB_ARGS --library \"$name\""
done

if [ "$USE_TIMEOUTS" = "1" ]; then
    PLEX_CMD=("$TIMEOUT_BIN" "$MAX_PLEX_STRM_TIME" "$PYTHON_BIN" "$PLEX_STRM_PY" --pg)
else
    PLEX_CMD=("$PYTHON_BIN" "$PLEX_STRM_PY" --pg)
fi

PLEX_STRM_OUT=$(${PLEX_CMD[@]} \
    $LIB_ARGS \
    update --protect --workers 4 --timeout 30 --retries 2 \
    --zurg-url "$ZURG_URL" \
    --zurg-data-dir "$ZURG_DATA_DIR" \
    --cleanup-broken \
    --base-url "$BASE_URL" 2>&1) || {
    EXIT_CODE=$?
    log "  plex_strm: exited with code $EXIT_CODE"
}

# Extract counts from output
NEW_URLS=$(echo "$PLEX_STRM_OUT" | grep -o "Replaced [0-9]* .strm" | grep -o "[0-9]*" || echo "0")
ANALYZED=$(echo "$PLEX_STRM_OUT" | grep -o "[0-9]* analyzed" | grep -o "[0-9]*" || echo "0")
FAILED=$(echo "$PLEX_STRM_OUT" | grep -o "[0-9]* failed" | grep -o "[0-9]*" | head -1 || echo "0")

log "  plex_strm: new_urls=$NEW_URLS analyzed=$ANALYZED failed=$FAILED"

# ── Step 2: Plex scan (only if changes detected) ─────────────
CHANGES=$((NEW_URLS + ANALYZED))
if [ "$CHANGES" -gt 0 ]; then
    log "Step 2: Triggering Plex scan ($CHANGES changes)"
    for LIB_ID in "${LIBRARY_IDS[@]}"; do
        curl -s "${PLEX_URL}/library/sections/${LIB_ID}/refresh?X-Plex-Token=${PLEX_TOKEN}" > /dev/null 2>&1 || true
    done
    log "  Plex scan triggered"
else
    log "Step 2: No changes, skipping Plex scan"
fi

log "=== Pipeline done ==="
