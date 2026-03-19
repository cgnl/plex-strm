#!/bin/bash
# strm_pipeline.sh — Automated STRM processing pipeline
#
# Runs every 5 minutes (via LaunchAgent) and handles the full chain:
#   1. plex_strm.py: inject new STRM URLs + ffprobe metadata into Plex DB
#   2. organize_strm.py: create/update symlinks (Spanish separation)
#   3. Plex library scan on changed libraries
#
# Only processes NEW items (missing media_streams). For bulk re-analysis
# of existing items, run plex_strm.py manually with --reanalyze N.
#
# Safety: lockfile prevents concurrent runs.
#
# Configuration: create a .env file next to this script with:
#   PLEX_TOKEN=your_token
#   PLEX_URL=http://localhost:32400
#   PLEX_PG_HOST=localhost
#   PLEX_PG_PORT=5432
#   PLEX_PG_DATABASE=plex
#   PLEX_PG_USER=plex
#   PLEX_PG_PASSWORD=plex
#   PLEX_PG_SCHEMA=plex
#   ZURG_USER=zurg
#   ZURG_PASS=your_zurg_password
#   ZURG_HOST=strm.yourdomain.com
#   PLEX_HOST=plex.yourdomain.com

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$SCRIPT_DIR/strm_pipeline.log"
LOCKFILE="/tmp/strm_pipeline.lock"
CHANGED_PATHS_FILE="/tmp/organize_strm_changed_paths.txt"
GTIMEOUT_BIN="${GTIMEOUT_BIN:-$(command -v gtimeout || command -v timeout || true)}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"
ENABLE_SCAN_GUARD="${ENABLE_SCAN_GUARD:-1}"
USE_TIMEOUTS="${USE_TIMEOUTS:-0}"
MAX_PLEX_STRM_TIME="${MAX_PLEX_STRM_TIME:-270}"

# Load credentials from .env file
ENV_FILE="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
else
    echo "ERROR: .env file not found at $ENV_FILE" >&2
    exit 1
fi

if [ -z "$PYTHON_BIN" ]; then
    log "ERROR: python3 not found"
    exit 1
fi
if [ "$USE_TIMEOUTS" = "1" ] && [ -z "$GTIMEOUT_BIN" ]; then
    log "ERROR: USE_TIMEOUTS=1 but no timeout binary found"
    exit 1
fi

export FFPROBE_WORKERS=4
export FFPROBE_TIMEOUT=30

# plex_strm.py location (adjust for your setup)
PLEX_STRM_PY="${PLEX_STRM_PY:-$SCRIPT_DIR/plex_strm.py}"

# Zurg data directory (for zurgtorrent hash mapping)
ZURG_DATA_DIR="${ZURG_DATA_DIR:-$SCRIPT_DIR/data}"

# Remote Zurg host for zurgtorrent sync (set to empty to disable)
ZURG_REMOTE="${ZURG_REMOTE:-}"

# Library names to process
LIBRARY_NAMES=("STRM Movies" "STRM TV Shows" "STRM Peliculas" "STRM TV Español")

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

# ── Pre-check: is Zurg up? ──────────────────────────────────
ZURG_HEALTH_URL="https://${ZURG_HOST}/strm/health"
ZURG_UP=0
if curl -s -o /dev/null -w '' --max-time 10 -u "${ZURG_USER}:${ZURG_PASS}" "$ZURG_HEALTH_URL" 2>/dev/null; then
    ZURG_UP=1
else
    log "WARNING: Zurg not responding on $ZURG_HEALTH_URL (plex_strm will be skipped)"
fi

MAX_SCAN_SKIP="${MAX_SCAN_SKIP:-600}"  # max seconds to skip for active scan (default 10 min)
SCAN_SKIP_FILE="/tmp/strm_pipeline_scan_skip_since"

if [ "$ENABLE_SCAN_GUARD" = "1" ]; then
# ── Guard: skip while STRM libraries are still scanning ────────
SCAN_ACTIVE=$(
"$PYTHON_BIN" - <<'PY'
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
    # Track how long we've been skipping
    if [ ! -f "$SCAN_SKIP_FILE" ]; then
        date +%s > "$SCAN_SKIP_FILE"
    fi
    SKIP_SINCE=$(cat "$SCAN_SKIP_FILE" 2>/dev/null || date +%s)
    NOW=$(date +%s)
    SKIP_DURATION=$((NOW - SKIP_SINCE))

    if [ "$SKIP_DURATION" -lt "$MAX_SCAN_SKIP" ]; then
        log "SKIP: STRM library scan still active (${SKIP_DURATION}s/${MAX_SCAN_SKIP}s)"
        log "=== Pipeline done (skipped) ==="
        exit 0
    else
        log "WARNING: Scan active for ${SKIP_DURATION}s, exceeds max skip ${MAX_SCAN_SKIP}s — running anyway"
    fi
else
    rm -f "$SCAN_SKIP_FILE"
fi
fi

# ── Step 0: Sync zurgtorrent data from remote Zurg ───────────
# Needed for --cleanup-broken hash mapping. Quick incremental sync.
if [ "$ZURG_UP" = "1" ] && [ -n "$ZURG_REMOTE" ]; then
    rsync -az --include='*.zurgtorrent' --exclude='*' \
        "$ZURG_REMOTE" \
        "$ZURG_DATA_DIR/" 2>/dev/null && \
        log "Step 0: zurgtorrent sync OK" || \
        log "Step 0: zurgtorrent sync FAILED (non-fatal)"
fi

# ── Step 1: plex_strm.py ──────────────────────────────────────
# Only processes items with missing media_streams (no --reanalyze).
# Requires Zurg to be reachable for ffprobe analysis.
NEW_URLS=0
MISSING=0
ANALYZED=0
FAILED=0

if [ "$ZURG_UP" = "1" ]; then
    if [ "$USE_TIMEOUTS" = "1" ]; then
        log "Step 1: Running plex_strm.py (new items only, timeout ${MAX_PLEX_STRM_TIME}s)"
        PLEX_CMD=("$GTIMEOUT_BIN" "$MAX_PLEX_STRM_TIME" "$PYTHON_BIN" "$PLEX_STRM_PY" --pg)
    else
        log "Step 1: Running plex_strm.py (new items only)"
        PLEX_CMD=("$PYTHON_BIN" "$PLEX_STRM_PY" --pg)
    fi

    LIB_ARGS=()
    for name in "${LIBRARY_NAMES[@]}"; do
        LIB_ARGS+=(--library "$name")
    done

    PLEX_STRM_OUT=$(${PLEX_CMD[@]} \
        "${LIB_ARGS[@]}" \
        update --protect --workers 8 --timeout 30 --retries 2 \
        --subtitles --subtitle-mode missing \
        --zurg-url "https://${ZURG_USER}:${ZURG_PASS}@${ZURG_HOST}" \
        --zurg-data-dir "$ZURG_DATA_DIR" \
        --cleanup-broken \
        --base-url "https://${ZURG_USER}:${ZURG_PASS}@${PLEX_HOST}" 2>&1) || {
        EXIT_CODE=$?
        log "  plex_strm: exited with code $EXIT_CODE"
    }

    # Extract counts from output
    NEW_URLS=$(echo "$PLEX_STRM_OUT" | grep -o "Replaced [0-9]* .strm" | grep -o "[0-9]*" || echo "0")
    MISSING=$(echo "$PLEX_STRM_OUT" | grep -o "Found [0-9]* HTTP URLs missing" | grep -o "[0-9]*" || echo "0")
    ANALYZED=$(echo "$PLEX_STRM_OUT" | grep -o "[0-9]* analyzed" | grep -o "[0-9]*" || echo "0")
    FAILED=$(echo "$PLEX_STRM_OUT" | grep -o "[0-9]* failed" | grep -o "[0-9]*" | head -1 || echo "0")

    log "  plex_strm: new_urls=$NEW_URLS missing=$MISSING analyzed=$ANALYZED failed=$FAILED"
else
    log "Step 1: SKIPPED plex_strm.py (Zurg unreachable)"
fi

# ── Step 2: organize_strm.py ──────────────────────────────────
# Always run — picks up new .strm files and updates symlinks.
log "Step 2: Running organize_strm.py"
ORGANIZE_CHANGED_PATHS_FILE="$CHANGED_PATHS_FILE" "$PYTHON_BIN" "$SCRIPT_DIR/organize_strm.py" >> "$LOG" 2>&1 || {
    log "  organize_strm.py failed"
}
log "  organize: done"

# ── Step 3: Plex scan (only if changes detected) ─────────────
CHANGES=$((NEW_URLS + MISSING + ANALYZED))
TARGETED_REFRESH=0
if [ -s "$CHANGED_PATHS_FILE" ]; then
    log "Step 3: Triggering targeted Plex refresh from organize changes"
    while IFS='|' read -r LIB_ID REFRESH_PATH; do
        [ -z "$LIB_ID" ] && continue
        [ -z "$REFRESH_PATH" ] && continue
        if [ ! -d "$REFRESH_PATH" ]; then
            continue
        fi
        curl -s --get "${PLEX_URL}/library/sections/${LIB_ID}/refresh" \
            --data-urlencode "path=${REFRESH_PATH}" \
            --data-urlencode "X-Plex-Token=${PLEX_TOKEN}" > /dev/null 2>&1 || true
        TARGETED_REFRESH=$((TARGETED_REFRESH + 1))
    done < "$CHANGED_PATHS_FILE"
    log "  Targeted refresh requests sent: $TARGETED_REFRESH"
fi

if [ "$CHANGES" -gt 0 ] && [ "$TARGETED_REFRESH" -eq 0 ]; then
    log "Step 3: Triggering full Plex scan fallback ($CHANGES changes)"
    STRM_LIB_IDS=$(curl -s "${PLEX_URL}/library/sections?X-Plex-Token=${PLEX_TOKEN}" 2>/dev/null \
        | "$PYTHON_BIN" -c "
import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
for d in root.findall('.//Directory'):
    if d.attrib.get('title','').startswith('STRM'):
        print(d.attrib.get('key'))
" 2>/dev/null || echo "")
    if [ -z "$STRM_LIB_IDS" ]; then
        log "  WARNING: Could not look up STRM library IDs from Plex API"
    fi
    for LIB_ID in $STRM_LIB_IDS; do
        curl -s "${PLEX_URL}/library/sections/${LIB_ID}/refresh?X-Plex-Token=${PLEX_TOKEN}" > /dev/null 2>&1 || true
    done
    log "  Full refresh triggered for libraries: $STRM_LIB_IDS"
elif [ "$CHANGES" -gt 0 ]; then
    log "Step 3: Skipped full scan (targeted refresh already triggered)"
else
    log "Step 3: No plex_strm changes"
fi

# ── Summary ──────────────────────────────────────────────────
ERRORS=""
[ "$FAILED" -gt 0 ] && ERRORS="${ERRORS} ${FAILED} ffprobe failures,"
[ "$ZURG_UP" = "0" ] && ERRORS="${ERRORS} Zurg unreachable,"
ERRORS="${ERRORS% ,}"
if [ -n "$ERRORS" ]; then
    log "SUMMARY: urls=$NEW_URLS analyzed=$ANALYZED failed=$FAILED refreshes=$TARGETED_REFRESH | ISSUES:$ERRORS"
else
    log "SUMMARY: urls=$NEW_URLS analyzed=$ANALYZED failed=$FAILED refreshes=$TARGETED_REFRESH | OK"
fi
log "=== Pipeline done ==="
