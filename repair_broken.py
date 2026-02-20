#!/usr/bin/env python3
"""
Repair broken STRM items one torrent at a time via Zurg.

For each torrent hash behind broken (version=-1) items:
  1. POST /manage/{hash}/repair to Zurg
  2. Wait a few seconds for RealDebrid to re-unrestrict
  3. Test each RD ID with tiny ranged GET to Zurg /strm/{id}
  4. If fixed: reset media_analysis_version to 0 so pipeline picks them up
  5. If still broken: leave at -1

Usage:
  python3 repair_broken.py [--dry-run] [--limit N] [--delay SECS]
"""

import base64
import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
import urllib.error
import subprocess

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [repair] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Config
ZURG_BASE = "http://localhost:9091"
ZURG_USER = os.environ.get("ZURG_USER", "")
ZURG_PASS = os.environ.get("ZURG_PASS", "")
ZURG_AUTH = base64.b64encode(f"{ZURG_USER}:{ZURG_PASS}".encode()).decode() if ZURG_USER else ""
ZURG_DATA_DIR = "/Users/sander/bin/zurg/data"

PLEX_DB_MODE = os.environ.get("PLEX_DB_MODE", "postgres").strip().lower()
PLEX_SQLITE_PATH = os.environ.get(
    "PLEX_SQLITE_PATH",
    "/Users/sander/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db",
)

PSQL = "/opt/homebrew/Cellar/postgresql@15/15.15/bin/psql"
PG_CONN = ["-h", "localhost", "-U", "plex", "-d", "plex"]
PG_SCHEMA = os.environ.get("PLEX_PG_SCHEMA", "plex")

REPAIR_DELAY = 3  # seconds to wait after repair before testing


def t(name):
    """Table reference for configured DB mode."""
    if PLEX_DB_MODE == "sqlite":
        return name
    return f"{PG_SCHEMA}.{name}"


def pg_query(sql):
    """Run a DB query and return raw pipe-delimited output."""
    if PLEX_DB_MODE == "sqlite":
        try:
            conn = sqlite3.connect(PLEX_SQLITE_PATH)
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            out = []
            for row in rows:
                out.append("|".join("" if v is None else str(v) for v in row))
            return "\n".join(out)
        except Exception as e:
            log.error("SQLite query failed: %s", e)
            return ""
    try:
        result = subprocess.run(
            [PSQL] + PG_CONN + ["-t", "-A", "-c", sql],
            capture_output=True, text=True, timeout=30,
        )
        return result.stdout.strip()
    except Exception as e:
        log.error("PostgreSQL query failed: %s", e)
        return ""


def pg_execute(sql):
    """Execute DB statement, return output with affected-row count."""
    if PLEX_DB_MODE == "sqlite":
        try:
            conn = sqlite3.connect(PLEX_SQLITE_PATH)
            cur = conn.cursor()
            cur.execute(sql)
            count = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
            conn.commit()
            cur.close()
            conn.close()
            return "UPDATE %d" % count
        except Exception as e:
            log.error("SQLite execute failed: %s", e)
            return ""
    try:
        result = subprocess.run(
            [PSQL] + PG_CONN + ["-c", sql],
            capture_output=True, text=True, timeout=30,
        )
        return result.stdout.strip()
    except Exception as e:
        log.error("PostgreSQL execute failed: %s", e)
        return ""


def build_index():
    """Build RD-ID -> torrent hash and hash -> name mappings."""
    rd_to_hash = {}
    hash_to_name = {}
    hash_to_state = {}
    hash_to_rd_ids = {}  # hash -> [rd_id, ...]

    for fname in os.listdir(ZURG_DATA_DIR):
        if not fname.endswith('.zurgtorrent'):
            continue
        path = os.path.join(ZURG_DATA_DIR, fname)
        try:
            with open(path, 'r') as f:
                d = json.load(f)
        except Exception:
            continue

        h = d.get('Hash', '')
        if not h:
            continue

        hash_to_state[h] = (d.get('State', ''), d.get('Unfixable', ''))
        hash_to_name[h] = d.get('Name', fname)

        for finfo in (d.get('SelectedFiles') or {}).values():
            link = finfo.get('Link', '')
            if '/d/' in link:
                rd_id = link.rsplit('/', 1)[-1]
                rd_to_hash[rd_id] = h
                hash_to_rd_ids.setdefault(h, []).append(rd_id)

    return rd_to_hash, hash_to_name, hash_to_state, hash_to_rd_ids


def get_broken_rd_ids():
    """Get all RD IDs that are at media_analysis_version = -1."""
    if PLEX_DB_MODE == "sqlite":
        raw = pg_query(f"""
            SELECT DISTINCT
                CASE
                    WHEN instr(mp.file, '/strm/') > 0 THEN substr(mp.file, instr(mp.file, '/strm/') + 6)
                    ELSE mp.file
                END
            FROM {t('media_items')} mi
            JOIN {t('media_parts')} mp ON mp.media_item_id = mi.id
            WHERE mp.file LIKE 'http%%' AND mi.media_analysis_version = -1
        """)
    else:
        raw = pg_query(f"""
            SELECT DISTINCT regexp_replace(mp.file, '.*/strm/', '')
            FROM {t('media_items')} mi
            JOIN {t('media_parts')} mp ON mp.media_item_id = mi.id
            WHERE mp.file LIKE 'http%%' AND mi.media_analysis_version = -1
        """)
    if not raw:
        return set()
    return set(raw.splitlines())


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Handler that stops urllib from following redirects."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def _zurg_request(path, method='GET', headers=None):
    """Make an authenticated request to Zurg. Returns HTTP status code."""
    url = ZURG_BASE + path
    req = urllib.request.Request(url, method=method,
                                data=b'' if method == 'POST' else None)
    if ZURG_AUTH:
        req.add_header('Authorization', 'Basic ' + ZURG_AUTH)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with _opener.open(req, timeout=15) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def trigger_repair(torrent_hash):
    """POST /manage/{hash}/repair to Zurg. Returns True on success."""
    status = _zurg_request("/manage/%s/repair" % torrent_hash, method='POST')
    if status in (200, 303):
        return True
    log.debug("Repair %s HTTP %d", torrent_hash[:12], status)
    return False


def test_rd_id(rd_id):
    """Probe Zurg /strm/{id} via tiny GET range.

    HEAD can return false-positive 200 while real GET fails during unrestrict.
    A ranged GET forces the real path with minimal bandwidth.
    """
    return _zurg_request(
        "/strm/%s" % rd_id,
        method='GET',
        headers={'Range': 'bytes=0-1'},
    )


def reset_rd_ids_to_zero(rd_ids):
    """Reset media_analysis_version to 0 for given RD IDs."""
    if not rd_ids:
        return 0
    sql = f"""
        UPDATE {t('media_items')} SET media_analysis_version = 0
        WHERE media_analysis_version = -1
          AND id IN (
            SELECT mi.id FROM {t('media_items')} mi
            JOIN {t('media_parts')} mp ON mp.media_item_id = mi.id
            WHERE mi.media_analysis_version = -1
              AND ({' OR '.join("mp.file LIKE '%%/strm/%s'" % rid for rid in rd_ids)})
          )
    """
    result = pg_execute(sql)
    # Extract count from "UPDATE N"
    if result and 'UPDATE' in result:
        try:
            return int(result.split('UPDATE')[-1].strip())
        except ValueError:
            pass
    return 0


def main():
    if PLEX_DB_MODE not in ("postgres", "sqlite"):
        log.warning("Invalid PLEX_DB_MODE=%s, defaulting to postgres", PLEX_DB_MODE)

    dry_run = "--dry-run" in sys.argv
    limit = None
    delay = REPAIR_DELAY

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--limit" and i < len(sys.argv) - 1:
            limit = int(sys.argv[i + 1])
        if arg == "--delay" and i < len(sys.argv) - 1:
            delay = float(sys.argv[i + 1])

    log.info("Building zurgtorrent index...")
    rd_to_hash, hash_to_name, hash_to_state, hash_to_rd_ids = build_index()
    log.info("Index: %d RD IDs, %d torrent hashes", len(rd_to_hash), len(hash_to_state))

    log.info("Fetching broken RD IDs from Plex DB...")
    broken_rd_ids = get_broken_rd_ids()
    log.info("Broken RD IDs: %d", len(broken_rd_ids))

    # Group by torrent hash
    broken_by_hash = {}
    unmapped = []
    for rd_id in broken_rd_ids:
        h = rd_to_hash.get(rd_id)
        if h:
            broken_by_hash.setdefault(h, []).append(rd_id)
        else:
            unmapped.append(rd_id)

    # Separate by state
    ok_hashes = []
    broken_hashes = []
    unfixable_hashes = []
    for h, rd_ids in broken_by_hash.items():
        state, unfixable = hash_to_state.get(h, ('?', ''))
        if unfixable:
            unfixable_hashes.append(h)
        elif state == 'broken_torrent':
            broken_hashes.append(h)
        else:
            ok_hashes.append(h)

    log.info("Torrent breakdown: %d ok, %d broken (repairable), %d unfixable, %d unmapped RD IDs",
             len(ok_hashes), len(broken_hashes), len(unfixable_hashes), len(unmapped))

    # Process ok_torrent and broken_torrent (non-unfixable) hashes
    to_repair = broken_hashes + ok_hashes  # broken first, they need repair most
    if limit:
        to_repair = to_repair[:limit]

    total = len(to_repair)
    total_fixed = 0
    total_still_broken = 0
    total_rd_fixed = 0
    total_rd_broken = 0

    log.info("Starting repair of %d torrents (delay=%ss, dry_run=%s)...",
             total, delay, dry_run)

    for i, h in enumerate(to_repair, 1):
        name = hash_to_name.get(h, h[:16])
        rd_ids = broken_by_hash[h]
        state, _ = hash_to_state.get(h, ('?', ''))

        log.info("[%d/%d] %s (%s, %d files)", i, total, name[:60], state, len(rd_ids))

        if dry_run:
            continue

        # Trigger repair
        repaired = trigger_repair(h)
        if not repaired:
            log.warning("  Repair request failed for %s", h[:12])
            total_still_broken += 1
            total_rd_broken += len(rd_ids)
            continue

        # Wait for RealDebrid to process
        time.sleep(delay)

        # Test each RD ID
        fixed_ids = []
        broken_ids = []
        for rd_id in rd_ids:
            status = test_rd_id(rd_id)
            if status in (200, 302, 307):
                fixed_ids.append(rd_id)
            else:
                broken_ids.append(rd_id)

        if fixed_ids:
            # Reset fixed items to version 0
            count = reset_rd_ids_to_zero(fixed_ids)
            log.info("  FIXED: %d/%d files (reset %d DB items)",
                     len(fixed_ids), len(rd_ids), count)
            total_rd_fixed += len(fixed_ids)

        if broken_ids:
            log.info("  STILL BROKEN: %d/%d files", len(broken_ids), len(rd_ids))
            total_rd_broken += len(broken_ids)

        if not broken_ids:
            total_fixed += 1
        elif not fixed_ids:
            total_still_broken += 1
        else:
            total_fixed += 1  # partially fixed

    log.info("=" * 60)
    log.info("RESULTS:")
    log.info("  Torrents processed: %d", total)
    log.info("  Torrents fixed (fully/partially): %d", total_fixed)
    log.info("  Torrents still broken: %d", total_still_broken)
    log.info("  RD files fixed: %d", total_rd_fixed)
    log.info("  RD files still broken: %d", total_rd_broken)
    log.info("  Unfixable torrents (skipped): %d", len(unfixable_hashes))
    log.info("  Unmapped RD IDs (skipped): %d", len(unmapped))


if __name__ == "__main__":
    main()
