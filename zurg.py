"""Zurg integration: zurgtorrent index, repair, and broken torrent cleanup."""

import json
import logging
import os
import re

from rd_client import RDClient

log = logging.getLogger("plex-strm")


def build_zurgtorrent_index(data_dir):
    """Parse all .zurgtorrent files and build RD-ID -> torrent-hash mapping.

    Returns:
        rd_id_to_hash: dict mapping RD download IDs (from /strm/<ID>) to torrent hashes
        hash_to_state: dict mapping torrent hashes to their State value
    """
    rd_id_to_hash = {}
    hash_to_state = {}
    if not data_dir or not os.path.isdir(data_dir):
        return rd_id_to_hash, hash_to_state

    count = 0
    for fname in os.listdir(data_dir):
        if not fname.endswith('.zurgtorrent'):
            continue
        path = os.path.join(data_dir, fname)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                d = json.load(f)
        except Exception:
            continue
        h = d.get('Hash', '')
        state = d.get('State', '')
        if h:
            hash_to_state[h] = state
        for finfo in (d.get('SelectedFiles') or {}).values():
            link = finfo.get('Link', '')
            if '/d/' in link:
                rd_id = link.rsplit('/', 1)[-1]
                rd_id_to_hash[rd_id] = h
                count += 1

    log.info("Zurgtorrent index: %d RD IDs -> %d unique torrent hashes",
             count, len(hash_to_state))
    return rd_id_to_hash, hash_to_state


def extract_rd_id_from_url(url):
    """Extract the RD download ID from a Zurg STRM URL.

    Example: https://user:pass@host/strm/LTGLPYW5QQJMO3D6 -> LTGLPYW5QQJMO3D6
    """
    if not url:
        return None
    m = re.search(r'/strm/([A-Za-z0-9]+)$', url)
    return m.group(1) if m else None


def trigger_repair_all(zurg_url):
    """Trigger Zurg repair-all via POST /torrents/repair. Returns True on success."""
    import requests
    try:
        resp = requests.post(f"{zurg_url.rstrip('/')}/torrents/repair", timeout=30)
        if resp.status_code == 200:
            log.info("Zurg repair-all triggered successfully")
            return True
        log.warning("Zurg repair returned status %d", resp.status_code)
    except Exception as e:
        log.warning("Zurg repair request failed: %s", e)
    return False


def _repair_one(base, h):
    """Repair a single torrent. Returns hash on success, None on failure."""
    import requests as _req
    try:
        resp = _req.post(f"{base}/manage/{h}/repair",
                         timeout=15, allow_redirects=False)
        if resp.status_code in (200, 303):
            return h
        log.debug("Repair %s returned status %d", h[:12], resp.status_code)
    except Exception as e:
        log.debug("Repair %s failed: %s", h[:12], e)
    return None


def trigger_repair_torrents(zurg_url, torrent_hashes, workers=8):
    """Trigger per-torrent repair via POST /manage/{hash}/repair.

    Returns set of hashes that were successfully submitted for repair.
    Zurg returns 303 See Other on success (redirect to manage page).
    Uses parallel workers (default 8) for speed.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    base = zurg_url.rstrip('/')
    hashes = list(torrent_hashes)
    repaired = set()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_repair_one, base, h): h for h in hashes}
        done = 0
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                repaired.add(result)
            done += 1
            if done % 500 == 0:
                log.info("Per-torrent repair: %d/%d submitted (%d ok)...",
                         done, len(hashes), len(repaired))

    log.info("Per-torrent repair: submitted %d/%d torrents",
             len(repaired), len(torrent_hashes))
    return repaired


def is_server_error_url(mid, url):
    """Check if a failed URL is likely a server error (5XX / unrestrict failure).
    All items that failed FFprobe are potential repair candidates.
    Zurg repair is idempotent and fast, so it's safe to trigger for all failures."""
    return True


def cleanup_broken_torrents(data_dir, rd_api_token, dry_run=False):
    """Delete fully broken+Unfixable torrents from RealDebrid.

    Only deletes torrents where:
    1. Zurg state is 'broken_torrent'
    2. The 'Unfixable' field is set (repair was truly attempted and failed)
    3. ALL files are broken or deleted (no ok_file remaining)

    Returns (deleted_count, skipped_count, error_count).
    """
    if not data_dir or not os.path.isdir(data_dir):
        log.warning("cleanup-broken: data dir %s not found", data_dir)
        return 0, 0, 0

    # Collect candidates
    candidates = []
    for fname in os.listdir(data_dir):
        if not fname.endswith('.zurgtorrent'):
            continue
        path = os.path.join(data_dir, fname)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                d = json.load(f)
        except Exception:
            continue

        if d.get('State') != 'broken_torrent':
            continue
        if not d.get('Unfixable'):
            continue  # Repair hasn't confirmed it's truly unfixable

        # Check ALL files are broken/deleted (none ok)
        selected = d.get('SelectedFiles') or {}
        if not selected:
            continue
        has_ok = any(info.get('State') == 'ok_file' for info in selected.values())
        if has_ok:
            continue  # Partially broken -- keep it, some files still work

        candidates.append({
            'name': d.get('Name', fname),
            'hash': d.get('Hash', ''),
            'reason': d.get('Unfixable', ''),
            'downloaded_ids': d.get('DownloadedIDs', ''),
            'file_count': len(selected),
        })

    if not candidates:
        log.info("cleanup-broken: no fully broken+unfixable torrents found")
        return 0, 0, 0

    log.info("cleanup-broken: found %d fully broken+unfixable torrents", len(candidates))

    if dry_run:
        for c in candidates[:10]:
            log.info("  [DRY-RUN] would delete: %s (%s)", c['name'][:60], c['reason'])
        if len(candidates) > 10:
            log.info("  ... and %d more", len(candidates) - 10)
        return 0, len(candidates), 0

    # Need to map zurgtorrent hashes to RD torrent IDs via the RD API
    # Keep this separate from Zurg itself; this only affects cleanup workflow.
    rd = RDClient(
        rd_api_token,
        rate_limit_per_minute=200,  # stay below RD hard cap (250/min)
        timeout=30,
        max_retries=6,
    )

    hash_to_rd_id = {}
    try:
        for t in rd.iter_torrents(page_size=2500):
            h = t.get('hash', '').lower()
            if h:
                hash_to_rd_id[h] = t['id']
    except Exception as e:
        log.error("cleanup-broken: failed to fetch RD torrents: %s", e)

    log.info("cleanup-broken: fetched %d RD torrents for hash mapping", len(hash_to_rd_id))

    deleted = 0
    skipped = 0
    errors = 0
    for c in candidates:
        h = c['hash'].lower()
        rd_id = hash_to_rd_id.get(h)
        if not rd_id:
            log.debug("cleanup-broken: hash %s not found on RD (already deleted?): %s",
                       h[:12], c['name'][:50])
            skipped += 1
            continue

        try:
            ok = rd.delete_torrent(rd_id)
            if ok:
                log.info("cleanup-broken: deleted %s (%s) [%s]",
                         c['name'][:50], c['reason'], rd_id)
                deleted += 1
            else:
                errors += 1
        except Exception as e:
            log.error("cleanup-broken: delete %s failed: %s", rd_id, e)
            errors += 1

    m = rd.metrics
    rd.close()
    log.info(
        "cleanup-broken: RD metrics requests=%d retries=%d 429=%d 5xx=%d net_err=%d",
        m.get("requests", 0),
        m.get("retries", 0),
        m.get("rate_limited_429", 0),
        m.get("server_5xx", 0),
        m.get("network_errors", 0),
    )

    log.info("cleanup-broken: deleted %d, skipped %d, errors %d (of %d candidates)",
             deleted, skipped, errors, len(candidates))
    return deleted, skipped, errors
