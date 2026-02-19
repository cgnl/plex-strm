#!/usr/bin/env python3
"""Smart STRM fallback proxy with auto-repair.

Sits between a reverse proxy (Caddy/nginx) and Zurg. On 5XX from Zurg,
looks up alternative STRM IDs for the same movie/episode in Plex DB and
tries them until one works. When all alternatives fail, triggers Zurg's
repair endpoint to fix broken RD links.

Architecture:
  Client -> Reverse Proxy (443) -> strm_proxy (8765) -> Zurg (9091)

Environment variables:
  ZURG_URL          Zurg base URL          (default: http://localhost:9091)
  ZURG_USER         Zurg basic auth user   (default: "")
  ZURG_PASS         Zurg basic auth pass   (default: "")
  PROXY_PORT        Listen port            (default: 8765)
  PLEX_PG_HOST      PostgreSQL host        (default: localhost)
  PLEX_PG_PORT      PostgreSQL port        (default: 5432)
  PLEX_PG_DATABASE  PostgreSQL database    (default: plex)
  PLEX_PG_USER      PostgreSQL user        (default: plex)
  PLEX_PG_PASSWORD  PostgreSQL password    (default: plex)
  PLEX_PG_SCHEMA    PostgreSQL schema      (default: plex)
"""

import logging
import os
import time
import threading
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests
from flask import Flask, request, Response

# ---------------------------------------------------------------------------
# Config (all from environment)
# ---------------------------------------------------------------------------
ZURG_URL = os.environ.get("ZURG_URL", "http://localhost:9091")
ZURG_USER = os.environ.get("ZURG_USER", "")
ZURG_PASS = os.environ.get("ZURG_PASS", "")
LISTEN_PORT = int(os.environ.get("PROXY_PORT", "8765"))

PG_HOST = os.environ.get("PLEX_PG_HOST", "localhost")
PG_PORT = os.environ.get("PLEX_PG_PORT", "5432")
PG_DB = os.environ.get("PLEX_PG_DATABASE", "plex")
PG_USER = os.environ.get("PLEX_PG_USER", "plex")
PG_PASS = os.environ.get("PLEX_PG_PASSWORD", "plex")
PG_SCHEMA = os.environ.get("PLEX_PG_SCHEMA", "plex")

# Cache broken STRM IDs (avoid repeated 5XX requests to Zurg)
BROKEN_CACHE_TTL = 300  # seconds

# Repair trigger: at most once per this many seconds
REPAIR_COOLDOWN = 600  # 10 minutes

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [strm-proxy] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("strm-proxy")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
broken_cache = {}  # strm_id -> expiry_time
broken_lock = threading.Lock()
last_repair_time = 0  # epoch timestamp of last repair trigger


def is_broken(strm_id: str) -> bool:
    with broken_lock:
        exp = broken_cache.get(strm_id)
        if exp and time.time() < exp:
            return True
        if exp:
            del broken_cache[strm_id]
        return False


def mark_broken(strm_id: str):
    with broken_lock:
        broken_cache[strm_id] = time.time() + BROKEN_CACHE_TTL
        log.info("Marked %s as broken (cached %ds)", strm_id, BROKEN_CACHE_TTL)


# ---------------------------------------------------------------------------
# Zurg repair trigger
# ---------------------------------------------------------------------------
def trigger_repair():
    """Ask Zurg to repair all broken torrents. Rate-limited to once per REPAIR_COOLDOWN."""
    global last_repair_time
    now = time.time()
    if now - last_repair_time < REPAIR_COOLDOWN:
        remaining = int(REPAIR_COOLDOWN - (now - last_repair_time))
        log.debug("Repair cooldown active (%ds remaining), skipping", remaining)
        return
    last_repair_time = now
    auth = (ZURG_USER, ZURG_PASS) if ZURG_USER else None
    try:
        resp = requests.post(
            f"{ZURG_URL}/torrents/repair",
            auth=auth,
            timeout=10,
        )
        log.info("Triggered Zurg repair (status %d)", resp.status_code)
    except Exception as e:
        log.warning("Failed to trigger Zurg repair: %s", e)


# ---------------------------------------------------------------------------
# Plex DB: find alternative STRM IDs for the same content
# ---------------------------------------------------------------------------
def get_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB,
        user=PG_USER, password=PG_PASS,
        options=f"-c search_path={PG_SCHEMA}",
    )


def find_alternatives(strm_id: str):
    """Find other STRM IDs that serve the same movie/episode.

    Logic: media_parts.file contains the STRM URL with the ID at the end.
    Multiple media_parts can point to the same metadata_item (= same movie/episode).
    We find all sibling media_parts for the same metadata_item.
    """
    try:
        conn = get_pg()
        cur = conn.cursor()

        # Step 1: Find ALL metadata_item_ids for this STRM ID
        cur.execute("""
            SELECT DISTINCT mi.metadata_item_id
            FROM media_parts mp
            JOIN media_items mi ON mi.id = mp.media_item_id
            WHERE mp.file LIKE %s
        """, (f"%/strm/{strm_id}",))

        meta_ids = [r[0] for r in cur.fetchall()]
        if not meta_ids:
            log.warning("No media_parts match for STRM ID %s", strm_id)
            cur.close()
            conn.close()
            return []

        # Step 2: Find all STRM URLs for ALL those metadata_items
        cur.execute("""
            SELECT DISTINCT mp.file
            FROM media_parts mp
            JOIN media_items mi ON mi.id = mp.media_item_id
            WHERE mi.metadata_item_id = ANY(%s)
            AND mp.file LIKE 'http%%'
        """, (meta_ids,))

        all_urls = cur.fetchall()
        log.info("Found %d STRM URLs across %d metadata_items for %s",
                 len(all_urls), len(meta_ids), strm_id)

        alternatives = []
        for (url,) in all_urls:
            if "/strm/" in url:
                alt_id = url.rsplit("/strm/", 1)[-1]
                if alt_id != strm_id:
                    alternatives.append(alt_id)

        cur.close()
        conn.close()
        return alternatives

    except Exception as e:
        log.error("DB lookup failed for %s: %s", strm_id, e, exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Proxy logic
# ---------------------------------------------------------------------------
def try_zurg(strm_id: str):
    """Send request to Zurg, return response or None on 5XX."""
    auth = (ZURG_USER, ZURG_PASS) if ZURG_USER else None
    try:
        resp = requests.get(
            f"{ZURG_URL}/strm/{strm_id}",
            auth=auth,
            allow_redirects=False,
            timeout=10,
        )
        if resp.status_code >= 500:
            return None
        return resp
    except requests.RequestException as e:
        log.warning("Zurg request failed for %s: %s", strm_id, e)
        return None


@app.route("/strm/<strm_id>")
def proxy_strm(strm_id: str):
    # Fast path: try Zurg directly (unless cached as broken)
    if not is_broken(strm_id):
        resp = try_zurg(strm_id)
        if resp is not None:
            headers = dict(resp.headers)
            for h in ("transfer-encoding", "connection", "keep-alive"):
                headers.pop(h, None)
            return Response(
                resp.content,
                status=resp.status_code,
                headers=headers,
            )
        mark_broken(strm_id)

    # Slow path: find alternatives
    log.info("STRM %s broken, looking for alternatives...", strm_id)
    alternatives = find_alternatives(strm_id)

    if not alternatives:
        log.warning("No alternatives found for %s", strm_id)
        trigger_repair()
        return Response("No working version available", status=502)

    log.info("Found %d alternatives for %s: %s", len(alternatives), strm_id, alternatives)

    for alt_id in alternatives:
        if is_broken(alt_id):
            continue

        resp = try_zurg(alt_id)
        if resp is not None:
            log.info("Fallback success: %s -> %s (status %d)", strm_id, alt_id, resp.status_code)
            headers = dict(resp.headers)
            for h in ("transfer-encoding", "connection", "keep-alive"):
                headers.pop(h, None)
            return Response(
                resp.content,
                status=resp.status_code,
                headers=headers,
            )
        mark_broken(alt_id)

    log.warning("All alternatives broken for %s", strm_id)
    trigger_repair()
    return Response("All versions broken", status=502)


@app.route("/strm_proxy/status")
def status():
    with broken_lock:
        active = {k: int(v - time.time()) for k, v in broken_cache.items() if time.time() < v}
    repair_ago = int(time.time() - last_repair_time) if last_repair_time else None
    return {
        "broken_cached": len(active),
        "broken_ids": active,
        "last_repair_secs_ago": repair_ago,
        "repair_cooldown_secs": REPAIR_COOLDOWN,
    }


# Also proxy other /strm/ paths straight to Zurg
@app.route("/strm/<path:path>")
def proxy_other(path):
    auth = (ZURG_USER, ZURG_PASS) if ZURG_USER else None
    try:
        resp = requests.get(
            f"{ZURG_URL}/strm/{path}",
            auth=auth,
            allow_redirects=False,
            timeout=10,
        )
        headers = dict(resp.headers)
        for h in ("transfer-encoding", "connection", "keep-alive"):
            headers.pop(h, None)
        return Response(resp.content, status=resp.status_code, headers=headers)
    except requests.RequestException as e:
        return Response(str(e), status=502)


if __name__ == "__main__":
    log.info("Starting STRM fallback proxy on port %d", LISTEN_PORT)
    log.info("Zurg: %s  Plex DB: %s@%s:%s/%s", ZURG_URL, PG_USER, PG_HOST, PG_PORT, PG_DB)
    app.run(host="127.0.0.1", port=LISTEN_PORT, threaded=True)
