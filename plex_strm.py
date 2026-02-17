#!/usr/bin/env python3
"""plex-strm: Inject streaming URLs into Plex database for Direct Play.

Reads .strm files, replaces local paths with HTTP URLs in the Plex database,
runs FFprobe to extract codec metadata, and optionally downloads subtitles.

Supports both SQLite and PostgreSQL (via plex-postgresql) backends.
"""

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

log = logging.getLogger("plex-strm")

# ---------------------------------------------------------------------------
# Database abstraction
# ---------------------------------------------------------------------------

class PlexDB:
    """Thin abstraction over SQLite and PostgreSQL connections."""

    def __init__(self, db_path=None, pg_config=None):
        self.is_pg = pg_config is not None
        self._schema = ""
        if self.is_pg:
            try:
                import psycopg2
            except ImportError:
                sys.exit("psycopg2 is required for PostgreSQL mode: pip install psycopg2-binary")
            schema = pg_config.pop("schema", "plex")
            self._schema = f"{schema}." if schema else ""
            self.conn = psycopg2.connect(**pg_config)
            self.conn.autocommit = False
            # Set search_path so unqualified table names resolve
            cur = self.conn.cursor()
            cur.execute(f"SET search_path TO {schema}, public")
            cur.close()
        else:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def ph(self, *indices):
        """Return placeholders. ph(1,2,3) -> '%s, %s, %s' or '?, ?, ?'."""
        if self.is_pg:
            return ", ".join("%s" for _ in indices)
        return ", ".join("?" for _ in indices)

    def q(self, name):
        """Quote a reserved column name. q('index') -> '[index]' or '"index"'."""
        if self.is_pg:
            return f'"{name}"'
        return f"[{name}]"

    def execute(self, cur, sql, params=None):
        """Execute SQL with the right placeholder style."""
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

    def fetchall(self, cur):
        """Fetch all rows as dicts."""
        if self.is_pg:
            cols = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        return [dict(row) for row in cur.fetchall()]

    def fetchone(self, cur):
        """Fetch one row as dict."""
        if self.is_pg:
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        row = cur.fetchone()
        return dict(row) if row else None


def _resolve_library_ids(db, names):
    """Resolve library section names to IDs. Returns list of ints or None if no filter."""
    if not names:
        return None
    cur = db.cursor()
    ids = []
    for name in names:
        ph = "%s" if db.is_pg else "?"
        db.execute(cur, f"SELECT id FROM library_sections WHERE TRIM(name) = {ph}", (name.strip(),))
        row = db.fetchone(cur)
        if row:
            ids.append(row["id"])
        else:
            log.warning("Library '%s' not found, skipping", name)
    cur.close()
    if not ids:
        sys.exit("None of the specified libraries were found")
    return ids


def _library_join(db, lib_ids, alias="mp"):
    """Return (JOIN clause, WHERE clause, params) for filtering by library section IDs."""
    if lib_ids is None:
        return "", "", []
    # media_parts.media_item_id -> media_items.id
    # media_items.metadata_item_id -> metadata_items.id
    # metadata_items.library_section_id -> library_sections.id
    join = (f" JOIN media_items mi_j ON {alias}.media_item_id = mi_j.id"
            f" JOIN metadata_items md_j ON mi_j.metadata_item_id = md_j.id")
    if db.is_pg:
        placeholders = ", ".join("%s" for _ in lib_ids)
    else:
        placeholders = ", ".join("?" for _ in lib_ids)
    where = f" AND md_j.library_section_id IN ({placeholders})"
    return join, where, tuple(lib_ids)


def connect_from_args(args):
    """Create PlexDB from parsed CLI arguments."""
    pg_config = _pg_config_from_env()
    if args.pg or pg_config:
        if not pg_config:
            sys.exit("--pg specified but no PLEX_PG_* environment variables set")
        return PlexDB(pg_config=pg_config)
    db_path = args.db or os.environ.get("PLEX_DB")
    if not db_path:
        sys.exit("Specify --db PATH or set PLEX_DB, or use --pg with PLEX_PG_* env vars")
    if not os.path.isfile(db_path):
        sys.exit(f"Database not found: {db_path}")
    return PlexDB(db_path=db_path)


def _pg_config_from_env():
    """Build psycopg2 connect kwargs from PLEX_PG_* env vars."""
    host = os.environ.get("PLEX_PG_HOST")
    if not host:
        return None
    cfg = {"host": host}
    if os.environ.get("PLEX_PG_PORT"):
        cfg["port"] = int(os.environ["PLEX_PG_PORT"])
    cfg["dbname"] = os.environ.get("PLEX_PG_DATABASE", "plex")
    cfg["user"] = os.environ.get("PLEX_PG_USER", "plex")
    pw = os.environ.get("PLEX_PG_PASSWORD")
    if pw:
        cfg["password"] = pw
    cfg["schema"] = os.environ.get("PLEX_PG_SCHEMA", "plex")
    return cfg


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def backup_database(args, db):
    """Create a timestamped backup of the Plex database."""
    if db.is_pg:
        log.info("PostgreSQL mode — skipping file backup (use pg_dump externally)")
        return
    backup_dir = args.backup_dir or os.environ.get("BACKUP_DIR", ".")
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(backup_dir, f"plex_db_backup_{ts}.db")
    shutil.copy2(args.db or os.environ["PLEX_DB"], dst)
    log.info("Database backed up to %s", dst)


# ---------------------------------------------------------------------------
# Protection triggers
# ---------------------------------------------------------------------------

_SQLITE_TRIGGERS = [
    # Layer 1: block non-HTTP replacement of HTTP URLs
    """CREATE TRIGGER protect_stream_urls_strict
BEFORE UPDATE ON media_parts
FOR EACH ROW
WHEN (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%')
     AND (NEW.file NOT LIKE 'http://%' AND NEW.file NOT LIKE 'https://%')
     AND NEW.file != OLD.file
BEGIN
    SELECT RAISE(IGNORE);
END""",
    # Layer 2: auto-backup URL changes
    """CREATE TRIGGER backup_stream_urls
AFTER UPDATE ON media_parts
FOR EACH ROW
WHEN (NEW.file LIKE 'http://%' OR NEW.file LIKE 'https://%')
     AND NEW.file != OLD.file
BEGIN
    INSERT OR REPLACE INTO stream_url_backup
    (part_id, backup_url, original_path, backup_time, url_length, url_hash)
    VALUES (
        NEW.id, NEW.file, OLD.file, CURRENT_TIMESTAMP,
        LENGTH(NEW.file), SUBSTR(NEW.file, 1, 100)
    );
END""",
    # Layer 3: auto-restore from backup if URL removed
    """CREATE TRIGGER restore_stream_urls
AFTER UPDATE ON media_parts
FOR EACH ROW
WHEN (NEW.file NOT LIKE 'http://%' AND NEW.file NOT LIKE 'https://%')
     AND (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%')
     AND EXISTS (SELECT 1 FROM stream_url_backup WHERE part_id = NEW.id)
BEGIN
    UPDATE media_parts
    SET file = (SELECT backup_url FROM stream_url_backup WHERE part_id = NEW.id)
    WHERE id = NEW.id;
END""",
    # Layer 4: block URL truncation (>10 chars shorter)
    """CREATE TRIGGER protect_stream_urls_before
BEFORE UPDATE ON media_parts
FOR EACH ROW
WHEN (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%')
     AND (NEW.file LIKE 'http://%' OR NEW.file LIKE 'https://%')
     AND LENGTH(NEW.file) < LENGTH(OLD.file) - 10
BEGIN
    SELECT RAISE(IGNORE);
END""",
]

_PG_FUNCTIONS = [
    # Layer 1
    """CREATE OR REPLACE FUNCTION plex_strm_protect_strict() RETURNS trigger AS $$
BEGIN
    IF (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%')
       AND (NEW.file NOT LIKE 'http://%' AND NEW.file NOT LIKE 'https://%')
       AND NEW.file != OLD.file THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql""",
    # Layer 2
    """CREATE OR REPLACE FUNCTION plex_strm_backup_urls() RETURNS trigger AS $$
BEGIN
    IF (NEW.file LIKE 'http://%' OR NEW.file LIKE 'https://%')
       AND NEW.file != OLD.file THEN
        INSERT INTO stream_url_backup (part_id, backup_url, original_path, backup_time, url_length, url_hash)
        VALUES (NEW.id, NEW.file, OLD.file, NOW(), LENGTH(NEW.file), SUBSTR(NEW.file, 1, 100))
        ON CONFLICT (part_id) DO UPDATE SET
            backup_url = EXCLUDED.backup_url,
            original_path = EXCLUDED.original_path,
            backup_time = EXCLUDED.backup_time,
            url_length = EXCLUDED.url_length,
            url_hash = EXCLUDED.url_hash;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql""",
    # Layer 3
    """CREATE OR REPLACE FUNCTION plex_strm_restore_urls() RETURNS trigger AS $$
BEGIN
    IF (NEW.file NOT LIKE 'http://%' AND NEW.file NOT LIKE 'https://%')
       AND (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%') THEN
        UPDATE media_parts
        SET file = (SELECT backup_url FROM stream_url_backup WHERE part_id = NEW.id)
        WHERE id = NEW.id
        AND EXISTS (SELECT 1 FROM stream_url_backup WHERE part_id = NEW.id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql""",
    # Layer 4
    """CREATE OR REPLACE FUNCTION plex_strm_protect_truncation() RETURNS trigger AS $$
BEGIN
    IF (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%')
       AND (NEW.file LIKE 'http://%' OR NEW.file LIKE 'https://%')
       AND LENGTH(NEW.file) < LENGTH(OLD.file) - 10 THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql""",
]

_PG_TRIGGERS = [
    """CREATE TRIGGER protect_stream_urls_strict
BEFORE UPDATE ON media_parts
FOR EACH ROW EXECUTE FUNCTION plex_strm_protect_strict()""",
    """CREATE TRIGGER backup_stream_urls
AFTER UPDATE ON media_parts
FOR EACH ROW EXECUTE FUNCTION plex_strm_backup_urls()""",
    """CREATE TRIGGER restore_stream_urls
AFTER UPDATE ON media_parts
FOR EACH ROW EXECUTE FUNCTION plex_strm_restore_urls()""",
    """CREATE TRIGGER protect_stream_urls_before
BEFORE UPDATE ON media_parts
FOR EACH ROW EXECUTE FUNCTION plex_strm_protect_truncation()""",
]

_TRIGGER_NAMES = [
    "protect_stream_urls_strict",
    "backup_stream_urls",
    "restore_stream_urls",
    "protect_stream_urls_before",
]

_PG_FUNCTION_NAMES = [
    "plex_strm_protect_strict",
    "plex_strm_backup_urls",
    "plex_strm_restore_urls",
    "plex_strm_protect_truncation",
]


def _create_backup_table(db):
    cur = db.cursor()
    if db.is_pg:
        db.execute(cur, """
            CREATE TABLE IF NOT EXISTS stream_url_backup (
                part_id INTEGER PRIMARY KEY,
                backup_url TEXT NOT NULL,
                original_path TEXT,
                backup_time TIMESTAMP DEFAULT NOW(),
                url_length INTEGER,
                url_hash TEXT
            )
        """)
    else:
        db.execute(cur, """
            CREATE TABLE IF NOT EXISTS stream_url_backup (
                part_id INTEGER PRIMARY KEY,
                backup_url TEXT NOT NULL,
                original_path TEXT,
                backup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                url_length INTEGER,
                url_hash TEXT
            )
        """)
    cur.close()


def _drop_triggers(db):
    cur = db.cursor()
    for name in _TRIGGER_NAMES:
        if db.is_pg:
            db.execute(cur, f"DROP TRIGGER IF EXISTS {name} ON media_parts")
        else:
            db.execute(cur, f"DROP TRIGGER IF EXISTS {name}")
    if db.is_pg:
        for name in _PG_FUNCTION_NAMES:
            db.execute(cur, f"DROP FUNCTION IF EXISTS {name}() CASCADE")
    cur.close()


def cmd_protect(args):
    """Install 4-layer trigger protection."""
    db = connect_from_args(args)
    try:
        _create_backup_table(db)
        _drop_triggers(db)
        cur = db.cursor()
        if db.is_pg:
            for func_sql in _PG_FUNCTIONS:
                db.execute(cur, func_sql)
            for trig_sql in _PG_TRIGGERS:
                db.execute(cur, trig_sql)
        else:
            for trig_sql in _SQLITE_TRIGGERS:
                db.execute(cur, trig_sql)
        cur.close()
        # Backup existing HTTP URLs
        _backup_existing_urls(db)
        db.commit()
        log.info("4-layer protection installed")
    except Exception as e:
        db.rollback()
        log.error("Failed to install protection: %s", e)
        raise
    finally:
        db.close()


def cmd_unprotect(args):
    """Remove all protection triggers."""
    db = connect_from_args(args)
    try:
        _drop_triggers(db)
        db.commit()
        log.info("Protection triggers removed")
    except Exception as e:
        db.rollback()
        log.error("Failed to remove protection: %s", e)
        raise
    finally:
        db.close()


def _backup_existing_urls(db):
    """Backup all current HTTP URLs to stream_url_backup."""
    cur = db.cursor()
    db.execute(cur, "SELECT id, file FROM media_parts WHERE file LIKE 'http%'")
    rows = db.fetchall(cur)
    if not rows:
        cur.close()
        return
    for row in rows:
        if db.is_pg:
            db.execute(cur,
                "INSERT INTO stream_url_backup (part_id, backup_url, backup_time, url_length, url_hash) "
                "VALUES (%s, %s, NOW(), %s, %s) ON CONFLICT (part_id) DO UPDATE SET "
                "backup_url = EXCLUDED.backup_url, backup_time = EXCLUDED.backup_time, "
                "url_length = EXCLUDED.url_length, url_hash = EXCLUDED.url_hash",
                (row["id"], row["file"], len(row["file"]), row["file"][:100]))
        else:
            db.execute(cur,
                "INSERT OR REPLACE INTO stream_url_backup "
                "(part_id, backup_url, backup_time, url_length, url_hash) "
                "VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?)",
                (row["id"], row["file"], len(row["file"]), row["file"][:100]))
    cur.close()
    log.info("Backed up %d HTTP URLs", len(rows))


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def cmd_status(args):
    """Show protection status and URL counts."""
    db = connect_from_args(args)
    try:
        lib_ids = _resolve_library_ids(db, getattr(args, 'library', None))
        cur = db.cursor()
        # Count triggers
        if db.is_pg:
            db.execute(cur,
                "SELECT tgname FROM pg_trigger "
                "WHERE tgrelid = 'media_parts'::regclass "
                "AND (tgname LIKE '%protect%' OR tgname LIKE '%backup%' OR tgname LIKE '%restore%')")
        else:
            db.execute(cur,
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND (name LIKE '%protect%' OR name LIKE '%backup%' OR name LIKE '%restore%')")
        triggers = db.fetchall(cur)
        trigger_names = [t.get("tgname") or t.get("name") for t in triggers]

        if len(triggers) >= 4:
            print(f"Protection: ACTIVE (4 layers)")
        elif len(triggers) >= 3:
            print(f"Protection: PARTIAL ({len(triggers)} layers)")
        else:
            print(f"Protection: INACTIVE")
        for name in trigger_names:
            print(f"  - {name}")

        # Count HTTP URLs
        join, lib_where, lib_params = _library_join(db, lib_ids, "mp")
        ph = "%s" if db.is_pg else "?"
        db.execute(cur,
            f"SELECT COUNT(*) AS cnt FROM media_parts mp{join} WHERE mp.file LIKE {ph}{lib_where}",
            ("http%",) + lib_params if lib_params else ("http%",))
        row = db.fetchone(cur)
        print(f"HTTP URLs: {row['cnt']}")

        # Count .strm paths
        db.execute(cur,
            f"SELECT COUNT(*) AS cnt FROM media_parts mp{join} WHERE LOWER(mp.file) LIKE {ph}{lib_where}",
            ("%.strm",) + lib_params if lib_params else ("%.strm",))
        row = db.fetchone(cur)
        print(f"Pending .strm files: {row['cnt']}")

        # Backup count
        db.execute(cur,
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_name = 'stream_url_backup'" if db.is_pg else
            "SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type='table' AND name='stream_url_backup'")
        row = db.fetchone(cur)
        if row["cnt"] > 0:
            db.execute(cur, "SELECT COUNT(*) AS cnt FROM stream_url_backup")
            row = db.fetchone(cur)
            print(f"Backup entries: {row['cnt']}")
        else:
            print("Backup table: not created")

        cur.close()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# FFprobe analysis
# ---------------------------------------------------------------------------

def run_ffprobe(url, ffprobe_path="ffprobe", timeout=30, retries=2):
    """Run ffprobe on a URL and return parsed metadata dict, or None on failure.
    
    Retries up to `retries` times on timeout or transient errors.
    Does NOT retry on server errors (5XX, 4XX) — those are dead links.
    """
    cmd = [ffprobe_path, "-v", "error", "-print_format", "json",
           "-show_format", "-show_streams", url]
    url_id = url.rsplit("/", 1)[-1] if "/" in url else url[-30:]
    last_error = None

    for attempt in range(1 + retries):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    encoding="utf-8", errors="ignore", timeout=timeout)
            if result.returncode != 0:
                stderr_snip = (result.stderr or "")[:300].strip()
                last_error = f"exit code {result.returncode}: {stderr_snip}"
                # Don't retry on server errors — the link is dead
                if any(x in stderr_snip.lower() for x in ("server error", "server returned",
                        "403", "404", "410", "5xx", "forbidden", "not found")):
                    log.warning("FFprobe FAILED [%s]: %s", url_id, last_error)
                    return None
                # Retry on other errors (network glitch, etc.)
                if attempt < retries:
                    time.sleep(1 * (attempt + 1))
                    continue
                log.warning("FFprobe FAILED after %d attempts [%s]: %s", attempt + 1, url_id, last_error)
                return None
            data = json.loads(result.stdout)
            if attempt > 0:
                log.info("FFprobe succeeded on retry %d [%s]", attempt, url_id)
            return _parse_ffprobe(data)
        except subprocess.TimeoutExpired:
            last_error = f"timeout ({timeout}s)"
            if attempt < retries:
                time.sleep(1 * (attempt + 1))
                continue
            log.warning("FFprobe FAILED after %d attempts [%s]: %s", attempt + 1, url_id, last_error)
            return None
        except json.JSONDecodeError as e:
            last_error = f"JSON parse error: {e}"
            log.warning("FFprobe FAILED [%s]: %s", url_id, last_error)
            return None
        except FileNotFoundError:
            log.error("FFprobe binary not found: %s", ffprobe_path)
            return None

    return None


def _parse_ffprobe(data):
    """Parse ffprobe JSON output into Plex metadata dict.
    
    Collects ALL streams (video, audio, subtitle) for proper multi-language support.
    """
    meta = {}
    fmt = data.get("format", {})

    if "duration" in fmt:
        meta["duration"] = int(float(fmt["duration"]) * 1000)
    if "size" in fmt:
        meta["size"] = int(fmt["size"])
    if "bit_rate" in fmt:
        meta["bitrate"] = int(fmt["bit_rate"])
    if "format_name" in fmt:
        meta["container"] = fmt["format_name"].split(",")[0]

    video_streams = []
    audio_streams = []
    subtitle_streams = []

    for s in data.get("streams", []):
        ct = s.get("codec_type")
        if ct == "video":
            video_streams.append(s)
        elif ct == "audio":
            audio_streams.append(s)
        elif ct == "subtitle":
            subtitle_streams.append(s)

    # Store all streams for create_media_streams
    meta["_video_streams"] = video_streams
    meta["_audio_streams"] = audio_streams
    meta["_subtitle_streams"] = subtitle_streams

    # Primary video stream for media_items fields
    if video_streams:
        video_stream = video_streams[0]
        meta["width"] = video_stream.get("width", 1920)
        meta["height"] = video_stream.get("height", 1080)
        meta["video_codec"] = video_stream.get("codec_name", "h264")

        # Aspect ratio
        dar = video_stream.get("display_aspect_ratio", "")
        if ":" in dar:
            w, h = dar.split(":")
            try:
                meta["display_aspect_ratio"] = round(float(w) / float(h), 4)
            except (ValueError, ZeroDivisionError):
                pass

        # Frame rate
        rfr = video_stream.get("r_frame_rate", "")
        if "/" in rfr:
            num, den = rfr.split("/")
            try:
                meta["frames_per_second"] = round(float(num) / float(den), 3)
            except (ValueError, ZeroDivisionError):
                pass

        # Video profile
        profile = (video_stream.get("profile") or "").lower()
        if "baseline" in profile:
            meta["video_profile"] = "baseline"
        elif "high" in profile and "10" in profile:
            meta["video_profile"] = "high10"
        elif "high" in profile:
            meta["video_profile"] = "high"
        elif "main" in profile:
            meta["video_profile"] = "main"
        elif profile:
            meta["video_profile"] = profile

        # Color transfer
        trc = (video_stream.get("color_transfer") or "").lower()
        if "bt709" in trc or "709" in trc:
            meta["color_trc"] = "bt709"
        elif any(x in trc for x in ("bt601", "601", "smpte170m")):
            meta["color_trc"] = "bt601"
        elif "bt2020" in trc or "2020" in trc:
            meta["color_trc"] = "bt2020"

    # Primary audio stream for media_items fields
    if audio_streams:
        audio_stream = audio_streams[0]
        meta["audio_codec"] = audio_stream.get("codec_name", "aac")
        meta["audio_channels"] = audio_stream.get("channels", 2)

        a_profile = (audio_stream.get("profile") or "").lower()
        if "lc" in a_profile or "low complexity" in a_profile:
            meta["audio_profile"] = "lc"
        elif "he" in a_profile or "high efficiency" in a_profile:
            meta["audio_profile"] = "he"
        elif "main" in a_profile:
            meta["audio_profile"] = "main"
        elif a_profile:
            meta["audio_profile"] = a_profile

    return meta if meta else None


def _build_extra_data(meta):
    """Build Plex-format extra_data JSON from metadata."""
    extra = {}
    url_parts = []
    if "video_profile" in meta:
        extra["ma:videoProfile"] = meta["video_profile"]
        url_parts.append(f"ma%3AvideoProfile={meta['video_profile']}")
    if "audio_profile" in meta:
        extra["ma:audioProfile"] = meta["audio_profile"]
        url_parts.append(f"ma%3AaudioProfile={meta['audio_profile']}")
    if "height" in meta:
        extra["ma:height"] = str(meta["height"])
        url_parts.append(f"ma%3Aheight={meta['height']}")
    if "width" in meta:
        extra["ma:width"] = str(meta["width"])
        url_parts.append(f"ma%3Awidth={meta['width']}")
    if url_parts:
        extra["url"] = "&".join(url_parts)
    return json.dumps(extra) if extra else None


def update_media_item(db, media_item_id, meta):
    """UPDATE media_items with FFprobe metadata."""
    # Pop profile fields — they go into extra_data
    video_profile = meta.pop("video_profile", None)
    audio_profile = meta.pop("audio_profile", None)
    color_trc = meta.pop("color_trc", None)

    # Build extra_data
    profile_meta = {}
    if video_profile:
        profile_meta["video_profile"] = video_profile
    if audio_profile:
        profile_meta["audio_profile"] = audio_profile
    extra_data = _build_extra_data(profile_meta)
    if extra_data:
        meta["extra_data"] = extra_data

    # Build dynamic UPDATE
    # Skip internal keys (_video_streams etc.) and profile fields (already in extra_data)
    updatable = {k: v for k, v in meta.items()
                 if k in ("duration", "size", "bitrate", "container", "width", "height",
                          "frames_per_second", "display_aspect_ratio", "video_codec",
                          "audio_codec", "audio_channels", "extra_data")}
    if not updatable:
        return

    cur = db.cursor()
    if db.is_pg:
        sets = ", ".join(f"{k} = %s" for k in updatable)
        vals = list(updatable.values()) + [media_item_id]
        db.execute(cur, f"UPDATE media_items SET {sets} WHERE id = %s", tuple(vals))
    else:
        sets = ", ".join(f"{k} = ?" for k in updatable)
        vals = list(updatable.values()) + [media_item_id]
        db.execute(cur, f"UPDATE media_items SET {sets} WHERE id = ?", tuple(vals))
    cur.close()


def create_media_streams(db, media_item_id, meta):
    """Insert ALL video, audio, and subtitle stream entries from ffprobe data.
    
    Writes every stream with correct codec, language, channels, bitrate,
    default/forced flags — no hardcoded values.
    """
    cur = db.cursor()

    # Find media_part_id
    ph = "%s" if db.is_pg else "?"
    db.execute(cur, f"SELECT id FROM media_parts WHERE media_item_id = {ph} LIMIT 1",
               (media_item_id,))
    row = db.fetchone(cur)
    if not row:
        cur.close()
        return
    media_part_id = row["id"]

    # Delete existing streams (re-analyze: replace old incomplete data)
    db.execute(cur, f"DELETE FROM media_streams WHERE media_item_id = {ph}",
               (media_item_id,))

    now = int(time.time())
    idx_col = db.q("index")
    dflt_col = db.q("default")
    stream_index = 0

    # ── Video streams ────────────────────────────────────────────
    for vs in meta.get("_video_streams", []):
        codec = vs.get("codec_name")
        if not codec:
            continue

        lang = (vs.get("tags") or {}).get("language")
        title = (vs.get("tags") or {}).get("title")
        w = vs.get("width")
        h = vs.get("height")
        bitrate = vs.get("bit_rate")
        if bitrate:
            bitrate = int(bitrate)
        elif meta.get("bitrate"):
            bitrate = meta["bitrate"]

        disp = vs.get("disposition", {})
        is_default = 1 if disp.get("default") else 0
        is_forced = 1 if disp.get("forced") else 0

        # Build extra_data
        extra_parts = {}
        if h:
            extra_parts["ma:height"] = str(h)
        if w:
            extra_parts["ma:width"] = str(w)

        profile = (vs.get("profile") or "").lower()
        plex_profile = None
        if "baseline" in profile:
            plex_profile = "baseline"
        elif "high" in profile and "10" in profile:
            plex_profile = "high10"
        elif "high" in profile:
            plex_profile = "high"
        elif "main" in profile:
            plex_profile = "main"
        elif profile:
            plex_profile = profile
        if plex_profile:
            extra_parts["ma:videoProfile"] = plex_profile

        url_parts = [f"ma%3A{k.split(':')[1]}={v}" for k, v in extra_parts.items()]
        if url_parts:
            extra_parts["url"] = "&".join(url_parts)
        extra_data = json.dumps(extra_parts) if extra_parts else None

        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, language, created_at, updated_at,
             {idx_col}, media_part_id, bitrate, channels, url_index,
             {dflt_col}, forced, extra_data)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        """, (1, media_item_id, codec, lang, now, now, stream_index,
              media_part_id, bitrate, None, None, is_default, is_forced, extra_data))
        stream_index += 1

    # ── Audio streams ────────────────────────────────────────────
    for aus in meta.get("_audio_streams", []):
        codec = aus.get("codec_name")
        if not codec:
            continue

        lang = (aus.get("tags") or {}).get("language")
        title = (aus.get("tags") or {}).get("title")
        channels = aus.get("channels")
        bitrate = aus.get("bit_rate")
        if bitrate:
            bitrate = int(bitrate)

        disp = aus.get("disposition", {})
        is_default = 1 if disp.get("default") else 0
        is_forced = 1 if disp.get("forced") else 0

        # Audio profile extra_data
        a_profile = (aus.get("profile") or "").lower()
        plex_profile = None
        if "lc" in a_profile or "low complexity" in a_profile:
            plex_profile = "lc"
        elif "he" in a_profile or "high efficiency" in a_profile:
            plex_profile = "he"
        elif "main" in a_profile:
            plex_profile = "main"
        elif "dts" in a_profile:
            plex_profile = a_profile

        extra_parts = {}
        if plex_profile:
            extra_parts["ma:audioProfile"] = plex_profile
        if channels:
            extra_parts["ma:channels"] = str(channels)
        url_parts = [f"ma%3A{k.split(':')[1]}={v}" for k, v in extra_parts.items()]
        if url_parts:
            extra_parts["url"] = "&".join(url_parts)
        extra_data = json.dumps(extra_parts) if extra_parts else None

        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, language, created_at, updated_at,
             {idx_col}, media_part_id, bitrate, channels, url_index,
             {dflt_col}, forced, extra_data)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        """, (2, media_item_id, codec, lang, now, now, stream_index,
              media_part_id, bitrate, channels, None, is_default, is_forced, extra_data))
        stream_index += 1

    # ── Subtitle streams ─────────────────────────────────────────
    for ss in meta.get("_subtitle_streams", []):
        codec = ss.get("codec_name")
        if not codec:
            continue

        lang = (ss.get("tags") or {}).get("language")
        title = (ss.get("tags") or {}).get("title")

        disp = ss.get("disposition", {})
        is_default = 1 if disp.get("default") else 0
        is_forced = 1 if disp.get("forced") else 0
        is_hearing_impaired = 1 if disp.get("hearing_impaired") else 0

        extra_parts = {}
        if is_hearing_impaired:
            extra_parts["ma:hearingImpaired"] = "1"
        if title:
            extra_parts["ma:title"] = title
        url_parts = [f"ma%3A{k.split(':')[1]}={v}" for k, v in extra_parts.items()]
        if url_parts:
            extra_parts["url"] = "&".join(url_parts)
        extra_data = json.dumps(extra_parts) if extra_parts else None

        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, language, created_at, updated_at,
             {idx_col}, media_part_id, bitrate, channels, url_index,
             {dflt_col}, forced, extra_data)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        """, (3, media_item_id, codec, lang, now, now, stream_index,
              media_part_id, None, None, None, is_default, is_forced, extra_data))
        stream_index += 1

    cur.close()


# ---------------------------------------------------------------------------
# OpenSubtitles
# ---------------------------------------------------------------------------

def _clean_title_for_search(title):
    """Clean a title for OpenSubtitles search.

    Strips CJK characters, Cyrillic, website spam, bracketed junk,
    and tries to extract the English portion of the title.
    """
    if not title:
        return ""
    # Remove bracketed content: 【...】, [...], (...)  with non-Latin chars
    title = re.sub(r'[【\[][^】\]]*[】\]]', ' ', title)
    # Remove website spam patterns
    title = re.sub(r'(?i)\b(www\s*\.?\s*\w+\s*\.?\s*(?:com|net|org|info))\b', ' ', title)
    # Remove CJK characters (Chinese/Japanese/Korean)
    title = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]+', ' ', title)
    # Remove Cyrillic characters
    title = re.sub(r'[\u0400-\u04ff]+', ' ', title)
    # Remove Arabic characters
    title = re.sub(r'[\u0600-\u06ff]+', ' ', title)
    # Remove year in parentheses (searched separately)
    title = re.sub(r'\(\d{4}\)', ' ', title)
    # Remove common torrent/release junk
    title = re.sub(r'(?i)\b(bluray|bdrip|dvdrip|webrip|web-dl|hdtv|hdrip|x264|x265|h264|h265|'
                   r'aac|ac3|dts|720p|1080p|2160p|4k|remux|remastered|extended|uncut)\b', ' ', title)
    # Remove special characters but keep apostrophes and hyphens within words
    title = re.sub(r"[^a-zA-Z0-9'\-\s]", ' ', title)
    # Collapse whitespace
    title = ' '.join(title.split()).strip()
    # If nothing useful remains, return empty
    if len(title) < 2:
        return ""
    return title


def _validate_imdb_id(imdb_id):
    """Return imdb_id if it looks valid (tt followed by digits), else None."""
    if not imdb_id:
        return None
    if not re.match(r'^tt\d+$', imdb_id):
        return None
    return imdb_id


def _subtitle_search(api_key, token, imdb_id=None, tmdb_id=None, title=None, year=None, lang="en"):
    """Search OpenSubtitles API. Returns list of subtitle dicts with match validation.

    Search priority: IMDb ID > TMDB ID > title+year.
    When searching by title, validates that the returned feature
    matches our year to avoid downloading subtitles for the wrong movie.
    """
    import requests
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "plex-strm/1.0",
    }
    params = {"languages": lang}
    searched_by = None

    if imdb_id:
        params["imdb_id"] = imdb_id.replace("tt", "")
        searched_by = "imdb"
    elif tmdb_id:
        params["tmdb_id"] = str(tmdb_id)
        searched_by = "tmdb"
    elif title:
        clean = _clean_title_for_search(title)
        if not clean:
            log.debug("OpenSubtitles: title '%s' cleaned to empty, skipping", title)
            return []
        params["query"] = clean
        if year:
            params["year"] = str(year)
        searched_by = "title"
    else:
        return []

    resp = requests.get("https://api.opensubtitles.com/api/v1/subtitles",
                        headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        log.debug("OpenSubtitles search failed: %d", resp.status_code)
        return []

    results = []
    for item in resp.json().get("data", []):
        attrs = item.get("attributes", {})
        files = attrs.get("files", [])
        if not files:
            continue

        # Extract feature details for validation
        feat = attrs.get("feature_details", {})
        feat_year = feat.get("year")
        feat_imdb = feat.get("imdb_id")

        # Validate match when searching by title — year must match within ±1
        if searched_by == "title" and year and feat_year:
            if abs(int(feat_year) - int(year)) > 1:
                continue  # Wrong movie, skip

        results.append({
            "id": str(files[0].get("file_id", "")),
            "name": attrs.get("release", "subtitle.srt"),
            "downloads": attrs.get("download_count", 0),
            "feature_imdb": feat_imdb,
            "feature_title": feat.get("title"),
            "feature_year": feat_year,
        })

    results.sort(key=lambda x: x["downloads"], reverse=True)
    return results


def _subtitle_download(api_key, token, file_id, output_path):
    """Download a subtitle file from OpenSubtitles."""
    import requests
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "plex-strm/1.0",
    }
    # Get download link
    resp = requests.post("https://api.opensubtitles.com/api/v1/download",
                         headers=headers,
                         json={"file_id": int(file_id), "sub_format": "srt"},
                         timeout=30)
    if resp.status_code != 200:
        return False
    link = resp.json().get("link")
    if not link:
        return False

    # Download file
    resp = requests.get(link, timeout=60)
    if resp.status_code != 200:
        return False

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(resp.content)
    return os.path.getsize(output_path) > 100


def _opensub_login(api_key, username, password):
    """Login to OpenSubtitles, return token or None."""
    import requests
    resp = requests.post("https://api.opensubtitles.com/api/v1/login",
                         headers={"Api-Key": api_key, "Content-Type": "application/json",
                                  "User-Agent": "plex-strm/1.0"},
                         json={"username": username, "password": password},
                         timeout=30)
    if resp.status_code == 200:
        return resp.json().get("token")
    return None


def _get_metadata_for_subtitle(db, media_item_id):
    """Get title, year, imdb_id for a media item."""
    cur = db.cursor()
    # media_item -> metadata_item
    if db.is_pg:
        db.execute(cur, "SELECT metadata_item_id FROM media_items WHERE id = %s LIMIT 1",
                   (media_item_id,))
    else:
        db.execute(cur, "SELECT metadata_item_id FROM media_items WHERE id = ? LIMIT 1",
                   (media_item_id,))
    row = db.fetchone(cur)
    if not row:
        cur.close()
        return None
    mid = row["metadata_item_id"]

    # Get title, year
    if db.is_pg:
        db.execute(cur, "SELECT title, year, guid FROM metadata_items WHERE id = %s", (mid,))
    else:
        db.execute(cur, "SELECT title, year, guid FROM metadata_items WHERE id = ?", (mid,))
    row = db.fetchone(cur)
    if not row:
        cur.close()
        return None

    info = {"title": row["title"], "year": row["year"], "imdb_id": None,
            "tmdb_id": None, "tvdb_id": None,
            "metadata_item_id": mid, "media_item_id": media_item_id}

    # Look up external IDs (IMDb, TMDB, TVDB).
    # Different Plex versions store these in different tables.
    # Each attempt uses a savepoint so a missing table won't abort the PG transaction.
    ph = "%s" if db.is_pg else "?"
    like_esc = "%%" if db.is_pg else "%"

    ext_id_queries = [
        # 1. tags + taggings (PostgreSQL / plex-postgresql)
        (f"SELECT t.tag FROM taggings tg "
         f"JOIN tags t ON tg.tag_id = t.id "
         f"WHERE tg.metadata_item_id = {ph} "
         f"AND (t.tag LIKE 'imdb://{like_esc}' "
         f"  OR t.tag LIKE 'tmdb://{like_esc}' "
         f"  OR t.tag LIKE 'tvdb://{like_esc}')"),
        # 2. guids table (newer Plex SQLite versions)
        (f"SELECT guid AS tag FROM guids "
         f"WHERE metadata_item_id = {ph} "
         f"AND (guid LIKE 'imdb://{like_esc}' "
         f"  OR guid LIKE 'tmdb://{like_esc}' "
         f"  OR guid LIKE 'tvdb://{like_esc}')"),
    ]

    for ext_sql in ext_id_queries:
        if info["imdb_id"] and info["tmdb_id"]:
            break  # Got everything we need
        try:
            if db.is_pg:
                db.execute(cur, "SAVEPOINT ext_id_lookup")
            db.execute(cur, ext_sql, (mid,))
            for grow in db.fetchall(cur):
                tag = grow.get("tag", "")
                if tag.startswith("imdb://") and not info["imdb_id"]:
                    imdb = tag.replace("imdb://", "")
                    if not imdb.startswith("tt"):
                        imdb = "tt" + imdb
                    info["imdb_id"] = imdb
                elif tag.startswith("tmdb://") and not info["tmdb_id"]:
                    info["tmdb_id"] = tag.replace("tmdb://", "")
                elif tag.startswith("tvdb://") and not info["tvdb_id"]:
                    info["tvdb_id"] = tag.replace("tvdb://", "")
            if db.is_pg:
                db.execute(cur, "RELEASE SAVEPOINT ext_id_lookup")
        except Exception:
            if db.is_pg:
                try:
                    db.execute(cur, "ROLLBACK TO SAVEPOINT ext_id_lookup")
                except Exception:
                    pass

    # Fallback: parse guid column on metadata_items directly
    if not info["imdb_id"]:
        match = re.search(r"(tt\d+)", row.get("guid", ""))
        if match:
            info["imdb_id"] = match.group(1)

    # Convert TVDB -> TMDB via TMDB API when we have no IMDb and no TMDB
    if not info["imdb_id"] and not info["tmdb_id"] and info["tvdb_id"]:
        tmdb_api_key = os.environ.get("TMDB_API_KEY")
        if tmdb_api_key:
            converted = _tvdb_to_tmdb(info["tvdb_id"], tmdb_api_key)
            if converted:
                info["tmdb_id"] = converted

    cur.close()
    return info


def _tvdb_to_tmdb(tvdb_id, tmdb_api_key):
    """Convert a TVDB ID to a TMDB ID via the TMDB API. Returns TMDB ID string or None."""
    import requests
    # Try TV show first (most TVDB IDs are shows)
    for media_type in ("tv", "movie"):
        try:
            resp = requests.get(
                f"https://api.themoviedb.org/3/find/{tvdb_id}",
                params={"api_key": tmdb_api_key, "external_source": "tvdb_id"},
                timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            results = data.get(f"{media_type}_results", [])
            if results:
                tmdb_id = str(results[0]["id"])
                log.debug("TVDB %s -> TMDB %s (%s)", tvdb_id, tmdb_id, media_type)
                return tmdb_id
        except Exception:
            pass
    return None


def _get_existing_subtitle_langs(db, media_item_id):
    """Return set of language codes that already have subtitle streams in DB for this media item."""
    cur = db.cursor()
    ph = "%s" if db.is_pg else "?"
    db.execute(cur,
        f"SELECT DISTINCT language FROM media_streams "
        f"WHERE media_item_id = {ph} AND stream_type_id = 3 AND language IS NOT NULL",
        (media_item_id,))
    rows = db.fetchall(cur)
    cur.close()
    return {r["language"] for r in rows}


def _register_subtitle_in_db(db, media_item_id, subtitle_path, language):
    """Register a subtitle file as media_streams entry (stream_type_id=3)."""
    cur = db.cursor()

    # Find media_part_id
    if db.is_pg:
        db.execute(cur, "SELECT id FROM media_parts WHERE media_item_id = %s LIMIT 1",
                   (media_item_id,))
    else:
        db.execute(cur, "SELECT id FROM media_parts WHERE media_item_id = ? LIMIT 1",
                   (media_item_id,))
    row = db.fetchone(cur)
    if not row:
        cur.close()
        return
    media_part_id = row["id"]

    # Build file:// URI
    norm_path = subtitle_path.replace("\\", "/")
    if len(norm_path) > 2 and norm_path[1:3] == ":/":
        file_uri = "file:///" + norm_path
    else:
        file_uri = "file://" + norm_path

    now = int(time.time())
    idx = db.q("index")
    dflt = db.q("default")
    extra = '{"ma:format":"srt","url":"ma%3Aformat=srt"}'

    if db.is_pg:
        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, url, codec, language,
             created_at, updated_at, {idx}, media_part_id,
             channels, bitrate, url_index, {dflt}, forced, extra_data)
            VALUES (3, %s, %s, 'srt', %s, %s, %s, NULL, %s, NULL, NULL, NULL, 0, 0, %s)
        """, (media_item_id, file_uri, language, now, now, media_part_id, extra))
    else:
        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, url, codec, language,
             created_at, updated_at, {idx}, media_part_id,
             channels, bitrate, url_index, {dflt}, forced, extra_data)
            VALUES (3, ?, ?, 'srt', ?, ?, ?, NULL, ?, NULL, NULL, NULL, 0, 0, ?)
        """, (media_item_id, file_uri, language, now, now, media_part_id, extra))

    cur.close()


def download_subtitles(db, media_item_ids, mode="missing"):
    """Download subtitles for given media items via OpenSubtitles API.

    Args:
        db: PlexDB instance
        media_item_ids: list of media_item IDs to process
        mode: "missing" = only download if language not yet in DB,
              "always" = download even if subtitle track exists
    """
    api_key = os.environ.get("OPENSUB_API_KEY")
    username = os.environ.get("OPENSUB_USER")
    password = os.environ.get("OPENSUB_PASS")
    if not all([api_key, username, password]):
        log.warning("Subtitle download requires OPENSUB_API_KEY, OPENSUB_USER, OPENSUB_PASS")
        return

    token = _opensub_login(api_key, username, password)
    if not token:
        log.error("OpenSubtitles login failed")
        return

    langs = os.environ.get("SUBTITLE_LANGS", "en").split(",")
    sub_dir = os.environ.get("SUBTITLE_DIR", "./subtitles")
    os.makedirs(sub_dir, exist_ok=True)

    success = 0
    skipped = 0
    for mid in media_item_ids:
        info = _get_metadata_for_subtitle(db, mid)
        if not info:
            continue
        title = info["title"]
        safe_title = re.sub(r'[<>:"/\\|?*]', "_", title)

        # Validate IMDb ID
        imdb_id = _validate_imdb_id(info.get("imdb_id"))

        # In "missing" mode, check which subtitle languages already exist
        existing_langs = set()
        if mode == "missing":
            existing_langs = _get_existing_subtitle_langs(db, mid)

        for lang in langs:
            lang = lang.strip()

            # Skip if this language already has a subtitle track in DB
            if mode == "missing" and lang in existing_langs:
                skipped += 1
                continue

            # Search — prefer IMDb ID > TMDB ID > cleaned title
            tmdb_id = info.get("tmdb_id")
            if imdb_id:
                results = _subtitle_search(api_key, token, imdb_id=imdb_id, lang=lang)
            elif tmdb_id:
                results = _subtitle_search(api_key, token, tmdb_id=tmdb_id, lang=lang)
            else:
                results = _subtitle_search(api_key, token, title=title,
                                           year=info["year"], lang=lang)
            if not results:
                continue

            fname = f"{safe_title}.{lang}.srt"
            fpath = os.path.join(sub_dir, fname)
            if os.path.exists(fpath) and mode == "missing":
                log.debug("Subtitle already exists on disk: %s", fpath)
                _register_subtitle_in_db(db, mid, fpath, lang)
                skipped += 1
                continue

            if _subtitle_download(api_key, token, results[0]["id"], fpath):
                _register_subtitle_in_db(db, mid, fpath, lang)
                log.info("Subtitle: %s", fname)
                success += 1
                time.sleep(0.3)  # rate limit

    log.info("Subtitles: %d downloaded, %d skipped (mode=%s)", success, skipped, mode)


# ---------------------------------------------------------------------------
# Update (main command)
# ---------------------------------------------------------------------------

def cmd_update(args):
    """Read .strm files, inject URLs, run FFprobe, optionally download subtitles."""
    db = connect_from_args(args)

    try:
        backup_database(args, db)

        # Install protection if requested
        if args.protect:
            _create_backup_table(db)
            _drop_triggers(db)
            cur = db.cursor()
            if db.is_pg:
                for sql in _PG_FUNCTIONS:
                    db.execute(cur, sql)
                for sql in _PG_TRIGGERS:
                    db.execute(cur, sql)
            else:
                for sql in _SQLITE_TRIGGERS:
                    db.execute(cur, sql)
            cur.close()
            log.info("Protection triggers installed")

        # Resolve library filter
        lib_ids = _resolve_library_ids(db, getattr(args, 'library', None))

        # Find .strm paths
        cur = db.cursor()
        join, lib_where, lib_params = _library_join(db, lib_ids, "mp")
        ph = "%s" if db.is_pg else "?"
        sql = (f"SELECT mp.id, mp.media_item_id, mp.file FROM media_parts mp"
               f"{join}"
               f" WHERE mp.file IS NOT NULL AND mp.file != ''"
               f" AND LOWER(mp.file) LIKE {ph}{lib_where}")
        params = ("%.strm",) + lib_params if lib_params else ("%.strm",)
        db.execute(cur, sql, params)
        strm_rows = db.fetchall(cur)
        cur.close()

        # Replace .strm paths with direct URLs
        url_mapping = {}  # media_item_id -> url
        updated = 0

        if not strm_rows:
            log.info("No .strm files found in database (all already converted)")
        else:
            log.info("Found %d .strm entries", len(strm_rows))

        cur = db.cursor()

        for row in strm_rows:
            strm_path = row["file"]
            if not os.path.isfile(strm_path):
                log.debug("File not found: %s", strm_path)
                continue

            try:
                with open(strm_path, "r", encoding="utf-8") as f:
                    direct_url = f.read().strip()
            except Exception as e:
                log.debug("Cannot read %s: %s", strm_path, e)
                continue

            if not direct_url.startswith("http"):
                log.debug("Not a URL in %s: %s", strm_path, direct_url[:80])
                continue

            # Rewrite base URL if configured
            base_url = getattr(args, 'base_url', None) or os.environ.get("STRM_BASE_URL")
            if base_url:
                from urllib.parse import urlparse
                old = urlparse(direct_url)
                new = urlparse(base_url)
                direct_url = direct_url.replace(f"{old.scheme}://{old.netloc}", f"{new.scheme}://{new.netloc}", 1)

            if db.is_pg:
                db.execute(cur, "UPDATE media_parts SET file = %s WHERE id = %s",
                           (direct_url, row["id"]))
            else:
                db.execute(cur, "UPDATE media_parts SET file = ? WHERE id = ?",
                           (direct_url, row["id"]))

            url_mapping[row["media_item_id"]] = direct_url
            updated += 1

            if updated % 50 == 0:
                log.info("Updated %d/%d URLs...", updated, len(strm_rows))

        cur.close()
        db.commit()
        log.info("Replaced %d .strm paths with direct URLs", updated)

        # Backup URLs
        if args.protect and url_mapping:
            _backup_existing_urls(db)
            db.commit()

        # Also find existing HTTP URLs missing media_streams (e.g. from a previous crashed run)
        cur = db.cursor()
        ph = "%s" if db.is_pg else "?"
        join, lib_where, lib_params = _library_join(db, lib_ids, "mp")
        db.execute(cur,
            f"SELECT mp.media_item_id, mp.file FROM media_parts mp"
            f"{join}"
            f" WHERE mp.file LIKE {ph}{lib_where}"
            f" AND mp.media_item_id NOT IN ("
            f"   SELECT DISTINCT media_item_id FROM media_streams"
            f" )",
            ("http%",) + lib_params if lib_params else ("http%",))
        missing = {r["media_item_id"]: r["file"] for r in db.fetchall(cur)}
        cur.close()
        if missing:
            log.info("Found %d HTTP URLs missing media_streams, adding to FFprobe queue", len(missing))
            url_mapping.update(missing)

        # --reanalyze: re-probe items with incomplete stream data (≤ max_streams)
        reanalyze_max = getattr(args, 'reanalyze', None)
        if reanalyze_max is not None:
            cur = db.cursor()
            ph = "%s" if db.is_pg else "?"
            join, lib_where, lib_params = _library_join(db, lib_ids, "mp")
            if lib_params:
                reanalyze_params = ("http%",) + tuple(lib_params) + (reanalyze_max,)
            else:
                reanalyze_params = ("http%", reanalyze_max)
            db.execute(cur,
                f"SELECT mp.media_item_id, mp.file FROM media_parts mp"
                f"{join}"
                f" WHERE mp.file LIKE {ph}{lib_where}"
                f" AND (SELECT COUNT(*) FROM media_streams ms"
                f"      WHERE ms.media_item_id = mp.media_item_id) <= {ph}",
                reanalyze_params)
            reanalyze_rows = {r["media_item_id"]: r["file"] for r in db.fetchall(cur)}
            cur.close()
            # Only add items not already in url_mapping
            new_reanalyze = {k: v for k, v in reanalyze_rows.items() if k not in url_mapping}
            if new_reanalyze:
                log.info("Reanalyze: found %d items with <= %d streams, adding to FFprobe queue",
                         len(new_reanalyze), reanalyze_max)
                url_mapping.update(new_reanalyze)

        # FFprobe analysis
        if url_mapping:
            ffprobe_path = args.ffprobe or os.environ.get("FFPROBE_PATH", "ffprobe")
            workers = args.workers or int(os.environ.get("FFPROBE_WORKERS", "4"))
            timeout = args.timeout or int(os.environ.get("FFPROBE_TIMEOUT", "30"))
            retries = args.retries if hasattr(args, 'retries') and args.retries is not None else 2

            log.info("Running FFprobe analysis on %d URLs (%d workers, %d retries)...",
                     len(url_mapping), workers, retries)

            analyzed = 0
            failed = 0
            failed_items = []  # (media_item_id, url) for failure log
            t0 = time.time()

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(run_ffprobe, url, ffprobe_path, timeout, retries): mid
                    for mid, url in url_mapping.items()
                }
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        meta = future.result()
                    except Exception as e:
                        log.warning("FFprobe exception for media_item_id %d: %s", mid, e)
                        meta = None

                    if meta:
                        update_media_item(db, mid, dict(meta))
                        create_media_streams(db, mid, dict(meta))
                        analyzed += 1
                    else:
                        failed += 1
                        failed_items.append((mid, url_mapping[mid]))

                    total = analyzed + failed
                    if total % 10 == 0:
                        db.commit()
                    if total % 100 == 0:
                        elapsed = time.time() - t0
                        rate = total / elapsed if elapsed > 0 else 0
                        eta_s = (len(url_mapping) - total) / rate if rate > 0 else 0
                        eta_m = int(eta_s / 60)
                        log.info("FFprobe: %d/%d (%.1f/s, %d ok, %d failed, ETA ~%dm)",
                                 total, len(url_mapping), rate, analyzed, failed, eta_m)

            db.commit()
            elapsed = time.time() - t0
            log.info("FFprobe done: %d analyzed, %d failed (%.1fs)", analyzed, failed, elapsed)

            # Write failed items to a separate log file for future retry
            if failed_items:
                fail_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffprobe_failures.log")
                with open(fail_log, "w", encoding="utf-8") as f:
                    f.write(f"# FFprobe failures: {len(failed_items)} items\n")
                    f.write(f"# Generated: {datetime.now().isoformat()}\n")
                    f.write(f"# Total: {analyzed + failed}, OK: {analyzed}, Failed: {failed}\n\n")
                    for mid, url in failed_items:
                        url_id = url.rsplit("/", 1)[-1] if "/" in url else url[-30:]
                        f.write(f"{mid}\t{url_id}\t{url}\n")
                log.info("Failed items written to %s", fail_log)

        # Subtitles
        if args.subtitles and url_mapping:
            sub_mode = getattr(args, 'subtitle_mode', 'missing') or 'missing'
            log.info("Downloading subtitles for %d items (mode=%s)...", len(url_mapping), sub_mode)
            download_subtitles(db, list(url_mapping.keys()), mode=sub_mode)
            db.commit()

    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        log.error("Update failed: %s", e)
        raise
    finally:
        try:
            db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Revert
# ---------------------------------------------------------------------------

def cmd_revert(args):
    """Revert URLs back to original .strm paths from backup."""
    db = connect_from_args(args)
    try:
        cur = db.cursor()
        # Check backup table exists
        if db.is_pg:
            db.execute(cur,
                "SELECT COUNT(*) AS cnt FROM information_schema.tables "
                "WHERE table_name = 'stream_url_backup'")
        else:
            db.execute(cur,
                "SELECT COUNT(*) AS cnt FROM sqlite_master "
                "WHERE type='table' AND name='stream_url_backup'")
        if db.fetchone(cur)["cnt"] == 0:
            log.error("No backup table found — cannot revert")
            cur.close()
            db.close()
            return

        db.execute(cur,
            "SELECT part_id, original_path FROM stream_url_backup "
            "WHERE original_path IS NOT NULL")
        rows = db.fetchall(cur)
        if not rows:
            log.info("No entries with original paths to revert")
            cur.close()
            db.close()
            return

        reverted = 0
        for row in rows:
            if db.is_pg:
                # Temporarily disable triggers for revert
                db.execute(cur, "UPDATE media_parts SET file = %s WHERE id = %s",
                           (row["original_path"], row["part_id"]))
            else:
                db.execute(cur, "UPDATE media_parts SET file = ? WHERE id = ?",
                           (row["original_path"], row["part_id"]))
            reverted += 1

        db.commit()
        log.info("Reverted %d URLs to original .strm paths", reverted)
        cur.close()
    except Exception as e:
        db.rollback()
        log.error("Revert failed: %s", e)
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="plex-strm",
        description="Inject streaming URLs into Plex database for Direct Play",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")

    # Global database options
    db_group = parser.add_argument_group("database")
    db_group.add_argument("--db", metavar="PATH", help="path to Plex SQLite database")
    db_group.add_argument("--pg", action="store_true",
                          help="use PostgreSQL (configure via PLEX_PG_* env vars)")

    parser.add_argument("--library", metavar="NAME", action="append",
                        help="limit to specific library section(s) by name (repeatable)")

    sub = parser.add_subparsers(dest="command", required=True)

    # update
    p_update = sub.add_parser("update", help="inject URLs from .strm files + FFprobe analysis")
    p_update.add_argument("--base-url", metavar="URL",
                          help="rewrite STRM base URL (e.g. http://localhost:9091 -> https://plex.example.com)")
    p_update.add_argument("--protect", action="store_true",
                          help="install 4-layer trigger protection")
    p_update.add_argument("--subtitles", action="store_true",
                          help="download subtitles via OpenSubtitles API")
    p_update.add_argument("--subtitle-mode", choices=["missing", "always"],
                          default="missing",
                          help="missing = only download if language not yet in DB (default), "
                               "always = download even if subtitle track already exists")
    p_update.add_argument("--ffprobe", metavar="PATH", help="path to ffprobe binary")
    p_update.add_argument("--workers", type=int, default=4,
                          help="FFprobe parallel workers (default: 4)")
    p_update.add_argument("--timeout", type=int, default=30,
                          help="FFprobe timeout per URL in seconds (default: 30)")
    p_update.add_argument("--backup-dir", metavar="DIR",
                           help="directory for database backups")
    p_update.add_argument("--reanalyze", metavar="N", type=int, default=None,
                           help="re-analyze items with <= N streams (e.g. --reanalyze 2 for incomplete)")
    p_update.add_argument("--retries", type=int, default=2,
                           help="number of FFprobe retries per URL on failure (default: 2)")

    # status
    sub.add_parser("status", help="show protection status and URL counts")

    # protect
    sub.add_parser("protect", help="install 4-layer trigger protection")

    # unprotect
    sub.add_parser("unprotect", help="remove all protection triggers")

    # revert
    sub.add_parser("revert", help="revert URLs to original .strm paths")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    commands = {
        "update": cmd_update,
        "status": cmd_status,
        "protect": cmd_protect,
        "unprotect": cmd_unprotect,
        "revert": cmd_revert,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
