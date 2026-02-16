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

def run_ffprobe(url, ffprobe_path="ffprobe", timeout=30):
    """Run ffprobe on a URL and return parsed metadata dict, or None on failure."""
    cmd = [ffprobe_path, "-v", "quiet", "-print_format", "json",
           "-show_format", "-show_streams", url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                encoding="utf-8", errors="ignore", timeout=timeout)
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
        log.debug("FFprobe failed for %s: %s", url, e)
        return None

    return _parse_ffprobe(data)


def _parse_ffprobe(data):
    """Parse ffprobe JSON output into Plex metadata dict."""
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

    video_stream = None
    audio_stream = None
    for s in data.get("streams", []):
        ct = s.get("codec_type")
        if ct == "video" and video_stream is None:
            video_stream = s
        elif ct == "audio" and audio_stream is None:
            audio_stream = s

    if video_stream:
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

    if audio_stream:
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
    # width/height are stored in media_items (not media_streams), but must NOT
    # also appear in extra_data to avoid duplicate XML attributes in Plex output.
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
    """Insert video and audio stream entries for Direct Play."""
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

    # Check if streams already exist
    if db.is_pg:
        db.execute(cur, "SELECT COUNT(*) AS cnt FROM media_streams WHERE media_item_id = %s",
                   (media_item_id,))
    else:
        db.execute(cur, "SELECT COUNT(*) AS cnt FROM media_streams WHERE media_item_id = ?",
                   (media_item_id,))
    if db.fetchone(cur)["cnt"] > 0:
        cur.close()
        return

    now = int(time.time())
    idx = db.q("index")
    dflt = db.q("default")

    # Video stream extra_data
    h = meta.get("height", 1080)
    w = meta.get("width", 1920)
    v_extra = json.dumps({
        "ma:height": str(h), "ma:width": str(w),
        "url": f"ma%3Aheight={h}&ma%3Awidth={w}"
    })

    # Insert video stream (stream_type_id=1)
    video_codec = meta.get("video_codec", "h264")
    video_bitrate = meta.get("bitrate", 5000000)
    if db.is_pg:
        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, created_at, updated_at,
             {idx}, media_part_id, bitrate, url_index, {dflt}, forced, extra_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (1, media_item_id, video_codec, now, now, 0, media_part_id,
              video_bitrate, None, 0, 0, v_extra))
    else:
        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, created_at, updated_at,
             {idx}, media_part_id, bitrate, url_index, {dflt}, forced, extra_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (1, media_item_id, video_codec, now, now, 0, media_part_id,
              video_bitrate, None, 0, 0, v_extra))

    # Insert audio stream (stream_type_id=2)
    audio_codec = meta.get("audio_codec", "aac")
    audio_channels = meta.get("audio_channels", 2)
    if db.is_pg:
        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, language, created_at, updated_at,
             {idx}, media_part_id, channels, url_index, {dflt}, forced)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (2, media_item_id, audio_codec, "en", now, now, 1, media_part_id,
              audio_channels, None, 0, 0))
    else:
        db.execute(cur, f"""
            INSERT INTO media_streams
            (stream_type_id, media_item_id, codec, language, created_at, updated_at,
             {idx}, media_part_id, channels, url_index, {dflt}, forced)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (2, media_item_id, audio_codec, "en", now, now, 1, media_part_id,
              audio_channels, None, 0, 0))

    cur.close()


# ---------------------------------------------------------------------------
# OpenSubtitles
# ---------------------------------------------------------------------------

def _subtitle_search(api_key, token, imdb_id=None, title=None, year=None, lang="en"):
    """Search OpenSubtitles API. Returns list of subtitle dicts."""
    import requests
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "plex-strm/1.0",
    }
    params = {"languages": lang}
    if imdb_id:
        params["imdb_id"] = imdb_id.replace("tt", "")
    elif title:
        params["query"] = re.sub(r"\(\d{4}\)", "", title).strip()
        if year:
            params["year"] = str(year)

    resp = requests.get("https://api.opensubtitles.com/api/v1/subtitles",
                        headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        log.debug("OpenSubtitles search failed: %d", resp.status_code)
        return []

    results = []
    for item in resp.json().get("data", []):
        attrs = item.get("attributes", {})
        files = attrs.get("files", [])
        if files:
            results.append({
                "id": str(files[0].get("file_id", "")),
                "name": attrs.get("release", "subtitle.srt"),
                "downloads": attrs.get("download_count", 0),
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
            "metadata_item_id": mid, "media_item_id": media_item_id}

    # Try guids table for IMDB
    try:
        if db.is_pg:
            db.execute(cur,
                "SELECT guid FROM guids WHERE metadata_item_id = %s AND guid LIKE 'imdb://%' LIMIT 1",
                (mid,))
        else:
            db.execute(cur,
                "SELECT guid FROM guids WHERE metadata_item_id = ? AND guid LIKE 'imdb://%' LIMIT 1",
                (mid,))
        grow = db.fetchone(cur)
        if grow:
            imdb = grow["guid"].replace("imdb://", "")
            if not imdb.startswith("tt"):
                imdb = "tt" + imdb
            info["imdb_id"] = imdb
    except Exception:
        # guids table may not exist
        match = re.search(r"(tt\d+)", row.get("guid", ""))
        if match:
            info["imdb_id"] = match.group(1)

    cur.close()
    return info


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


def download_subtitles(db, media_item_ids):
    """Download subtitles for given media items via OpenSubtitles API."""
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

    langs = os.environ.get("SUBTITLE_LANGS", "nl,en").split(",")
    sub_dir = os.environ.get("SUBTITLE_DIR", "./subtitles")
    os.makedirs(sub_dir, exist_ok=True)

    success = 0
    for mid in media_item_ids:
        info = _get_metadata_for_subtitle(db, mid)
        if not info:
            continue
        title = info["title"]
        safe_title = re.sub(r'[<>:"/\\|?*]', "_", title)

        for lang in langs:
            lang = lang.strip()
            # Search
            if info["imdb_id"]:
                results = _subtitle_search(api_key, token, imdb_id=info["imdb_id"], lang=lang)
            else:
                results = _subtitle_search(api_key, token, title=title,
                                           year=info["year"], lang=lang)
            if not results:
                continue

            fname = f"{safe_title}.{lang}.srt"
            fpath = os.path.join(sub_dir, fname)
            if os.path.exists(fpath):
                log.debug("Subtitle already exists: %s", fpath)
                _register_subtitle_in_db(db, mid, fpath, lang)
                continue

            if _subtitle_download(api_key, token, results[0]["id"], fpath):
                _register_subtitle_in_db(db, mid, fpath, lang)
                log.info("Subtitle: %s", fname)
                success += 1
                time.sleep(0.3)  # rate limit

    log.info("Downloaded %d subtitles", success)


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

        if not strm_rows:
            log.info("No .strm files found in database")
            db.commit()
            db.close()
            return

        log.info("Found %d .strm entries", len(strm_rows))

        # Replace .strm paths with direct URLs
        url_mapping = {}  # media_item_id -> url
        updated = 0
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

        # FFprobe analysis
        if url_mapping:
            ffprobe_path = args.ffprobe or os.environ.get("FFPROBE_PATH", "ffprobe")
            workers = args.workers or int(os.environ.get("FFPROBE_WORKERS", "4"))
            timeout = args.timeout or int(os.environ.get("FFPROBE_TIMEOUT", "30"))

            log.info("Running FFprobe analysis on %d URLs (%d workers)...",
                     len(url_mapping), workers)

            analyzed = 0
            failed = 0
            t0 = time.time()

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(run_ffprobe, url, ffprobe_path, timeout): mid
                    for mid, url in url_mapping.items()
                }
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        meta = future.result()
                    except Exception:
                        meta = None

                    if meta:
                        update_media_item(db, mid, dict(meta))
                        create_media_streams(db, mid, dict(meta))
                        analyzed += 1
                    else:
                        failed += 1

                    total = analyzed + failed
                    if total % 10 == 0:
                        db.commit()
                    if total % 20 == 0:
                        elapsed = time.time() - t0
                        rate = total / elapsed if elapsed > 0 else 0
                        log.info("FFprobe: %d/%d (%.1f/s, %d failed)",
                                 total, len(url_mapping), rate, failed)

            db.commit()
            elapsed = time.time() - t0
            log.info("FFprobe done: %d analyzed, %d failed (%.1fs)", analyzed, failed, elapsed)

        # Subtitles
        if args.subtitles and url_mapping:
            log.info("Downloading subtitles for %d items...", len(url_mapping))
            download_subtitles(db, list(url_mapping.keys()))
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
    p_update.add_argument("--ffprobe", metavar="PATH", help="path to ffprobe binary")
    p_update.add_argument("--workers", type=int, default=4,
                          help="FFprobe parallel workers (default: 4)")
    p_update.add_argument("--timeout", type=int, default=30,
                          help="FFprobe timeout per URL in seconds (default: 30)")
    p_update.add_argument("--backup-dir", metavar="DIR",
                          help="directory for database backups")

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
