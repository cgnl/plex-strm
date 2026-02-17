"""Plex database protection: triggers to prevent URL overwrites."""

import logging

from db import connect_from_args

log = logging.getLogger("plex-strm")

# ---------------------------------------------------------------------------
# Trigger definitions
# ---------------------------------------------------------------------------

_SQLITE_TRIGGERS = [
    """CREATE TRIGGER protect_stream_urls_strict
BEFORE UPDATE ON media_parts
FOR EACH ROW
WHEN (OLD.file LIKE 'http://%' OR OLD.file LIKE 'https://%')
     AND (NEW.file NOT LIKE 'http://%' AND NEW.file NOT LIKE 'https://%')
     AND NEW.file != OLD.file
BEGIN
    SELECT RAISE(IGNORE);
END""",
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_backup_table(db):
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


def drop_triggers(db):
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


def install_protection(db):
    """Install 4-layer trigger protection."""
    create_backup_table(db)
    drop_triggers(db)
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
    backup_existing_urls(db)
    log.info("4-layer protection installed")


def backup_existing_urls(db):
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
# CLI commands
# ---------------------------------------------------------------------------

def cmd_protect(args):
    """Install 4-layer trigger protection."""
    db = connect_from_args(args)
    try:
        install_protection(db)
        db.commit()
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
        drop_triggers(db)
        db.commit()
        log.info("Protection triggers removed")
    except Exception as e:
        db.rollback()
        log.error("Failed to remove protection: %s", e)
        raise
    finally:
        db.close()


def cmd_status(args):
    """Show protection status and URL counts."""
    from db import resolve_library_ids, library_join
    db = connect_from_args(args)
    try:
        lib_ids = resolve_library_ids(db, getattr(args, 'library', None))
        cur = db.cursor()
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
            print("Protection: ACTIVE (4 layers)")
        elif len(triggers) >= 3:
            print(f"Protection: PARTIAL ({len(triggers)} layers)")
        else:
            print("Protection: INACTIVE")
        for name in trigger_names:
            print(f"  - {name}")

        join, lib_where, lib_params = library_join(db, lib_ids, "mp")
        ph = "%s" if db.is_pg else "?"
        db.execute(cur,
            f"SELECT COUNT(*) AS cnt FROM media_parts mp{join} WHERE mp.file LIKE {ph}{lib_where}",
            ("http%",) + lib_params if lib_params else ("http%",))
        row = db.fetchone(cur)
        print(f"HTTP URLs: {row['cnt']}")

        db.execute(cur,
            f"SELECT COUNT(*) AS cnt FROM media_parts mp{join} WHERE LOWER(mp.file) LIKE {ph}{lib_where}",
            ("%.strm",) + lib_params if lib_params else ("%.strm",))
        row = db.fetchone(cur)
        print(f"Pending .strm files: {row['cnt']}")

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


def cmd_revert(args):
    """Revert URLs back to original .strm paths from backup."""
    db = connect_from_args(args)
    try:
        cur = db.cursor()
        if db.is_pg:
            db.execute(cur,
                "SELECT COUNT(*) AS cnt FROM information_schema.tables "
                "WHERE table_name = 'stream_url_backup'")
        else:
            db.execute(cur,
                "SELECT COUNT(*) AS cnt FROM sqlite_master "
                "WHERE type='table' AND name='stream_url_backup'")
        if db.fetchone(cur)["cnt"] == 0:
            log.error("No backup table found -- cannot revert")
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
