"""Database abstraction for Plex (SQLite + PostgreSQL)."""

import logging
import os
import shutil
import sqlite3
import sys
from datetime import datetime

log = logging.getLogger("plex-strm")


class PlexDB:
    """Thin abstraction over SQLite and PostgreSQL connections."""

    def __init__(self, db_path=None, pg_config=None):
        self.is_pg = pg_config is not None
        self._schema = ""
        if self.is_pg:
            try:
                import psycopg2
                import psycopg2.extensions
            except ImportError:
                sys.exit("psycopg2 is required for PostgreSQL mode: pip install psycopg2-binary")
            schema = pg_config.pop("schema", "plex")
            self._schema = f"{schema}." if schema else ""
            self._pg_config = dict(pg_config)
            self._pg_schema = schema
            # Enable TCP keepalive to prevent idle connection drops
            pg_config.setdefault("keepalives", 1)
            pg_config.setdefault("keepalives_idle", 30)
            pg_config.setdefault("keepalives_interval", 10)
            pg_config.setdefault("keepalives_count", 5)
            self.conn = psycopg2.connect(**pg_config)
            self.conn.autocommit = False
            cur = self.conn.cursor()
            cur.execute(f"SET search_path TO {schema}, public")
            cur.close()
        else:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row

    def ensure_connected(self):
        """Test the PG connection and reconnect if it was dropped."""
        if not self.is_pg:
            return
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except Exception:
            log.warning("PostgreSQL connection lost, reconnecting...")
            try:
                self.conn.close()
            except Exception:
                pass
            import psycopg2
            cfg = dict(self._pg_config)
            cfg.setdefault("keepalives", 1)
            cfg.setdefault("keepalives_idle", 30)
            cfg.setdefault("keepalives_interval", 10)
            cfg.setdefault("keepalives_count", 5)
            self.conn = psycopg2.connect(**cfg)
            self.conn.autocommit = False
            cur = self.conn.cursor()
            cur.execute(f"SET search_path TO {self._pg_schema}, public")
            cur.close()
            log.info("PostgreSQL reconnected")

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


def resolve_library_ids(db, names):
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


def library_join(db, lib_ids, alias="mp"):
    """Return (JOIN clause, WHERE clause, params) for filtering by library section IDs."""
    if lib_ids is None:
        return "", "", []
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
    pg_config = pg_config_from_env()
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


def pg_config_from_env():
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
