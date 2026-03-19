#!/usr/bin/env python3
"""
Organize Zurg STRM files for Plex using PTT (Parse Torrent Title).

Creates symlink-based organized directories from Zurg's raw output:
  - movies-organized/   ← all movies (symlinks to movies/)
  - movies-spanish/     ← movies with Spanish audio (symlinks to movies/)
  - shows-organized/    ← shows organized by Show/Season/Episode (symlinks to shows/)
  - shows-spanish/      ← shows with Spanish audio, same structure (symlinks to shows/)

Language detection uses Plex's PostgreSQL database (ffprobe audio stream data).
Items with Spanish audio go to the spanish dirs.
Items with English audio OR not yet analyzed go to the organized dirs.
Multi-audio items (both en+es) go to BOTH dirs.

Runs every 5 minutes via LaunchAgent.
"""

import json
import os
import re
import sys
import time
import logging
import subprocess
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

from PTT import parse_title

# ── Configuration ─────────────────────────────────────────────────
STRM_BASE = Path("/Users/sander/bin/zurg/strm")

# Zurg raw output (source)
ZURG_MOVIES_DIR = STRM_BASE / "movies"
ZURG_SHOWS_DIR = STRM_BASE / "shows"

# Organized output (symlinks)
MOVIES_ORGANIZED = STRM_BASE / "movies-organized"
MOVIES_SPANISH = STRM_BASE / "movies-spanish"
SHOWS_ORGANIZED = STRM_BASE / "shows-organized"
SHOWS_SPANISH = STRM_BASE / "shows-spanish"
CHANGED_PATHS_FILE = Path(
    os.environ.get("ORGANIZE_CHANGED_PATHS_FILE", "/tmp/organize_strm_changed_paths.txt")
)

# PostgreSQL
PSQL = "/opt/homebrew/Cellar/postgresql@15/15.15/bin/psql"
PG_CONN = "-h localhost -U plex -d plex"

# ── Logging ───────────────────────────────────────────────────────
import io
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [organize-strm] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, line_buffering=True)
log = logging.getLogger(__name__)

ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*]')

# Special characters to strip before PTT parsing (trademark, registered, copyright)
SPECIAL_CHARS_RE = re.compile(r'[™®©]')

# Leading release/site noise prefixes seen in some torrent names, e.g.
# "Www UIndex Org ...", "www.hdbthd.com ...", "[... www.site.com] ..."
BRACKETED_SITE_PREFIX_RE = re.compile(r'^[\[\(【].*?www\.[^\]\)】]+.*?[\]\)】]\s*', re.IGNORECASE)
DOMAIN_STYLE_PREFIX_RE = re.compile(
    r'^(?:www[\W_]+[a-z0-9\-]+[\W_]+(?:com|org|net|info|io)\b[\W_]*)+',
    re.IGNORECASE,
)
SITE_WORD_PREFIX_RE = re.compile(
    r'^(?:www\s+[a-z0-9\-]+\s+(?:com|org|net|info|io)\b\s*)+',
    re.IGNORECASE,
)

# Short lowercase release group prefix before a dash, e.g. "lwrtd-the" → "the",
# "wou-snapshot" → "snapshot". Only match if followed by a word starting with a letter.
RELEASE_GROUP_PREFIX_RE = re.compile(r'^[a-z]{2,6}-(?=[a-zA-Z])')

# Chinese-prefixed torrent names: 【Chinese site】Chinese title[tags].English.Title.Year...
# Extract the English portion after the last 】 or ] before the dot-separated English title.
CHINESE_TORRENT_RE = re.compile(
    r'[\]】][^\]】]*?\.([A-Za-z][A-Za-z0-9].*)',
)

# Common release/noise suffixes for show folder names
SHOW_NOISE_SUFFIX_RE = re.compile(
    r'(?:\s+|^)(?:'
    r'S\d{1,2}(?:E\d{1,2})?'          # S02 / S02E03
    r'|Season\s*\d{1,2}'              # Season 2
    r'|Sezon\s*\d{1,2}'               # Sezon 2
    r'|FW|DUAL|AMAZ|HDLigh|POKTV'       # common scene suffix fragments
    r'|K\d{2,4}'                       # K83, K216, etc
    r'|WEB(?:[- ]?DL)?|WEBRIP|BLURAY|BDRIP|DVDRIP|REMUX'
    r'|X264|X265|HEVC|AVC|H\.?264|H\.?265'
    r'|DDP?\s*\d(?:\.\d)?|AAC|AC3|EAC3|DTS(?:-HD)?'
    r')\b.*$',
    re.IGNORECASE,
)

CHINESE_SITE_PREFIX_RE = re.compile(r'^(?:高清影视之家(?:发布|首发)?|高清剧集网(?:发布|首发)?)\s*', re.IGNORECASE)

# Detect if title is predominantly non-Latin (Chinese/Japanese/Korean etc.)
NON_LATIN_TITLE_RE = re.compile(r'^[^\x00-\x7F\s]+$')

# Torrent name patterns that indicate Spanish content (fallback for unanalyzed items)
SPANISH_NAME_RE = re.compile(
    r'(?i)(?:'
    r'\.SPANISH\.'
    r'|Castellano'
    r'|LATINO'
    r'|\.ESP\.'
    r'|\bSPA\b'
    r'|LatTeam'
    r'|\[ES-EN\]'
    r'|\bES-EN\b'
    r')',
)

# Filename prefix that explicitly labels extras (e.g. "Extras - Deleted Scenes - ...")
EXTRAS_PREFIX_RE = re.compile(r'(?i)^Extras\s*[-–—]')


def clean_name(name: str) -> str:
    """Remove illegal filesystem characters."""
    return ILLEGAL_CHARS.sub("", name).strip()


def _extract_english_from_chinese_torrent(raw_name: str) -> tuple:
    """Try to extract English title from Chinese-prefixed torrent names.

    Pattern: 【Chinese site】Chinese title[Chinese tags].English.Title.Year...
    Returns (parsed_dict, True) if English portion found and parsed, else (None, False).
    """
    m = CHINESE_TORRENT_RE.search(raw_name)
    if m:
        english_part = m.group(1)
        parsed = parse_title(english_part)
        eng_title = parsed.get("title", "")
        if eng_title and len(eng_title) >= 2:
            return parsed, True
    return None, False


def normalize_title(title: str, fallback_raw: str = "") -> str:
    """Normalize parsed title by stripping common leading site/domain noise."""
    t = clean_name(title) if title else clean_name(fallback_raw)
    if not t:
        return ""

    # Repeatedly strip known prefix patterns from the beginning.
    for _ in range(3):
        prev = t
        t = BRACKETED_SITE_PREFIX_RE.sub("", t).strip()
        t = DOMAIN_STYLE_PREFIX_RE.sub("", t).strip()
        t = SITE_WORD_PREFIX_RE.sub("", t).strip()
        t = CHINESE_SITE_PREFIX_RE.sub("", t).strip()
        if t == prev:
            break

    # Strip short release group prefix (e.g. "lwrtd-the..." → "the...")
    t = RELEASE_GROUP_PREFIX_RE.sub("", t).strip()

    if not t:
        return clean_name(fallback_raw)
    return t


def _extract_title_before_season(raw: str) -> str:
    """Extract title by splitting at season marker (S01, Season 1, etc.).

    Used as fallback when PTT produces a too-short or mangled title.
    E.g. "The.Diamond.Heist.S01.2160p..." → "The Diamond Heist"
         "FBI.True.S02.1080p..." → "FBI True"
    """
    # Replace dots/underscores with spaces first
    cleaned = re.sub(r'[._]', ' ', raw)
    # Split at season marker
    m = re.split(r'\s+S\d{1,2}(?:E\d{1,2})?\b|\s+Season\s*\d{1,2}\b', cleaned, maxsplit=1, flags=re.IGNORECASE)
    if m and len(m[0].strip()) > 3:
        # Clean site prefixes from the extracted title
        title = normalize_title(m[0].strip())
        if len(title) > 3:
            return title
    return ""


def normalize_show_title(title: str, fallback_raw: str = "") -> str:
    """Normalize show folder names, removing common release suffix noise."""
    t = normalize_title(title, fallback_raw)
    if not t:
        return ""
    t2 = SHOW_NOISE_SUFFIX_RE.sub("", t).strip(" -_.")
    result = t2 or t

    # Fallback: if title is too short (<=4 chars like "The") or looks like
    # a PTT merge bug (e.g. "FBIS02"), try to re-extract from raw dir name
    is_merge_bug = bool(re.match(r'^[A-Z]+S\d{2,}$', result))
    if (len(result) <= 4 or is_merge_bug) and fallback_raw:
        better = _extract_title_before_season(fallback_raw)
        if better and len(better) > len(result):
            result = better

    return result


def _is_likely_extras(file_parsed: dict, parent_parsed: dict, file_stem: str) -> bool:
    """Detect bonus/extras content using PTT metadata.

    Strategy (PTT-first, no giant regex lists):
      1. PTT tagged it as extras or trash → skip
      2. Filename starts with "Extras - " → skip
      3. File has NO year, NO resolution, NO quality, AND parent dir
         has a year but NO seasons → it's a short bonus clip alongside
         a movie main feature → skip
    """
    # 1. PTT detected extras/trash
    if file_parsed.get('extras') or file_parsed.get('trash'):
        return True

    # 2. Filename explicitly labelled "Extras - ..."
    if EXTRAS_PREFIX_RE.match(file_stem):
        return True

    # 3. Bare title alongside a movie parent
    #    Movie BluRay REMUXes often have a main feature + many short extras
    #    with plain names like "Wink", "Big Baby", "Dark Energy".
    #    These have no year, no resolution, no quality — unlike the main feature.
    parent_year = parent_parsed.get('year')
    parent_seasons = parent_parsed.get('seasons', [])
    file_year = file_parsed.get('year')
    file_res = file_parsed.get('resolution')
    file_qual = file_parsed.get('quality')

    if (parent_year and not parent_seasons
            and not file_year and not file_res and not file_qual):
        return True

    return False


def _is_file_a_movie(file_parsed: dict, parent_parsed: dict,
                     dir_name: str, file_stem: str) -> bool:
    """Detect if a no-S/E file is actually a standalone movie.

    Uses PTT metadata — if the file has its own year, it's likely a movie
    (either the main feature or an item in a collection pack).
    Also checks for main-feature pattern (filename ≈ dir name).
    """
    # File has its own year → standalone film
    if file_parsed.get('year'):
        return True

    # File has resolution/quality AND name matches parent dir → main feature
    if (file_parsed.get('resolution') or file_parsed.get('quality')):
        # Compare normalized title prefixes
        dn = re.sub(r'[\.\[\]\(\)\{\}\-~]', ' ', dir_name).lower().strip()
        fn = re.sub(r'[\.\[\]\(\)\{\}\-~]', ' ', file_stem).lower().strip()
        for sep in ['1080p', '2160p', '720p', '480p', 'bluray', 'bdrip',
                    'webrip', 'web dl', 'webdl', 'remux', 'x264', 'x265',
                    'hevc', 'avc', 'bdremux', 'bd remux']:
            dn = dn.split(sep)[0].strip()
            fn = fn.split(sep)[0].strip()
        if len(dn) >= 5 and len(fn) >= 5:
            if dn[:min(25, len(dn))] == fn[:min(25, len(fn))]:
                return True

    return False


# ── Plex DB queries ──────────────────────────────────────────────

def pg_query(sql: str, timeout: int = 120) -> str:
    """Run a PostgreSQL query via psycopg2 and return pipe-delimited rows."""
    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = '{timeout}s'")
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        lines = []
        for row in rows:
            lines.append("|".join("" if v is None else str(v) for v in row))
        return "\n".join(lines)
    except Exception as e:
        log.error("PostgreSQL query failed: %s", e)
        return ""


def _get_pg_conn():
    """Get a psycopg2 connection to the Plex DB."""
    import psycopg2
    host = os.environ.get("PLEX_PG_HOST", "localhost")
    port = int(os.environ.get("PLEX_PG_PORT", "5432"))
    database = os.environ.get("PLEX_PG_DATABASE", "plex")
    user = os.environ.get("PLEX_PG_USER", "plex")
    password = os.environ.get("PLEX_PG_PASSWORD", "plex")
    schema = os.environ.get("PLEX_PG_SCHEMA", "plex")
    return psycopg2.connect(
        host=host, port=port, database=database,
        user=user, password=password,
        options=f"-c search_path={schema}",
    )


def get_library_id_map() -> dict:
    """Look up Plex library section IDs by matching root_path to our output dirs.

    Returns dict mapping Path → library_section_id, e.g.:
        {MOVIES_ORGANIZED: 11, SHOWS_ORGANIZED: 12, ...}
    """
    raw = pg_query("""
        SELECT sl.root_path, sl.library_section_id
        FROM plex.section_locations sl
        JOIN plex.library_sections ls ON ls.id = sl.library_section_id
        WHERE ls.name LIKE 'STRM%'
    """)
    result = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 2:
            root_path = Path(parts[0])
            lib_id = int(parts[1])
            result[root_path] = lib_id
    if not result:
        log.warning("Could not look up library IDs from DB, using defaults")
        result = {
            MOVIES_ORGANIZED: 11,
            SHOWS_ORGANIZED: 12,
            MOVIES_SPANISH: 13,
            SHOWS_SPANISH: 14,
        }
    return result


def deduplicate_plex_db():
    """Deduplicate metadata_items in Plex DB per library.

    Shows: merge duplicate show rows (same guid + library_section_id).
      - Canonical = show with most season children
      - Reparent unique seasons from dupes to canonical
      - For conflicting seasons (same index): merge episodes
      - For conflicting episodes (same index): move media_items to canonical episode
      - Delete empty dupe episodes, seasons, shows

    Movies: merge duplicate movie rows (same guid + library_section_id).
      - Canonical = movie with most media_items
      - Move media_items from dupes to canonical
      - Delete dupe movie rows

    All operations are per-library to avoid cross-library merging.
    """
    try:
        import psycopg2
    except ImportError:
        log.error("psycopg2 not installed, skipping Plex DB dedup")
        return

    try:
        conn = _get_pg_conn()
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as e:
        log.error("Failed to connect to Plex DB for dedup: %s", e)
        return

    try:
        _deduplicate_shows(cur)
        _deduplicate_movies(cur)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error("Plex DB dedup failed, rolled back: %s", e)
    finally:
        cur.close()
        conn.close()


def _reparent_references(cur, old_id: int, new_id: int):
    """Move all referencing rows from old_id to new_id in related tables."""
    # taggings: update metadata_item_id, skip if would create duplicate
    cur.execute("""
        UPDATE taggings SET metadata_item_id = %s
        WHERE metadata_item_id = %s
        AND tag_id NOT IN (
            SELECT tag_id FROM taggings WHERE metadata_item_id = %s
        )
    """, (new_id, old_id, new_id))
    cur.execute("DELETE FROM taggings WHERE metadata_item_id = %s", (old_id,))

    # metadata_relations
    cur.execute("""
        UPDATE metadata_relations SET metadata_item_id = %s
        WHERE metadata_item_id = %s
        AND related_metadata_item_id NOT IN (
            SELECT related_metadata_item_id FROM metadata_relations WHERE metadata_item_id = %s
        )
    """, (new_id, old_id, new_id))
    cur.execute("DELETE FROM metadata_relations WHERE metadata_item_id = %s", (old_id,))

    # metadata_item_accounts
    cur.execute("""
        UPDATE metadata_item_accounts SET metadata_item_id = %s
        WHERE metadata_item_id = %s
        AND account_id NOT IN (
            SELECT account_id FROM metadata_item_accounts WHERE metadata_item_id = %s
        )
    """, (new_id, old_id, new_id))
    cur.execute("DELETE FROM metadata_item_accounts WHERE metadata_item_id = %s", (old_id,))

    # Other tables (usually empty but handle them)
    for tbl in ['metadata_item_clusterings', 'versioned_metadata_items',
                'play_queue_items', 'play_queue_generators',
                'media_grabs', 'download_queue_items']:
        cur.execute(
            f"UPDATE {tbl} SET metadata_item_id = %s WHERE metadata_item_id = %s",
            (new_id, old_id),
        )


def _move_media_items(cur, from_metadata_id: int, to_metadata_id: int) -> int:
    """Move media_items from one metadata_item to another.

    Skips media_items whose media_parts point to the same file hash as
    existing media on the target (to avoid true duplicates).
    Returns number of media_items moved.
    """
    # Get file hashes already on target
    cur.execute("""
        SELECT DISTINCT substring(mp.file from '/strm/(.*)$')
        FROM media_items mi
        JOIN media_parts mp ON mp.media_item_id = mi.id
        WHERE mi.metadata_item_id = %s AND mp.file LIKE '%%/strm/%%'
    """, (to_metadata_id,))
    existing_hashes = {r[0] for r in cur.fetchall() if r[0]}

    # Get media_items to potentially move
    cur.execute("""
        SELECT mi.id, substring(mp.file from '/strm/(.*)$') as hash
        FROM media_items mi
        JOIN media_parts mp ON mp.media_item_id = mi.id
        WHERE mi.metadata_item_id = %s AND mp.file LIKE '%%/strm/%%'
    """, (from_metadata_id,))
    source_media = cur.fetchall()

    moved = 0
    for media_id, file_hash in source_media:
        if file_hash and file_hash in existing_hashes:
            # Same file already exists on target — delete this duplicate
            cur.execute("DELETE FROM media_streams WHERE media_item_id = %s", (media_id,))
            cur.execute("DELETE FROM media_parts WHERE media_item_id = %s", (media_id,))
            cur.execute("DELETE FROM media_items WHERE id = %s", (media_id,))
        else:
            # Different file — move it (multi-version)
            cur.execute(
                "UPDATE media_items SET metadata_item_id = %s WHERE id = %s",
                (to_metadata_id, media_id),
            )
            if file_hash:
                existing_hashes.add(file_hash)
            moved += 1

    # Also handle any media_items without strm files (shouldn't happen but be safe)
    cur.execute("""
        UPDATE media_items SET metadata_item_id = %s WHERE metadata_item_id = %s
    """, (to_metadata_id, from_metadata_id))
    moved += cur.rowcount

    return moved


def _delete_metadata_item(cur, item_id: int):
    """Delete a metadata_item and all its direct references (no children)."""
    _reparent_references(cur, item_id, item_id)  # cleans up remaining refs
    # Final cleanup of any remaining references
    for tbl in ['taggings', 'metadata_relations', 'metadata_item_accounts',
                'metadata_item_clusterings', 'versioned_metadata_items',
                'play_queue_items', 'play_queue_generators',
                'media_grabs', 'download_queue_items']:
        cur.execute(f"DELETE FROM {tbl} WHERE metadata_item_id = %s", (item_id,))
    cur.execute("DELETE FROM media_streams WHERE media_item_id IN (SELECT id FROM media_items WHERE metadata_item_id = %s)", (item_id,))
    cur.execute("DELETE FROM media_parts WHERE media_item_id IN (SELECT id FROM media_items WHERE metadata_item_id = %s)", (item_id,))
    cur.execute("DELETE FROM media_items WHERE metadata_item_id = %s", (item_id,))
    cur.execute("DELETE FROM metadata_items WHERE id = %s", (item_id,))


def _deduplicate_shows(cur):
    """Deduplicate show metadata_items per library."""
    # Find duplicate show groups per library
    cur.execute("""
        SELECT library_section_id, guid,
               array_agg(id ORDER BY (
                   SELECT COUNT(*) FROM metadata_items c WHERE c.parent_id = metadata_items.id
               ) DESC, id ASC) as show_ids
        FROM metadata_items
        WHERE metadata_type = 2 AND guid IS NOT NULL AND guid != ''
        GROUP BY library_section_id, guid
        HAVING COUNT(*) > 1
    """)
    groups = cur.fetchall()
    if not groups:
        log.info("Plex DB dedup: no duplicate shows found")
        return

    total_merged = 0
    total_deleted = 0
    total_seasons_moved = 0
    total_episodes_moved = 0
    total_media_moved = 0

    for lib_id, guid, show_ids in groups:
        canonical_id = show_ids[0]
        dupe_ids = show_ids[1:]

        # Get canonical's existing seasons: {season_index: season_id}
        cur.execute("""
            SELECT "index", id FROM metadata_items
            WHERE parent_id = %s AND metadata_type = 3
        """, (canonical_id,))
        canonical_seasons = {r[0]: r[1] for r in cur.fetchall()}

        for dupe_show_id in dupe_ids:
            # Get dupe's seasons
            cur.execute("""
                SELECT "index", id FROM metadata_items
                WHERE parent_id = %s AND metadata_type = 3
            """, (dupe_show_id,))
            dupe_seasons = cur.fetchall()

            for season_idx, dupe_season_id in dupe_seasons:
                if season_idx not in canonical_seasons:
                    # No conflict — reparent entire season to canonical show
                    cur.execute(
                        "UPDATE metadata_items SET parent_id = %s WHERE id = %s",
                        (canonical_id, dupe_season_id),
                    )
                    _reparent_references(cur, dupe_season_id, dupe_season_id)
                    canonical_seasons[season_idx] = dupe_season_id
                    total_seasons_moved += 1
                else:
                    # Season conflict — merge episodes
                    canonical_season_id = canonical_seasons[season_idx]

                    # Get canonical season's episodes: {ep_index: ep_id}
                    cur.execute("""
                        SELECT "index", id FROM metadata_items
                        WHERE parent_id = %s AND metadata_type = 4
                    """, (canonical_season_id,))
                    canonical_episodes = {r[0]: r[1] for r in cur.fetchall()}

                    # Get dupe season's episodes
                    cur.execute("""
                        SELECT "index", id FROM metadata_items
                        WHERE parent_id = %s AND metadata_type = 4
                    """, (dupe_season_id,))
                    dupe_episodes = cur.fetchall()

                    for ep_idx, dupe_ep_id in dupe_episodes:
                        if ep_idx not in canonical_episodes:
                            # No conflict — reparent episode to canonical season
                            cur.execute(
                                "UPDATE metadata_items SET parent_id = %s WHERE id = %s",
                                (canonical_season_id, dupe_ep_id),
                            )
                            _reparent_references(cur, dupe_ep_id, dupe_ep_id)
                            canonical_episodes[ep_idx] = dupe_ep_id
                            total_episodes_moved += 1
                        else:
                            # Episode conflict — move media_items, then delete dupe episode
                            canonical_ep_id = canonical_episodes[ep_idx]
                            moved = _move_media_items(cur, dupe_ep_id, canonical_ep_id)
                            total_media_moved += moved
                            _reparent_references(cur, dupe_ep_id, canonical_ep_id)
                            _delete_metadata_item(cur, dupe_ep_id)

                    # Delete now-empty dupe season
                    _reparent_references(cur, dupe_season_id, canonical_season_id)
                    _delete_metadata_item(cur, dupe_season_id)

            # Move show-level references and delete dupe show
            _reparent_references(cur, dupe_show_id, canonical_id)
            _delete_metadata_item(cur, dupe_show_id)
            total_deleted += 1

        total_merged += 1

    log.info(
        "Plex DB dedup shows: %d groups merged, %d dupe shows deleted, "
        "%d seasons moved, %d episodes moved, %d media items moved",
        total_merged, total_deleted, total_seasons_moved,
        total_episodes_moved, total_media_moved,
    )


def _deduplicate_movies(cur):
    """Deduplicate movie metadata_items per library."""
    # Find duplicate movie groups per library
    cur.execute("""
        SELECT library_section_id, guid,
               array_agg(id ORDER BY (
                   SELECT COUNT(*) FROM media_items m WHERE m.metadata_item_id = metadata_items.id
               ) DESC, id ASC) as movie_ids
        FROM metadata_items
        WHERE metadata_type = 1 AND guid IS NOT NULL AND guid != ''
        GROUP BY library_section_id, guid
        HAVING COUNT(*) > 1
    """)
    groups = cur.fetchall()
    if not groups:
        log.info("Plex DB dedup: no duplicate movies found")
        return

    total_merged = 0
    total_deleted = 0
    total_media_moved = 0

    for lib_id, guid, movie_ids in groups:
        canonical_id = movie_ids[0]
        dupe_ids = movie_ids[1:]

        for dupe_id in dupe_ids:
            moved = _move_media_items(cur, dupe_id, canonical_id)
            total_media_moved += moved
            _reparent_references(cur, dupe_id, canonical_id)
            _delete_metadata_item(cur, dupe_id)
            total_deleted += 1

        total_merged += 1

    log.info(
        "Plex DB dedup movies: %d groups merged, %d dupe movies deleted, "
        "%d media items moved",
        total_merged, total_deleted, total_media_moved,
    )


def pg_update_directory_paths(renames: dict):
    """Batch-update directories.path in Plex DB for renamed folders.

    renames: {old_path: new_path, ...}
    Only updates directories in STRM libraries.
    """
    if not renames:
        return
    try:
        conn = _get_pg_conn()
        lib_ids = list(get_library_id_map().values())
        cur = conn.cursor()
        updated = 0
        for old_path, new_path in renames.items():
            cur.execute("""
                UPDATE directories SET path = %s
                WHERE path = %s AND library_section_id = ANY(%s)
            """, (new_path, old_path, lib_ids))
            updated += cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if updated:
            log.info("Updated %d directory paths in Plex DB (%d renames)",
                     updated, len(renames))
    except Exception as e:
        log.error("Failed to update directory paths in Plex DB: %s", e)


def get_spanish_hashes() -> set:
    """Get Zurg hashes for items with Spanish audio in Plex."""
    sql = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1)
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.media_streams ms ON ms.media_item_id = mi.id
    WHERE mp.file LIKE 'http%/strm/%'
      AND ms.stream_type_id = 2
      AND ms.language = 'es';
    """
    raw = pg_query(sql)
    if not raw:
        return set()
    hashes = set(raw.splitlines())
    log.info("Found %d items with Spanish audio in Plex", len(hashes))
    return hashes


def get_english_or_unanalyzed_hashes() -> set:
    """Get Zurg hashes for items with English audio or not yet analyzed."""
    sql = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1)
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    WHERE mp.file LIKE 'http%/strm/%'
      AND (
        EXISTS (
            SELECT 1 FROM plex.media_streams ms
            WHERE ms.media_item_id = mi.id AND ms.stream_type_id = 2 AND ms.language = 'en'
        )
        OR NOT EXISTS (
            SELECT 1 FROM plex.media_streams ms
            WHERE ms.media_item_id = mi.id AND ms.stream_type_id = 2
        )
      );
    """
    raw = pg_query(sql)
    if not raw:
        return set()
    return set(raw.splitlines())


def get_nonenglish_nonspanish_hashes() -> set:
    """Get hashes for items that ARE analyzed but have neither English nor Spanish audio.
    These still go to the organized dirs (they're just foreign films)."""
    sql = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1)
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    WHERE mp.file LIKE 'http%/strm/%'
      AND EXISTS (
          SELECT 1 FROM plex.media_streams ms
          WHERE ms.media_item_id = mi.id AND ms.stream_type_id = 2
      )
      AND NOT EXISTS (
          SELECT 1 FROM plex.media_streams ms
          WHERE ms.media_item_id = mi.id AND ms.stream_type_id = 2 AND ms.language = 'en'
      )
      AND NOT EXISTS (
          SELECT 1 FROM plex.media_streams ms
          WHERE ms.media_item_id = mi.id AND ms.stream_type_id = 2 AND ms.language = 'es'
      );
    """
    raw = pg_query(sql)
    if not raw:
        return set()
    return set(raw.splitlines())


# ── TMDB/TVDB → IMDb fallback ────────────────────────────────────

TMDB_CACHE_FILE = Path(__file__).parent / ".tmdb_imdb_cache.json"
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")


def _load_tmdb_cache() -> dict:
    """Load the TMDB→IMDb cache from disk. Returns {tmdb_key: imdb_id}."""
    if TMDB_CACHE_FILE.exists():
        try:
            return json.loads(TMDB_CACHE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_tmdb_cache(cache: dict):
    """Save the TMDB→IMDb cache to disk."""
    try:
        TMDB_CACHE_FILE.write_text(json.dumps(cache, indent=2))
    except OSError as e:
        log.warning("Failed to save TMDB cache: %s", e)


def _tmdb_api(path: str) -> dict:
    """Call a TMDB API endpoint. Returns parsed JSON or empty dict on error."""
    url = "https://api.themoviedb.org/3" + path
    sep = "&" if "?" in url else "?"
    url += sep + "api_key=" + TMDB_API_KEY
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        log.debug("TMDB API error for %s: %s", path, e)
        return {}


def _resolve_tmdb_to_imdb(tmdb_id: str, media_type: str, cache: dict) -> str:
    """Resolve a TMDB ID to an IMDb ID, using cache.

    media_type: 'movie' or 'tv'
    Returns IMDb ID (e.g. 'tt1234567') or empty string.
    """
    cache_key = "%s:%s" % (media_type, tmdb_id)
    if cache_key in cache:
        return cache[cache_key]

    data = _tmdb_api("/%s/%s/external_ids" % (media_type, tmdb_id))
    imdb_id = data.get("imdb_id") or ""
    cache[cache_key] = imdb_id
    if imdb_id:
        log.debug("TMDB %s/%s → %s", media_type, tmdb_id, imdb_id)
    return imdb_id


def _resolve_tvdb_to_imdb(tvdb_id: str, cache: dict) -> str:
    """Resolve a TVDB ID to an IMDb ID via TMDB /find endpoint.

    Tries both tvdb_id source for movies and TV, then fetches external_ids.
    Returns IMDb ID or empty string.
    """
    cache_key = "tvdb:%s" % tvdb_id
    if cache_key in cache:
        return cache[cache_key]

    data = _tmdb_api("/find/%s?external_source=tvdb_id" % tvdb_id)
    imdb_id = ""

    # Check movie results first
    for movie in data.get("movie_results", []):
        tmdb_id = str(movie.get("id", ""))
        if tmdb_id:
            imdb_id = _resolve_tmdb_to_imdb(tmdb_id, "movie", cache)
            if imdb_id:
                break

    # Then TV results
    if not imdb_id:
        for tv in data.get("tv_results", []):
            tmdb_id = str(tv.get("id", ""))
            if tmdb_id:
                imdb_id = _resolve_tmdb_to_imdb(tmdb_id, "tv", cache)
                if imdb_id:
                    break

    cache[cache_key] = imdb_id
    if imdb_id:
        log.debug("TVDB %s → %s", tvdb_id, imdb_id)
    return imdb_id


def _get_tmdb_tvdb_fallback_map(existing_imdb_map: dict) -> dict:
    """Get additional STRM hash → IMDb mappings via TMDB/TVDB → IMDb conversion.

    Only queries items that are NOT already in existing_imdb_map.
    Returns dict like {'HASH123': 'tt1234567', ...}
    """
    if not TMDB_API_KEY:
        log.debug("No TMDB_API_KEY set, skipping TMDB/TVDB fallback")
        return {}

    # Get STRM hashes with TMDB/TVDB tags but no IMDb (movies)
    sql_movie_tmdb = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1),
        regexp_replace(t.tag, 'tmdb://', ''),
        'movie'
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.metadata_items md ON md.id = mi.metadata_item_id
    JOIN plex.taggings tg ON tg.metadata_item_id = md.id
    JOIN plex.tags t ON t.id = tg.tag_id
    WHERE mp.file LIKE 'http%/strm/%'
      AND t.tag_type = 314 AND t.tag LIKE 'tmdb://%'
      AND md.metadata_type = 1
      AND md.id NOT IN (
          SELECT tg2.metadata_item_id FROM plex.taggings tg2
          JOIN plex.tags t2 ON t2.id = tg2.tag_id
          WHERE t2.tag_type = 314 AND t2.tag ~ '^imdb://tt[0-9]+'
      );
    """
    # Shows via grandparent TMDB
    sql_show_tmdb = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1),
        regexp_replace(t.tag, 'tmdb://', ''),
        'tv'
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.metadata_items ep ON ep.id = mi.metadata_item_id
    JOIN plex.metadata_items se ON se.id = ep.parent_id
    JOIN plex.metadata_items sh ON sh.id = se.parent_id
    JOIN plex.taggings tg ON tg.metadata_item_id = sh.id
    JOIN plex.tags t ON t.id = tg.tag_id
    WHERE mp.file LIKE 'http%/strm/%'
      AND t.tag_type = 314 AND t.tag LIKE 'tmdb://%'
      AND ep.metadata_type = 4
      AND sh.id NOT IN (
          SELECT tg2.metadata_item_id FROM plex.taggings tg2
          JOIN plex.tags t2 ON t2.id = tg2.tag_id
          WHERE t2.tag_type = 314 AND t2.tag ~ '^imdb://tt[0-9]+'
      );
    """
    # Movies with TVDB only (no IMDb, no TMDB)
    sql_movie_tvdb = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1),
        regexp_replace(t.tag, 'tvdb://', ''),
        'tvdb'
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.metadata_items md ON md.id = mi.metadata_item_id
    JOIN plex.taggings tg ON tg.metadata_item_id = md.id
    JOIN plex.tags t ON t.id = tg.tag_id
    WHERE mp.file LIKE 'http%/strm/%'
      AND t.tag_type = 314 AND t.tag LIKE 'tvdb://%'
      AND md.metadata_type IN (1, 4)
      AND md.id NOT IN (
          SELECT tg2.metadata_item_id FROM plex.taggings tg2
          JOIN plex.tags t2 ON t2.id = tg2.tag_id
          WHERE t2.tag_type = 314 AND (t2.tag ~ '^imdb://tt[0-9]+' OR t2.tag LIKE 'tmdb://%')
      );
    """
    # Shows with TVDB only via grandparent
    sql_show_tvdb = """
    SELECT DISTINCT
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1),
        regexp_replace(t.tag, 'tvdb://', ''),
        'tvdb'
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.metadata_items ep ON ep.id = mi.metadata_item_id
    JOIN plex.metadata_items se ON se.id = ep.parent_id
    JOIN plex.metadata_items sh ON sh.id = se.parent_id
    JOIN plex.taggings tg ON tg.metadata_item_id = sh.id
    JOIN plex.tags t ON t.id = tg.tag_id
    WHERE mp.file LIKE 'http%/strm/%'
      AND t.tag_type = 314 AND t.tag LIKE 'tvdb://%'
      AND ep.metadata_type = 4
      AND sh.id NOT IN (
          SELECT tg2.metadata_item_id FROM plex.taggings tg2
          JOIN plex.tags t2 ON t2.id = tg2.tag_id
          WHERE t2.tag_type = 314 AND (t2.tag ~ '^imdb://tt[0-9]+' OR t2.tag LIKE 'tmdb://%')
      );
    """

    # Collect all hashes needing resolution: [(hash, ext_id, source_type), ...]
    pending = []
    for sql in [sql_movie_tmdb, sql_show_tmdb, sql_movie_tvdb, sql_show_tvdb]:
        raw = pg_query(sql)
        if raw:
            for line in raw.splitlines():
                parts = line.split("|")
                if len(parts) == 3:
                    strm_hash, ext_id, source = parts
                    if strm_hash not in existing_imdb_map:
                        pending.append((strm_hash, ext_id, source))

    if not pending:
        return {}

    # Deduplicate by ext_id+source (many hashes share the same show/movie)
    unique_ids = {}
    for strm_hash, ext_id, source in pending:
        key = "%s:%s" % (source, ext_id)
        if key not in unique_ids:
            unique_ids[key] = (ext_id, source)

    log.info("TMDB/TVDB fallback: %d STRM files, %d unique IDs to resolve",
             len(pending), len(unique_ids))

    cache = _load_tmdb_cache()
    resolved = 0

    for ext_id, source in unique_ids.values():
        if source == "tvdb":
            _resolve_tvdb_to_imdb(ext_id, cache)
        else:
            _resolve_tmdb_to_imdb(ext_id, source, cache)

    # Now map hashes using resolved cache
    fallback_map = {}
    for strm_hash, ext_id, source in pending:
        if source == "tvdb":
            cache_key = "tvdb:%s" % ext_id
        else:
            cache_key = "%s:%s" % (source, ext_id)
        imdb_id = cache.get(cache_key, "")
        if imdb_id:
            fallback_map[strm_hash] = imdb_id
            resolved += 1

    _save_tmdb_cache(cache)
    if resolved:
        log.info("TMDB/TVDB fallback resolved %d STRM files to IMDb", resolved)
    return fallback_map


def _tmdb_search_movie(title: str, year=None) -> str:
    """Search TMDB by title (and optionally year) to find IMDb ID. Uses cache.

    Returns IMDb ID (e.g. 'tt1234567') or empty string.
    Only makes API calls for titles not already cached.
    """
    if not TMDB_API_KEY:
        return ""

    cache = _load_tmdb_cache()
    cache_key = "search:%s:%s" % (title.lower(), year)
    if cache_key in cache:
        return cache[cache_key]

    # Clean title for TMDB search: replace dots with spaces, strip "AKA ..." suffix
    search_title = re.sub(r'\.', ' ', title).strip()
    search_title = re.sub(r'\s+AKA\s+.*$', '', search_title, flags=re.IGNORECASE).strip()
    # Strip "Quel Natale - " style prefix translations (foreign title before dash)
    m = re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*[-–—]\s*(.+)$', search_title)
    if m and len(m.group(1)) > 5:
        search_title = m.group(1)

    # Search TMDB
    try:
        query = urllib.parse.quote(search_title)
        url = "https://api.themoviedb.org/3/search/movie?api_key=%s&query=%s" % (TMDB_API_KEY, query)
        if year:
            url += "&year=%s" % year
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        log.debug("TMDB search error for '%s' (%s): %s", title, year, e)
        cache[cache_key] = ""
        _save_tmdb_cache(cache)
        return ""

    results = data.get("results", [])
    if not results:
        cache[cache_key] = ""
        _save_tmdb_cache(cache)
        return ""

    # Take the first result and resolve to IMDb
    tmdb_id = str(results[0].get("id", ""))
    if tmdb_id:
        imdb_id = _resolve_tmdb_to_imdb(tmdb_id, "movie", cache)
        cache[cache_key] = imdb_id
        _save_tmdb_cache(cache)
        if imdb_id:
            log.debug("TMDB search '%s' (%s) → %s → %s", title, year, tmdb_id, imdb_id)
        return imdb_id

    cache[cache_key] = ""
    _save_tmdb_cache(cache)
    return ""


def get_imdb_map() -> dict:
    """Get STRM hash → IMDb ID mapping from Plex DB.

    For movies: tag is directly on the metadata_item.
    For episodes: tag is on the grandparent (show) metadata_item.
    Falls back to TMDB/TVDB → IMDb conversion via TMDB API for items
    without direct IMDb tags.
    Returns dict like {'HASH123': 'tt1234567', ...}
    """
    # Movies: direct metadata_item → tag
    sql_movies = """
    SELECT DISTINCT ON (regexp_replace(mp.file, '.*/strm/', ''))
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1),
        regexp_replace(t.tag, 'imdb://', '')
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.metadata_items md ON md.id = mi.metadata_item_id
    JOIN plex.taggings tg ON tg.metadata_item_id = md.id
    JOIN plex.tags t ON t.id = tg.tag_id
    WHERE mp.file LIKE 'http%/strm/%'
      AND t.tag_type = 314
      AND t.tag ~ '^imdb://tt[0-9]+'
      AND md.metadata_type IN (1, 4);
    """
    # Episodes: grandparent (show) has the IMDb tag
    sql_shows = """
    SELECT DISTINCT ON (regexp_replace(mp.file, '.*/strm/', ''))
        split_part(regexp_replace(mp.file, '.*/strm/', ''), '/', 1),
        regexp_replace(t.tag, 'imdb://', '')
    FROM plex.media_parts mp
    JOIN plex.media_items mi ON mp.media_item_id = mi.id
    JOIN plex.metadata_items ep ON ep.id = mi.metadata_item_id
    JOIN plex.metadata_items se ON se.id = ep.parent_id
    JOIN plex.metadata_items sh ON sh.id = se.parent_id
    JOIN plex.taggings tg ON tg.metadata_item_id = sh.id
    JOIN plex.tags t ON t.id = tg.tag_id
    WHERE mp.file LIKE 'http%/strm/%'
      AND t.tag_type = 314
      AND t.tag ~ '^imdb://tt[0-9]+'
      AND ep.metadata_type = 4;
    """
    imdb_map = {}
    for sql in [sql_movies, sql_shows]:
        raw = pg_query(sql)
        if raw:
            for line in raw.splitlines():
                parts = line.split("|")
                if len(parts) == 2 and parts[1].startswith("tt"):
                    imdb_map[parts[0]] = parts[1]
    log.info("Loaded %d STRM → IMDb mappings from Plex", len(imdb_map))

    # Fallback: resolve TMDB/TVDB → IMDb for items without direct IMDb tags
    fallback = _get_tmdb_tvdb_fallback_map(imdb_map)
    if fallback:
        imdb_map.update(fallback)
        log.info("Total IMDb mappings after TMDB/TVDB fallback: %d", len(imdb_map))

    return imdb_map


# ── Hash extraction ──────────────────────────────────────────────

def read_strm_hash(strm_path: Path):
    """Read a .strm file and extract the Zurg hash from the URL.

    Handles both old format (``/strm/HASH``) and new format
    (``/strm/HASH/torrent_name.mkv``).
    """
    try:
        content = strm_path.read_text().strip()
        # URL format: https://user:pass@host/strm/HASH[/filename.mkv]
        if "/strm/" in content:
            after_strm = content.split("/strm/")[-1]
            # Strip any trailing path segment (e.g. /filename.mkv)
            return after_strm.split("/")[0]
    except OSError:
        pass
    return None


def _read_strm_url(strm_path: Path) -> str | None:
    """Read the raw URL from a .strm file."""
    try:
        return strm_path.read_text().strip()
    except OSError:
        return None


def _build_strm_url_with_filename(base_url: str, torrent_name: str) -> str:
    """Append torrent filename to a STRM URL for CLI Debrid quality detection.

    Transforms:  https://user:pass@host/strm/HASH
    Into:        https://user:pass@host/strm/HASH/Torrent.Name.2024.1080p.mkv

    The strm-proxy on Hetzner strips the filename and forwards only the hash
    to Zurg. CLI Debrid's ``os.path.basename()`` then sees the torrent name
    instead of the raw hash, enabling proper quality/resolution parsing.
    """
    # Ensure we only append once — if URL already has a path after the hash, strip it
    if "/strm/" in base_url:
        before, after = base_url.rsplit("/strm/", 1)
        zurg_hash = after.split("/")[0]
        # Replace spaces with dots so os.path.basename() returns parseable text
        # for CLI Debrid's quality detection (reverse parser).
        safe_name = torrent_name.replace(" ", ".")
        # URL-encode remaining unsafe chars but keep dots, dashes, parens
        safe_name = urllib.parse.quote(safe_name, safe=".-_()[]")
        return f"{before}/strm/{zurg_hash}/{safe_name}.mkv"
    return base_url


# ── STRM file management ────────────────────────────────────────

def ensure_strm_file(source_strm: Path, dest: Path, torrent_name: str):
    """Write an organized .strm file with the torrent name appended to the URL.

    Instead of symlinking to the raw STRM, this writes the modified URL so
    that CLI Debrid can extract quality info from ``os.path.basename(url)``.

    Returns True if the file was created or changed.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    base_url = _read_strm_url(source_strm)
    if not base_url:
        return False

    new_content = _build_strm_url_with_filename(base_url, torrent_name)

    # Check if file already has the correct content
    if dest.exists():
        # Remove stale symlinks from previous implementation
        if dest.is_symlink():
            dest.unlink()
        else:
            try:
                existing = dest.read_text().strip()
                if existing == new_content:
                    return False  # Already up to date
            except OSError:
                pass
            dest.unlink(missing_ok=True)
    elif dest.is_symlink():
        # Broken symlink
        dest.unlink()

    dest.write_text(new_content, encoding="utf-8")
    return True


def _top_level_refresh_path(root_dir: Path, file_path: Path) -> Path:
    """Map a file path to the top-level folder path for targeted Plex refresh."""
    try:
        rel = file_path.relative_to(root_dir)
        if rel.parts:
            return root_dir / rel.parts[0]
    except Exception:
        pass
    return root_dir


def _record_changed(changed: dict, root_dir: Path, file_path: Path):
    refresh_path = _top_level_refresh_path(root_dir, file_path)
    changed.setdefault(root_dir, set()).add(str(refresh_path))


def write_changed_paths(changed: dict):
    """Write library-id + path lines for targeted refresh in pipeline.

    Format per line: <library_id>|<absolute_path>
    """
    lib_map = get_library_id_map()

    lines = []
    for root_dir, lib_id in lib_map.items():
        for p in sorted(changed.get(root_dir, set())):
            lines.append(f"{lib_id}|{p}")

    CHANGED_PATHS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHANGED_PATHS_FILE.with_suffix(CHANGED_PATHS_FILE.suffix + ".tmp")
    tmp.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    tmp.replace(CHANGED_PATHS_FILE)
    log.info("Wrote %d targeted refresh path(s) to %s", len(lines), CHANGED_PATHS_FILE)


def cleanup_dir(organized_dir: Path, valid_links: set, changed: dict):
    """Remove stale .strm files that are no longer valid, and empty dirs."""
    if not organized_dir.exists():
        return
    removed = 0
    for strm in organized_dir.rglob("*.strm"):
        if strm not in valid_links:
            _record_changed(changed, organized_dir, strm)
            strm.unlink()
            removed += 1
            # Clean empty parent dirs
            for parent in [strm.parent, strm.parent.parent]:
                if parent == organized_dir:
                    break
                try:
                    parent.rmdir()
                except OSError:
                    break
    if removed:
        log.info("Cleaned %d stale .strm files from %s", removed, organized_dir.name)


# ── Movies organization ─────────────────────────────────────────

def organize_movies(spanish_hashes: set, organized_hashes: set,
                    imdb_map: dict, renames: dict, changed: dict) -> tuple[set, set]:
    """Organize movie .strm files into movies-organized/ and movies-spanish/.

    Folder naming: "Title (Year) {imdb-ttXXXXXXX}" using PTT + Plex IMDb data.
    Returns (valid_organized, valid_spanish) sets for use by organize_shows
    when redirecting movies found in the shows directory.
    """
    if not ZURG_MOVIES_DIR.exists():
        log.error("Zurg movies dir not found: %s", ZURG_MOVIES_DIR)
        return set(), set()

    MOVIES_ORGANIZED.mkdir(parents=True, exist_ok=True)
    MOVIES_SPANISH.mkdir(parents=True, exist_ok=True)

    valid_organized = set()
    valid_spanish = set()
    used_filenames = set()  # Track used filenames to avoid collisions
    stats = {"organized_new": 0, "spanish_new": 0, "skipped": 0,
             "skipped_tv": 0, "chinese_fixed": 0}

    for torrent_dir in ZURG_MOVIES_DIR.iterdir():
        if not torrent_dir.is_dir():
            continue

        # Strip special chars (™®©) before PTT parsing
        cleaned_dir_name = SPECIAL_CHARS_RE.sub('', torrent_dir.name)

        # Parse torrent dir name with PTT for clean title + year
        parsed = parse_title(cleaned_dir_name)
        title = parsed.get("title", "")
        year = parsed.get("year")

        # ── Chinese-prefixed torrent handling ─────────────────────
        # If PTT title is non-Latin (Chinese), try to extract English title
        # from the raw torrent name: 【site】Chinese[tags].English.Title.Year...
        if title and NON_LATIN_TITLE_RE.match(title):
            eng_parsed, found = _extract_english_from_chinese_torrent(torrent_dir.name)
            if found:
                parsed = eng_parsed
                title = eng_parsed.get("title", title)
                year = eng_parsed.get("year") or year
                stats["chinese_fixed"] += 1
                log.debug("Chinese torrent → English: %s → %s (%s)",
                          torrent_dir.name[:40], title, year)

        # ── Detect TV episodes in movies dir ──────────────────────
        # If PTT finds seasons/episodes on the dir name, skip — it's a TV episode
        if parsed.get("seasons") or parsed.get("episodes"):
            log.debug("Skipped (TV in movies dir): %s", torrent_dir.name)
            stats["skipped_tv"] += 1
            continue

        for strm_file in torrent_dir.glob("*.strm"):
            zurg_hash = read_strm_hash(strm_file)

            # Also check file-level for episodes (single episode files in movies dir)
            file_parsed = parse_title(SPECIAL_CHARS_RE.sub('', strm_file.stem))
            if file_parsed.get("seasons") or file_parsed.get("episodes"):
                log.debug("Skipped (TV episode file in movies): %s", strm_file.name)
                stats["skipped_tv"] += 1
                continue

            # Determine destinations based on language
            go_organized = True
            go_spanish = False

            if zurg_hash:
                if zurg_hash in spanish_hashes:
                    go_spanish = True
                if zurg_hash in organized_hashes:
                    go_organized = True
                elif zurg_hash in spanish_hashes and zurg_hash not in organized_hashes:
                    # Only Spanish audio, no English — still goes to organized
                    go_organized = True

            # Fallback: check torrent dir name for Spanish keywords
            # (for items not yet analyzed by Plex ffprobe)
            if not go_spanish and SPANISH_NAME_RE.search(torrent_dir.name):
                go_spanish = True

            # Build clean folder name: "Title (Year) {imdb-ttXXXXXXX}"
            # Use PTT title, fall back to raw dir name if PTT fails
            clean_title = normalize_title(title, torrent_dir.name)
            if year:
                folder_name = f"{clean_title} ({year})"
            else:
                folder_name = clean_title

            # Append IMDb ID if available (from Plex DB or TMDB search)
            imdb_id = None
            if zurg_hash and zurg_hash in imdb_map:
                imdb_id = imdb_map[zurg_hash]
            # TMDB title+year search fallback for items without IMDb in Plex
            if not imdb_id and clean_title:
                imdb_id = _tmdb_search_movie(clean_title, year)
            if imdb_id:
                folder_name = f"{folder_name} {{imdb-{imdb_id}}}"

            # Build unique filename per version using quality info from torrent name
            # Multiple torrents for the same movie go into the same folder
            res = parsed.get("resolution", "")
            qual = parsed.get("quality", "")
            version_tag = f"{res} {qual}".strip()
            if version_tag:
                base_name = f"{folder_name} - {clean_name(version_tag)}"
            else:
                base_name = folder_name

            # Deduplicate: if filename already used, append counter
            file_name = f"{base_name}.strm"
            if file_name in used_filenames:
                counter = 2
                while f"{base_name} ({counter}).strm" in used_filenames:
                    counter += 1
                file_name = f"{base_name} ({counter}).strm"
            used_filenames.add(file_name)

            # Track renames: old dir name → new clean name (for Plex DB update)
            old_dir_name = torrent_dir.name
            if old_dir_name != folder_name:
                renames[old_dir_name] = folder_name

            if go_organized:
                link = MOVIES_ORGANIZED / folder_name / file_name
                if ensure_strm_file(strm_file, link, torrent_dir.name):
                    stats["organized_new"] += 1
                    _record_changed(changed, MOVIES_ORGANIZED, link)
                valid_organized.add(link)

            if go_spanish:
                link = MOVIES_SPANISH / folder_name / file_name
                if ensure_strm_file(strm_file, link, torrent_dir.name):
                    stats["spanish_new"] += 1
                    _record_changed(changed, MOVIES_SPANISH, link)
                valid_spanish.add(link)

            if not go_organized and not go_spanish:
                stats["skipped"] += 1

    # NOTE: cleanup_dir for movies is deferred to organize_once() so that
    # movies redirected from shows dir are included in the valid set.

    log.info(
        "Movies: %d organized (%d new), %d spanish (%d new), %d skipped, "
        "%d TV skipped, %d Chinese fixed",
        len(valid_organized), stats["organized_new"],
        len(valid_spanish), stats["spanish_new"],
        stats["skipped"], stats["skipped_tv"], stats["chinese_fixed"],
    )

    return valid_organized, valid_spanish


# ── Shows organization ──────────────────────────────────────────

def _redirect_to_movies(strm_file: Path, title: str, year,
                        spanish_hashes: set, organized_hashes: set,
                        zurg_hash,
                        imdb_map: dict,
                        movie_valid_organized: set, movie_valid_spanish: set,
                        stats: dict, changed: dict,
                        torrent_name: str = ""):
    """Write a movie STRM file (found in shows dir) into movies-organized/movies-spanish."""
    MOVIES_ORGANIZED.mkdir(parents=True, exist_ok=True)
    MOVIES_SPANISH.mkdir(parents=True, exist_ok=True)

    folder_name = f"{title} ({year})" if year else title
    folder_name = clean_name(folder_name)

    # Append IMDb ID if available
    if zurg_hash and zurg_hash in imdb_map:
        folder_name = f"{folder_name} {{imdb-{imdb_map[zurg_hash]}}}"

    dest_name = f"{folder_name}.strm"

    go_organized = True
    go_spanish = False
    if zurg_hash:
        if zurg_hash in spanish_hashes:
            go_spanish = True
    if SPANISH_NAME_RE.search(strm_file.parent.name):
        go_spanish = True

    t_name = torrent_name or strm_file.parent.name

    if go_organized:
        link = MOVIES_ORGANIZED / folder_name / dest_name
        if ensure_strm_file(strm_file, link, t_name):
            stats["redirected_new"] += 1
            _record_changed(changed, MOVIES_ORGANIZED, link)
        movie_valid_organized.add(link)

    if go_spanish:
        link = MOVIES_SPANISH / folder_name / dest_name
        if ensure_strm_file(strm_file, link, t_name):
            stats["redirected_new"] += 1
            _record_changed(changed, MOVIES_SPANISH, link)
        movie_valid_spanish.add(link)


def organize_shows(spanish_hashes: set, organized_hashes: set,
                   imdb_map: dict, renames: dict,
                   movie_valid_organized: set, movie_valid_spanish: set,
                   changed: dict):
    """Organize show .strm files into shows-organized/ and shows-spanish/.

    Items without S/E data are handled as follows:
      - Extras/bonus content → skipped
      - Movies misplaced in shows dir → redirected to movies-organized
      - Collection items (individual movies in a pack) → redirected to movies-organized
      - Main feature (filename ≈ parent dir name) → redirected to movies-organized
      - Numbered files (01.xxx, 02.xxx) → auto-numbered as episodes
    """
    if not ZURG_SHOWS_DIR.exists():
        log.error("Zurg shows dir not found: %s", ZURG_SHOWS_DIR)
        return

    SHOWS_ORGANIZED.mkdir(parents=True, exist_ok=True)
    SHOWS_SPANISH.mkdir(parents=True, exist_ok=True)

    valid_organized = set()
    valid_spanish = set()
    canonical_by_imdb = {}
    canonical_by_title_year = {}
    stats = {"organized_new": 0, "spanish_new": 0, "skipped_extras": 0,
             "redirected": 0, "redirected_new": 0, "specials": 0, "failed": 0}

    for torrent_dir in sorted(ZURG_SHOWS_DIR.iterdir()):
        if not torrent_dir.is_dir():
            continue

        # Strip special chars (™®©) before PTT parsing
        cleaned_dir_name = SPECIAL_CHARS_RE.sub('', torrent_dir.name)

        # Parse the torrent folder name with PTT
        parent_parsed = parse_title(cleaned_dir_name)
        parent_title = parent_parsed.get("title", "")
        parent_seasons = parent_parsed.get("seasons", [])
        parent_year = parent_parsed.get("year")

        # Chinese-prefixed torrent handling for shows
        if parent_title and NON_LATIN_TITLE_RE.match(parent_title):
            eng_parsed, found = _extract_english_from_chinese_torrent(torrent_dir.name)
            if found:
                parent_parsed = eng_parsed
                parent_title = eng_parsed.get("title", parent_title)
                parent_seasons = eng_parsed.get("seasons", parent_seasons)
                parent_year = eng_parsed.get("year") or parent_year

        # Resolve show-level IMDb ID from first episode's hash
        show_imdb = None
        for first_strm in torrent_dir.rglob("*.strm"):
            h = read_strm_hash(first_strm)
            if h and h in imdb_map:
                show_imdb = imdb_map[h]
                break

        # Build clean show folder name: "Title (Year) {imdb-ttXXXX}"
        show_title = normalize_show_title(parent_title, torrent_dir.name)
        if not show_title:
            show_title = "Unknown"
        show_folder = show_title
        if parent_year:
            show_folder = f"{show_title} ({parent_year})"
        if show_imdb:
            show_folder = f"{show_folder} {{imdb-{show_imdb}}}"

        # Canonicalize folder naming to avoid duplicate show dirs for the
        # same show across differently-named torrents.
        if show_imdb:
            show_folder = canonical_by_imdb.setdefault(show_imdb, show_folder)
        else:
            key = (show_title.lower().strip(), int(parent_year) if parent_year else 0)
            show_folder = canonical_by_title_year.setdefault(key, show_folder)

        # Track renames for Plex DB update
        old_dir_name = torrent_dir.name
        if old_dir_name != show_folder:
            renames[old_dir_name] = show_folder

        for strm_file in sorted(torrent_dir.rglob("*.strm")):
            zurg_hash = read_strm_hash(strm_file)

            # Determine language destinations
            go_organized = True
            go_spanish = False

            if zurg_hash:
                if zurg_hash in spanish_hashes:
                    go_spanish = True
                if zurg_hash in organized_hashes:
                    go_organized = True
                elif zurg_hash in spanish_hashes and zurg_hash not in organized_hashes:
                    go_organized = True

            # Fallback: check torrent dir name for Spanish keywords
            if not go_spanish and SPANISH_NAME_RE.search(torrent_dir.name):
                go_spanish = True

            # Parse filename for show/season/episode
            stem = strm_file.stem
            file_parsed = parse_title(SPECIAL_CHARS_RE.sub('', stem))

            title = show_title
            seasons = file_parsed.get("seasons", []) or parent_seasons
            episodes = file_parsed.get("episodes", [])

            # ── Normal case: S/E data found ─────────────────────────
            if seasons or episodes:
                season_num = seasons[0] if seasons else 0
                season_folder = f"Season {season_num:02d}" if season_num > 0 else "Season 00"

                if episodes:
                    if seasons:
                        ep_tag = f"S{season_num:02d}E{episodes[0]:02d}"
                    else:
                        ep_tag = f"E{episodes[0]:02d}"
                    dest_name = f"{title} {ep_tag}.strm"
                else:
                    dest_name = f"{title} - {clean_name(stem)}.strm"

                if go_organized:
                    link = SHOWS_ORGANIZED / show_folder / season_folder / dest_name
                    if ensure_strm_file(strm_file, link, torrent_dir.name):
                        stats["organized_new"] += 1
                        _record_changed(changed, SHOWS_ORGANIZED, link)
                    valid_organized.add(link)

                if go_spanish:
                    link = SHOWS_SPANISH / show_folder / season_folder / dest_name
                    if ensure_strm_file(strm_file, link, torrent_dir.name):
                        stats["spanish_new"] += 1
                        _record_changed(changed, SHOWS_SPANISH, link)
                    valid_spanish.add(link)
                continue

            # ── No S/E data — apply PTT-based fallback logic ────────

            # 1. Extras/bonus content (PTT extras/trash, "Extras -" prefix,
            #    or bare title alongside a movie parent)
            if _is_likely_extras(file_parsed, parent_parsed, stem):
                stats["skipped_extras"] += 1
                log.debug("Skipped (extras): %s / %s", torrent_dir.name, strm_file.name)
                continue

            # 2. Standalone movie (file has year, or name matches parent dir)
            if _is_file_a_movie(file_parsed, parent_parsed, torrent_dir.name, stem):
                file_year = file_parsed.get("year") or parent_year
                file_title = file_parsed.get("title") or parent_title or "Unknown"
                _redirect_to_movies(strm_file, normalize_title(file_title, stem), file_year,
                                    spanish_hashes, organized_hashes, zurg_hash,
                                    imdb_map,
                                    movie_valid_organized, movie_valid_spanish, stats, changed,
                                    torrent_name=torrent_dir.name)
                stats["redirected"] += 1
                continue

            # 3. Numbered files: "01.F1..." or leading digits → treat as episodes
            num_match = re.match(r'^(\d{1,3})[\.\s\-_]', stem)
            if num_match:
                ep_num = int(num_match.group(1))
                season_folder = "Season 00"
                dest_name = f"{title} E{ep_num:02d}.strm"
                if go_organized:
                    link = SHOWS_ORGANIZED / show_folder / season_folder / dest_name
                    if ensure_strm_file(strm_file, link, torrent_dir.name):
                        stats["organized_new"] += 1
                        _record_changed(changed, SHOWS_ORGANIZED, link)
                    valid_organized.add(link)
                if go_spanish:
                    link = SHOWS_SPANISH / show_folder / season_folder / dest_name
                    if ensure_strm_file(strm_file, link, torrent_dir.name):
                        stats["spanish_new"] += 1
                        _record_changed(changed, SHOWS_SPANISH, link)
                    valid_spanish.add(link)
                continue

            # 4. Show specials: parent dir is a real show (has other files
            #    with S/E data), and this file has resolution or quality
            #    (i.e. it's a real episode/special, not a short bonus clip).
            #    → place in Season 00 with a cleaned filename.
            if (file_parsed.get('resolution') or file_parsed.get('quality')):
                season_folder = "Season 00"
                special_title = file_parsed.get("title") or clean_name(stem)
                dest_name = f"{title} - {clean_name(special_title)}.strm"
                if go_organized:
                    link = SHOWS_ORGANIZED / show_folder / season_folder / dest_name
                    if ensure_strm_file(strm_file, link, torrent_dir.name):
                        stats["organized_new"] += 1
                        _record_changed(changed, SHOWS_ORGANIZED, link)
                    valid_organized.add(link)
                if go_spanish:
                    link = SHOWS_SPANISH / show_folder / season_folder / dest_name
                    if ensure_strm_file(strm_file, link, torrent_dir.name):
                        stats["spanish_new"] += 1
                        _record_changed(changed, SHOWS_SPANISH, link)
                    valid_spanish.add(link)
                stats["specials"] += 1
                continue

            # 5. Still unresolved — skip as likely extras
            stats["skipped_extras"] += 1
            log.debug("Skipped (no S/E, bare title — likely extras): %s / %s",
                      torrent_dir.name, strm_file.name)

    cleanup_dir(SHOWS_ORGANIZED, valid_organized, changed)
    cleanup_dir(SHOWS_SPANISH, valid_spanish, changed)

    log.info(
        "Shows: %d organized (%d new), %d spanish (%d new), "
        "%d extras skipped, %d redirected to movies, %d specials",
        len(valid_organized), stats["organized_new"],
        len(valid_spanish), stats["spanish_new"],
        stats["skipped_extras"], stats["redirected"], stats["specials"],
    )


# ── Main ─────────────────────────────────────────────────────────

def organize_once():
    """Single pass: query Plex DB for languages, organize all STRM files."""
    log.info("Starting organization pass...")

    # Query Plex DB for language info and IMDb mappings
    spanish_hashes = get_spanish_hashes()
    organized_hashes = get_english_or_unanalyzed_hashes()
    other_hashes = get_nonenglish_nonspanish_hashes()
    imdb_map = get_imdb_map()
    # Non-English non-Spanish analyzed items also go to organized
    organized_hashes.update(other_hashes)

    # Track folder renames: {old_dir_name: new_clean_name}
    renames = {}
    changed = {
        MOVIES_ORGANIZED: set(),
        SHOWS_ORGANIZED: set(),
        MOVIES_SPANISH: set(),
        SHOWS_SPANISH: set(),
    }

    # Movies first — returns valid sets so shows can redirect movies there
    movie_valid_organized, movie_valid_spanish = organize_movies(
        spanish_hashes, organized_hashes, imdb_map, renames, changed)

    # Shows — may redirect movies-in-shows to movies-organized/movies-spanish
    organize_shows(spanish_hashes, organized_hashes, imdb_map, renames,
                   movie_valid_organized, movie_valid_spanish, changed)

    # Now clean up movie dirs (after shows has added any redirected movies)
    cleanup_dir(MOVIES_ORGANIZED, movie_valid_organized, changed)
    cleanup_dir(MOVIES_SPANISH, movie_valid_spanish, changed)

    # Update Plex DB directory paths for renamed folders
    if renames:
        log.info("Updating %d renamed directories in Plex DB...", len(renames))
        pg_update_directory_paths(renames)

    # Deduplicate Plex DB metadata_items (shows + movies) per library
    deduplicate_plex_db()

    write_changed_paths(changed)

    log.info("Organization pass complete")


def main():
    watch = "--watch" in sys.argv
    interval = 300  # 5 minutes

    for arg in sys.argv[1:]:
        if arg.startswith("--interval="):
            interval = int(arg.split("=")[1])

    if watch:
        log.info("Watch mode: running every %ds", interval)
        while True:
            organize_once()
            time.sleep(interval)
    else:
        organize_once()


if __name__ == "__main__":
    main()
