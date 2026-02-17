#!/usr/bin/env python3
"""plex-strm: Inject streaming URLs into Plex database for Direct Play.

Reads .strm files, replaces local paths with HTTP URLs in the Plex database,
runs FFprobe to extract codec metadata, and optionally downloads subtitles.

Supports both SQLite and PostgreSQL (via plex-postgresql) backends.
"""

import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from db import connect_from_args, resolve_library_ids, library_join, backup_database
from ffprobe import run_ffprobe, update_media_item, create_media_streams
from protect import (install_protection, backup_existing_urls,
                     cmd_protect, cmd_unprotect, cmd_status, cmd_revert)
from subtitles import download_subtitles
from zurg import (build_zurgtorrent_index, extract_rd_id_from_url,
                  trigger_repair_all, trigger_repair_torrents,
                  is_server_error_url, cleanup_broken_torrents)

log = logging.getLogger("plex-strm")


def cmd_update(args):
    """Read .strm files, inject URLs, run FFprobe, optionally download subtitles."""
    db = connect_from_args(args)

    try:
        backup_database(args, db)

        # Install protection if requested
        if args.protect:
            install_protection(db)
            db.commit()
            log.info("Protection triggers installed")

        # Resolve library filter
        lib_ids = resolve_library_ids(db, getattr(args, 'library', None))

        # Find .strm paths
        cur = db.cursor()
        join, lib_where, lib_params = library_join(db, lib_ids, "mp")
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
                direct_url = direct_url.replace(
                    f"{old.scheme}://{old.netloc}", f"{new.scheme}://{new.netloc}", 1)

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
            backup_existing_urls(db)
            db.commit()

        # Also find existing HTTP URLs missing media_streams
        cur = db.cursor()
        ph = "%s" if db.is_pg else "?"
        join, lib_where, lib_params = library_join(db, lib_ids, "mp")
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

        # --reanalyze: re-probe items with incomplete stream data
        reanalyze_max = getattr(args, 'reanalyze', None)
        if reanalyze_max is not None:
            cur = db.cursor()
            ph = "%s" if db.is_pg else "?"
            join, lib_where, lib_params = library_join(db, lib_ids, "mp")
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
            failed_items = []
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

            # Zurg repair + retry
            zurg_url = getattr(args, 'zurg_url', None)
            zurg_data_dir = getattr(args, 'zurg_data_dir', None)
            server_error_items = [(mid, url) for mid, url in failed_items
                                  if is_server_error_url(mid, url)]
            if zurg_url and server_error_items:
                log.info("Triggering Zurg repair for %d failed items...",
                         len(server_error_items))

                rd_index, hash_states = {}, {}
                if zurg_data_dir:
                    rd_index, hash_states = build_zurgtorrent_index(zurg_data_dir)

                repair_ok = False
                repaired_hashes = set()
                if rd_index:
                    failed_hashes = set()
                    rd_id_to_mid = {}
                    for mid, url in server_error_items:
                        rd_id = extract_rd_id_from_url(url)
                        if rd_id and rd_id in rd_index:
                            h = rd_index[rd_id]
                            failed_hashes.add(h)
                            rd_id_to_mid.setdefault(h, []).append(mid)

                    if failed_hashes:
                        log.info("Mapped %d failed items to %d unique torrents",
                                 len(server_error_items), len(failed_hashes))
                        repaired_hashes = trigger_repair_torrents(zurg_url, failed_hashes)
                        repair_ok = len(repaired_hashes) > 0
                    else:
                        log.info("No RD IDs matched zurgtorrent index, falling back to repair-all")
                        repair_ok = trigger_repair_all(zurg_url)
                else:
                    repair_ok = trigger_repair_all(zurg_url)

                if repair_ok:
                    n_torrents = len(repaired_hashes) if repaired_hashes else len(server_error_items)
                    repair_wait = min(120, max(15, n_torrents * 2))
                    log.info("Waiting %ds for Zurg repair to complete (%d torrents)...",
                             repair_wait, n_torrents)
                    time.sleep(repair_wait)

                    if repaired_hashes and rd_index:
                        retry_items = []
                        for mid, url in server_error_items:
                            rd_id = extract_rd_id_from_url(url)
                            if rd_id and rd_id in rd_index:
                                if rd_index[rd_id] in repaired_hashes:
                                    retry_items.append((mid, url))
                            else:
                                retry_items.append((mid, url))
                    else:
                        retry_items = server_error_items

                    log.info("Retrying %d items after repair (of %d total failed)...",
                             len(retry_items), len(server_error_items))
                    retry_ok = 0
                    retry_fail = 0
                    retry_mapping = {mid: url for mid, url in retry_items}
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futs = {
                            executor.submit(
                                run_ffprobe, retry_mapping[mid],
                                ffprobe_path, timeout, 1
                            ): mid
                            for mid in retry_mapping
                        }
                        for fut in as_completed(futs):
                            mid = futs[fut]
                            try:
                                meta = fut.result()
                            except Exception:
                                meta = None
                            if meta:
                                update_media_item(db, mid, dict(meta))
                                create_media_streams(db, mid, dict(meta))
                                retry_ok += 1
                                analyzed += 1
                                failed -= 1
                                failed_items = [(m, u) for m, u in failed_items if m != mid]
                            else:
                                retry_fail += 1
                            if (retry_ok + retry_fail) % 10 == 0:
                                db.commit()
                    db.commit()
                    log.info("Post-repair retry: %d recovered, %d still failed",
                             retry_ok, retry_fail)

            # Write failed items log
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

            # Cleanup broken torrents
            cleanup = getattr(args, 'cleanup_broken', False)
            cleanup_dry = getattr(args, 'cleanup_broken_dry_run', False)
            if (cleanup or cleanup_dry) and zurg_data_dir:
                rd_token = os.environ.get("RD_API_TOKEN")
                if rd_token:
                    cleanup_broken_torrents(zurg_data_dir, rd_token, dry_run=cleanup_dry)
                else:
                    log.warning("--cleanup-broken requires RD_API_TOKEN env var")

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
    p_update.add_argument("--zurg-url", metavar="URL",
                          help="Zurg base URL for triggering repair on 5XX failures "
                               "(e.g. http://user:pass@localhost:9091)")
    p_update.add_argument("--zurg-data-dir", metavar="DIR",
                           help="Path to Zurg data directory containing .zurgtorrent files. "
                                "Enables per-torrent repair instead of repair-all. "
                                "(e.g. /Users/sander/bin/zurg/data)")
    p_update.add_argument("--cleanup-broken", action="store_true",
                           help="Delete fully broken+unfixable torrents from RealDebrid. "
                                "Only removes torrents where Zurg confirmed repair is impossible "
                                "(Unfixable field set) AND all files are broken/deleted. "
                                "Requires --zurg-data-dir and RD_API_TOKEN env var.")
    p_update.add_argument("--cleanup-broken-dry-run", action="store_true",
                           help="Show which broken torrents would be deleted without actually deleting")

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
