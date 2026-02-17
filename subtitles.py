"""OpenSubtitles integration: search, download, and Plex DB registration."""

import logging
import os
import re
import time

log = logging.getLogger("plex-strm")


def clean_title_for_search(title):
    """Clean a title for OpenSubtitles search.

    Strips CJK characters, Cyrillic, website spam, bracketed junk,
    and tries to extract the English portion of the title.
    """
    if not title:
        return ""
    title = re.sub(r'[\u3010\[][^\u3011\]]*[\u3011\]]', ' ', title)
    title = re.sub(r'(?i)\b(www\s*\.?\s*\w+\s*\.?\s*(?:com|net|org|info))\b', ' ', title)
    title = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]+', ' ', title)
    title = re.sub(r'[\u0400-\u04ff]+', ' ', title)
    title = re.sub(r'[\u0600-\u06ff]+', ' ', title)
    title = re.sub(r'\(\d{4}\)', ' ', title)
    title = re.sub(r'(?i)\b(bluray|bdrip|dvdrip|webrip|web-dl|hdtv|hdrip|x264|x265|h264|h265|'
                   r'aac|ac3|dts|720p|1080p|2160p|4k|remux|remastered|extended|uncut)\b', ' ', title)
    title = re.sub(r"[^a-zA-Z0-9'\-\s]", ' ', title)
    title = ' '.join(title.split()).strip()
    if len(title) < 2:
        return ""
    return title


def validate_imdb_id(imdb_id):
    """Return imdb_id if it looks valid (tt followed by digits), else None."""
    if not imdb_id:
        return None
    if not re.match(r'^tt\d+$', imdb_id):
        return None
    return imdb_id


def subtitle_search(api_key, token, imdb_id=None, tmdb_id=None, title=None, year=None, lang="en"):
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
        clean = clean_title_for_search(title)
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

        feat = attrs.get("feature_details", {})
        feat_year = feat.get("year")
        feat_imdb = feat.get("imdb_id")

        if searched_by == "title" and year and feat_year:
            if abs(int(feat_year) - int(year)) > 1:
                continue

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


def subtitle_download(api_key, token, file_id, output_path):
    """Download a subtitle file from OpenSubtitles."""
    import requests
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "plex-strm/1.0",
    }
    resp = requests.post("https://api.opensubtitles.com/api/v1/download",
                         headers=headers,
                         json={"file_id": int(file_id), "sub_format": "srt"},
                         timeout=30)
    if resp.status_code != 200:
        return False
    link = resp.json().get("link")
    if not link:
        return False

    resp = requests.get(link, timeout=60)
    if resp.status_code != 200:
        return False

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(resp.content)
    return os.path.getsize(output_path) > 100


def opensub_login(api_key, username, password):
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


def get_metadata_for_subtitle(db, media_item_id):
    """Get title, year, imdb_id for a media item."""
    cur = db.cursor()
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

    if db.is_pg:
        db.execute(cur, "SELECT title, original_title, year, guid FROM metadata_items WHERE id = %s", (mid,))
    else:
        db.execute(cur, "SELECT title, original_title, year, guid FROM metadata_items WHERE id = ?", (mid,))
    row = db.fetchone(cur)
    if not row:
        cur.close()
        return None

    original = row.get("original_title") or ""
    info = {"title": row["title"], "original_title": original if original != row["title"] else "",
            "year": row["year"], "imdb_id": None,
            "tmdb_id": None, "tvdb_id": None,
            "metadata_item_id": mid, "media_item_id": media_item_id}

    ph = "%s" if db.is_pg else "?"
    like_esc = "%%" if db.is_pg else "%"

    ext_id_queries = [
        (f"SELECT t.tag FROM taggings tg "
         f"JOIN tags t ON tg.tag_id = t.id "
         f"WHERE tg.metadata_item_id = {ph} "
         f"AND (t.tag LIKE 'imdb://{like_esc}' "
         f"  OR t.tag LIKE 'tmdb://{like_esc}' "
         f"  OR t.tag LIKE 'tvdb://{like_esc}')"),
        (f"SELECT guid AS tag FROM guids "
         f"WHERE metadata_item_id = {ph} "
         f"AND (guid LIKE 'imdb://{like_esc}' "
         f"  OR guid LIKE 'tmdb://{like_esc}' "
         f"  OR guid LIKE 'tvdb://{like_esc}')"),
    ]

    for ext_sql in ext_id_queries:
        if info["imdb_id"] and info["tmdb_id"]:
            break
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

    if not info["imdb_id"]:
        match = re.search(r"(tt\d+)", row.get("guid", ""))
        if match:
            info["imdb_id"] = match.group(1)

    if not info["imdb_id"] and not info["tmdb_id"] and info["tvdb_id"]:
        tmdb_api_key = os.environ.get("TMDB_API_KEY")
        if tmdb_api_key:
            converted = tvdb_to_tmdb(info["tvdb_id"], tmdb_api_key)
            if converted:
                info["tmdb_id"] = converted

    cur.close()
    return info


def tvdb_to_tmdb(tvdb_id, tmdb_api_key):
    """Convert a TVDB ID to a TMDB ID via the TMDB API. Returns TMDB ID string or None."""
    import requests
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


def get_existing_subtitle_langs(db, media_item_id):
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


def register_subtitle_in_db(db, media_item_id, subtitle_path, language):
    """Register a subtitle file as media_streams entry (stream_type_id=3)."""
    cur = db.cursor()

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

    token = opensub_login(api_key, username, password)
    if not token:
        log.error("OpenSubtitles login failed")
        return

    langs = os.environ.get("SUBTITLE_LANGS", "en").split(",")
    sub_dir = os.environ.get("SUBTITLE_DIR", "./subtitles")
    os.makedirs(sub_dir, exist_ok=True)

    success = 0
    skipped = 0
    for mid in media_item_ids:
        info = get_metadata_for_subtitle(db, mid)
        if not info:
            continue
        title = info["title"]
        safe_title = re.sub(r'[<>:"/\\|?*]', "_", title)

        imdb_id = validate_imdb_id(info.get("imdb_id"))

        existing_langs = set()
        if mode == "missing":
            existing_langs = get_existing_subtitle_langs(db, mid)

        for lang in langs:
            lang = lang.strip()

            if mode == "missing" and lang in existing_langs:
                skipped += 1
                continue

            tmdb_id = info.get("tmdb_id")
            if imdb_id:
                results = subtitle_search(api_key, token, imdb_id=imdb_id, lang=lang)
            elif tmdb_id:
                results = subtitle_search(api_key, token, tmdb_id=tmdb_id, lang=lang)
            else:
                results = subtitle_search(api_key, token, title=title,
                                           year=info["year"], lang=lang)
                if not results and info.get("original_title"):
                    results = subtitle_search(api_key, token, title=info["original_title"],
                                               year=info["year"], lang=lang)
            if not results:
                continue

            fname = f"{safe_title}.{lang}.srt"
            fpath = os.path.join(sub_dir, fname)
            if os.path.exists(fpath) and mode == "missing":
                log.debug("Subtitle already exists on disk: %s", fpath)
                register_subtitle_in_db(db, mid, fpath, lang)
                skipped += 1
                continue

            if subtitle_download(api_key, token, results[0]["id"], fpath):
                register_subtitle_in_db(db, mid, fpath, lang)
                log.info("Subtitle: %s", fname)
                success += 1
                time.sleep(0.3)

    log.info("Subtitles: %d downloaded, %d skipped (mode=%s)", success, skipped, mode)
