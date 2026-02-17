"""FFprobe analysis and Plex media stream management."""

import json
import logging
import os
import subprocess
import time

log = logging.getLogger("plex-strm")


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
                if any(x in stderr_snip.lower() for x in ("server error", "server returned",
                        "403", "404", "410", "5xx", "forbidden", "not found")):
                    log.warning("FFprobe FAILED [%s]: %s", url_id, last_error)
                    return None
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

    meta["_video_streams"] = video_streams
    meta["_audio_streams"] = audio_streams
    meta["_subtitle_streams"] = subtitle_streams

    # Primary video stream for media_items fields
    if video_streams:
        video_stream = video_streams[0]
        meta["width"] = video_stream.get("width", 1920)
        meta["height"] = video_stream.get("height", 1080)
        meta["video_codec"] = video_stream.get("codec_name", "h264")

        dar = video_stream.get("display_aspect_ratio", "")
        if ":" in dar:
            w, h = dar.split(":")
            try:
                meta["display_aspect_ratio"] = round(float(w) / float(h), 4)
            except (ValueError, ZeroDivisionError):
                pass

        rfr = video_stream.get("r_frame_rate", "")
        if "/" in rfr:
            num, den = rfr.split("/")
            try:
                meta["frames_per_second"] = round(float(num) / float(den), 3)
            except (ValueError, ZeroDivisionError):
                pass

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
    video_profile = meta.pop("video_profile", None)
    audio_profile = meta.pop("audio_profile", None)
    color_trc = meta.pop("color_trc", None)

    profile_meta = {}
    if video_profile:
        profile_meta["video_profile"] = video_profile
    if audio_profile:
        profile_meta["audio_profile"] = audio_profile
    extra_data = _build_extra_data(profile_meta)
    if extra_data:
        meta["extra_data"] = extra_data

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

    ph = "%s" if db.is_pg else "?"
    db.execute(cur, f"SELECT id FROM media_parts WHERE media_item_id = {ph} LIMIT 1",
               (media_item_id,))
    row = db.fetchone(cur)
    if not row:
        cur.close()
        return
    media_part_id = row["id"]

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
        channels = aus.get("channels")
        bitrate = aus.get("bit_rate")
        if bitrate:
            bitrate = int(bitrate)

        disp = aus.get("disposition", {})
        is_default = 1 if disp.get("default") else 0
        is_forced = 1 if disp.get("forced") else 0

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
