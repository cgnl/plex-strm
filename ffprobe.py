"""FFprobe analysis and Plex media stream management."""

import json
import logging
import os
import subprocess
import threading
import time

log = logging.getLogger("plex-strm")


class TokenBucket:
    """Simple token bucket rate limiter (thread-safe).

    Allows up to `rate` requests per second with bursts up to `burst`.
    Call acquire() before each request — it blocks until a token is available.
    """

    def __init__(self, rate_per_sec, burst=None):
        self.rate = rate_per_sec
        self.burst = burst or max(1, int(rate_per_sec / 2))
        self.tokens = float(self.burst)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            time.sleep(0.05)  # 50ms poll

# FFprobe codec name -> Plex codec name
CODEC_MAP = {
    "dts": "dca",
    "subrip": "srt",
    "ass": "ssa",
    "mov_text": "mov_text",
    "dvd_subtitle": "dvdsub",
    "hdmv_pgs_subtitle": "pgs",
}

# FFprobe 3-letter language -> Plex 2-letter language (ISO 639-2 -> 639-1)
# Only common languages; unknown codes pass through as-is.
LANG_MAP = {
    "eng": "en", "deu": "de", "ger": "de", "fra": "fr", "fre": "fr",
    "spa": "es", "ita": "it", "por": "pt", "nld": "nl", "dut": "nl",
    "rus": "ru", "jpn": "ja", "zho": "zh", "chi": "zh", "kor": "ko",
    "ara": "ar", "hin": "hi", "tur": "tr", "pol": "pl", "swe": "sv",
    "nor": "no", "dan": "da", "fin": "fi", "ces": "cs", "cze": "cs",
    "slk": "sk", "slo": "sk", "hun": "hu", "ron": "ro", "rum": "ro",
    "bul": "bg", "hrv": "hr", "srp": "sr", "ukr": "uk", "ell": "el",
    "gre": "el", "heb": "he", "tha": "th", "vie": "vi", "ind": "id",
    "msa": "ms", "may": "ms", "cat": "ca", "eus": "eu", "baq": "eu",
    "glg": "gl", "isl": "is", "ice": "is", "lav": "lv", "lit": "lt",
    "est": "et", "slv": "sl", "tam": "ta", "tel": "te", "ben": "bn",
    "und": "und",
}


def _plex_codec(ffprobe_codec):
    """Convert ffprobe codec name to Plex convention."""
    return CODEC_MAP.get(ffprobe_codec, ffprobe_codec)


def _plex_lang(lang_3):
    """Convert 3-letter language code to 2-letter for Plex."""
    if not lang_3:
        return None
    if len(lang_3) == 2:
        return lang_3
    return LANG_MAP.get(lang_3, lang_3)


def _normalize_audio_profile(profile_str):
    """Normalize ffprobe audio profile to Plex convention.

    Plex strips codec prefixes: 'DTS-HD MA' -> 'ma', 'HE-AAC' -> 'he-aac', etc.
    """
    if not profile_str:
        return None
    p = profile_str.strip()
    low = p.lower()
    # DTS variants: "DTS-HD MA" -> "ma", "DTS-HD HRA" -> "hra", "DTS:X" -> "x", "DTS-ES" -> "es"
    if low.startswith("dts"):
        if "ma" in low:
            return "ma"
        if "hra" in low or "high resolution" in low:
            return "hra"
        if "es" in low:
            return "es"
        if ":x" in low or " x" in low:
            return "x"
        return "dts"  # plain DTS gets profile "dts" in Plex
    # AAC variants
    if "lc" in low or "low complexity" in low:
        return "lc"
    if "he" in low or "high efficiency" in low:
        return "he-aac" if "v2" not in low else "he-aac v2"
    # TrueHD, FLAC, etc. — no profile
    if "main" in low:
        return "main"
    if low:
        return low
    return None


def run_ffprobe(url, ffprobe_path="ffprobe", timeout=30, retries=2, rate_limiter=None):
    """Run ffprobe on a URL and return parsed metadata dict, or None on failure.

    Retries up to `retries` times on timeout or transient errors.
    If rate_limiter (TokenBucket) is provided, acquires a token before each attempt.
    """
    cmd = [ffprobe_path, "-v", "error",
           "-probesize", "128000", "-analyzeduration", "500000",
           "-print_format", "json",
           "-show_format", "-show_streams", "-show_chapters", url]
    url_id = url.rsplit("/", 1)[-1] if "/" in url else url[-30:]
    last_error = None

    # Errors that mean the link is permanently dead — no point retrying
    _permanent_errors = ("403", "404", "410", "forbidden", "not found", "gone")

    for attempt in range(1 + retries):
        if rate_limiter:
            rate_limiter.acquire()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                     encoding="utf-8", errors="ignore", timeout=timeout)
            if result.returncode != 0:
                stderr_snip = (result.stderr or "")[:300].strip()
                last_error = f"exit code {result.returncode}: {stderr_snip}"
                stderr_low = stderr_snip.lower()

                # Permanent errors: don't retry
                if any(x in stderr_low for x in _permanent_errors):
                    log.warning("FFprobe FAILED [%s]: %s", url_id, last_error)
                    return None

                # 5XX / transient: retry with exponential backoff
                if attempt < retries:
                    backoff = 2 ** attempt  # 1s, 2s, 4s
                    log.debug("FFprobe retry %d/%d [%s] in %ds: %s",
                              attempt + 1, retries, url_id, backoff, last_error)
                    time.sleep(backoff)
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
                time.sleep(2 ** attempt)
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
        container = fmt["format_name"].split(",")[0]
        # Normalize container names to match Plex conventions
        container_map = {"matroska": "mkv", "mov,mp4,m4a,3gp,3g2,mj2": "mp4"}
        meta["container"] = container_map.get(container, container)

    video_streams = []
    audio_streams = []
    subtitle_streams = []

    # Codecs that are embedded images/thumbnails, not real video streams
    _thumbnail_codecs = {"mjpeg", "png", "bmp", "gif", "tiff", "webp"}
    has_thumbnail = False

    for s in data.get("streams", []):
        ct = s.get("codec_type")
        if ct == "video":
            # Skip embedded cover art / thumbnails (Plex ignores these)
            if s.get("codec_name") in _thumbnail_codecs:
                has_thumbnail = True
                continue
            # Also skip if disposition says attached_pic
            if (s.get("disposition") or {}).get("attached_pic"):
                has_thumbnail = True
                continue
            video_streams.append(s)
        elif ct == "audio":
            audio_streams.append(s)
        elif ct == "subtitle":
            subtitle_streams.append(s)

    meta["_has_thumbnail"] = has_thumbnail

    meta["_video_streams"] = video_streams
    meta["_audio_streams"] = audio_streams
    meta["_subtitle_streams"] = subtitle_streams

    # Primary video stream for media_items fields
    if video_streams:
        video_stream = video_streams[0]
        meta["width"] = video_stream.get("width", 1920)
        meta["height"] = video_stream.get("height", 1080)
        meta["video_codec"] = _plex_codec(video_stream.get("codec_name", "h264"))

        dar = video_stream.get("display_aspect_ratio", "")
        if ":" in dar:
            w, h = dar.split(":")
            try:
                meta["display_aspect_ratio"] = float(w) / float(h)
            except (ValueError, ZeroDivisionError):
                pass

        rfr = video_stream.get("r_frame_rate", "")
        if "/" in rfr:
            num, den = rfr.split("/")
            try:
                meta["frames_per_second"] = float(num) / float(den)
            except (ValueError, ZeroDivisionError):
                pass

        profile = (video_stream.get("profile") or "").lower()
        bit_depth = video_stream.get("bits_per_raw_sample")
        if "baseline" in profile:
            meta["video_profile"] = "baseline"
        elif "high" in profile and "10" in profile:
            meta["video_profile"] = "high 10"
        elif "high" in profile:
            meta["video_profile"] = "high"
        elif "main" in profile and "10" in profile:
            meta["video_profile"] = "main 10"
        elif "main" in profile:
            # Check bit depth: 10-bit HEVC reports "Main" but is really "main 10"
            if bit_depth and int(bit_depth) > 8:
                meta["video_profile"] = "main 10"
            else:
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
        meta["audio_codec"] = _plex_codec(audio_stream.get("codec_name", "aac"))
        meta["audio_channels"] = audio_stream.get("channels", 2)

        a_profile = _normalize_audio_profile(audio_stream.get("profile"))
        if a_profile:
            meta["audio_profile"] = a_profile

    # Chapters
    chapters = data.get("chapters", [])
    if chapters:
        ch_list = []
        for ch in chapters:
            ch_entry = {}
            ch_tags = ch.get("tags", {})
            ch_entry["name"] = ch_tags.get("title", "")
            # ffprobe chapters use time_base; start_time/end_time are in seconds
            start = ch.get("start_time")
            end = ch.get("end_time")
            if start is not None:
                ch_entry["start"] = float(start)
            if end is not None:
                ch_entry["end"] = float(end)
            ch_list.append(ch_entry)
        if ch_list:
            meta["_chapters"] = ch_list

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

    # Always set media_analysis_version to 6 (current Plex version)
    meta["media_analysis_version"] = 6

    updatable = {k: v for k, v in meta.items()
                 if k in ("duration", "size", "bitrate", "container", "width", "height",
                           "frames_per_second", "display_aspect_ratio", "video_codec",
                           "audio_codec", "audio_channels", "extra_data",
                           "media_analysis_version")}
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


def update_media_part(db, media_item_id, meta, strm_url=None):
    """UPDATE media_parts with real file size and extra_data.

    Uses file size from FFprobe format output (no extra HEAD request needed).
    Builds Plex-compatible extra_data with container, videoProfile, and chapters.
    """
    ph = "%s" if db.is_pg else "?"

    # Get the media_part ID
    cur = db.cursor()
    db.execute(cur, f"SELECT id, file FROM media_parts WHERE media_item_id = {ph} LIMIT 1",
               (media_item_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return
    part_id = row[0]

    # Use file size from FFprobe (already parsed from format.size)
    real_size = meta.get("size")

    # Build extra_data for media_parts
    container = meta.get("container", "mkv")
    video_profile = meta.get("video_profile", "")
    audio_profile = meta.get("audio_profile", "")
    extra = {}
    url_parts = []

    if audio_profile:
        extra["ma:audioProfile"] = audio_profile
        url_parts.append(f"ma%3AaudioProfile={audio_profile}")

    extra["ma:container"] = container
    url_parts.append(f"ma%3Acontainer={container}")

    if meta.get("_has_thumbnail"):
        extra["ma:hasThumbnail"] = "1"
        url_parts.append("ma%3AhasThumbnail=1")

    if video_profile:
        extra["ma:videoProfile"] = video_profile
        url_parts.append(f"ma%3AvideoProfile={video_profile}")

    # Chapters
    chapters = meta.get("_chapters")
    if chapters:
        ch_json = json.dumps({"Chapters": {"Chapter": chapters}})
        extra["pv:chapters"] = ch_json
        from urllib.parse import quote
        url_parts.append(f"pv%3Achapters={quote(ch_json, safe='')}")

    if url_parts:
        extra["url"] = "&".join(url_parts)

    extra_data = json.dumps(extra) if extra else None

    # Update media_parts
    updates = {}
    if real_size and real_size > 1000:
        updates["size"] = real_size
    if extra_data:
        updates["extra_data"] = extra_data

    if updates:
        sets = ", ".join(f"{k} = {ph}" for k in updates)
        vals = list(updates.values()) + [part_id]
        db.execute(cur, f"UPDATE media_parts SET {sets} WHERE id = {ph}", tuple(vals))

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
        codec = _plex_codec(vs.get("codec_name") or "")
        if not codec:
            continue

        lang = _plex_lang((vs.get("tags") or {}).get("language"))
        w = vs.get("width")
        h = vs.get("height")
        coded_w = vs.get("coded_width")
        coded_h = vs.get("coded_height")
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
        if coded_h:
            extra_parts["ma:codedHeight"] = str(coded_h)
        if coded_w:
            extra_parts["ma:codedWidth"] = str(coded_w)

        profile = (vs.get("profile") or "").lower()
        vs_bit_depth = vs.get("bits_per_raw_sample")
        plex_profile = None
        if "baseline" in profile:
            plex_profile = "baseline"
        elif "high" in profile and "10" in profile:
            plex_profile = "high 10"
        elif "high" in profile:
            plex_profile = "high"
        elif "main" in profile and "10" in profile:
            plex_profile = "main 10"
        elif "main" in profile:
            if vs_bit_depth and int(vs_bit_depth) > 8:
                plex_profile = "main 10"
            else:
                plex_profile = "main"
        elif profile:
            plex_profile = profile
        if plex_profile:
            extra_parts["ma:profile"] = plex_profile
            extra_parts["ma:videoProfile"] = plex_profile

        # Rich metadata matching Plex analyze
        rfr = vs.get("r_frame_rate", "")
        if "/" in rfr:
            try:
                n, d = rfr.split("/")
                extra_parts["ma:frameRate"] = str(round(float(n) / float(d), 3))
            except (ValueError, ZeroDivisionError):
                pass

        level = vs.get("level")
        if level is not None:
            extra_parts["ma:level"] = str(level)

        refs = vs.get("refs")
        if refs is not None:
            extra_parts["ma:refFrames"] = str(refs)

        bits_per_raw = vs.get("bits_per_raw_sample")
        if bits_per_raw:
            extra_parts["ma:bitDepth"] = str(bits_per_raw)

        pix_fmt = vs.get("pix_fmt", "")
        if "420" in pix_fmt:
            extra_parts["ma:chromaSubsampling"] = "4:2:0"
        elif "422" in pix_fmt:
            extra_parts["ma:chromaSubsampling"] = "4:2:2"
        elif "444" in pix_fmt:
            extra_parts["ma:chromaSubsampling"] = "4:4:4"

        chroma_loc = vs.get("chroma_location")
        if chroma_loc:
            extra_parts["ma:chromaLocation"] = chroma_loc

        field_order = vs.get("field_order", "")
        if field_order in ("progressive", ""):
            extra_parts["ma:scanType"] = "progressive"
        elif field_order != "unknown":
            extra_parts["ma:scanType"] = "interlaced"

        # Color metadata
        color_primaries = vs.get("color_primaries")
        if color_primaries and color_primaries != "unknown":
            extra_parts["ma:colorPrimaries"] = color_primaries
        color_range = vs.get("color_range")
        if color_range and color_range != "unknown":
            extra_parts["ma:colorRange"] = color_range
        color_space = vs.get("color_space")
        if color_space and color_space != "unknown":
            extra_parts["ma:colorSpace"] = color_space
        color_transfer = vs.get("color_transfer")
        if color_transfer and color_transfer != "unknown":
            extra_parts["ma:colorTrc"] = color_transfer

        title = (vs.get("tags") or {}).get("title")
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
        """, (1, media_item_id, codec, lang, now, now, stream_index,
              media_part_id, bitrate, None, None, is_default, is_forced, extra_data))
        stream_index += 1

    # ── Audio streams ────────────────────────────────────────────
    for aus in meta.get("_audio_streams", []):
        codec = _plex_codec(aus.get("codec_name") or "")
        if not codec:
            continue

        lang = _plex_lang((aus.get("tags") or {}).get("language"))
        channels = aus.get("channels")
        bitrate = aus.get("bit_rate")
        if bitrate:
            bitrate = int(bitrate)

        disp = aus.get("disposition", {})
        is_default = 1 if disp.get("default") else 0
        is_forced = 1 if disp.get("forced") else 0

        plex_profile = _normalize_audio_profile(aus.get("profile"))

        extra_parts = {}
        if plex_profile:
            extra_parts["ma:profile"] = plex_profile
            extra_parts["ma:audioProfile"] = plex_profile
        if channels:
            extra_parts["ma:channels"] = str(channels)

        # Rich audio metadata
        channel_layout = aus.get("channel_layout")
        if channel_layout:
            extra_parts["ma:audioChannelLayout"] = channel_layout

        sample_rate = aus.get("sample_rate")
        if sample_rate:
            extra_parts["ma:samplingRate"] = str(sample_rate)

        bits_per_raw = aus.get("bits_per_raw_sample")
        if bits_per_raw and bits_per_raw != "0":
            extra_parts["ma:bitDepth"] = str(bits_per_raw)

        title = (aus.get("tags") or {}).get("title")
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
        """, (2, media_item_id, codec, lang, now, now, stream_index,
              media_part_id, bitrate, channels, None, is_default, is_forced, extra_data))
        stream_index += 1

    # ── Subtitle streams ─────────────────────────────────────────
    for ss in meta.get("_subtitle_streams", []):
        codec = _plex_codec(ss.get("codec_name") or "")
        if not codec:
            continue

        lang = _plex_lang((ss.get("tags") or {}).get("language"))
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
