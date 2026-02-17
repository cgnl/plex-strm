# plex-strm

Inject streaming URLs into the Plex database for Direct Play. Reads `.strm` files, replaces local paths with HTTP URLs, runs FFprobe to populate codec metadata, and optionally downloads subtitles from OpenSubtitles.

Supports both **SQLite** and **PostgreSQL** ([plex-postgresql](https://github.com/cgnl/plex-postgresql)) backends.

## How it works

1. Finds all `.strm` file references in `media_parts`
2. Reads each `.strm` file to get the streaming URL inside
3. Optionally rewrites the base URL (e.g. `localhost` → your public domain)
4. Updates `media_parts.file` with the direct HTTP URL
5. Runs FFprobe on each URL to extract **all** streams (video, audio, subtitle) with correct codecs, languages, channels, and bitrates
6. Creates `media_streams` entries so Plex shows correct info and enables Direct Play
7. Installs SQLite/PostgreSQL triggers that prevent Plex from reverting the URLs during library scans
8. Optionally downloads missing subtitles from OpenSubtitles

## Using with Zurg

[Zurg](https://github.com/debridmediamanager/zurg-testing) can generate `.strm` files with `save_strm_files: true` in its config. These files contain URLs like `http://localhost:9091/strm/<id>` that redirect to Real-Debrid download links.

**The problem:** Plex doesn't proxy HTTP URLs — it sends a 302 redirect to the client. If the URL contains `localhost`, remote clients can't reach it.

**The solution:** Use `--base-url` to rewrite URLs to a publicly reachable address:

```yaml
# zurg config.yml
save_strm_files: true
```

```bash
# Point a Plex library at zurg's strm/ directory, then:
plex-strm update --pg --protect \
  --library "STRM Movies" \
  --base-url https://plex.example.com
```

This rewrites `http://localhost:9091/strm/ABC123` → `https://plex.example.com/strm/ABC123`.

### Reverse proxy setup

Route `/strm/*` to Zurg in your reverse proxy (Caddy, nginx, etc.):

```
# Caddyfile example
plex.example.com {
    handle /strm/* {
        reverse_proxy localhost:9091
    }
    handle {
        reverse_proxy localhost:32400
    }
}
```

This way both Plex and Zurg share the same domain. Clients requesting `/strm/*` hit Zurg directly, everything else goes to Plex.

### Basic authentication

Without protection, anyone who discovers your `/strm/*` endpoint can stream from your Real-Debrid account. Add basic auth in your reverse proxy and embed the credentials in the `--base-url`:

```
# Caddyfile with basic_auth
plex.example.com {
    handle /strm/* {
        basic_auth {
            # Generate hash: caddy hash-password --plaintext 'your-password'
            myuser $2a$14$...hashed-password...
        }
        reverse_proxy localhost:9091
    }
    handle {
        reverse_proxy localhost:32400
    }
}
```

Then pass credentials in the URL so Plex can authenticate:

```bash
plex-strm update --pg --protect \
  --base-url https://myuser:mypassword@plex.example.com
```

This rewrites all STRM URLs to `https://myuser:mypassword@plex.example.com/strm/...`. Plex stores the full URL in the database and sends the credentials when streaming.

> **Note:** Plex's built-in player (Lavf) does not send `Authorization` headers from the URL — it only works when credentials are embedded as `user:pass@host` in the URL itself. This is why `--base-url` includes the credentials rather than relying on header-based auth.

## Install

```bash
pip install -r requirements.txt
```

Requires `ffprobe` (part of ffmpeg) on PATH.

## Usage

```bash
# SQLite
plex-strm update --db /path/to/com.plexapp.plugins.library.db --protect

# PostgreSQL
export PLEX_PG_HOST=localhost PLEX_PG_DATABASE=plex PLEX_PG_USER=plex PLEX_PG_PASSWORD=plex
plex-strm update --pg --protect

# Limit to specific libraries + rewrite URLs
plex-strm update --pg --protect \
  --library "STRM Movies" --library "STRM TV Shows" \
  --base-url https://plex.example.com

# With subtitles (download only missing languages)
export OPENSUB_API_KEY=... OPENSUB_USER=... OPENSUB_PASS=...
plex-strm update --pg --protect --subtitles --subtitle-mode missing

# Re-analyze items with incomplete metadata (e.g. ≤2 streams)
plex-strm update --pg --reanalyze 2 --workers 8
```

### Commands

| Command | Description |
|---------|-------------|
| `update` | Inject URLs from .strm files, run FFprobe, optionally download subtitles |
| `status` | Show protection status, HTTP URL count, backup count |
| `protect` | Install 4-layer trigger protection |
| `unprotect` | Remove all protection triggers |
| `revert` | Revert URLs back to original .strm paths from backup |

### Global options

| Flag | Description |
|------|-------------|
| `--db PATH` | Path to Plex SQLite database |
| `--pg` | Use PostgreSQL (configure via `PLEX_PG_*` env vars) |
| `--library NAME` | Limit to specific library section(s) by name (repeatable) |
| `-v` | Verbose output |

### Options for `update`

| Flag | Default | Description |
|------|---------|-------------|
| `--base-url URL` | | Rewrite STRM base URL for remote access (or env `STRM_BASE_URL`) |
| `--protect` | off | Install trigger protection during update |
| `--subtitles` | off | Download subtitles via OpenSubtitles API |
| `--subtitle-mode` | `missing` | `missing` = only download if language not in DB; `always` = download regardless |
| `--ffprobe PATH` | `ffprobe` | Path to ffprobe binary |
| `--workers N` | `4` | FFprobe parallel workers |
| `--timeout N` | `30` | FFprobe timeout per URL in seconds |
| `--retries N` | `2` | FFprobe retries per URL (only retries on timeouts/transient errors, not on dead links) |
| `--reanalyze N` | off | Re-probe items with ≤ N existing streams (useful for fixing incomplete metadata) |
| `--zurg-url URL` | | Zurg base URL; triggers repair + retry on 5XX failures (e.g. `http://user:pass@localhost:9091`) |
| `--backup-dir DIR` | `.` | Directory for SQLite database backups |

## Environment variables

### Database

| Variable | Description |
|----------|-------------|
| `PLEX_DB` | Path to Plex SQLite database |
| `PLEX_PG_HOST` | PostgreSQL host (enables PG mode) |
| `PLEX_PG_PORT` | PostgreSQL port (default: 5432) |
| `PLEX_PG_DATABASE` | PostgreSQL database (default: plex) |
| `PLEX_PG_USER` | PostgreSQL user (default: plex) |
| `PLEX_PG_PASSWORD` | PostgreSQL password |
| `PLEX_PG_SCHEMA` | PostgreSQL schema (default: plex) |

### FFprobe

| Variable | Description |
|----------|-------------|
| `FFPROBE_PATH` | Path to ffprobe binary (default: ffprobe) |
| `FFPROBE_WORKERS` | Parallel workers (default: 4) |
| `FFPROBE_TIMEOUT` | Timeout per URL in seconds (default: 30) |

### Subtitles

| Variable | Description |
|----------|-------------|
| `OPENSUB_API_KEY` | OpenSubtitles.com API key ([get one here](https://www.opensubtitles.com/consumers)) |
| `OPENSUB_USER` | OpenSubtitles username |
| `OPENSUB_PASS` | OpenSubtitles password |
| `SUBTITLE_LANGS` | Comma-separated language codes (default: `en`) |
| `SUBTITLE_DIR` | Directory for downloaded .srt files (default: `./subtitles`) |
| `TMDB_API_KEY` | TMDB API key for TVDB→TMDB conversion ([get one here](https://www.themoviedb.org/settings/api)) |

### URL rewriting

| Variable | Description |
|----------|-------------|
| `STRM_BASE_URL` | Rewrite base URL (alternative to `--base-url` flag) |

## FFprobe details

### Multi-stream support

FFprobe extracts **all** video, audio, and subtitle streams from each URL — not just the primary ones. Each stream is written to `media_streams` with:

- Correct codec, language, channels, bitrate
- Default/forced flags preserved
- Video profile (baseline/main/high/high10) and color transfer
- Audio profile (LC/HE-AAC, DTS variants)
- Subtitle hearing-impaired and title metadata

### Smart retries

FFprobe retries on timeouts and transient network errors, but **skips immediately** on server errors (403, 404, 5XX) since those indicate dead links. Failed items are written to `ffprobe_failures.log` for review.

### Zurg repair integration

When `--zurg-url` is set, plex-strm automatically triggers Zurg's repair process after FFprobe encounters server errors:

1. FFprobe runs on all items, collecting failures
2. If there are failures, `POST /torrents/repair` is sent to Zurg
3. Waits for repair to complete (30-120s depending on failure count)
4. Retries all previously failed items

This recovers torrents that RealDebrid temporarily couldn't serve.

## Subtitle download

When `--subtitles` is enabled, plex-strm searches OpenSubtitles.com for each processed media item:

1. Searches by IMDb ID > TMDB ID > TVDB ID (auto-converted to TMDB) > title + year
2. Downloads the most popular `.srt` file for each configured language
3. Registers the subtitle as a `media_streams` entry (stream_type_id=3) in the Plex database

### Subtitle modes

- **`--subtitle-mode missing`** (default) — Only downloads a subtitle if that language doesn't already exist as a subtitle stream in the database. This prevents redundant downloads and API quota usage.
- **`--subtitle-mode always`** — Downloads subtitles regardless of existing tracks. Useful if you want to replace embedded subtitles with better OpenSubtitles versions.

## Docker

```bash
docker compose run --rm plex-strm update --pg --protect --subtitles
```

## Trigger protection

Four layers protect injected URLs from being overwritten by Plex during library scans:

1. **Layer 1** — Blocks any update that replaces an HTTP URL with a non-HTTP path
2. **Layer 2** — Auto-backs up URL changes to `stream_url_backup` table
3. **Layer 3** — Auto-restores URLs from backup if they get removed
4. **Layer 4** — Blocks URL truncation (prevents corruption)

Works with both SQLite triggers and PostgreSQL trigger functions.

## Automation

Run periodically to process new `.strm` files:

```bash
# crontab
*/5 * * * * /path/to/run-plex-strm.sh >> /path/to/plex-strm.log 2>&1
```

Example wrapper script:

```bash
#!/bin/bash
export PLEX_PG_HOST=localhost
export PLEX_PG_DATABASE=plex
export PLEX_PG_USER=plex
export PLEX_PG_PASSWORD=plex

# Optional: OpenSubtitles
export OPENSUB_API_KEY=your-api-key
export OPENSUB_USER=your-username
export OPENSUB_PASS=your-password
export SUBTITLE_LANGS=en

python3 /path/to/plex_strm.py --pg \
  --library "My STRM Library" \
  update --protect --subtitles --subtitle-mode missing \
  --base-url https://plex.example.com
```

## License

MIT
