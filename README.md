# plex-strm

Inject streaming URLs into the Plex database for Direct Play. Reads `.strm` files, replaces local paths with HTTP URLs, runs FFprobe to populate codec metadata, and optionally downloads subtitles from OpenSubtitles.

Supports both **SQLite** and **PostgreSQL** ([plex-postgresql](https://github.com/cgnl/plex-postgresql)) backends.

## How it works

1. Finds all `.strm` file references in `media_parts`
2. Reads each `.strm` file to get the streaming URL inside
3. Updates `media_parts.file` with the direct HTTP URL
4. Runs FFprobe on each URL to extract codec, resolution, bitrate, etc.
5. Creates `media_streams` entries (video + audio) so Plex shows correct info and enables Direct Play
6. Installs SQLite/PostgreSQL triggers that prevent Plex from reverting the URLs during library scans

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

# With subtitles
export OPENSUB_API_KEY=... OPENSUB_USER=... OPENSUB_PASS=...
plex-strm update --pg --protect --subtitles
```

### Commands

| Command | Description |
|---------|-------------|
| `update` | Inject URLs from .strm files, run FFprobe, optionally download subtitles |
| `status` | Show protection status, HTTP URL count, backup count |
| `protect` | Install 4-layer trigger protection |
| `unprotect` | Remove all protection triggers |
| `revert` | Revert URLs back to original .strm paths from backup |

### Options for `update`

| Flag | Description |
|------|-------------|
| `--protect` | Install trigger protection during update |
| `--subtitles` | Download subtitles via OpenSubtitles API |
| `--ffprobe PATH` | Path to ffprobe binary (default: `ffprobe`) |
| `--workers N` | FFprobe parallel workers (default: 4) |
| `--timeout N` | FFprobe timeout per URL in seconds (default: 30) |
| `--backup-dir DIR` | Directory for SQLite database backups |
| `-v` | Verbose output |

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
| `OPENSUB_API_KEY` | OpenSubtitles API key |
| `OPENSUB_USER` | OpenSubtitles username |
| `OPENSUB_PASS` | OpenSubtitles password |
| `SUBTITLE_LANGS` | Comma-separated language codes (default: nl,en) |
| `SUBTITLE_DIR` | Directory for downloaded .srt files (default: ./subtitles) |

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

## Cron / systemd timer

Run periodically to process new `.strm` files:

```bash
# crontab -e
0 */6 * * * cd /opt/plex-strm && python plex_strm.py update --pg --protect --subtitles 2>&1 | logger -t plex-strm
```
