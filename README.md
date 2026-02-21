# plex-strm

Inject streaming URLs into the Plex database for Direct Play. Reads `.strm` files, replaces local paths with HTTP URLs, runs FFprobe to populate codec metadata, and optionally downloads subtitles from OpenSubtitles.

Supports both **SQLite** and **PostgreSQL** ([plex-postgresql](https://github.com/cgnl/plex-postgresql)) backends.

> **Note:** Currently only tested with PostgreSQL. SQLite support is implemented but untested — use at your own risk.

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

> **Important:** The `/strm/<id>` endpoint and `save_strm_files` feature require a **Zurg sponsor (nightly) build**. The public release (v0.9.3-final) does not include this endpoint. You need to be a [GitHub sponsor](https://github.com/sponsors/debridmediamanager) to access nightly builds.

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

For a production-style setup with STRM fallback, use the bundled examples:

- `examples/Caddyfile` — Plex + `/strm/*` split routing
- `examples/docker-compose.strm-stack.yml` — runs `strm_proxy` + Caddy
- `strm_proxy.py` — tries alternative STRM IDs on Zurg 5XX and triggers repair
- `examples/strm_pipeline.sh` — automation wrapper (scan guard + optional timeout knobs)

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

### Running Zurg on a remote server (dedicated server / VPS)

Running Zurg on a dedicated server has significant advantages:

- **Single IP to Real-Debrid** — RD can flag accounts that access from multiple IPs. A dedicated server ensures only one IP ever contacts RD.
- **Faster uplink** — Dedicated servers typically have 1–10 Gbit/s symmetric connections with better routing to RD's CDN nodes than residential ISPs.
- **Always-on** — No need to keep a local machine running for Zurg.

**Architecture:**

```
Plex (local) → strm_proxy (remote) → Zurg (remote) → Real-Debrid CDN
                    ↓ (fallback)
              Plex local files
```

The `strm_proxy.py` sits between your reverse proxy and Zurg. When Zurg returns a 5XX (expired/broken link), it looks up alternative STRM IDs for the same content in the Plex database and tries them. If all STRM versions fail, it can fall back to local files.

**Setup on the remote server:**

1. Deploy Zurg + strm_proxy + Caddy via Docker Compose (see `examples/docker-compose.strm-stack.yml`)
2. Point strm_proxy at your Plex PostgreSQL database (expose PostgreSQL to the remote server via VPN/Tailscale)
3. Set `FOLLOW_RD_REDIRECTS=1` so the proxy streams bytes instead of exposing RD CDN URLs to clients
4. Use `save_strm_files: true` in Zurg config and sync the generated `.strm` files to your local machine (e.g. rsync over Tailscale every 5 minutes)

**Zurg config for remote deployment:**

```yaml
# /opt/rd-stack/zurg/config.yml
zurg: v1
token: YOUR_RD_API_TOKEN
port: 9091
base_url: https://user:pass@strm.example.com

save_strm_files: true
serve_from_rd: true
enable_repair: true
repair_every_mins: 180
delete_error_torrents: true

directories:
  shows:
    group: media
    group_order: 10
    only_show_files_with_size_gte: 157286400
    filters:
      - has_episodes: true
  movies:
    group: media
    group_order: 20
    only_show_the_biggest_file: true
    only_show_files_with_size_gte: 157286400
    filters:
      - regex: /.*/
```

**Docker Compose for the remote stack:**

```yaml
services:
  zurg:
    image: your-zurg-nightly-image
    volumes:
      - ./zurg/config.yml:/app/config.yml
      - ./zurg/data:/app/data
    restart: unless-stopped

  strm-proxy:
    build: ./strm-proxy
    environment:
      - ZURG_URL=http://zurg:9091
      - ZURG_USER=user
      - ZURG_PASS=pass
      - FOLLOW_RD_REDIRECTS=1
      - STREAM_CHUNK_SIZE=4194304
      - ZURG_TIMEOUT=45
      - PLEX_DB_MODE=postgres
      - PLEX_PG_HOST=your-plex-db-host   # e.g. Tailscale IP
      - PLEX_PG_PORT=5432
      - PLEX_PG_DATABASE=plex
      - PLEX_PG_USER=plex
      - PLEX_PG_PASSWORD=plex
      - PLEX_PG_SCHEMA=plex
      - PLEX_TOKEN=your-plex-token
      - ENABLE_LOCAL_FALLBACK=1
    depends_on:
      - zurg
    restart: unless-stopped

  caddy:
    image: ghcr.io/caddybuilds/caddy-cloudflare:latest
    ports:
      - "443:443"
    volumes:
      - ./caddy/Caddyfile:/etc/caddy/Caddyfile:ro
      - ./caddy/data:/data
    depends_on:
      - strm-proxy
    restart: unless-stopped
```

**Performance tips for remote Zurg:**

- **TCP tuning** on the remote server is critical for high-latency links. Set BBR congestion control and increase TCP buffers:
  ```bash
  # /etc/sysctl.d/99-tcp-tuning.conf
  net.core.rmem_max = 16777216
  net.core.wmem_max = 16777216
  net.ipv4.tcp_rmem = 4096 1048576 16777216
  net.ipv4.tcp_wmem = 4096 1048576 16777216
  net.core.default_qdisc = fq
  net.ipv4.tcp_congestion_control = bbr
  net.ipv4.tcp_mtu_probing = 1
  ```
- **Force HTTP/1.1** in Caddy (`protocols h1`) — HTTP/2 multiplexes all streams over one TCP connection, which limits throughput on high-latency links. HTTP/1.1 gives each stream its own connection.
- **Gunicorn** instead of Flask dev server for strm_proxy — use gthread workers for concurrent stream handling (see `strm-proxy/Dockerfile` example).
- **IPv6** — if your server has IPv6 and RD supports it, enable `force_ipv6: true` in Zurg config to avoid IPv4 NAT overhead.

## Project structure

| File | Description |
|------|-------------|
| `plex_strm.py` | Entry point — CLI parsing, `update` command orchestration |
| `db.py` | Database abstraction (SQLite + PostgreSQL), library helpers, backup |
| `ffprobe.py` | FFprobe runner, stream parser, `media_items`/`media_streams` updates |
| `rd_client.py` | Real-Debrid REST client (rate limiting, backoff, retries, metrics) |
| `zurg.py` | Zurgtorrent index, per-torrent repair, broken torrent cleanup |
| `subtitles.py` | OpenSubtitles search/download/login, Plex DB registration |
| `protect.py` | 4-layer trigger protection — install, drop, status, revert |
| `strm_proxy.py` | STRM fallback proxy: alternate STRM IDs, optional local-file fallback, repair trigger |
| `repair_broken.py` | One-by-one Zurg repair helper for broken STRM IDs (uses tiny ranged GET validation) |

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
| `--zurg-data-dir DIR` | | Path to Zurg data directory (`.zurgtorrent` files). Enables per-torrent repair instead of repair-all |
| `--cleanup-broken` | off | Delete fully broken+unfixable torrents from RealDebrid (requires `--zurg-data-dir` + `RD_API_TOKEN`) |
| `--cleanup-broken-dry-run` | off | Show which broken torrents would be deleted without actually deleting |
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

### Zurg / RealDebrid

| Variable | Description |
|----------|-------------|
| `RD_API_TOKEN` | RealDebrid API token (required for `--cleanup-broken`) |

### STRM proxy / repair helper

| Variable | Description |
|----------|-------------|
| `ZURG_URL` | Zurg base URL (default: `http://localhost:9091`) |
| `ZURG_USER` | Zurg basic auth username (optional) |
| `ZURG_PASS` | Zurg basic auth password (optional) |
| `PROXY_PORT` | `strm_proxy.py` listen port (default: `8765`) |
| `PROXY_HOST` | `strm_proxy.py` listen address (default: `0.0.0.0`) |
| `PLEX_DB_MODE` | DB mode for `strm_proxy.py` and `repair_broken.py`: `postgres` or `sqlite` (default: `postgres`) |
| `PLEX_SQLITE_PATH` | Plex SQLite DB path when `PLEX_DB_MODE=sqlite` |
| `ENABLE_LOCAL_FALLBACK` | Enable local file fallback in `strm_proxy.py` (`1`/`0`, default `1`) |
| `LOCAL_FALLBACK_STRICT` | Enforce codec/container compatibility for local fallback (default `1`) |
| `LOCAL_FALLBACK_MATCH_MODE` | Strictness mode: `all`, `av`, or `audio` (default `all`) |
| `LOCAL_FALLBACK_RESOLUTION_PREFERENCE` | Fallback ranking: `1080p`, `balanced`, `4k` (default `1080p`) |
| `PLEX_TOKEN` | Plex token used for local fallback `/library/parts/...` redirect |
| `FOLLOW_RD_REDIRECTS` | Proxy RD CDN bytes instead of exposing redirect URLs (`1`/`0`, default `0`) |
| `STREAM_CHUNK_SIZE` | Chunk size in bytes for proxied streams (default: `1048576` = 1MB) |
| `ZURG_TIMEOUT` | Timeout in seconds for requests to Zurg (default: `45`) |

### Pipeline example knobs

| Variable | Description |
|----------|-------------|
| `ENABLE_SCAN_GUARD` | In `examples/strm_pipeline.sh`, skip run when STRM library scans are active (`1` default) |
| `USE_TIMEOUTS` | Enable command timeouts in `examples/strm_pipeline.sh` (`0` default) |
| `MAX_PLEX_STRM_TIME` | Timeout seconds for `plex_strm.py` when `USE_TIMEOUTS=1` (default `270`) |

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
2. Failed URLs are mapped to torrent hashes (if `--zurg-data-dir` is set)
3. Per-torrent repair requests are sent (`POST /manage/{hash}/repair`)
4. Waits for repair to complete (scaled by number of affected torrents)
5. Only retries items whose specific torrents were repaired

**Per-torrent repair** (with `--zurg-data-dir`):
- Parses `.zurgtorrent` files to build an RD download ID → torrent hash index
- Only repairs the specific broken torrents (not all 23K+), making repair much faster
- Retries are targeted to items whose torrents were actually submitted for repair

**Fallback** (without `--zurg-data-dir`):
- Falls back to `POST /torrents/repair` (repair-all)
- Retries all failed items after a global wait

This recovers torrents that RealDebrid temporarily couldn't serve.

### Broken torrent cleanup

When `--cleanup-broken` is set (with `--zurg-data-dir` and `RD_API_TOKEN` env var), plex-strm scans all `.zurgtorrent` files and deletes torrents from RealDebrid that are:

1. Marked **Unfixable** by Zurg (repair was already attempted and failed)
2. **Fully broken** — every file in the torrent is in a broken/deleted state

This cleans up dead weight in your RealDebrid account. Partially broken torrents (some files still work) are left untouched.

```bash
# Dry run first — see what would be deleted
plex-strm update --pg --zurg-data-dir /path/to/zurg/data --cleanup-broken-dry-run

# Actually delete
export RD_API_TOKEN=your-rd-api-token
plex-strm update --pg --zurg-data-dir /path/to/zurg/data --cleanup-broken
```

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

# Optional: RealDebrid (for --cleanup-broken)
export RD_API_TOKEN=your-rd-api-token

python3 /path/to/plex_strm.py --pg \
  --library "My STRM Library" \
  update --protect --subtitles --subtitle-mode missing \
  --zurg-url http://user:pass@localhost:9091 \
  --zurg-data-dir /path/to/zurg/data \
  --cleanup-broken \
  --base-url https://plex.example.com
```

## License

MIT
