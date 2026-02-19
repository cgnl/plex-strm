"""Real-Debrid API client with global rate limiting and backoff."""

import logging
import random
import threading
import time

import requests

log = logging.getLogger("plex-strm")


class _TokenBucket:
    """Simple thread-safe token bucket limiter (requests per minute)."""

    def __init__(self, per_minute):
        self.capacity = max(1.0, float(per_minute))
        self.tokens = self.capacity
        self.refill_per_sec = self.capacity / 60.0
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
                    self.updated_at = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                deficit = 1.0 - self.tokens
                sleep_for = deficit / self.refill_per_sec
            time.sleep(max(0.01, sleep_for))


class RDClient:
    """Resilient Real-Debrid REST client.

    Features:
    - global per-client rate limiting (token bucket)
    - retry with exponential backoff + jitter on 429/5XX/network errors
    - shared requests.Session() for connection pooling
    - lightweight metrics for observability
    """

    BASE = "https://api.real-debrid.com/rest/1.0"

    def __init__(self, api_token, rate_limit_per_minute=200, timeout=30, max_retries=6):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_token}"})
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.bucket = _TokenBucket(rate_limit_per_minute)
        self.metrics = {
            "requests": 0,
            "retries": 0,
            "rate_limited_429": 0,
            "server_5xx": 0,
            "network_errors": 0,
        }

    def close(self):
        self.session.close()

    def _request(self, method, path, expected=(200,), timeout=None):
        url = f"{self.BASE}{path}"
        timeout = timeout or self.timeout

        for attempt in range(self.max_retries + 1):
            self.bucket.wait()
            self.metrics["requests"] += 1
            try:
                resp = self.session.request(method, url, timeout=timeout)
            except requests.RequestException as e:
                self.metrics["network_errors"] += 1
                if attempt >= self.max_retries:
                    raise RuntimeError(f"RD {method} {path} failed after retries: {e}") from e
                self.metrics["retries"] += 1
                backoff = min(20.0, 0.5 * (2 ** attempt)) + random.uniform(0, 0.4)
                time.sleep(backoff)
                continue

            if resp.status_code in expected:
                return resp

            # Retry-friendly statuses
            if resp.status_code == 429 or resp.status_code >= 500:
                if resp.status_code == 429:
                    self.metrics["rate_limited_429"] += 1
                elif resp.status_code >= 500:
                    self.metrics["server_5xx"] += 1

                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"RD {method} {path} returned {resp.status_code} after retries"
                    )

                self.metrics["retries"] += 1
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    backoff = float(retry_after)
                else:
                    backoff = min(30.0, 1.0 * (2 ** attempt))
                backoff += random.uniform(0, 0.5)
                time.sleep(backoff)
                continue

            # Non-retryable (4xx other than 429)
            raise RuntimeError(f"RD {method} {path} returned {resp.status_code}: {resp.text[:200]}")

        raise RuntimeError(f"RD {method} {path} failed unexpectedly")

    def iter_torrents(self, page_size=2500):
        """Yield all torrents across paginated /torrents endpoint."""
        page = 1
        while True:
            resp = self._request("GET", f"/torrents?limit={page_size}&page={page}")
            data = resp.json()
            if not data:
                break
            for item in data:
                yield item
            page += 1

    def delete_torrent(self, rd_id):
        """Delete RD torrent by id. Returns True if deleted/not found safely."""
        resp = self._request("DELETE", f"/torrents/delete/{rd_id}", expected=(200, 204, 404))
        # 404 means already gone; treat as non-fatal idempotent success.
        return resp.status_code in (200, 204, 404)
