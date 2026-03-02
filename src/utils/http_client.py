"""Shared HTTP client with retry logic and rate limiting."""

from __future__ import annotations

import time
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.logging_setup import get_logger

log = get_logger(__name__)

# Reusable session with connection pooling
_session: requests.Session | None = None


def get_session() -> requests.Session:
    """Return a shared requests.Session with default headers."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": "GuadalupeSCM/0.1 (academic-research; guadalupe.scm@example.com)"
        })
    return _session


class RateLimiter:
    """Simple rate limiter that enforces minimum delay between calls."""

    def __init__(self, min_delay: float = 0.5):
        self.min_delay = min_delay
        self._last_call: float = 0.0

    def wait(self) -> None:
        """Block until min_delay seconds have elapsed since the last call."""
        elapsed = time.monotonic() - self._last_call
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self._last_call = time.monotonic()


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def fetch_json(
    url: str,
    params: dict[str, Any] | None = None,
    rate_limiter: RateLimiter | None = None,
    timeout: int = 60,
) -> Any:
    """Fetch JSON from a URL with retry and optional rate limiting.

    Raises requests.exceptions.HTTPError on 4xx/5xx responses.
    """
    if rate_limiter:
        rate_limiter.wait()

    session = get_session()
    log.debug("http_get", url=url, params=params)
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def fetch_csv_text(
    url: str,
    params: dict[str, Any] | None = None,
    rate_limiter: RateLimiter | None = None,
    timeout: int = 60,
) -> str:
    """Fetch CSV text content from a URL with retry."""
    if rate_limiter:
        rate_limiter.wait()

    session = get_session()
    log.debug("http_get_csv", url=url)
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.text


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def download_file(
    url: str,
    dest: str | None = None,
    rate_limiter: RateLimiter | None = None,
    timeout: int = 120,
) -> bytes:
    """Download a binary file (zip, gzip, etc.) with retry.

    If dest is provided, writes to that path and returns empty bytes.
    Otherwise returns the content as bytes.
    """
    if rate_limiter:
        rate_limiter.wait()

    session = get_session()
    log.debug("http_download", url=url, dest=dest)
    resp = session.get(url, timeout=timeout, stream=bool(dest))
    resp.raise_for_status()

    if dest:
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return b""

    return resp.content
