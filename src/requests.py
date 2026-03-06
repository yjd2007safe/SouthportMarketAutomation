"""Network request helpers with conservative URL validation."""

from __future__ import annotations

import random
import time
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

SAFE_SCHEMES = {"http", "https"}


class BlockedSourceError(RuntimeError):
    """Raised when a source appears to be actively blocked (for example HTTP 429)."""

    def __init__(self, url: str, *, status: int, attempts: int) -> None:
        super().__init__(f"Blocked source after {attempts} attempts: {url} (HTTP {status})")
        self.url = url
        self.status = status
        self.attempts = attempts


def validate_url_scheme(url: str) -> None:
    """Validate that URL uses a safe and supported scheme."""
    parsed = urlparse(url)
    if parsed.scheme not in SAFE_SCHEMES:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")


def fetch_text(
    url: str,
    *,
    timeout: int = 10,
    opener=urlopen,
    max_retries: int = 3,
    backoff_base: float = 0.5,
    jitter_ratio: float = 0.2,
    sleep_fn=time.sleep,
    random_fn=random.random,
) -> str:
    """Fetch text content over HTTP(S) with conservative retries/backoff."""
    validate_url_scheme(url)
    attempts = max_retries + 1

    for attempt in range(1, attempts + 1):
        try:
            with opener(url, timeout=timeout) as response:
                payload = response.read()
            return payload.decode("utf-8")
        except HTTPError as exc:
            should_retry = exc.code in {429, 500, 502, 503, 504} and attempt < attempts
            if should_retry:
                base = backoff_base * (2 ** (attempt - 1))
                jitter = base * jitter_ratio * random_fn()
                sleep_fn(base + jitter)
                continue
            if exc.code == 429:
                raise BlockedSourceError(url, status=exc.code, attempts=attempt) from exc
            raise
        except URLError:
            if attempt >= attempts:
                raise
            base = backoff_base * (2 ** (attempt - 1))
            jitter = base * jitter_ratio * random_fn()
            sleep_fn(base + jitter)

    raise RuntimeError(f"Unexpected retry loop termination for URL: {url}")
