"""Network request helpers with conservative URL validation."""

from __future__ import annotations

from urllib.parse import urlparse
from urllib.request import urlopen

SAFE_SCHEMES = {"http", "https"}


def validate_url_scheme(url: str) -> None:
    """Validate that URL uses a safe and supported scheme."""
    parsed = urlparse(url)
    if parsed.scheme not in SAFE_SCHEMES:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")


def fetch_text(url: str, *, timeout: int = 10, opener=urlopen) -> str:
    """Fetch text content over HTTP(S)."""
    validate_url_scheme(url)
    with opener(url, timeout=timeout) as response:
        payload = response.read()
    return payload.decode("utf-8")
