"""Utilities for opening remote ingest URLs."""

from urllib.parse import urlsplit
from urllib.request import urlopen

_ALLOWED_URL_SCHEMES = {"http", "https"}


def open_ingest_url(url: str):
    """Open an ingest URL and return the response handle.

    The ingest pipeline is intentionally unchanged apart from explicit
    URL-scheme validation to prevent non-network schemes.
    """
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()

    if scheme not in _ALLOWED_URL_SCHEMES:
        raise ValueError(
            f"Unsupported URL scheme '{parsed.scheme or '<missing>'}'. "
            "Only http and https URLs are allowed."
        )

    return urlopen(url)
