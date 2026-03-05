"""Ingestion helpers for Southport market data."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse


ALLOWED_REMOTE_SCHEMES = {"http", "https"}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments for ingest runs."""
    parser = argparse.ArgumentParser(description="Ingest Southport market source data")
    parser.add_argument("--source", required=True, help="Local file path or http(s) URL")
    parser.add_argument("--output-dir", default="data/raw", help="Directory for ingest output")
    parser.add_argument(
        "--filename",
        default=None,
        help="Optional explicit output filename stem (without extension)",
    )
    return parser.parse_args(argv)


def resolve_source(source: str) -> Tuple[str, str]:
    """Resolve source type and normalized value.

    Returns a tuple of (source_type, normalized_value) where source_type is
    either "url" or "file".
    """
    parsed = urlparse(source)
    if parsed.scheme in ALLOWED_REMOTE_SCHEMES:
        return "url", source

    file_path = Path(source).expanduser().resolve()
    return "file", str(file_path)


def create_output_path(
    output_dir: Union[str, Path],
    source: str,
    *,
    filename: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> Path:
    """Create output directory if needed and return a deterministic output path."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if filename:
        stem = filename
    else:
        parsed = urlparse(source)
        if parsed.scheme in ALLOWED_REMOTE_SCHEMES:
            stem = Path(parsed.path).stem or "source"
        else:
            stem = Path(source).stem or "source"

    ts = timestamp or datetime.now(timezone.utc)
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    return output_dir / f"{stem}_{stamp}.json"
