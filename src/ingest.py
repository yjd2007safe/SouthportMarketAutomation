#!/usr/bin/env python3
"""Minimal ingest pipeline for fetching market data snippets."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path
from typing import Iterable
from urllib.error import URLError
from urllib.request import urlopen

DEFAULT_SOURCES = [
    "https://example.com",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch source URLs and write daily JSONL output.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Source URL to ingest (repeat flag for multiple sources).",
    )
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Directory where YYYY-MM-DD.jsonl will be created.",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds.")
    parser.add_argument("--retries", type=int, default=2, help="Retry count after initial request failure.")
    parser.add_argument(
        "--snippet-length",
        type=int,
        default=280,
        help="Maximum number of characters to store in text_snippet.",
    )
    return parser.parse_args(argv)


def resolve_sources(cli_sources: list[str] | None) -> list[str]:
    return cli_sources or list(DEFAULT_SOURCES)


def output_path(output_dir: str, run_date: dt.date | None = None) -> Path:
    current_date = run_date or dt.date.today()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{current_date.isoformat()}.jsonl"


def fetch_with_retry(url: str, timeout: float, retries: int) -> str:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urlopen(url, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (URLError, TimeoutError, OSError) as error:
            last_error = error
            if attempt < retries:
                time.sleep(min(2**attempt, 5))
    assert last_error is not None
    raise RuntimeError(f"Failed to fetch {url}: {last_error}") from last_error


def build_record(source: str, body_text: str, snippet_length: int) -> dict[str, str]:
    normalized = " ".join(body_text.split())
    return {
        "source": source,
        "url": source,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "text_snippet": normalized[:snippet_length],
    }


def write_jsonl(records: Iterable[dict[str, str]], file_path: Path) -> None:
    with file_path.open("w", encoding="utf-8") as out:
        for record in records:
            out.write(json.dumps(record, ensure_ascii=False) + "\n")


def run(argv: list[str] | None = None) -> Path:
    args = parse_args(argv)
    sources = resolve_sources(args.sources)
    records: list[dict[str, str]] = []
    for source in sources:
        body_text = fetch_with_retry(source, timeout=args.timeout, retries=args.retries)
        records.append(build_record(source, body_text, snippet_length=args.snippet_length))

    target_file = output_path(args.output_dir)
    write_jsonl(records, target_file)
    return target_file


if __name__ == "__main__":
    destination = run()
    print(f"Wrote ingest output to {destination}")
