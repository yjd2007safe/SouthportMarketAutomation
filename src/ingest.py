from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, List

import requests


@dataclass
class Source:
    source: str
    url: str


def load_sources(path: Path) -> List[Source]:
    """Load sources from a text file.

    Accepted line formats:
    - source,url
    - url

    Blank lines and lines beginning with # are ignored.
    """
    sources: List[Source] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "," in line:
            source_name, url = [part.strip() for part in line.split(",", 1)]
        else:
            url = line
            source_name = url

        sources.append(Source(source=source_name, url=url))

    return sources


def fetch_url(
    session: requests.Session,
    url: str,
    timeout: float,
    retries: int,
    retry_delay_seconds: float = 0.5,
) -> tuple[int | None, str, str | None]:
    """Fetch URL with retry support."""
    last_error: str | None = None

    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.status_code, response.text, None
        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt < retries:
                time.sleep(retry_delay_seconds)

    return None, "", last_error


def make_record(
    source: Source,
    status: int | None,
    text: str,
    error: str | None,
    max_snippet_chars: int,
) -> dict:
    return {
        "source": source.source,
        "url": source.url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "text_snippet": text[:max_snippet_chars],
        "status": status,
        "error": error,
    }


def write_jsonl(records: Iterable[dict], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal ingest scaffold")
    parser.add_argument("--sources-file", required=True, type=Path, help="Path to sources list")
    parser.add_argument(
        "--max-snippet-chars",
        type=int,
        default=300,
        help="Maximum characters to store in text_snippet",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    parser.add_argument("--retries", type=int, default=2, help="Retries per URL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sources = load_sources(args.sources_file)

    output_file = Path("data/raw") / f"{date.today().isoformat()}.jsonl"
    records: List[dict] = []

    with requests.Session() as session:
        for source in sources:
            status, text, error = fetch_url(
                session=session,
                url=source.url,
                timeout=args.timeout,
                retries=args.retries,
            )
            records.append(
                make_record(
                    source=source,
                    status=status,
                    text=text,
                    error=error,
                    max_snippet_chars=args.max_snippet_chars,
                )
            )

    write_jsonl(records, output_file)
    print(f"Wrote {len(records)} records to {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
