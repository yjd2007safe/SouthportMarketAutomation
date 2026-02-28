"""Normalization and deduplication helpers for Southport market records."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_FIELDS = [
    "source",
    "url",
    "fetched_at",
    "address",
    "suburb",
    "state",
    "postcode",
    "contract_date",
    "price_raw",
    "price_value",
    "bedrooms",
    "bathrooms",
    "car_spaces",
    "property_type",
    "floor_level",
    "view_tag",
    "area_sqm",
    "text_snippet",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line args for normalization stage."""
    parser = argparse.ArgumentParser(description="Normalize and deduplicate Southport records")
    parser.add_argument(
        "--input",
        default=None,
        help="Raw JSONL file path (default: data/raw/YYYY-MM-DD.jsonl for --date)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date selector in YYYY-MM-DD format (default: today in UTC)",
    )
    parser.add_argument("--raw-dir", default="data/raw", help="Base raw-data directory")
    parser.add_argument("--clean-dir", default="data/clean", help="Base clean-data directory")
    return parser.parse_args(argv)


def parse_contract_date(value: Any) -> str | None:
    """Normalize date-like values to YYYY-MM-DD."""
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    candidates = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    iso_prefix = text[:10]
    try:
        return datetime.strptime(iso_prefix, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def parse_price_value(value: Any) -> int | None:
    """Extract integer dollar amount from price-like values."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return int(value)

    text = str(value).strip()
    if not text:
        return None

    matched = re.findall(r"\d+", text)
    if not matched:
        return None

    number = int("".join(matched))
    return number if number > 0 else None


def parse_number(value: Any) -> float | int | None:
    """Parse numeric fields from mixed values."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value

    text = str(value).strip()
    if not text:
        return None

    matched = re.search(r"\d+(?:\.\d+)?", text)
    if not matched:
        return None

    num_text = matched.group(0)
    if "." in num_text:
        return float(num_text)
    return int(num_text)


def pick_first(record: dict[str, Any], *keys: str) -> Any:
    """Return first non-empty value from candidate keys."""
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw record into the clean schema."""
    url = pick_first(record, "url", "listing_url", "link")
    contract_date = parse_contract_date(
        pick_first(record, "contract_date", "sold_date", "date_sold", "sale_date")
    )
    price_raw = pick_first(record, "price_raw", "price", "sold_price")

    normalized = {
        "source": str(pick_first(record, "source", "origin", "provider") or "unknown"),
        "url": str(url) if url else None,
        "fetched_at": str(pick_first(record, "fetched_at", "ingested_at", "scraped_at") or ""),
        "address": str(pick_first(record, "address", "street_address") or "").strip(),
        "suburb": str(pick_first(record, "suburb", "city") or "").strip(),
        "state": str(pick_first(record, "state", "region") or "").strip(),
        "postcode": str(pick_first(record, "postcode", "zip", "post_code") or "").strip(),
        "contract_date": contract_date,
        "price_raw": str(price_raw).strip() if price_raw is not None else None,
        "price_value": parse_price_value(price_raw),
        "bedrooms": parse_number(pick_first(record, "bedrooms", "beds", "bed")),
        "bathrooms": parse_number(pick_first(record, "bathrooms", "baths", "bath")),
        "car_spaces": parse_number(pick_first(record, "car_spaces", "parking", "carparks")),
        "property_type": str(pick_first(record, "property_type", "type") or "").strip(),
        "floor_level": str(pick_first(record, "floor_level", "floor") or "").strip(),
        "view_tag": str(pick_first(record, "view_tag", "aspect", "view") or "").strip(),
        "area_sqm": parse_number(pick_first(record, "area_sqm", "land_size", "internal_area")),
        "text_snippet": str(pick_first(record, "text_snippet", "description", "summary") or "").strip(),
    }
    return normalized


def make_dedup_key(record: dict[str, Any]) -> str | None:
    """Build deterministic dedup key.

    Prefer address+contract_date+price_value. Fallback to url hash.
    """
    address = str(record.get("address") or "").strip().lower()
    contract_date = record.get("contract_date")
    price_value = record.get("price_value")
    if address and contract_date and price_value is not None:
        return f"addr::{address}::{contract_date}::{price_value}"

    url = record.get("url")
    if not url:
        return None
    digest = hashlib.sha256(str(url).encode("utf-8")).hexdigest()
    return f"url::{digest}"


def resolve_input_path(input_path: str | None, raw_dir: str | Path, run_date: str | None) -> Path:
    """Resolve input path from explicit --input or date pattern."""
    if input_path:
        return Path(input_path)

    if run_date:
        date_str = run_date
    else:
        date_str = datetime.now(UTC).date().isoformat()
    return Path(raw_dir) / f"{date_str}.jsonl"


def create_output_paths(clean_dir: str | Path, run_date: str) -> tuple[Path, Path]:
    """Create clean dir and return (jsonl_path, csv_path)."""
    output_dir = Path(clean_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"{run_date}.jsonl", output_dir / f"{run_date}.csv"


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    """Load JSONL records from disk."""
    records: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = line.strip()
            if not payload:
                continue
            item = json.loads(payload)
            if isinstance(item, dict):
                records.append(item)
    return records


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write normalized rows as JSONL."""
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write normalized rows as CSV with schema headers."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCHEMA_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in SCHEMA_FIELDS})


def normalize_and_dedup(raw_records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Normalize and deduplicate records while collecting counts."""
    normalized_count = 0
    dropped_count = 0
    seen_keys: set[str] = set()
    deduped_rows: list[dict[str, Any]] = []

    for raw in raw_records:
        normalized = normalize_record(raw)
        normalized_count += 1

        dedup_key = make_dedup_key(normalized)
        if dedup_key is None:
            dropped_count += 1
            continue
        if dedup_key in seen_keys:
            continue

        seen_keys.add(dedup_key)
        deduped_rows.append(normalized)

    summary = {
        "input": len(raw_records),
        "normalized": normalized_count,
        "deduped": len(deduped_rows),
        "dropped": dropped_count,
    }
    return deduped_rows, summary


def run(argv: list[str] | None = None) -> dict[str, int]:
    """CLI entrypoint behavior for normalization stage."""
    args = parse_args(argv)
    input_path = resolve_input_path(args.input, args.raw_dir, args.date)

    raw_records = load_jsonl(input_path)
    deduped_rows, summary = normalize_and_dedup(raw_records)

    run_date = args.date or datetime.now(UTC).date().isoformat()
    jsonl_out, csv_out = create_output_paths(args.clean_dir, run_date)
    write_jsonl(jsonl_out, deduped_rows)
    write_csv(csv_out, deduped_rows)

    print(f"input={summary['input']}")
    print(f"normalized={summary['normalized']}")
    print(f"deduped={summary['deduped']}")
    print(f"dropped={summary['dropped']}")
    print(f"jsonl={jsonl_out}")
    print(f"csv={csv_out}")

    return summary


if __name__ == "__main__":
    run()
