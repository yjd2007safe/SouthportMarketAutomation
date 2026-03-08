"""Supabase persistence helpers for Southport daily pipeline."""

from __future__ import annotations

import argparse
import csv
import json
import re
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from record_cleaning import normalize_and_dedupe_records, stable_global_key

JsonDict = Dict[str, Any]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Southport artifacts into Supabase tables")
    parser.add_argument("--normalized-input", required=True, help="Path to normalized JSON/CSV input")
    parser.add_argument("--summary-json", help="Path to analysis JSON summary")
    parser.add_argument("--raw-input", help="Path to raw ingestion snapshot (JSON/CSV)")
    parser.add_argument("--date", required=True, help="Snapshot date (YYYY-MM-DD)")
    parser.add_argument("--source", default="southport_daily", help="Source label used in table keys")
    parser.add_argument("--report-json", help="Path to market report JSON artifact")
    parser.add_argument("--report-markdown", help="Path to market report markdown artifact")
    parser.add_argument("--report-type", default="market_report", help="Report artifact type")
    parser.add_argument("--report-version", default="v1", help="Report schema/version label")
    return parser.parse_args(argv)


def load_supabase_config(env: Optional[Dict[str, str]] = None) -> Tuple[str, str]:
    values = os.environ if env is None else env
    url = values.get("SUPABASE_URL", "").strip()
    key = values.get("SUPABASE_KEY", "").strip()
    if not url:
        raise RuntimeError("Missing required environment variable SUPABASE_URL")
    if not key:
        raise RuntimeError("Missing required environment variable SUPABASE_KEY")
    return url.rstrip("/"), key


def _parse_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()


def _looks_like_html(raw: str) -> bool:
    return bool(re.search(r"<\s*(?:!doctype|html|head|body|script)\b", raw, flags=re.IGNORECASE))


def _read_rows(path: Path) -> List[JsonDict]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        raw = path.read_text(encoding="utf-8")
        if _looks_like_html(raw):
            return []
        payload = json.loads(raw)
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            rows = payload.get("rows", [])
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return []
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    raise ValueError(f"Unsupported input format: {suffix!r}")


def _listing_key(row: JsonDict) -> str:
    existing = str(row.get("global_key") or "").strip()
    if existing:
        return existing
    return stable_global_key(row)


def _as_json_text(value: JsonDict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def prepare_raw_rows(rows: Iterable[JsonDict], snapshot_date: str, source: str) -> List[JsonDict]:
    output = []
    for row in rows:
        listing_key = _listing_key(row)
        output.append(
            {
                "snapshot_date": snapshot_date,
                "source": source,
                "listing_key": listing_key,
                "payload": _as_json_text(row),
            }
        )
    return output


def prepare_clean_rows(rows: Iterable[JsonDict], snapshot_date: str, source: str) -> List[JsonDict]:
    output = []
    for row in normalize_and_dedupe_records(rows):
        listing_key = _listing_key(row)
        output.append(
            {
                "snapshot_date": snapshot_date,
                "source": source,
                "listing_key": listing_key,
                "payload": _as_json_text(row),
                "rent": row.get("rent") or row.get("price") or row.get("monthly_rent"),
                "bedrooms": row.get("bedrooms") or row.get("beds"),
                "size_sqft": row.get("size_sqft") or row.get("sqft"),
                "property_category": row.get("property_category"),
                "land_area": row.get("land_area"),
                "land_area_unit": row.get("land_area_unit"),
                "building_area": row.get("building_area"),
                "building_area_unit": row.get("building_area_unit"),
            }
        )
    return output


def prepare_daily_summary_rows(summary: JsonDict, snapshot_date: str, source: str) -> List[JsonDict]:
    return [
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "metric": "record_count",
            "value": str(summary.get("record_count", "")),
        },
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "metric": "missing_price",
            "value": str(summary.get("price_level_distribution", {}).get("missing_price", "")),
        },
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "metric": "age_sample_size",
            "value": str(summary.get("listing_age_proxy", {}).get("sample_size", "")),
        },
    ]


def prepare_market_report_row(
    *,
    snapshot_date: str,
    source: str,
    report_type: str,
    report_version: str,
    record_count: int,
    report_markdown: str,
    report_json: JsonDict,
) -> JsonDict:
    return {
        "snapshot_date": snapshot_date,
        "source": source,
        "report_type": report_type,
        "report_version": report_version,
        "record_count": int(record_count),
        "report_markdown": report_markdown,
        "report_json": report_json,
    }


def upsert_rows(
    *,
    supabase_url: str,
    supabase_key: str,
    table: str,
    rows: Sequence[JsonDict],
    on_conflict: str,
    request_fn: Optional[Callable[..., Any]] = None,
) -> None:
    if not rows:
        return

    params = urlencode({"on_conflict": on_conflict})
    url = f"{supabase_url}/rest/v1/{table}?{params}"
    payload = json.dumps(list(rows)).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    if request_fn is None:
        req = Request(url, data=payload, headers=headers, method="POST")
        try:
            with urlopen(req, timeout=20):
                return
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase upsert failed for table {table}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Supabase connection failed for table {table}: {exc}") from exc

    request_fn(url=url, headers=headers, payload=payload)


def run_load(
    *,
    normalized_input: Path,
    snapshot_date: str,
    source: str,
    summary_json: Optional[Path] = None,
    raw_input: Optional[Path] = None,
    report_json: Optional[Path] = None,
    report_markdown: Optional[Path] = None,
    report_type: str = "market_report",
    report_version: str = "v1",
    env: Optional[Dict[str, str]] = None,
    request_fn: Optional[Callable[..., Any]] = None,
) -> None:
    supabase_url, supabase_key = load_supabase_config(env)

    clean_rows = prepare_clean_rows(_read_rows(normalized_input), snapshot_date, source)
    upsert_rows(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table="clean_listings_snapshot",
        rows=clean_rows,
        on_conflict="snapshot_date,source,listing_key",
        request_fn=request_fn,
    )

    if raw_input is not None and raw_input.exists():
        try:
            raw_rows = prepare_raw_rows(_read_rows(raw_input), snapshot_date, source)
        except (ValueError, json.JSONDecodeError):
            raw_rows = []
        upsert_rows(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            table="raw_listings",
            rows=raw_rows,
            on_conflict="snapshot_date,source,listing_key",
            request_fn=request_fn,
        )

    if summary_json is not None and summary_json.exists():
        summary = json.loads(summary_json.read_text(encoding="utf-8"))
        summary_rows = prepare_daily_summary_rows(summary, snapshot_date, source)
        upsert_rows(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            table="daily_market_summary",
            rows=summary_rows,
            on_conflict="snapshot_date,source,metric",
            request_fn=request_fn,
        )

    if report_json is not None and report_json.exists() and report_markdown is not None and report_markdown.exists():
        report_payload = json.loads(report_json.read_text(encoding="utf-8"))
        report_md = report_markdown.read_text(encoding="utf-8")
        report_row = prepare_market_report_row(
            snapshot_date=snapshot_date,
            source=source,
            report_type=report_type,
            report_version=report_version,
            record_count=int(report_payload.get("record_count", 0) or 0),
            report_markdown=report_md,
            report_json=report_payload,
        )
        upsert_rows(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            table="market_reports",
            rows=[report_row],
            on_conflict="snapshot_date,source,report_type,report_version",
            request_fn=request_fn,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    snapshot_date = _parse_date(args.date)
    run_load(
        normalized_input=Path(args.normalized_input),
        summary_json=Path(args.summary_json) if args.summary_json else None,
        raw_input=Path(args.raw_input) if args.raw_input else None,
        report_json=Path(args.report_json) if args.report_json else None,
        report_markdown=Path(args.report_markdown) if args.report_markdown else None,
        report_type=args.report_type,
        report_version=args.report_version,
        snapshot_date=snapshot_date,
        source=args.source,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
