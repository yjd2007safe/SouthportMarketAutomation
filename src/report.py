"""Report generation module for SouthportMarketAutomation.

This module consumes analysis outputs and produces final market report payloads.
By default, generated report artifacts are persisted to Supabase.
"""

from __future__ import annotations

import argparse
import csv
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import load_to_supabase

DIMENSIONS: Tuple[Tuple[str, str], ...] = (
    ("price_level_distribution", "Price Level Distribution"),
    ("rent_trend", "Rent Trend"),
    ("listing_volume_trend", "Listing Volume Trend"),
    ("listing_age_proxy", "Listing Age Proxy"),
    ("bedroom_size_mix", "Bedroom/Size Mix"),
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line args for report generation runs."""
    parser = argparse.ArgumentParser(
        description="Generate market report artifacts from analysis outputs"
    )
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--analysis-prefix", default="market_analysis")
    parser.add_argument("--output-prefix", default="market_report")
    parser.add_argument("--source", default="southport_daily")
    parser.add_argument("--date", required=True, help="Snapshot date (YYYY-MM-DD)")
    parser.add_argument("--report-type", default="market_report")
    parser.add_argument("--report-version", default="v1")
    parser.add_argument(
        "--local-output-mode",
        choices=("none", "persist", "temp"),
        default="none",
        help="Whether to write local report files: none (default), persist, or temp(cleaned)",
    )
    parser.add_argument(
        "--persist-supabase",
        action="store_true",
        help="Persist generated report payload into Supabase table market_reports",
    )
    return parser.parse_args(argv)


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _parse_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()


def _fallback_analysis() -> Dict[str, Any]:
    return {
        "record_count": 0,
        "price_level_distribution": {"bins": {}, "missing_price": None},
        "rent_trend": [],
        "listing_volume_trend": [],
        "listing_age_proxy": {"sample_size": None, "average_days": None, "median_days": None},
        "bedroom_size_mix": [],
    }


def load_analysis_stats(reports_dir: Path, analysis_prefix: str) -> Dict[str, Any]:
    analysis_path = reports_dir / f"{analysis_prefix}.json"
    if not analysis_path.exists():
        return _fallback_analysis()
    try:
        payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _fallback_analysis()
    if not isinstance(payload, dict):
        return _fallback_analysis()

    merged = _fallback_analysis()
    for key in merged:
        if key in payload:
            merged[key] = payload[key]
    return merged


def _stringify(value: Any, placeholder: str = "N/A") -> str:
    if value in (None, "", [], {}):
        return placeholder
    return str(value)


def _rows_for_dimensions(stats: Dict[str, Any]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    price = stats.get("price_level_distribution") or {}
    bins = price.get("bins") if isinstance(price, dict) else {}
    missing_price = price.get("missing_price") if isinstance(price, dict) else None
    if isinstance(bins, dict) and bins:
        for band in sorted(bins):
            rows.append({"dimension": "price_level_distribution", "section": "Price Level Distribution", "metric": f"bin:{band}", "value": _stringify(bins[band])})
    else:
        rows.append({"dimension": "price_level_distribution", "section": "Price Level Distribution", "metric": "bin:all", "value": "N/A"})
    rows.append({"dimension": "price_level_distribution", "section": "Price Level Distribution", "metric": "missing_price", "value": _stringify(missing_price)})

    rent_trend = stats.get("rent_trend")
    if isinstance(rent_trend, list) and rent_trend:
        for item in rent_trend:
            month = item.get("month") if isinstance(item, dict) else None
            value = item.get("average_rent") if isinstance(item, dict) else None
            sample = item.get("sample_size") if isinstance(item, dict) else None
            rows.append({"dimension": "rent_trend", "section": "Rent Trend", "metric": f"{_stringify(month)} average_rent", "value": f"{_stringify(value)} (n={_stringify(sample)})"})
    else:
        rows.append({"dimension": "rent_trend", "section": "Rent Trend", "metric": "monthly_average_rent", "value": "N/A"})

    volume = stats.get("listing_volume_trend")
    if isinstance(volume, list) and volume:
        for item in volume:
            month = item.get("month") if isinstance(item, dict) else None
            listing_count = item.get("listing_count") if isinstance(item, dict) else None
            rows.append({"dimension": "listing_volume_trend", "section": "Listing Volume Trend", "metric": f"{_stringify(month)} listing_count", "value": _stringify(listing_count)})
    else:
        rows.append({"dimension": "listing_volume_trend", "section": "Listing Volume Trend", "metric": "monthly_listing_count", "value": "N/A"})

    age = stats.get("listing_age_proxy") if isinstance(stats.get("listing_age_proxy"), dict) else {}
    rows.extend([
        {"dimension": "listing_age_proxy", "section": "Listing Age Proxy", "metric": "sample_size", "value": _stringify(age.get("sample_size") if isinstance(age, dict) else None)},
        {"dimension": "listing_age_proxy", "section": "Listing Age Proxy", "metric": "average_days", "value": _stringify(age.get("average_days") if isinstance(age, dict) else None)},
        {"dimension": "listing_age_proxy", "section": "Listing Age Proxy", "metric": "median_days", "value": _stringify(age.get("median_days") if isinstance(age, dict) else None)},
    ])

    mix = stats.get("bedroom_size_mix")
    if isinstance(mix, list) and mix:
        for item in mix:
            segment = item.get("segment") if isinstance(item, dict) else None
            count = item.get("count") if isinstance(item, dict) else None
            median_rent = item.get("median_rent") if isinstance(item, dict) else None
            rows.append({"dimension": "bedroom_size_mix", "section": "Bedroom/Size Mix", "metric": f"{_stringify(segment)} count", "value": f"{_stringify(count)} (median_rent={_stringify(median_rent)})"})
    else:
        rows.append({"dimension": "bedroom_size_mix", "section": "Bedroom/Size Mix", "metric": "segment_mix", "value": "N/A"})

    return rows


def build_report_payload(stats: Dict[str, Any]) -> Dict[str, Any]:
    rows = _rows_for_dimensions(stats)
    report_json = {
        "generated_at": _iso_now(),
        "record_count": stats.get("record_count", 0),
        "dimensions": {key: title for key, title in DIMENSIONS},
        "rows": rows,
    }

    markdown_lines = [
        "# Southport Market Report",
        "",
        f"Generated at: `{report_json['generated_at']}`",
        f"Records analyzed: **{_stringify(stats.get('record_count', 0), placeholder='0')}**",
    ]
    by_dimension: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        by_dimension.setdefault(row["dimension"], []).append(row)

    for dim_key, section_name in DIMENSIONS:
        markdown_lines.extend(["", f"## {section_name}"])
        section_rows = by_dimension.get(dim_key, [])
        if not section_rows:
            markdown_lines.append("- N/A")
            continue
        for item in section_rows:
            markdown_lines.append(f"- {item['metric']}: {item['value']}")

    report_markdown = "\n".join(markdown_lines) + "\n"
    return {"json": report_json, "markdown": report_markdown, "rows": rows}


def write_report_artifacts(report_payload: Dict[str, Any], reports_dir: Path, output_prefix: str) -> Dict[str, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        "json": reports_dir / f"{output_prefix}.json",
        "csv": reports_dir / f"{output_prefix}.csv",
        "markdown": reports_dir / f"{output_prefix}.md",
    }
    output_paths["json"].write_text(json.dumps(report_payload["json"], indent=2), encoding="utf-8")

    with output_paths["csv"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["dimension", "section", "metric", "value"])
        writer.writeheader()
        writer.writerows(report_payload["rows"])

    output_paths["markdown"].write_text(report_payload["markdown"], encoding="utf-8")
    return output_paths


def persist_report_to_supabase(
    *,
    report_payload: Dict[str, Any],
    snapshot_date: str,
    source: str,
    report_type: str,
    report_version: str,
) -> None:
    supabase_url, supabase_key = load_to_supabase.load_supabase_config()
    row = load_to_supabase.prepare_market_report_row(
        snapshot_date=snapshot_date,
        source=source,
        report_type=report_type,
        report_version=report_version,
        record_count=int(report_payload["json"].get("record_count", 0) or 0),
        report_markdown=report_payload["markdown"],
        report_json=report_payload["json"],
    )
    load_to_supabase.upsert_rows(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table="market_reports",
        rows=[row],
        on_conflict="snapshot_date,source,report_type,report_version",
    )


def run_report(
    reports_dir: Path,
    analysis_prefix: str,
    output_prefix: str,
    *,
    snapshot_date: str,
    source: str,
    report_type: str,
    report_version: str,
    local_output_mode: str = "none",
    persist_supabase: bool = False,
) -> Dict[str, Path]:
    stats = load_analysis_stats(reports_dir, analysis_prefix)
    report_payload = build_report_payload(stats)

    if persist_supabase:
        persist_report_to_supabase(
            report_payload=report_payload,
            snapshot_date=snapshot_date,
            source=source,
            report_type=report_type,
            report_version=report_version,
        )

    if local_output_mode == "persist":
        return write_report_artifacts(report_payload, reports_dir, output_prefix)
    if local_output_mode == "temp":
        with tempfile.TemporaryDirectory(prefix="southport_report_") as tmpdir:
            write_report_artifacts(report_payload, Path(tmpdir), output_prefix)
        return {}
    return {}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    run_report(
        Path(args.reports_dir),
        args.analysis_prefix,
        args.output_prefix,
        snapshot_date=_parse_date(args.date),
        source=args.source,
        report_type=args.report_type,
        report_version=args.report_version,
        local_output_mode=args.local_output_mode,
        persist_supabase=args.persist_supabase,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
