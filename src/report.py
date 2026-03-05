"""Report generation module for SouthportMarketAutomation.

This module consumes analysis outputs from ``reports/`` and produces
final market report artifacts geared toward human consumption.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory that contains analysis outputs and report artifacts",
    )
    parser.add_argument(
        "--analysis-prefix",
        default="market_analysis",
        help="Filename prefix of analyze outputs to consume",
    )
    parser.add_argument(
        "--output-prefix",
        default="market_report",
        help="Filename prefix for generated report artifacts",
    )
    return parser.parse_args(argv)


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _fallback_analysis() -> Dict[str, Any]:
    return {
        "record_count": 0,
        "price_level_distribution": {
            "bins": {},
            "missing_price": None,
        },
        "rent_trend": [],
        "listing_volume_trend": [],
        "listing_age_proxy": {
            "sample_size": None,
            "average_days": None,
            "median_days": None,
        },
        "bedroom_size_mix": [],
    }


def load_analysis_stats(reports_dir: Path, analysis_prefix: str) -> Dict[str, Any]:
    """Load analysis stats JSON if present, otherwise return safe placeholders."""
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
            rows.append(
                {
                    "dimension": "price_level_distribution",
                    "section": "Price Level Distribution",
                    "metric": f"bin:{band}",
                    "value": _stringify(bins[band]),
                }
            )
    else:
        rows.append(
            {
                "dimension": "price_level_distribution",
                "section": "Price Level Distribution",
                "metric": "bin:all",
                "value": "N/A",
            }
        )

    rows.append(
        {
            "dimension": "price_level_distribution",
            "section": "Price Level Distribution",
            "metric": "missing_price",
            "value": _stringify(missing_price),
        }
    )

    rent_trend = stats.get("rent_trend")
    if isinstance(rent_trend, list) and rent_trend:
        for item in rent_trend:
            month = item.get("month") if isinstance(item, dict) else None
            value = item.get("average_rent") if isinstance(item, dict) else None
            sample = item.get("sample_size") if isinstance(item, dict) else None
            rows.append(
                {
                    "dimension": "rent_trend",
                    "section": "Rent Trend",
                    "metric": f"{_stringify(month)} average_rent",
                    "value": f"{_stringify(value)} (n={_stringify(sample)})",
                }
            )
    else:
        rows.append(
            {
                "dimension": "rent_trend",
                "section": "Rent Trend",
                "metric": "monthly_average_rent",
                "value": "N/A",
            }
        )

    volume = stats.get("listing_volume_trend")
    if isinstance(volume, list) and volume:
        for item in volume:
            month = item.get("month") if isinstance(item, dict) else None
            listing_count = item.get("listing_count") if isinstance(item, dict) else None
            rows.append(
                {
                    "dimension": "listing_volume_trend",
                    "section": "Listing Volume Trend",
                    "metric": f"{_stringify(month)} listing_count",
                    "value": _stringify(listing_count),
                }
            )
    else:
        rows.append(
            {
                "dimension": "listing_volume_trend",
                "section": "Listing Volume Trend",
                "metric": "monthly_listing_count",
                "value": "N/A",
            }
        )

    age = stats.get("listing_age_proxy") if isinstance(stats.get("listing_age_proxy"), dict) else {}
    rows.extend(
        [
            {
                "dimension": "listing_age_proxy",
                "section": "Listing Age Proxy",
                "metric": "sample_size",
                "value": _stringify(age.get("sample_size") if isinstance(age, dict) else None),
            },
            {
                "dimension": "listing_age_proxy",
                "section": "Listing Age Proxy",
                "metric": "average_days",
                "value": _stringify(age.get("average_days") if isinstance(age, dict) else None),
            },
            {
                "dimension": "listing_age_proxy",
                "section": "Listing Age Proxy",
                "metric": "median_days",
                "value": _stringify(age.get("median_days") if isinstance(age, dict) else None),
            },
        ]
    )

    mix = stats.get("bedroom_size_mix")
    if isinstance(mix, list) and mix:
        for item in mix:
            segment = item.get("segment") if isinstance(item, dict) else None
            count = item.get("count") if isinstance(item, dict) else None
            median_rent = item.get("median_rent") if isinstance(item, dict) else None
            rows.append(
                {
                    "dimension": "bedroom_size_mix",
                    "section": "Bedroom/Size Mix",
                    "metric": f"{_stringify(segment)} count",
                    "value": f"{_stringify(count)} (median_rent={_stringify(median_rent)})",
                }
            )
    else:
        rows.append(
            {
                "dimension": "bedroom_size_mix",
                "section": "Bedroom/Size Mix",
                "metric": "segment_mix",
                "value": "N/A",
            }
        )

    return rows


def write_report_artifacts(
    stats: Dict[str, Any], reports_dir: Path, output_prefix: str
) -> Dict[str, Path]:
    """Write report outputs as markdown, csv, and machine-readable json."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    rows = _rows_for_dimensions(stats)

    output_paths = {
        "json": reports_dir / f"{output_prefix}.json",
        "csv": reports_dir / f"{output_prefix}.csv",
        "markdown": reports_dir / f"{output_prefix}.md",
    }

    report_json = {
        "generated_at": _iso_now(),
        "record_count": stats.get("record_count", 0),
        "dimensions": {key: title for key, title in DIMENSIONS},
        "rows": rows,
    }
    output_paths["json"].write_text(json.dumps(report_json, indent=2), encoding="utf-8")

    with output_paths["csv"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["dimension", "section", "metric", "value"])
        writer.writeheader()
        writer.writerows(rows)

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

    output_paths["markdown"].write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    return output_paths


def run_report(reports_dir: Path, analysis_prefix: str, output_prefix: str) -> Dict[str, Path]:
    """End-to-end helper for tests and CLI."""
    stats = load_analysis_stats(reports_dir, analysis_prefix)
    return write_report_artifacts(stats, reports_dir, output_prefix)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    run_report(Path(args.reports_dir), args.analysis_prefix, args.output_prefix)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
