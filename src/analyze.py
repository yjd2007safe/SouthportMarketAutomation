"""Market analysis module for SouthportMarketAutomation.

This module reads normalized listing rows from JSON/CSV and produces
five-dimensional market stats plus report artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PRICE_BINS: Sequence[Tuple[str, float, float]] = (
    ("budget", 0, 1500),
    ("mid", 1500, 2500),
    ("premium", 2500, 3500),
    ("luxury", 3500, float("inf")),
)

SIZE_BINS: Sequence[Tuple[str, float, float]] = (
    ("compact", 0, 500),
    ("standard", 500, 900),
    ("spacious", 900, 1300),
    ("large", 1300, float("inf")),
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments for analysis runs."""
    parser = argparse.ArgumentParser(
        description="Analyze normalized Southport listings and build reports"
    )
    parser.add_argument("--input", required=True, help="Input normalized JSON/CSV")
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory where analysis outputs are written",
    )
    parser.add_argument(
        "--prefix",
        default="market_analysis",
        help="Output filename prefix for generated artifacts",
    )
    return parser.parse_args(argv)


def load_records(input_path: Path) -> List[Dict[str, Any]]:
    """Load rows from JSON or CSV with conservative parsing."""
    suffix = input_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            rows = payload.get("rows", [])
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return []

    if suffix == ".csv":
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    raise ValueError(f"Unsupported input format: {suffix!r}")


def _pick_first(record: Dict[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        text = str(value).strip().replace(",", "").replace("$", "")
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    as_float = _to_float(value)
    if as_float is None:
        return None
    return int(as_float)


def _to_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None

    candidates = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in candidates:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def _bucket(value: float, bins: Sequence[Tuple[str, float, float]]) -> str:
    for label, lower, upper in bins:
        if lower <= value < upper:
            return label
    return "unknown"


def analyze_records(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute five-dimensional market stats with missing-field tolerance."""
    price_distribution: Dict[str, int] = {label: 0 for label, _, _ in PRICE_BINS}
    missing_price = 0

    rent_trend_values: Dict[str, List[float]] = defaultdict(list)
    volume_trend: Dict[str, int] = defaultdict(int)

    age_days: List[float] = []

    mix_stats: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "rents": []}
    )

    total = 0
    for row in records:
        total += 1

        rent_raw = _pick_first(row, ("rent", "price", "monthly_rent", "list_price"))
        rent = _to_float(rent_raw)
        if rent is None:
            missing_price += 1
        else:
            price_distribution[_bucket(rent, PRICE_BINS)] += 1

        trend_date = _to_datetime(
            _pick_first(
                row,
                (
                    "snapshot_date",
                    "observed_at",
                    "listed_date",
                    "first_seen",
                    "created_at",
                ),
            )
        )
        if trend_date is not None:
            month = _month_key(trend_date)
            volume_trend[month] += 1
            if rent is not None:
                rent_trend_values[month].append(rent)

        first_seen = _to_datetime(_pick_first(row, ("first_seen", "listed_date")))
        last_seen = _to_datetime(
            _pick_first(row, ("last_seen", "snapshot_date", "observed_at"))
        )
        if first_seen is not None and last_seen is not None and last_seen >= first_seen:
            delta_days = (last_seen - first_seen).total_seconds() / 86400.0
            age_days.append(round(delta_days, 2))

        beds = _to_int(_pick_first(row, ("bedrooms", "beds", "bedroom_count")))
        size = _to_float(_pick_first(row, ("size_sqft", "sqft", "area_sqft")))

        bed_label = "unknown_bed"
        if beds is not None:
            if beds <= 0:
                bed_label = "studio"
            elif beds >= 4:
                bed_label = "4plus_bed"
            else:
                bed_label = f"{beds}_bed"

        size_label = "unknown_size"
        if size is not None:
            size_label = _bucket(size, SIZE_BINS)

        segment = f"{bed_label}|{size_label}"
        mix_stats[segment]["count"] += 1
        if rent is not None:
            mix_stats[segment]["rents"].append(rent)

    rent_trend = []
    for month in sorted(volume_trend):
        rents = rent_trend_values.get(month, [])
        avg_rent = round(sum(rents) / len(rents), 2) if rents else None
        rent_trend.append(
            {
                "month": month,
                "average_rent": avg_rent,
                "sample_size": len(rents),
            }
        )

    volume = [
        {"month": month, "listing_count": volume_trend[month]}
        for month in sorted(volume_trend)
    ]

    age_summary = {
        "sample_size": len(age_days),
        "average_days": round(sum(age_days) / len(age_days), 2) if age_days else None,
        "median_days": round(median(age_days), 2) if age_days else None,
    }

    mix = []
    for segment in sorted(mix_stats):
        rents = mix_stats[segment]["rents"]
        mix.append(
            {
                "segment": segment,
                "count": mix_stats[segment]["count"],
                "median_rent": round(median(rents), 2) if rents else None,
            }
        )

    return {
        "record_count": total,
        "price_level_distribution": {
            "bins": price_distribution,
            "missing_price": missing_price,
        },
        "rent_trend": rent_trend,
        "listing_volume_trend": volume,
        "listing_age_proxy": age_summary,
        "bedroom_size_mix": mix,
    }


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_reports(stats: Dict[str, Any], reports_dir: Path, prefix: str) -> Dict[str, Path]:
    """Write machine-readable (JSON/CSV) plus markdown summary outputs."""
    reports_dir.mkdir(parents=True, exist_ok=True)

    output_paths = {
        "json": reports_dir / f"{prefix}.json",
        "price_csv": reports_dir / f"{prefix}_price_distribution.csv",
        "rent_csv": reports_dir / f"{prefix}_rent_trend.csv",
        "volume_csv": reports_dir / f"{prefix}_volume_trend.csv",
        "mix_csv": reports_dir / f"{prefix}_bedroom_size_mix.csv",
        "markdown": reports_dir / f"{prefix}_summary.md",
    }

    output_paths["json"].write_text(json.dumps(stats, indent=2), encoding="utf-8")

    price_rows = [
        {"price_band": key, "count": value}
        for key, value in stats["price_level_distribution"]["bins"].items()
    ]
    price_rows.append(
        {
            "price_band": "missing_price",
            "count": stats["price_level_distribution"]["missing_price"],
        }
    )
    _write_csv(output_paths["price_csv"], ["price_band", "count"], price_rows)

    _write_csv(
        output_paths["rent_csv"],
        ["month", "average_rent", "sample_size"],
        stats["rent_trend"],
    )
    _write_csv(
        output_paths["volume_csv"],
        ["month", "listing_count"],
        stats["listing_volume_trend"],
    )
    _write_csv(
        output_paths["mix_csv"],
        ["segment", "count", "median_rent"],
        stats["bedroom_size_mix"],
    )

    summary_lines = [
        "# Southport Market Analysis Summary",
        "",
        f"- Records analyzed: **{stats['record_count']}**",
        (
            "- Listings with missing rent: "
            f"**{stats['price_level_distribution']['missing_price']}**"
        ),
        (
            "- Time-on-market sample size: "
            f"**{stats['listing_age_proxy']['sample_size']}**"
        ),
        "",
        "## Price Distribution",
    ]
    for band, count in stats["price_level_distribution"]["bins"].items():
        summary_lines.append(f"- {band}: {count}")

    summary_lines.extend(["", "## Monthly Trend"])
    for row in stats["rent_trend"]:
        summary_lines.append(
            f"- {row['month']}: avg rent={row['average_rent']} "
            f"(n={row['sample_size']})"
        )

    output_paths["markdown"].write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return output_paths


def run_analysis(input_path: Path, reports_dir: Path, prefix: str) -> Dict[str, Any]:
    """End-to-end analysis helper used by CLI and tests."""
    records = load_records(input_path)
    stats = analyze_records(records)
    write_reports(stats, reports_dir, prefix)
    return stats


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    run_analysis(Path(args.input), Path(args.reports_dir), args.prefix)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
