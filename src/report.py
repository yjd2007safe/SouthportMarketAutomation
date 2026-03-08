"""Report generation module for SouthportMarketAutomation."""

from __future__ import annotations

import argparse
import csv
import json
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

import load_to_supabase
from reporting_schedule import previous_month_window_for_run, weekly_window_for_run

REPORT_JSON_SCHEMA_VERSION = "v3"
CATEGORY_ORDER = ("detached_house", "townhouse", "apartment")
PRICE_BANDS = (
    ("<500k", 0, 500000),
    ("500k-750k", 500000, 750000),
    ("750k-1m", 750000, 1000000),
    ("1m-1.5m", 1000000, 1500000),
    ("1.5m+", 1500000, float("inf")),
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate market report artifacts from analysis outputs")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--analysis-prefix", default="market_analysis")
    parser.add_argument("--output-prefix", default="market_report")
    parser.add_argument("--source", default="southport_daily")
    parser.add_argument("--date", required=True, help="Run date (YYYY-MM-DD)")
    parser.add_argument("--report-type", default="market_report")
    parser.add_argument("--report-version", default=REPORT_JSON_SCHEMA_VERSION)
    parser.add_argument("--report-mode", choices=("daily", "weekly", "monthly"), default="daily")
    parser.add_argument("--report-product", choices=("exec", "detailed"), default="detailed")
    parser.add_argument("--records-input", help="Normalized records JSON/CSV for sold transaction reports")
    parser.add_argument("--period-start", help="Inclusive period start YYYY-MM-DD")
    parser.add_argument("--period-end", help="Inclusive period end YYYY-MM-DD")
    parser.add_argument("--local-output-mode", choices=("none", "persist", "temp"), default="none")
    parser.add_argument("--persist-supabase", action="store_true")
    return parser.parse_args(argv)


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def _to_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return round(numerator / denominator, 4)


def _quantile(sorted_values: List[float], q: float) -> Optional[float]:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return round(sorted_values[0], 2)
    idx = (len(sorted_values) - 1) * q
    lower = int(idx)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = idx - lower
    value = sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction
    return round(value, 2)


def _summary_stats(values: List[float]) -> Dict[str, Optional[float]]:
    sorted_values = sorted(values)
    return {
        "avg": round(sum(values) / len(values), 2) if values else None,
        "median": round(median(values), 2) if values else None,
        "p25": _quantile(sorted_values, 0.25),
        "p50": _quantile(sorted_values, 0.5),
        "p75": _quantile(sorted_values, 0.75),
        "p90": _quantile(sorted_values, 0.9),
    }


def _load_records(path: Path) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            return [row for row in payload["rows"] if isinstance(row, dict)]
        return []
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    raise ValueError(f"Unsupported input format: {path.suffix}")


def _is_sold_record(row: Dict[str, Any]) -> bool:
    sold_tokens = (
        str(row.get("status") or "").lower(),
        str(row.get("transaction_type") or "").lower(),
        str(row.get("listing_status") or "").lower(),
        str(row.get("raw_snippet") or "").lower(),
    )
    return any("sold" in token for token in sold_tokens)


def _record_date(row: Dict[str, Any]) -> Optional[date]:
    for key in ("sold_date", "transaction_date", "settled_date", "snapshot_date", "listed_date"):
        raw = row.get(key)
        if not raw:
            continue
        text = str(raw).strip()[:10]
        try:
            return _parse_date(text)
        except ValueError:
            continue
    return None


def _normalize_category(value: Any) -> str:
    text = str(value or "").lower()
    if "town" in text:
        return "townhouse"
    if any(tok in text for tok in ("apartment", "unit", "flat")):
        return "apartment"
    if any(tok in text for tok in ("detached", "house", "single")):
        return "detached_house"
    return "detached_house"


def _window_for_mode(mode: str, run_date: date) -> tuple[date, date]:
    if mode == "weekly":
        window = weekly_window_for_run(run_date)
        return window.period_start, window.period_end
    if mode == "monthly":
        window = previous_month_window_for_run(run_date)
        return window.period_start, window.period_end
    return run_date, run_date


def _period_label(mode: str, start: date, end: date) -> str:
    if mode == "weekly":
        return f"Week {start.isoformat()} to {end.isoformat()}"
    if mode == "monthly":
        return start.strftime("%B %Y")
    return start.isoformat()


def _extract_price(row: Dict[str, Any]) -> Optional[float]:
    return _to_float(row.get("sold_price") or row.get("price") or row.get("rent"))


def _extract_area(row: Dict[str, Any], field: str) -> Optional[float]:
    return _to_float(row.get(field))


def _extract_suburb(row: Dict[str, Any]) -> str:
    for key in ("suburb", "locality", "city"):
        if row.get(key):
            return str(row.get(key)).strip().upper()
    address = str(row.get("address") or "").strip()
    if not address:
        return "UNKNOWN"
    parts = [part.strip() for part in address.split(",") if part.strip()]
    if len(parts) >= 2:
        return parts[-2].upper()
    return "UNKNOWN"


def _collect_period_rows(records: Iterable[Dict[str, Any]], start: date, end: date) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for row in records:
        if not _is_sold_record(row):
            continue
        dt = _record_date(row)
        if dt is None or dt < start or dt > end:
            continue
        filtered.append(row)
    return filtered


def _delta(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None:
        return None
    return round(current - previous, 2)


def _category_metrics(rows: List[Dict[str, Any]], total_count: int, baseline_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    prices = [p for p in (_extract_price(row) for row in rows) if p is not None]
    lands = [v for v in (_extract_area(row, "land_area") for row in rows) if v is not None]
    buildings = [v for v in (_extract_area(row, "building_area") for row in rows) if v is not None]
    price_per_building = []
    for row in rows:
        price = _extract_price(row)
        building = _extract_area(row, "building_area")
        if price is not None and building and building > 0:
            price_per_building.append(price / building)

    baseline_prices = [p for p in (_extract_price(row) for row in baseline_rows) if p is not None]
    current_summary = _summary_stats(prices)
    baseline_summary = _summary_stats(baseline_prices)

    return {
        "count": len(rows),
        "share": _safe_div(len(rows), total_count),
        "pricing": current_summary,
        "land_area_median": round(median(lands), 2) if lands else None,
        "building_area_median": round(median(buildings), 2) if buildings else None,
        "price_per_building_area_median": round(median(price_per_building), 2) if price_per_building else None,
        "period_over_period_delta": {
            "count": len(rows) - len(baseline_rows),
            "avg": _delta(current_summary["avg"], baseline_summary["avg"]),
            "median": _delta(current_summary["median"], baseline_summary["median"]),
            "p75": _delta(current_summary["p75"], baseline_summary["p75"]),
        },
    }


def _appendix_record(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": _record_date(row).isoformat() if _record_date(row) else None,
        "address": row.get("address"),
        "category": _normalize_category(row.get("property_category")),
        "price": _extract_price(row),
        "beds": row.get("bedrooms") or row.get("beds"),
        "baths": row.get("bathrooms") or row.get("baths"),
        "parking": row.get("parking") or row.get("car_spaces"),
        "land_area": row.get("land_area"),
        "building_area": row.get("building_area"),
        "source": row.get("source_site") or row.get("source"),
        "url": row.get("url"),
        "global_key": row.get("global_key") or row.get("listing_id") or row.get("id"),
    }


def build_sales_report_payload(
    records: Iterable[Dict[str, Any]],
    *,
    run_date: date,
    mode: str,
    report_product: str,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None,
    report_type: Optional[str] = None,
) -> Dict[str, Any]:
    start, end = period_start or _window_for_mode(mode, run_date)[0], period_end or _window_for_mode(mode, run_date)[1]
    filtered = _collect_period_rows(records, start, end)

    span_days = (end - start).days + 1
    baseline_end = start - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=span_days - 1)
    baseline_filtered = _collect_period_rows(records, baseline_start, baseline_end)

    prices = [p for p in (_extract_price(row) for row in filtered) if p is not None]
    overall = {
        "sold_count": len(filtered),
        "valid_sample_count": len(prices),
        "total_value": round(sum(prices), 2) if prices else None,
    }
    overall.update(_summary_stats(prices))

    category_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    baseline_category_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in filtered:
        category_map[_normalize_category(row.get("property_category"))].append(row)
    for row in baseline_filtered:
        baseline_category_map[_normalize_category(row.get("property_category"))].append(row)

    category_breakdown = {
        category: _category_metrics(category_map.get(category, []), len(filtered), baseline_category_map.get(category, []))
        for category in CATEGORY_ORDER
    }

    price_bands = []
    for label, lower, upper in PRICE_BANDS:
        in_band = [p for p in prices if lower <= p < upper]
        price_bands.append(
            {
                "band": label,
                "count": len(in_band),
                "share": _safe_div(len(in_band), len(prices)),
            }
        )

    suburb_prices: Dict[str, List[float]] = defaultdict(list)
    baseline_suburb_prices: Dict[str, List[float]] = defaultdict(list)
    for row in filtered:
        price = _extract_price(row)
        if price is not None:
            suburb_prices[_extract_suburb(row)].append(price)
    for row in baseline_filtered:
        price = _extract_price(row)
        if price is not None:
            baseline_suburb_prices[_extract_suburb(row)].append(price)

    hotspots = sorted(
        (
            {
                "suburb": suburb,
                "count": len(vals),
                "median_price": round(median(vals), 2),
            }
            for suburb, vals in suburb_prices.items()
        ),
        key=lambda item: item["count"],
        reverse=True,
    )[:5]

    top_movers = []
    for suburb, vals in suburb_prices.items():
        if suburb not in baseline_suburb_prices:
            continue
        current_median = round(median(vals), 2)
        baseline_median = round(median(baseline_suburb_prices[suburb]), 2)
        top_movers.append(
            {
                "suburb": suburb,
                "median_price_delta": round(current_median - baseline_median, 2),
                "current_median": current_median,
                "baseline_median": baseline_median,
            }
        )
    top_movers = sorted(top_movers, key=lambda item: abs(item["median_price_delta"]), reverse=True)[:5]

    appendix = {category: [_appendix_record(row) for row in category_map.get(category, [])] for category in CATEGORY_ORDER}
    if report_product == "exec":
        appendix = {category: rows[:3] for category, rows in appendix.items()}

    category_counts_sorted = sorted(
        ((category, data["count"]) for category, data in category_breakdown.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    lead_category = category_counts_sorted[0][0] if category_counts_sorted else "detached_house"
    risks = []
    if len(filtered) < 5:
        risks.append("Low transaction volume may reduce confidence.")
    if overall["valid_sample_count"] < overall["sold_count"]:
        risks.append("Some sold records are missing prices and excluded from value metrics.")

    payload = {
        "schema_version": REPORT_JSON_SCHEMA_VERSION,
        "report_type": report_type,
        "report_product": "executive_summary" if report_product == "exec" else "detailed_analytics",
        "report_mode": mode,
        "generated_at": _iso_now(),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "period": {
            "run_date": run_date.isoformat(),
            "timezone": "Asia/Shanghai",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "label": _period_label(mode, start, end),
            "comparison_baseline": {
                "start": baseline_start.isoformat(),
                "end": baseline_end.isoformat(),
                "label": f"Prior {span_days}-day period",
            },
        },
        "sections": {
            "cover_summary": {
                "key_takeaways": [
                    f"{overall['sold_count']} sold transactions were captured for the period.",
                    f"Median sale price is {overall['median']} across {overall['valid_sample_count']} priced sales.",
                    f"{lead_category} is the highest-volume category this period.",
                ],
                "risks": risks[:2] or ["No material risks detected from available records."],
            },
            "overall_transactions": overall,
            "category_breakdown": category_breakdown,
            "market_dynamics": {
                "price_band_distribution": price_bands,
                "hotspots": hotspots,
                "top_movers": top_movers,
            },
            "appendix": {
                "grouped_transaction_records": appendix,
            },
            "data_quality_methodology": {
                "coverage": {
                    "sold_records_considered": len(filtered),
                    "priced_records": len(prices),
                    "price_coverage_rate": _safe_div(len(prices), len(filtered)),
                },
                "dedup_rules": "Source normalization and global_key-first deduplication are applied upstream before reporting.",
                "missing_rates": {
                    "missing_price_rate": _safe_div(len(filtered) - len(prices), len(filtered)),
                    "missing_building_area_rate": _safe_div(
                        len([row for row in filtered if _extract_area(row, 'building_area') is None]),
                        len(filtered),
                    ),
                },
                "anomaly_handling": "Invalid dates/prices are excluded from metric calculations; records remain in appendix when transaction date is in-range.",
                "caveats": "Metrics rely on available listing feeds and may not cover all private/off-market transactions.",
            },
        },
        "record_count": len(filtered),
    }
    return payload


def build_report_payload(stats: Dict[str, Any]) -> Dict[str, Any]:
    report_json = {
        "schema_version": REPORT_JSON_SCHEMA_VERSION,
        "generated_at": _iso_now(),
        "record_count": stats.get("record_count", 0),
        "dimensions": stats,
    }
    markdown = (
        "# Southport Market Report\n\n"
        f"Generated at: `{report_json['generated_at']}`\n"
        f"Records analyzed: **{report_json['record_count']}**\n"
    )
    return {"json": report_json, "markdown": markdown, "rows": []}


def write_report_artifacts(report_payload: Dict[str, Any], reports_dir: Path, output_prefix: str) -> Dict[str, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        "json": reports_dir / f"{output_prefix}.json",
        "csv": reports_dir / f"{output_prefix}.csv",
        "markdown": reports_dir / f"{output_prefix}.md",
    }
    output_paths["json"].write_text(json.dumps(report_payload["json"], indent=2), encoding="utf-8")

    with output_paths["csv"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerow(["record_count", report_payload["json"].get("record_count", 0)])

    output_paths["markdown"].write_text(report_payload["markdown"], encoding="utf-8")
    return output_paths


def persist_report_to_supabase(*, report_payload: Dict[str, Any], snapshot_date: str, source: str, report_type: str, report_version: str) -> None:
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
    report_mode: str = "daily",
    report_product: str = "detailed",
    records_input: Optional[Path] = None,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    local_output_mode: str = "none",
    persist_supabase: bool = False,
) -> Dict[str, Path]:
    run_date = _parse_date(snapshot_date)
    if report_mode in ("weekly", "monthly") and records_input is not None:
        payload_json = build_sales_report_payload(
            _load_records(records_input),
            run_date=run_date,
            mode=report_mode,
            report_product=report_product,
            period_start=_parse_date(period_start) if period_start else None,
            period_end=_parse_date(period_end) if period_end else None,
            report_type=report_type,
        )
        report_payload = {
            "json": payload_json,
            "markdown": f"# Southport {report_mode.title()} Sales Report ({report_product})\n\nPeriod: {payload_json['period_start']} to {payload_json['period_end']}\n",
            "rows": [],
        }
    else:
        analysis_path = reports_dir / f"{analysis_prefix}.json"
        stats = json.loads(analysis_path.read_text(encoding="utf-8")) if analysis_path.exists() else {"record_count": 0}
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
        snapshot_date=_parse_date(args.date).isoformat(),
        source=args.source,
        report_type=args.report_type,
        report_version=args.report_version,
        report_mode=args.report_mode,
        report_product=args.report_product,
        records_input=Path(args.records_input) if args.records_input else None,
        period_start=args.period_start,
        period_end=args.period_end,
        local_output_mode=args.local_output_mode,
        persist_supabase=args.persist_supabase,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
