"""Report generation module for SouthportMarketAutomation."""

from __future__ import annotations

import argparse
import csv
import json
import tempfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

import load_to_supabase
from reporting_schedule import previous_month_window_for_run, weekly_window_for_run

REPORT_JSON_SCHEMA_VERSION = "v2"
CATEGORY_ORDER = ("detached_house", "townhouse", "apartment", "unknown")


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
    return "unknown"


def _window_for_mode(mode: str, run_date: date) -> tuple[date, date]:
    if mode == "weekly":
        window = weekly_window_for_run(run_date)
        return window.period_start, window.period_end
    if mode == "monthly":
        window = previous_month_window_for_run(run_date)
        return window.period_start, window.period_end
    return run_date, run_date


def build_sales_report_payload(
    records: Iterable[Dict[str, Any]], *, run_date: date, mode: str, period_start: Optional[date] = None, period_end: Optional[date] = None
) -> Dict[str, Any]:
    start, end = period_start or _window_for_mode(mode, run_date)[0], period_end or _window_for_mode(mode, run_date)[1]
    filtered: List[Dict[str, Any]] = []
    for row in records:
        if not _is_sold_record(row):
            continue
        dt = _record_date(row)
        if dt is None or dt < start or dt > end:
            continue
        filtered.append(row)

    category_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    prices: List[float] = []
    for row in filtered:
        category = _normalize_category(row.get("property_category"))
        category_map[category].append(row)
        price = _to_float(row.get("sold_price") or row.get("price") or row.get("rent"))
        if price is not None:
            prices.append(price)

    def summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        vals = [_to_float(r.get("sold_price") or r.get("price") or r.get("rent")) for r in rows]
        nums = [v for v in vals if v is not None]
        return {
            "sold_count": len(rows),
            "priced_count": len(nums),
            "avg_price": round(sum(nums) / len(nums), 2) if nums else None,
            "median_price": round(median(nums), 2) if nums else None,
        }

    category_breakdown = {cat: summary(category_map.get(cat, [])) for cat in CATEGORY_ORDER if category_map.get(cat) or cat != "unknown"}

    details_by_category: Dict[str, List[Dict[str, Any]]] = {}
    for cat in CATEGORY_ORDER:
        rows = category_map.get(cat, [])
        if not rows and cat == "unknown":
            continue
        details_by_category[cat] = [
            {
                "listing_key": row.get("global_key") or row.get("listing_id") or row.get("id"),
                "address": row.get("address"),
                "url": row.get("url"),
                "transaction_date": _record_date(row).isoformat() if _record_date(row) else None,
                "sold_price": _to_float(row.get("sold_price") or row.get("price") or row.get("rent")),
                "bedrooms": row.get("bedrooms") or row.get("beds"),
                "bathrooms": row.get("bathrooms") or row.get("baths"),
                "land_area": row.get("land_area"),
                "land_area_unit": row.get("land_area_unit"),
                "building_area": row.get("building_area"),
                "building_area_unit": row.get("building_area_unit"),
                "source_site": row.get("source_site"),
            }
            for row in rows
        ]

    payload = {
        "schema_version": REPORT_JSON_SCHEMA_VERSION,
        "report_mode": mode,
        "generated_at": _iso_now(),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "overall_stats": {
            "sold_count": len(filtered),
            "priced_count": len(prices),
            "avg_price": round(sum(prices) / len(prices), 2) if prices else None,
            "median_price": round(median(prices), 2) if prices else None,
        },
        "category_breakdown": category_breakdown,
        "detailed_records": details_by_category,
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
            period_start=_parse_date(period_start) if period_start else None,
            period_end=_parse_date(period_end) if period_end else None,
        )
        report_payload = {
            "json": payload_json,
            "markdown": f"# Southport {report_mode.title()} Sales Report\n\nPeriod: {payload_json['period_start']} to {payload_json['period_end']}\n",
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
        records_input=Path(args.records_input) if args.records_input else None,
        period_start=args.period_start,
        period_end=args.period_end,
        local_output_mode=args.local_output_mode,
        persist_supabase=args.persist_supabase,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
