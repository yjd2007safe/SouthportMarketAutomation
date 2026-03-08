import json

import report


def test_weekly_sales_report_includes_category_breakdown_and_details(tmp_path):
    rows = [
        {
            "global_key": "id:a",
            "status": "sold",
            "sold_date": "2025-03-03",
            "property_category": "house",
            "price": 1000000,
            "address": "1 A St",
            "land_area": 400,
            "land_area_unit": "sqm",
            "building_area": 180,
            "building_area_unit": "sqm",
        },
        {
            "global_key": "id:b",
            "status": "sold",
            "sold_date": "2025-03-04",
            "property_category": "apartment",
            "price": 700000,
            "address": "2 B St",
        },
        {
            "global_key": "id:c",
            "status": "listed",
            "snapshot_date": "2025-03-04",
            "property_category": "townhouse",
            "price": 800000,
        },
    ]
    normalized = tmp_path / "normalized.json"
    normalized.write_text(json.dumps(rows), encoding="utf-8")

    outputs = report.run_report(
        tmp_path,
        "market_analysis",
        "weekly",
        snapshot_date="2025-03-08",
        source="southport_daily",
        report_type="weekly_sales_report",
        report_version="v2",
        report_mode="weekly",
        records_input=normalized,
        local_output_mode="persist",
        persist_supabase=False,
    )

    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "v2"
    assert payload["period_start"] == "2025-03-02"
    assert payload["period_end"] == "2025-03-08"
    assert payload["overall_stats"]["sold_count"] == 2
    assert payload["category_breakdown"]["detached_house"]["sold_count"] == 1
    assert payload["category_breakdown"]["apartment"]["sold_count"] == 1
    assert payload["detailed_records"]["detached_house"][0]["land_area"] == 400


def test_parse_args_accepts_report_mode_and_records_input():
    args = report.parse_args(
        [
            "--reports-dir",
            "reports",
            "--date",
            "2025-03-08",
            "--report-mode",
            "weekly",
            "--records-input",
            "data/normalized.json",
        ]
    )
    assert args.report_mode == "weekly"
    assert args.records_input == "data/normalized.json"
