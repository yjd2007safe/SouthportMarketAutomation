import json

import report


def test_weekly_sales_report_v3_exec_includes_required_sections_and_category_metrics(tmp_path):
    rows = [
        {
            "global_key": "id:a",
            "status": "sold",
            "sold_date": "2025-03-03",
            "property_category": "house",
            "price": 1000000,
            "address": "1 A St, Southport, QLD",
            "land_area": 400,
            "building_area": 180,
            "bedrooms": 4,
            "bathrooms": 2,
            "parking": 2,
            "source_site": "demo",
            "url": "https://example/a",
        },
        {
            "global_key": "id:b",
            "status": "sold",
            "sold_date": "2025-03-04",
            "property_category": "apartment",
            "price": 700000,
            "address": "2 B St, Southport, QLD",
            "building_area": 90,
            "beds": 2,
            "baths": 1,
            "source_site": "demo",
            "url": "https://example/b",
        },
        {
            "global_key": "id:c",
            "status": "sold",
            "sold_date": "2025-02-27",
            "property_category": "townhouse",
            "price": 800000,
            "address": "3 C St, Southport, QLD",
            "building_area": 120,
            "source_site": "demo",
            "url": "https://example/c",
        },
        {
            "global_key": "id:d",
            "status": "listed",
            "snapshot_date": "2025-03-04",
            "property_category": "townhouse",
            "price": 810000,
        },
    ]
    normalized = tmp_path / "normalized.json"
    normalized.write_text(json.dumps(rows), encoding="utf-8")

    outputs = report.run_report(
        tmp_path,
        "market_analysis",
        "weekly_exec",
        snapshot_date="2025-03-08",
        source="southport_daily",
        report_type="weekly_sales_report_exec",
        report_version="v3",
        report_mode="weekly",
        report_product="exec",
        records_input=normalized,
        local_output_mode="persist",
        persist_supabase=False,
    )

    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "v3"
    assert payload["report_mode"] == "weekly"
    assert payload["report_product"] == "executive_summary"
    assert payload["period_start"] == "2025-03-02"
    assert payload["period_end"] == "2025-03-08"
    assert payload["period"]["comparison_baseline"] == {
        "start": "2025-02-23",
        "end": "2025-03-01",
        "label": "Prior 7-day period",
    }

    sections = payload["sections"]
    assert len(sections["cover_summary"]["key_takeaways"]) == 3
    assert 1 <= len(sections["cover_summary"]["risks"]) <= 2

    overall = sections["overall_transactions"]
    assert overall["sold_count"] == 2
    assert overall["valid_sample_count"] == 2
    assert overall["total_value"] == 1700000
    assert overall["median"] == 850000
    assert overall["p25"] == 775000
    assert overall["p90"] == 970000

    category = sections["category_breakdown"]
    assert category["detached_house"]["count"] == 1
    assert category["detached_house"]["share"] == 0.5
    assert category["detached_house"]["pricing"]["median"] == 1000000
    assert category["detached_house"]["period_over_period_delta"]["count"] == 1
    assert category["apartment"]["price_per_building_area_median"] == round(700000 / 90, 2)
    assert category["townhouse"]["count"] == 0

    dynamics = sections["market_dynamics"]
    assert any(band["count"] >= 0 for band in dynamics["price_band_distribution"])
    assert isinstance(dynamics["hotspots"], list)
    assert isinstance(dynamics["top_movers"], list)

    appendix = sections["appendix"]["grouped_transaction_records"]
    assert len(appendix["detached_house"]) <= 3
    assert appendix["detached_house"][0]["global_key"] == "id:a"


def test_monthly_sales_report_detailed_uses_previous_month_window(tmp_path):
    rows = [
        {"global_key": "id:a", "status": "sold", "sold_date": "2025-02-10", "property_category": "house", "price": 900000},
        {"global_key": "id:b", "status": "sold", "sold_date": "2025-02-11", "property_category": "townhouse", "price": 750000},
        {"global_key": "id:c", "status": "sold", "sold_date": "2025-03-01", "property_category": "apartment", "price": 650000},
    ]
    normalized = tmp_path / "normalized.json"
    normalized.write_text(json.dumps(rows), encoding="utf-8")

    outputs = report.run_report(
        tmp_path,
        "market_analysis",
        "monthly_detailed",
        snapshot_date="2025-03-01",
        source="southport_daily",
        report_type="monthly_sales_report_detailed",
        report_version="v3",
        report_mode="monthly",
        report_product="detailed",
        records_input=normalized,
        local_output_mode="persist",
        persist_supabase=False,
    )

    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert payload["period_start"] == "2025-02-01"
    assert payload["period_end"] == "2025-02-28"
    assert payload["report_product"] == "detailed_analytics"
    assert payload["sections"]["overall_transactions"]["sold_count"] == 2


def test_parse_args_accepts_report_product():
    args = report.parse_args(
        [
            "--reports-dir",
            "reports",
            "--date",
            "2025-03-08",
            "--report-mode",
            "weekly",
            "--report-product",
            "exec",
            "--records-input",
            "data/normalized.json",
        ]
    )
    assert args.report_mode == "weekly"
    assert args.report_product == "exec"
    assert args.records_input == "data/normalized.json"


def test_persist_uses_report_type_key(monkeypatch, tmp_path):
    rows = [{"global_key": "id:a", "status": "sold", "sold_date": "2025-03-03", "property_category": "house", "price": 1000000}]
    normalized = tmp_path / "normalized.json"
    normalized.write_text(json.dumps(rows), encoding="utf-8")

    calls = {}

    monkeypatch.setattr(report.load_to_supabase, "load_supabase_config", lambda: ("https://demo.supabase.co", "key"))

    def fake_prepare_market_report_row(**kwargs):
        calls["prepare"] = kwargs
        return {"row": True}

    def fake_upsert_rows(**kwargs):
        calls["upsert"] = kwargs

    monkeypatch.setattr(report.load_to_supabase, "prepare_market_report_row", fake_prepare_market_report_row)
    monkeypatch.setattr(report.load_to_supabase, "upsert_rows", fake_upsert_rows)

    report.run_report(
        tmp_path,
        "market_analysis",
        "weekly_exec",
        snapshot_date="2025-03-08",
        source="southport_daily",
        report_type="weekly_sales_report_exec",
        report_version="v3",
        report_mode="weekly",
        report_product="exec",
        records_input=normalized,
        local_output_mode="none",
        persist_supabase=True,
    )

    assert calls["prepare"]["report_type"] == "weekly_sales_report_exec"
    assert calls["prepare"]["report_version"] == "v3"
    assert calls["upsert"]["on_conflict"] == "snapshot_date,source,report_type,report_version"
